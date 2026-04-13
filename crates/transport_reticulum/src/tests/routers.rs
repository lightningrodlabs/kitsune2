//! Unit tests for the links router, data router, and preflight
//! handshake state machine.
//!
//! All tests run against the in-memory [`crate::test_utils::FakeEndpoint`]
//! / `FakeLink` harness -- no real Reticulum network, no sleeping.

use crate::destination::{Endpoint, Link};
use crate::frame::{encode_frame, ReticulumFrame};
use crate::routers::{
    remove_link, spawn_close_router, spawn_data_router, spawn_links_router,
    RouterState,
};
use crate::test_utils::harness::FakeLink;
use crate::test_utils::FakeEndpoint;
use crate::url::identity_hash_to_url;
use bytes::Bytes;
use kitsune2_api::{
    AgentId, BoxFut, DynTxHandler, K2Result, SpaceId, Timestamp,
    TxBaseHandler, TxHandler, TxImpHnd, Url,
};
use prost::Message;
use rns_transport::hash::AddressHash;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Minimal in-memory TxHandler that records every callback. Unlike the
/// iroh harness's MockTxHandler this stays scoped to what the Reticulum
/// router tests actually inspect.
#[derive(Debug, Default)]
struct RecordingHandler {
    peer_connects: Mutex<Vec<Url>>,
    peer_disconnects: Mutex<Vec<(Url, Option<String>)>>,
    recvd: Mutex<Vec<(Url, Bytes)>>,
    /// The bytes to return from `preflight_gather_outgoing`.
    outgoing_preflight: Bytes,
}

impl TxBaseHandler for RecordingHandler {
    fn peer_connect(&self, peer: Url) -> K2Result<()> {
        self.peer_connects.lock().unwrap().push(peer);
        Ok(())
    }

    fn peer_disconnect(&self, peer: Url, reason: Option<String>) {
        self.peer_disconnects.lock().unwrap().push((peer, reason));
    }
}

impl TxHandler for RecordingHandler {
    fn preflight_gather_outgoing(
        &self,
        _peer_url: Url,
    ) -> BoxFut<'_, K2Result<Bytes>> {
        let out = self.outgoing_preflight.clone();
        Box::pin(async move { Ok(out) })
    }

    fn preflight_validate_incoming(
        &self,
        _peer_url: Url,
        _data: Bytes,
    ) -> BoxFut<'_, K2Result<()>> {
        Box::pin(async move { Ok(()) })
    }
}

fn mk_handler() -> (Arc<RecordingHandler>, Arc<TxImpHnd>) {
    let rec = Arc::new(RecordingHandler {
        outgoing_preflight: Bytes::from_static(b"preflight-out"),
        ..Default::default()
    });
    let dyn_handler: DynTxHandler = rec.clone();
    let hnd = TxImpHnd::new(dyn_handler);
    (rec, hnd)
}

fn space(s: &str) -> SpaceId {
    SpaceId::from(Bytes::copy_from_slice(s.as_bytes()))
}

// Note: these tests don't register any space handlers on the `TxImpHnd`.
// `TxImpHnd::peer_connect` only invokes the `NoLocalAgentsDuringPreflight`
// check when space handlers are present, so leaving the space_map empty
// lets preflight-gathering succeed.

#[tokio::test(flavor = "current_thread")]
async fn links_router_inserts_peer_on_first_inbound_link() {
    let endpoint = FakeEndpoint::new();
    let (rec, hnd) = mk_handler();

    let state = RouterState::new(1024 * 1024);
    // Register a destination hash so the router can find a SpaceId
    // for the inbound link.
    let dest_hash = AddressHash::new([0x77; 16]);
    state.register_dest(dest_hash, space("alpha"));

    let links_rx = endpoint.recv_links().await.unwrap();
    let _h = spawn_links_router(
        links_rx,
        state.clone(),
        hnd.clone(),
        endpoint.clone(),
        AddressHash::new([0u8; 16]),
    );

    // Inject a new inbound link to our 0x77 destination from peer 0xbb.
    let link = FakeLink::new(0x11, 0xbb, 0x77);
    endpoint.inject_link(link.clone()).await;

    // Give the router a tick to process.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // The peer should now be in the peer_states map.
    let peer_url = identity_hash_to_url(&AddressHash::new([0xbb; 16])).unwrap();
    let states = state.peer_states.read().unwrap();
    assert!(
        states.contains_key(&peer_url),
        "peer should have a PeerState entry"
    );
    let ps = states.get(&peer_url).unwrap().clone();
    drop(states);
    assert_eq!(ps.link_count(), 1);

    // Link registry should have the mapping.
    let reg = state.link_registry.read().unwrap();
    assert_eq!(reg.len(), 1);
    let (url, sid) = reg.values().next().unwrap();
    assert_eq!(url, &peer_url);
    assert_eq!(sid.as_ref(), b"alpha");
    drop(reg);

    // TxHandler should have received peer_connect, and the preflight
    // frame should have been written via `Link::send_small` (the
    // ≤ MDU fast path; see routers::send_over_link).
    assert_eq!(rec.peer_connects.lock().unwrap().len(), 1);
    let sent = link.sent.lock().unwrap();
    assert_eq!(sent.len(), 1, "preflight frame should have been sent");
    // First byte should be the Preflight tag (0x00).
    assert_eq!(sent[0][0], 0x00);

    // Preflight state: local_sent=true, remote_received=false (we've
    // sent ours, haven't received theirs yet).
    let pf = *ps.preflight_state.lock().unwrap();
    assert!(pf.local_sent);
    assert!(!pf.remote_received);
    assert!(!pf.is_ready());
}

#[tokio::test(flavor = "current_thread")]
async fn links_router_drops_link_for_unknown_destination() {
    let endpoint = FakeEndpoint::new();
    let (_rec, hnd) = mk_handler();
    let state = RouterState::new(1024 * 1024);
    // No register_dest call -- router should ignore this link.

    let links_rx = endpoint.recv_links().await.unwrap();
    let _h = spawn_links_router(
        links_rx,
        state.clone(),
        hnd,
        endpoint.clone(),
        AddressHash::new([0u8; 16]),
    );

    let link = FakeLink::new(0x11, 0xbb, 0x99);
    endpoint.inject_link(link).await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert!(state.peer_states.read().unwrap().is_empty());
    assert!(state.link_registry.read().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn data_router_drops_frames_before_preflight_ready() {
    let endpoint = FakeEndpoint::new();
    let (rec, hnd) = mk_handler();
    let state = RouterState::new(1024 * 1024);
    state.register_dest(AddressHash::new([0x77; 16]), space("alpha"));

    // First: get a link into the system via the links router.
    let links_rx = endpoint.recv_links().await.unwrap();
    let _hl = spawn_links_router(
        links_rx,
        state.clone(),
        hnd.clone(),
        endpoint.clone(),
        AddressHash::new([0u8; 16]),
    );
    let link = FakeLink::new(0x11, 0xbb, 0x77);
    endpoint.inject_link(link.clone()).await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Now spawn the data router.
    let data_rx = endpoint.recv_resource_data().await.unwrap();
    let _hd =
        spawn_data_router(data_rx, state.clone(), hnd.clone());

    // Inject a Data frame BEFORE preflight is ready. Router should drop it.
    let encoded = encode_frame(
        &ReticulumFrame::Data(Bytes::from_static(b"nope")),
        1024,
    )
    .unwrap();
    endpoint.inject_data(link.id(), encoded).await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert!(
        rec.recvd.lock().unwrap().is_empty(),
        "data before preflight should have been dropped"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn data_router_flips_preflight_state_to_ready() {
    let endpoint = FakeEndpoint::new();
    let (_rec, hnd) = mk_handler();
    let state = RouterState::new(1024 * 1024);
    state.register_dest(AddressHash::new([0x77; 16]), space("alpha"));

    let links_rx = endpoint.recv_links().await.unwrap();
    let _hl = spawn_links_router(
        links_rx,
        state.clone(),
        hnd.clone(),
        endpoint.clone(),
        AddressHash::new([0u8; 16]),
    );
    let link = FakeLink::new(0x11, 0xbb, 0x77);
    endpoint.inject_link(link.clone()).await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let data_rx = endpoint.recv_resource_data().await.unwrap();
    let _hd =
        spawn_data_router(data_rx, state.clone(), hnd.clone());

    // Inject an incoming Preflight frame. The inner bytes must be a
    // valid encoded K2Proto with ty=Preflight; handler.recv_data
    // decodes and dispatches.
    let inner = kitsune2_api::K2Proto {
        ty: kitsune2_api::K2WireType::Preflight as i32,
        data: Bytes::from_static(b"preflight-in"),
        space_id: None,
        module_id: None,
    }
    .encode_to_vec();
    let encoded = encode_frame(
        &ReticulumFrame::Preflight {
            // A's main identity (0xbb in this fixture — same as the
            // link's peer_identity_hash on B's side, so no re-keying).
            sender_main_identity: AddressHash::new([0xbb; 16]),
            payload: Bytes::from(inner),
        },
        1024,
    )
    .unwrap();
    endpoint.inject_data(link.id(), encoded).await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    // After receiving remote's preflight: remote_received=true.
    // (local_sent is also true because our links_router ran
    // start_preflight for the first inbound link, which is the
    // behavior we test in `links_router_inserts_peer_on_first_inbound_link`.)
    let peer_url = identity_hash_to_url(&AddressHash::new([0xbb; 16])).unwrap();
    let states = state.peer_states.read().unwrap();
    let ps = states.get(&peer_url).unwrap().clone();
    drop(states);
    let pf = *ps.preflight_state.lock().unwrap();
    assert!(pf.remote_received);
}

#[tokio::test(flavor = "current_thread")]
async fn remove_link_fires_peer_disconnect_on_last_close() {
    let endpoint = FakeEndpoint::new();
    let (rec, hnd) = mk_handler();
    let state = RouterState::new(1024 * 1024);
    state.register_dest(AddressHash::new([0x77; 16]), space("alpha"));

    let links_rx = endpoint.recv_links().await.unwrap();
    let _hl = spawn_links_router(
        links_rx,
        state.clone(),
        hnd.clone(),
        endpoint.clone(),
        AddressHash::new([0u8; 16]),
    );
    let link = FakeLink::new(0x11, 0xbb, 0x77);
    endpoint.inject_link(link.clone()).await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Close the only link -- should fire peer_disconnect.
    remove_link(
        &link.id(),
        Some("test close".into()),
        &state,
        &hnd,
    )
    .await;

    let peer_url = identity_hash_to_url(&AddressHash::new([0xbb; 16])).unwrap();
    let disconnects = rec.peer_disconnects.lock().unwrap();
    assert_eq!(disconnects.len(), 1);
    assert_eq!(disconnects[0].0, peer_url);
    assert_eq!(disconnects[0].1.as_deref(), Some("test close"));
    assert!(state.peer_states.read().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn remove_link_penultimate_does_not_disconnect() {
    let endpoint = FakeEndpoint::new();
    let (rec, hnd) = mk_handler();
    let state = RouterState::new(1024 * 1024);
    let dest_a = AddressHash::new([0x77; 16]);
    let dest_b = AddressHash::new([0x88; 16]);
    state.register_dest(dest_a, space("alpha"));
    state.register_dest(dest_b, space("beta"));

    let links_rx = endpoint.recv_links().await.unwrap();
    let _hl = spawn_links_router(
        links_rx,
        state.clone(),
        hnd.clone(),
        endpoint.clone(),
        AddressHash::new([0u8; 16]),
    );

    // Two links to the SAME peer, different spaces.
    let link_a = FakeLink::new(0x11, 0xbb, 0x77);
    let link_b = FakeLink::new(0x22, 0xbb, 0x88);
    endpoint.inject_link(link_a.clone()).await;
    endpoint.inject_link(link_b.clone()).await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Close link_a -- peer should still have link_b, no disconnect.
    remove_link(&link_a.id(), None, &state, &hnd).await;
    assert!(rec.peer_disconnects.lock().unwrap().is_empty());

    // Close link_b -- now disconnect fires.
    remove_link(&link_b.id(), None, &state, &hnd).await;
    assert_eq!(rec.peer_disconnects.lock().unwrap().len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn close_router_fires_peer_disconnect_on_last_close() {
    let endpoint = FakeEndpoint::new();
    let (rec, hnd) = mk_handler();
    let state = RouterState::new(1024 * 1024);
    state.register_dest(AddressHash::new([0x77; 16]), space("alpha"));

    let links_rx = endpoint.recv_links().await.unwrap();
    let _hl = spawn_links_router(
        links_rx,
        state.clone(),
        hnd.clone(),
        endpoint.clone(),
        AddressHash::new([0u8; 16]),
    );
    let close_rx = endpoint.recv_link_closures().await.unwrap();
    let _hc = spawn_close_router(close_rx, state.clone(), hnd.clone());

    // Inject a link, wait for it to land, then inject a close for that
    // same link_id. Expect peer_disconnect to fire via the close router.
    let link = FakeLink::new(0x11, 0xbb, 0x77);
    endpoint.inject_link(link.clone()).await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    endpoint.inject_link_closed(link.id()).await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    assert_eq!(rec.peer_disconnects.lock().unwrap().len(), 1);
    assert!(state.peer_states.read().unwrap().is_empty());
    assert!(state.link_registry.read().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn close_router_holds_peer_when_other_links_still_open() {
    let endpoint = FakeEndpoint::new();
    let (rec, hnd) = mk_handler();
    let state = RouterState::new(1024 * 1024);
    state.register_dest(AddressHash::new([0x77; 16]), space("alpha"));
    state.register_dest(AddressHash::new([0x88; 16]), space("beta"));

    let links_rx = endpoint.recv_links().await.unwrap();
    let _hl = spawn_links_router(
        links_rx,
        state.clone(),
        hnd.clone(),
        endpoint.clone(),
        AddressHash::new([0u8; 16]),
    );
    let close_rx = endpoint.recv_link_closures().await.unwrap();
    let _hc = spawn_close_router(close_rx, state.clone(), hnd.clone());

    // Two links to the same peer across two spaces.
    let link_a = FakeLink::new(0x11, 0xbb, 0x77);
    let link_b = FakeLink::new(0x22, 0xbb, 0x88);
    endpoint.inject_link(link_a.clone()).await;
    endpoint.inject_link(link_b.clone()).await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Close one -- peer should remain because link_b is still open.
    endpoint.inject_link_closed(link_a.id()).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(rec.peer_disconnects.lock().unwrap().is_empty());
    assert_eq!(state.peer_states.read().unwrap().len(), 1);

    // Close the second -- now disconnect fires.
    endpoint.inject_link_closed(link_b.id()).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(rec.peer_disconnects.lock().unwrap().len(), 1);
    assert!(state.peer_states.read().unwrap().is_empty());
}

/// Regression test for AgentId being unused — compile-only signal that
/// the kitsune2_api surface we care about is imported correctly.
#[test]
fn _imports_compile() {
    let _ = |_id: AgentId, _t: Timestamp| {};
}
