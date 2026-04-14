//! End-to-end functional test: two full Beechat-backed
//! `ReticulumTransport`s exchange a kitsune2 space-notify payload over
//! localhost TCP.
//!
//! Mirrors [`two_node_data.rs`] (LXMF-rs), but driven against the
//! Beechat crate. The LXMF test uses an in-process `InterfaceChannel`
//! loopback — Beechat exposes no equivalent test channel on its
//! `InterfaceManager`, so we use real TCP interfaces on distinct
//! 127.0.0.1 ports. Same end-to-end path otherwise:
//!
//! ```text
//!   A.send_space_notify(b_url, space, payload)
//!     -> DefaultTransport encodes K2Proto
//!        -> ReticulumTransport::send
//!           -> RealEndpoint::link_to (Beechat Link request)
//!              -> [TCP] -> B's Beechat Transport
//!                 -> in_link_events Activated
//!                    -> spawn_links_router -> peer_connect + preflight
//!           -> start_preflight (A side)
//!           -> send_over_link:
//!                ≤ MDU -> Link::send_small -> Transport::send_packet
//!                >  MDU -> Endpoint::send_resource  (Beechat: errors)
//!              -> [TCP] -> B
//!                 -> LinkEvent::Data
//!                    -> spawn_data_router
//!                       -> ReticulumFrame::Data (after preflight ready)
//!                          -> TxImpHnd::recv_data
//!                             -> TxSpaceHandler::recv_space_notify  ← target
//! ```
//!
//! Beechat backend only.

#![cfg(feature = "backend-beechat")]

use bytes::Bytes;
use kitsune2_api::{
    BoxFut, DynTxHandler, K2Result, SpaceId, TxBaseHandler, TxHandler,
    TxSpaceHandler, Url,
};
use kitsune2_transport_reticulum::{
    ReticulumInterfaceConfig, ReticulumNode, ReticulumTransportConfig,
    internal_testing,
};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

// ---------------------------------------------------------------------------
// Recording handlers (identical to the LXMF test harness — the
// kitsune2 TxHandler / TxSpaceHandler traits are backend-agnostic).
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct RecHandler {
    peer_connects: Mutex<Vec<Url>>,
    peer_disconnects: Mutex<Vec<(Url, Option<String>)>>,
}

impl TxBaseHandler for RecHandler {
    fn peer_connect(&self, peer: Url) -> K2Result<()> {
        self.peer_connects.lock().unwrap().push(peer);
        Ok(())
    }
    fn peer_disconnect(&self, peer: Url, reason: Option<String>) {
        self.peer_disconnects.lock().unwrap().push((peer, reason));
    }
}

impl TxHandler for RecHandler {}

#[derive(Debug, Default)]
struct RecSpaceHandler {
    notifies: Mutex<Vec<(Url, SpaceId, Bytes)>>,
}

impl TxBaseHandler for RecSpaceHandler {}

impl TxSpaceHandler for RecSpaceHandler {
    fn recv_space_notify(
        &self,
        peer: Url,
        space_id: SpaceId,
        data: Bytes,
    ) -> K2Result<()> {
        self.notifies.lock().unwrap().push((peer, space_id, data));
        Ok(())
    }
    fn is_any_agent_at_url_blocked(&self, _peer_url: &Url) -> K2Result<bool> {
        Ok(false)
    }
    fn has_local_agents(&self) -> BoxFut<'_, K2Result<bool>> {
        Box::pin(async { Ok(true) })
    }
}

// ---------------------------------------------------------------------------
// Harness.
// ---------------------------------------------------------------------------

/// Allocate a unique-in-process port so concurrent test runs don't
/// collide. Starts above the IANA ephemeral range to avoid system
/// allocations.
fn next_port() -> u16 {
    static COUNTER: AtomicU16 = AtomicU16::new(19_500);
    COUNTER.fetch_add(1, Ordering::SeqCst)
}

fn make_config(
    interfaces: Vec<ReticulumInterfaceConfig>,
) -> ReticulumTransportConfig {
    ReticulumTransportConfig {
        interfaces,
        identity_path: None,
        max_frame_bytes: 1024 * 1024,
        connect_timeout_s: 10,
        // Tight enough for the test to see an announce within a couple
        // of cycles.
        announce_interval_s: 1,
        link_idle_timeout_s: 60,
        beechat: Default::default(),
    }
}

// ---------------------------------------------------------------------------
// The test.
// ---------------------------------------------------------------------------

/// End-to-end data roundtrip on the Beechat backend: A's
/// `send_space_notify(...)` surfaces on B's
/// `TxSpaceHandler::recv_space_notify` through the real Beechat
/// transport over localhost TCP.
///
/// This exercises every piece the smoke test in `two_node_beechat.rs`
/// does *not*: announce propagation, identity-cache population,
/// outbound-link establishment, preflight handshake, and ≤ MDU data
/// delivery via `Link::send_small`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn data_roundtrip_a_to_b_beechat() {
    let port_a = next_port();
    let port_b = next_port();
    let bind_a = format!("127.0.0.1:{port_a}");
    let bind_b = format!("127.0.0.1:{port_b}");

    // Asymmetric wiring: A listens, B listens + dials A. With Beechat
    // this is enough for announces to propagate both directions.
    let cfg_a = make_config(vec![ReticulumInterfaceConfig::TcpServer {
        bind: bind_a.clone(),
    }]);
    let cfg_b = make_config(vec![
        ReticulumInterfaceConfig::TcpServer {
            bind: bind_b.clone(),
        },
        ReticulumInterfaceConfig::TcpClient {
            target: bind_a.clone(),
        },
    ]);

    let node_a = ReticulumNode::from_config(cfg_a.clone())
        .await
        .expect("node A");
    let node_b = ReticulumNode::from_config(cfg_b.clone())
        .await
        .expect("node B");

    // Give the TCP interfaces a beat to finish handshaking. Without
    // this, the first announce we publish can race the dial.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Record kitsune2-layer callbacks.
    let h_a = Arc::new(RecHandler::default());
    let h_b = Arc::new(RecHandler::default());
    let dyn_a: DynTxHandler = h_a.clone();
    let dyn_b: DynTxHandler = h_b.clone();

    // Build full DynTransports — kicks off the announce listener,
    // links router, data router, close router, and bootstrap drain.
    let trans_a = internal_testing::create_transport(
        cfg_a.clone(),
        dyn_a,
        node_a.clone(),
    )
    .await
    .expect("trans A");
    let trans_b = internal_testing::create_transport(
        cfg_b.clone(),
        dyn_b,
        node_b.clone(),
    )
    .await
    .expect("trans B");

    // Register the space on both sides; this starts the per-space
    // announce publisher tasks.
    let space = SpaceId::from(Bytes::from_static(b"beechat-e2e"));
    let space_a = Arc::new(RecSpaceHandler::default());
    let space_b = Arc::new(RecSpaceHandler::default());
    let _ = trans_a.register_space_handler(space.clone(), space_a.clone());
    let _ = trans_b.register_space_handler(space.clone(), space_b.clone());

    // Wait for at least one announce cycle so the identity caches
    // populate in both directions. `announce_interval_s = 1` plus
    // Beechat's own announce scheduling — a couple of seconds covers
    // it comfortably on localhost.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Build B's URL so A knows whom to link to.
    let b_hash = node_b.local_identity_hash();
    let b_url = internal_testing::identity_hash_to_url(&b_hash).expect("b url");

    // Fire the notify. Payload sized well below the 2048-byte
    // Beechat PACKET_MDU so the `Link::send_small` path is exercised
    // (the `send_resource` path isn't wired for Beechat yet; see
    // PLAN-beechat-backend.md Phase 4).
    let payload = Bytes::from_static(b"hello, kitsune over Beechat Reticulum");
    trans_a
        .send_space_notify(b_url.clone(), space.clone(), payload.clone())
        .await
        .expect("send_space_notify");

    // Poll B's space handler until the notify surfaces.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        if !space_b.notifies.lock().unwrap().is_empty() {
            break;
        }
        if std::time::Instant::now() >= deadline {
            let a_pcs = h_a.peer_connects.lock().unwrap().len();
            let b_pcs = h_b.peer_connects.lock().unwrap().len();
            panic!(
                "timed out waiting for B's recv_space_notify. \
                 A.peer_connects={a_pcs}, B.peer_connects={b_pcs}",
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let notifies = space_b.notifies.lock().unwrap();
    assert_eq!(notifies.len(), 1, "expected exactly one notify on B");
    let (_peer, sid, data) = &notifies[0];
    assert_eq!(sid, &space);
    assert_eq!(data, &payload);

    // Drop the transports so their background tasks shut down cleanly
    // before the runtime tears down.
    drop(trans_a);
    drop(trans_b);
}
