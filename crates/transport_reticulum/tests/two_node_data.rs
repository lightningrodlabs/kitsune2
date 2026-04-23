//! Functional test: two full `ReticulumTransport`s exchange a data
//! payload over an in-process loopback bridge.
//!
//! Exercises the path that ships kitsune2 traffic in production:
//!
//! ```text
//!   A.send_space_notify(b_url, space, payload)
//!     -> DefaultTransport encodes K2Proto
//!        -> ReticulumTransport::send
//!           -> RealEndpoint::link_to (rns Link request)
//!              -> [bridge] -> B's rns Transport
//!                 -> in_link_events Activated
//!                    -> spawn_links_router -> peer_connect + preflight
//!           -> start_preflight (A side)
//!           -> send_over_link -> RealEndpoint::send_resource
//!              -> [bridge] -> B
//!                 -> ResourceEvent::Complete
//!                    -> spawn_data_router
//!                       -> ReticulumFrame::Data (after preflight ready)
//!                          -> TxImpHnd::recv_data
//!                             -> TxSpaceHandler::recv_space_notify  ← target
//! ```
//!
//! This is the test that surfaces the remaining step-15 risks
//! (event ordering between activation and find_in_link, single-
//! fragment Resource for tiny preflight frames, link-close cleanup).
//!
//! LXMF-rs backend only — this test drives `rns_transport` directly
//! through an in-process interface loopback. The Beechat backend
//! has a separate TCP-loopback test (`two_node_beechat.rs`).

#![cfg(feature = "backend-lxmf")]

use bytes::Bytes;
use kitsune2_api::{
    BoxFut, DynTxHandler, K2Result, SpaceId, TxBaseHandler, TxHandler,
    TxSpaceHandler, Url,
};
#[allow(unused_imports)]
use std::sync::atomic::{AtomicUsize, Ordering};
use kitsune2_transport_reticulum::{
    ReticulumInterfaceConfig, ReticulumNode, ReticulumTransportConfig,
    internal_testing,
};
use rand_core::OsRng;
use rns_transport::identity::PrivateIdentity;
use rns_transport::iface::{IfaceSource, RxMessage, TxMessage};
use rns_transport::transport::{Transport as RnsTransport, TransportConfig};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Mutex as TokioMutex;

// ---------------------------------------------------------------------------
// Recording handlers.
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct RecHandler {
    peer_connects: Mutex<Vec<Url>>,
    peer_disconnects: Mutex<Vec<(Url, Option<String>)>>,
    /// Preflight bytes this side sends in `preflight_gather_outgoing`.
    /// When empty (default), the baseline tiny-preflight path runs; when
    /// non-empty and > MDU, exercises the oversized-preflight Resource
    /// path that is the production shape on LXMF.
    outgoing_preflight: Mutex<Bytes>,
    /// Preflight bytes observed via `preflight_validate_incoming`.
    incoming_preflight: Mutex<Vec<Bytes>>,
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

impl TxHandler for RecHandler {
    fn preflight_gather_outgoing(
        &self,
        _peer_url: Url,
    ) -> BoxFut<'_, K2Result<Bytes>> {
        let out = self.outgoing_preflight.lock().unwrap().clone();
        Box::pin(async move { Ok(out) })
    }
    fn preflight_validate_incoming(
        &self,
        _peer_url: Url,
        data: Bytes,
    ) -> BoxFut<'_, K2Result<()>> {
        self.incoming_preflight.lock().unwrap().push(data);
        Box::pin(async { Ok(()) })
    }
}

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
// Loopback bridge between two rns Transports.
// ---------------------------------------------------------------------------

async fn wire_loopback(
    tp_a: Arc<TokioMutex<RnsTransport>>,
    tp_b: Arc<TokioMutex<RnsTransport>>,
) {
    let (a_iface_addr, mut a_tx_recv, a_rx_send) = {
        let tp = tp_a.lock().await;
        let mgr = tp.iface_manager();
        let mut mgr = mgr.lock().await;
        let ch = mgr.new_channel(256);
        (ch.address, ch.tx_channel, ch.rx_channel)
    };
    let (b_iface_addr, mut b_tx_recv, b_rx_send) = {
        let tp = tp_b.lock().await;
        let mgr = tp.iface_manager();
        let mut mgr = mgr.lock().await;
        let ch = mgr.new_channel(256);
        (ch.address, ch.tx_channel, ch.rx_channel)
    };

    let b_rx_send_clone = b_rx_send.clone();
    tokio::spawn(async move {
        while let Some(TxMessage { packet, .. }) = a_tx_recv.recv().await {
            let _ = b_rx_send_clone
                .send(RxMessage {
                    address: b_iface_addr,
                    packet,
                    source: IfaceSource::None,
                })
                .await;
        }
    });
    let a_rx_send_clone = a_rx_send.clone();
    tokio::spawn(async move {
        while let Some(TxMessage { packet, .. }) = b_tx_recv.recv().await {
            let _ = a_rx_send_clone
                .send(RxMessage {
                    address: a_iface_addr,
                    packet,
                    source: IfaceSource::None,
                })
                .await;
        }
    });
}

fn make_rns_transport(
    name: &str,
) -> (Arc<TokioMutex<RnsTransport>>, PrivateIdentity) {
    let identity = PrivateIdentity::new_from_rand(OsRng);
    let mut cfg = TransportConfig::new(name, &identity, true);
    cfg.set_link_proof_timeout_secs(5);
    cfg.set_link_idle_timeout_secs(60);
    let tp = RnsTransport::new(cfg);
    (Arc::new(TokioMutex::new(tp)), identity)
}

fn k2_config() -> ReticulumTransportConfig {
    ReticulumTransportConfig {
        // We bypass interface startup via from_rns_transport, but
        // validate() rejects an empty interfaces list. Fill it with
        // a placeholder that's never actually started.
        interfaces: vec![ReticulumInterfaceConfig::TcpClient {
            target: "0.0.0.0:0".to_string(),
        }],
        identity_path: None,
        max_frame_bytes: 1024 * 1024,
        connect_timeout_s: 10,
        // Tight enough for the test to see an announce within a couple
        // of cycles, slow enough not to flood the bridge.
        announce_interval_s: 1,
        link_idle_timeout_s: 60,
        chunk_reassembly_timeout_s: 30,
        beechat: Default::default(),
    }
}

// ---------------------------------------------------------------------------
// The test.
// ---------------------------------------------------------------------------

/// End-to-end data roundtrip: A's `send_space_notify(...)` surfaces on
/// B's `TxSpaceHandler::recv_space_notify` through the real
/// `rns_transport` backend.
///
/// The preflight handshake correctness hinges on `PeerState`
/// tracking **local-sent** and **remote-received** independently.
/// The old single-state enum (None→Sent→Ready) had a window where
/// B's preflight could arrive at A during `wait_for_link_active` and
/// flip A's state to `Ready` before A had a chance to send its own
/// preflight. `start_preflight` then saw `!= None` and short-circuited,
/// so A never sent its preflight, B never flipped to Ready, and A's
/// subsequent data frame was dropped on the receiver.
/// `PreflightState { local_sent, remote_received }` fixes that by
/// making the two directions orthogonal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn data_roundtrip_a_to_b() {
    // Two rns Transports wired via the loopback bridge.
    let (tp_a, id_a) = make_rns_transport("node-a");
    let (tp_b, id_b) = make_rns_transport("node-b");
    wire_loopback(tp_a.clone(), tp_b.clone()).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Wrap each in a ReticulumNode.
    let node_a = ReticulumNode::from_rns_transport(tp_a.clone(), id_a.clone())
        .await
        .unwrap();
    let node_b = ReticulumNode::from_rns_transport(tp_b.clone(), id_b.clone())
        .await
        .unwrap();

    // Per-node TxHandlers for the kitsune2-side recording.
    let h_a = Arc::new(RecHandler::default());
    let h_b = Arc::new(RecHandler::default());
    let dyn_a: DynTxHandler = h_a.clone();
    let dyn_b: DynTxHandler = h_b.clone();

    // Build full DynTransports — kicks off announce listener + routers.
    let cfg = k2_config();
    let trans_a =
        internal_testing::create_transport(cfg.clone(), dyn_a, node_a.clone())
            .await
            .unwrap();
    let trans_b =
        internal_testing::create_transport(cfg.clone(), dyn_b, node_b.clone())
            .await
            .unwrap();

    // Register space + space handlers on both sides. This triggers
    // the per-space announce publisher tasks.
    let space = SpaceId::from(Bytes::from_static(b"alpha"));
    let space_a = Arc::new(RecSpaceHandler::default());
    let space_b = Arc::new(RecSpaceHandler::default());
    let _local_url_a =
        trans_a.register_space_handler(space.clone(), space_a.clone());
    let _local_url_b =
        trans_b.register_space_handler(space.clone(), space_b.clone());

    // Wait for at least one announce cycle so A's identity_cache
    // learns B (and vice-versa). With announce_interval_s=1 this is
    // ~1s after register_space.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Build B's URL from its identity hash. (A's send needs this to
    // know who to link to.)
    let b_hash = id_b.as_identity().address_hash;
    let b_url = internal_testing::identity_hash_to_url(&b_hash).unwrap();

    // Send a small notify from A to B.
    let payload = Bytes::from_static(b"hello, kitsune over reticulum");
    trans_a
        .send_space_notify(b_url.clone(), space.clone(), payload.clone())
        .await
        .expect("send_space_notify");

    // Wait for B's space handler to observe the notify.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if !space_b.notifies.lock().unwrap().is_empty() {
            break;
        }
        if std::time::Instant::now() >= deadline {
            let a_pcs = h_a.peer_connects.lock().unwrap().len();
            let b_pcs = h_b.peer_connects.lock().unwrap().len();
            panic!(
                "timed out waiting for B's recv_space_notify. \
                 A.peer_connects={}, B.peer_connects={}",
                a_pcs, b_pcs,
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let notifies = space_b.notifies.lock().unwrap();
    assert_eq!(notifies.len(), 1, "expected exactly one notify on B");
    let (_peer, sid, data) = &notifies[0];
    assert_eq!(sid, &space);
    assert_eq!(data, &payload);

    // Drop the transports so background tasks shut down before the
    // tokio runtime is torn down.
    drop(trans_a);
    drop(trans_b);
}

/// Same two-node setup as `data_roundtrip_a_to_b`, but both sides
/// synthesize a preflight payload bigger than LXMF's 400-byte plaintext
/// MDU. This regresses the production shape that was silently broken
/// on the `transport-reticulum` branch between 9817ee9 (chunking
/// rewrite) and the Resource rewire: `send_over_link` refused to ship
/// non-Data frames above MDU, so kitsune2's real preflight (which
/// carries a signed `AgentInfo` per local agent and typically runs
/// ~445 bytes) hit `non-Data frame 462 bytes exceeds plaintext MDU 400`
/// and the handshake never completed. Gossip timed out after 30 s with
/// `Failed to initiate gossip: timed out waiting for preflight Ready`.
///
/// This test fails (preflight validate never fires on the peer, and
/// the data notify times out) if the LXMF backend isn't using
/// rns `send_resource` for > MDU frames — either because the capability
/// bit is cleared, `send_resource` is restubbed, or `send_over_link`
/// stops routing oversized frames to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preflight_over_mdu_roundtrip_a_to_b() {
    let (tp_a, id_a) = make_rns_transport("node-a");
    let (tp_b, id_b) = make_rns_transport("node-b");
    wire_loopback(tp_a.clone(), tp_b.clone()).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let node_a = ReticulumNode::from_rns_transport(tp_a.clone(), id_a.clone())
        .await
        .unwrap();
    let node_b = ReticulumNode::from_rns_transport(tp_b.clone(), id_b.clone())
        .await
        .unwrap();

    // 500-byte preflight payload per side. After K2Proto encoding plus
    // `ReticulumFrame::Preflight`'s 1-byte tag + 16-byte identity
    // header, the wire frame is ~520 bytes — comfortably over the
    // 400-byte LXMF plaintext MDU, forcing the Resource path.
    let pf_from_a: Bytes = Bytes::from(vec![0xAAu8; 500]);
    let pf_from_b: Bytes = Bytes::from(vec![0xBBu8; 500]);
    let h_a = Arc::new(RecHandler::default());
    let h_b = Arc::new(RecHandler::default());
    *h_a.outgoing_preflight.lock().unwrap() = pf_from_a.clone();
    *h_b.outgoing_preflight.lock().unwrap() = pf_from_b.clone();
    let dyn_a: DynTxHandler = h_a.clone();
    let dyn_b: DynTxHandler = h_b.clone();

    let cfg = k2_config();
    let trans_a =
        internal_testing::create_transport(cfg.clone(), dyn_a, node_a.clone())
            .await
            .unwrap();
    let trans_b =
        internal_testing::create_transport(cfg.clone(), dyn_b, node_b.clone())
            .await
            .unwrap();

    let space = SpaceId::from(Bytes::from_static(b"alpha"));
    let space_a = Arc::new(RecSpaceHandler::default());
    let space_b = Arc::new(RecSpaceHandler::default());
    let _ = trans_a.register_space_handler(space.clone(), space_a.clone());
    let _ = trans_b.register_space_handler(space.clone(), space_b.clone());
    tokio::time::sleep(Duration::from_secs(2)).await;

    let b_hash = id_b.as_identity().address_hash;
    let b_url = internal_testing::identity_hash_to_url(&b_hash).unwrap();

    // Firing a data notify. Data delivery is gated on preflight Ready,
    // so its arrival on B proves both sides' Resource-sized preflights
    // completed through rns `send_resource` without the 30 s timeout.
    let payload = Bytes::from_static(b"post-preflight data");
    trans_a
        .send_space_notify(b_url.clone(), space.clone(), payload.clone())
        .await
        .expect("send_space_notify");

    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        if !space_b.notifies.lock().unwrap().is_empty() {
            break;
        }
        if std::time::Instant::now() >= deadline {
            let a_in = h_a.incoming_preflight.lock().unwrap().len();
            let b_in = h_b.incoming_preflight.lock().unwrap().len();
            panic!(
                "timed out waiting for post-preflight data. \
                 A.validate_incoming={}, B.validate_incoming={}. \
                 This is the exact failure mode the LXMF Resource \
                 rewire fixes — check send_over_link dispatch.",
                a_in, b_in,
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Each side's `preflight_validate_incoming` must have seen the
    // peer's 500-byte preflight bytes verbatim — proof that the oversize
    // frame round-tripped intact through rns Resource's
    // fragment/reassemble/proof pipeline.
    let a_rx = h_a.incoming_preflight.lock().unwrap().clone();
    let b_rx = h_b.incoming_preflight.lock().unwrap().clone();
    assert_eq!(a_rx.len(), 1, "A should have validated exactly one preflight");
    assert_eq!(b_rx.len(), 1, "B should have validated exactly one preflight");
    assert_eq!(a_rx[0], pf_from_b, "A must receive B's 500-byte preflight");
    assert_eq!(b_rx[0], pf_from_a, "B must receive A's 500-byte preflight");

    // And the post-preflight data arrived.
    let notifies = space_b.notifies.lock().unwrap();
    assert_eq!(notifies.len(), 1);
    assert_eq!(&notifies[0].2, &payload);

    drop(trans_a);
    drop(trans_b);
}
