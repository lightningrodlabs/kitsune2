//! End-to-end functional test: two Beechat-backed
//! `ReticulumTransport`s exchange a **50 KiB** kitsune2 space-notify
//! payload over localhost TCP.
//!
//! This is the Phase-3 functional test for the chunking layer
//! (`PLAN-beechat-chunking.md` §10). At the Beechat plaintext MDU of
//! 1984 bytes, a 50 KiB payload fragments into `⌈51200 / 1975⌉ = 26`
//! `TAG_CHUNKED` packets. The test succeeds iff all 26 fragments
//! arrive, reassemble in the correct order, and surface as a single
//! `recv_space_notify` callback on B with byte-exact equality to the
//! sent payload.
//!
//! Modelled on [`two_node_beechat_data.rs`] — the small-payload
//! variant — so anything outside the chunker path stays a
//! regression-comparison target.
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
// Recording handlers.
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

fn next_port() -> u16 {
    static COUNTER: AtomicU16 = AtomicU16::new(19_900);
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
        announce_interval_s: 1,
        link_idle_timeout_s: 60,
        chunk_reassembly_timeout_s: 30,
        beechat: Default::default(),
    }
}

// ---------------------------------------------------------------------------
// The test.
// ---------------------------------------------------------------------------

/// Send a 50 KiB `recv_space_notify` payload from A to B over real
/// Beechat TCP loopback and assert byte-exact delivery in one
/// callback. Exercises the full chunker: send-side fragmentation in
/// `routers::send_over_link`, the `TAG_CHUNKED` wire format, and
/// receive-side reassembly in `spawn_data_router`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chunked_50kib_roundtrip_a_to_b_beechat() {
    let port_a = next_port();
    let port_b = next_port();
    let bind_a = format!("127.0.0.1:{port_a}");
    let bind_b = format!("127.0.0.1:{port_b}");

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

    tokio::time::sleep(Duration::from_millis(200)).await;

    let h_a = Arc::new(RecHandler::default());
    let h_b = Arc::new(RecHandler::default());
    let dyn_a: DynTxHandler = h_a.clone();
    let dyn_b: DynTxHandler = h_b.clone();

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

    let space = SpaceId::from(Bytes::from_static(b"beechat-chunking"));
    let space_a = Arc::new(RecSpaceHandler::default());
    let space_b = Arc::new(RecSpaceHandler::default());
    let _ = trans_a.register_space_handler(space.clone(), space_a.clone());
    let _ = trans_b.register_space_handler(space.clone(), space_b.clone());

    // Let the announce loop run so identity caches populate both ways.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let b_hash = node_b.local_identity_hash();
    let b_url = internal_testing::identity_hash_to_url(&b_hash).expect("b url");

    // Build a 50 KiB payload with non-trivial content so a silent
    // reassembly bug (off-by-one, wrong ordering) would mangle it.
    // At plaintext MDU = 1984 → body_cap = 1975 → 26 fragments.
    //
    // `routers::send_over_link` paces by 1 ms between fragments
    // to stay under Beechat upstream's 16-slot
    // `link_in_event_tx` broadcast channel, which otherwise drops
    // events when fragments arrive faster than the receiver's
    // bridge task can drain.
    let payload: Bytes =
        Bytes::from((0u8..=255).cycle().take(50 * 1024).collect::<Vec<_>>());
    assert_eq!(payload.len(), 50 * 1024);

    trans_a
        .send_space_notify(b_url.clone(), space.clone(), payload.clone())
        .await
        .expect("send_space_notify");

    // Poll B's space handler. 50 KiB over 26 Beechat packets on
    // localhost TCP is quick, but give it a generous window so CI
    // flakes don't fail the test.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if !space_b.notifies.lock().unwrap().is_empty() {
            break;
        }
        if std::time::Instant::now() >= deadline {
            let a_pcs = h_a.peer_connects.lock().unwrap().len();
            let b_pcs = h_b.peer_connects.lock().unwrap().len();
            panic!(
                "timed out waiting for B's recv_space_notify with chunked payload. \
                 A.peer_connects={a_pcs}, B.peer_connects={b_pcs}",
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let notifies = space_b.notifies.lock().unwrap();
    assert_eq!(
        notifies.len(),
        1,
        "expected exactly one notify on B (no duplicate reassembly)"
    );
    let (_peer, sid, data) = &notifies[0];
    assert_eq!(sid, &space);
    assert_eq!(data.len(), payload.len(), "reassembled length mismatch");
    assert_eq!(data, &payload, "reassembled bytes differ from sent payload");

    drop(trans_a);
    drop(trans_b);
}
