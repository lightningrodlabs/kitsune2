//! Regression test for the "freshly-Active link" `send_resource` race.
//!
//! # Why this test exists
//!
//! The pre-existing `two_node_data.rs` test uses an in-process loopback
//! bridge between two `rns_transport::Transport` instances and a tiny
//! 29-byte payload. That hides two bugs that only surface in real
//! deployments:
//!
//! 1. **No freshly-Active-link race.** The loopback bridge delivers
//!    packets synchronously via tokio channels, so by the time the
//!    proof round-trip "completes" on one side, the other side has
//!    also observed every prior packet. rns's internal `path_table`
//!    populates in lockstep with the bridge. In real TCP, the proof
//!    round-trip + path_table update are asynchronous — there is a
//!    window where a Link reads as `Active` but `path_table` has not
//!    yet learned the Link ID route, and calls to `send_resource`
//!    fail with `DroppedNoRoute` (surfaced as `RnsError::ConnectionError`).
//! 2. **Tiny payloads bypass the resource manager.** Our
//!    `send_over_link` routes sub-MDU frames through `Link::send_small`
//!    (`Link::data_packet` + `Transport::send_packet`), which talks
//!    through the Link object directly without going through the
//!    resource manager or the path_table lookup that `send_resource`
//!    requires. Kitsune2's real preflight payload is ~445 bytes, which
//!    overflows the 400-byte MDU and goes through `send_resource`.
//!
//! This test reproduces the production scenario:
//! - Two `ReticulumNode`s with **real TCP interfaces** (`TcpServer`
//!   on one side, `TcpClient` on the other, both on localhost).
//! - A payload >MDU, forcing the `send_resource` path.
//! - Asserts the payload eventually reaches the remote handler.
//!
//! If the freshly-Active race is not mitigated, this test fails.

use bytes::Bytes;
use kitsune2_api::{
    BoxFut, DynTxHandler, K2Result, SpaceId, TxBaseHandler, TxHandler,
    TxSpaceHandler, Url,
};
use kitsune2_transport_reticulum::{
    internal_testing, ReticulumInterfaceConfig, ReticulumNode,
    ReticulumTransportConfig,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug, Default)]
struct RecHandler {
    peer_connects: Mutex<Vec<Url>>,
}

impl TxBaseHandler for RecHandler {
    fn peer_connect(&self, peer: Url) -> K2Result<()> {
        self.peer_connects.lock().unwrap().push(peer);
        Ok(())
    }
    fn peer_disconnect(&self, _peer: Url, _reason: Option<String>) {}
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

/// Grab a free localhost port by binding ephemerally and dropping the
/// listener. Has a tiny race window but fine for a single test.
async fn pick_free_port() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    port
}

fn tmp_identity_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "kitsune2-transport-reticulum-tcp-preflight-{}-{}-{}",
        name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    p
}

fn cfg_with_interface(
    iface: ReticulumInterfaceConfig,
    identity_path: std::path::PathBuf,
) -> ReticulumTransportConfig {
    ReticulumTransportConfig {
        interfaces: vec![iface],
        identity_path: Some(identity_path),
        max_frame_bytes: 1024 * 1024,
        connect_timeout_s: 10,
        announce_interval_s: 1,
        link_idle_timeout_s: 60,
    }
}

/// Preflight-size (>MDU) payload roundtrip over real TCP interfaces.
///
/// This test exercises the exact path used by holochain's production
/// conductor: real TCP `TcpServer`/`TcpClient` interfaces, a payload
/// comfortably larger than `packet_mdu` (400 bytes), which therefore
/// routes through `send_resource` rather than `send_small`.
///
/// Expected failure mode before the fix: the receiving side never
/// observes `recv_space_notify`, because every `send_resource` on the
/// freshly-Active Link fails with `ConnectionError` indefinitely until
/// the test times out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preflight_roundtrip_over_tcp() {
    let port = pick_free_port().await;
    let server_bind = format!("127.0.0.1:{port}");

    // Node A: TcpServer on the picked port. Node B: TcpClient to same.
    let cfg_a = cfg_with_interface(
        ReticulumInterfaceConfig::TcpServer {
            bind: server_bind.clone(),
        },
        tmp_identity_path("a"),
    );
    let cfg_b = cfg_with_interface(
        ReticulumInterfaceConfig::TcpClient {
            target: server_bind.clone(),
        },
        tmp_identity_path("b"),
    );

    // A comes up first (bind), then B (connect). Small gap so the
    // TCP listener is accepting before the client dials.
    let node_a = ReticulumNode::from_config(cfg_a.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let node_b = ReticulumNode::from_config(cfg_b.clone()).await.unwrap();

    let h_a = Arc::new(RecHandler::default());
    let h_b = Arc::new(RecHandler::default());
    let dyn_a: DynTxHandler = h_a.clone();
    let dyn_b: DynTxHandler = h_b.clone();

    let trans_a =
        internal_testing::create_transport(cfg_a.clone(), dyn_a, node_a.clone())
            .await
            .unwrap();
    let trans_b =
        internal_testing::create_transport(cfg_b.clone(), dyn_b, node_b.clone())
            .await
            .unwrap();

    // Register space handlers — this kicks off per-space destinations
    // and their announce publishers, enabling cross-node discovery.
    let space = SpaceId::from(Bytes::from_static(b"alpha"));
    let space_a = Arc::new(RecSpaceHandler::default());
    let space_b = Arc::new(RecSpaceHandler::default());
    trans_a.register_space_handler(space.clone(), space_a.clone());
    trans_b.register_space_handler(space.clone(), space_b.clone());

    // Wait out a few announce cycles so both sides have each other in
    // their identity_cache.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Resolve B's URL so A knows who to send to.
    let b_hash = node_b.local_identity_hash();
    let b_url = internal_testing::identity_hash_to_url(&b_hash).unwrap();

    // Use a payload large enough to force the `send_resource` path.
    // packet_mdu is 400 bytes; real kitsune preflight is ~445; pick
    // something comfortably over, and something the default loopback
    // test would never cover.
    let payload = Bytes::from(vec![0xABu8; 800]);

    trans_a
        .send_space_notify(b_url.clone(), space.clone(), payload.clone())
        .await
        .expect("send_space_notify returned error");

    // Wait for B's space handler to observe the notify. Generous
    // window since preflight itself must succeed first.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if !space_b.notifies.lock().unwrap().is_empty() {
            break;
        }
        if std::time::Instant::now() >= deadline {
            let a_pcs = h_a.peer_connects.lock().unwrap().len();
            let b_pcs = h_b.peer_connects.lock().unwrap().len();
            panic!(
                "timed out waiting for B.recv_space_notify over real TCP. \
                 A.peer_connects={a_pcs}, B.peer_connects={b_pcs}. \
                 Preflight likely stuck (freshly-Active-link send_resource race)."
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let notifies = space_b.notifies.lock().unwrap();
    assert_eq!(notifies.len(), 1, "expected exactly one notify on B");
    let (_peer, sid, data) = &notifies[0];
    assert_eq!(sid, &space);
    assert_eq!(data, &payload);

    drop(trans_a);
    drop(trans_b);
}
