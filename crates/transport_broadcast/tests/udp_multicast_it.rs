//! Real-multicast integration test for the udp multicast medium.
//!
//! Actually joins a multicast group on the host, so it is gated behind
//! the `K2_BCAST_IT` env var (mirroring the mdns bootstrap crate's
//! `K2_MDNS_IT` pattern) to keep CI environments without multicast
//! green:
//!
//! ```sh
//! K2_BCAST_IT=1 cargo test -p kitsune2_transport_broadcast --test udp_multicast_it
//! ```
//!
//! Unlike the in-crate unit tests this goes through the public
//! config-driven factory path, so it also covers module-config
//! plumbing.

use kitsune2_api::*;
use kitsune2_transport_broadcast::BroadcastTransportFactory;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug)]
struct TestHandler {
    notify: tokio::sync::mpsc::UnboundedSender<(Url, SpaceId, bytes::Bytes)>,
}

impl TxBaseHandler for TestHandler {}
impl TxHandler for TestHandler {}

impl TxSpaceHandler for TestHandler {
    fn recv_space_notify(
        &self,
        peer: Url,
        space_id: SpaceId,
        data: bytes::Bytes,
    ) -> K2Result<()> {
        let _ = self.notify.send((peer, space_id, data));
        Ok(())
    }

    fn is_any_agent_at_url_blocked(&self, _peer_url: &Url) -> K2Result<bool> {
        Ok(false)
    }
}

fn test_space() -> SpaceId {
    SpaceId::from(bytes::Bytes::from_static(b"udpm-test-space"))
}

/// A full Builder around the config-driven broadcast factory,
/// mirroring `kitsune2::default_builder` without a transport backend
/// dependency.
fn test_builder(port: u16) -> Arc<Builder> {
    use kitsune2_core::factories;
    let builder = Builder {
        config: Config::default(),
        verifier: Arc::new(kitsune2_core::Ed25519Verifier),
        auth_material_bootstrap: None,
        auth_material_relay: None,
        kitsune: factories::CoreKitsuneFactory::create(),
        space: factories::CoreSpaceFactory::create(),
        peer_store: factories::MemPeerStoreFactory::create(),
        bootstrap: factories::CoreBootstrapFactory::create(),
        fetch: factories::CoreFetchFactory::create(),
        report: factories::CoreReportFactory::create(),
        transport: BroadcastTransportFactory::create(),
        op_store: factories::MemOpStoreFactory::create(),
        peer_meta_store: factories::MemPeerMetaStoreFactory::create(),
        gossip: kitsune2_gossip::K2GossipFactory::create(),
        local_agent_store: factories::CoreLocalAgentStoreFactory::create(),
        publish: factories::CorePublishFactory::create(),
        blocks: factories::MemBlocksFactory::create(),
        known_peers: factories::CoreKnownPeersFactory::create(),
    }
    .with_default_config()
    .unwrap();
    builder
        .config
        .set_module_config(&serde_json::json!({
            "broadcastTransport": {
                "medium": "udpMulticast",
                "udpMulticast": { "port": port },
            }
        }))
        .unwrap();
    Arc::new(builder)
}

struct TestNode {
    transport: DynTransport,
    url: Url,
    notify: tokio::sync::mpsc::UnboundedReceiver<(Url, SpaceId, bytes::Bytes)>,
}

async fn test_node(port: u16) -> TestNode {
    let (notify_tx, notify_rx) = tokio::sync::mpsc::unbounded_channel();
    let handler = Arc::new(TestHandler { notify: notify_tx });
    let builder = test_builder(port);
    let transport = builder
        .transport
        .create(builder.clone(), handler.clone())
        .await
        .unwrap();
    let url = transport
        .register_space_handler(test_space(), handler)
        .expect("transport should know its url");
    TestNode {
        transport,
        url,
        notify: notify_rx,
    }
}

async fn recv_timeout<T>(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<T>,
) -> T {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for multicast event")
        .expect("event channel closed")
}

fn gated() -> bool {
    if std::env::var("K2_BCAST_IT").is_err() {
        eprintln!("skipping real-multicast test; set K2_BCAST_IT=1 to enable");
        return true;
    }
    false
}

/// A per-process port so concurrent test runs on one host do not
/// cross-talk.
fn test_port() -> u16 {
    20_000 + (std::process::id() % 20_000) as u16
}

#[tokio::test(flavor = "multi_thread")]
async fn multicast_notify_round_trip() {
    if gated() {
        return;
    }
    let port = test_port();
    let mut alice = test_node(port).await;
    let mut bob = test_node(port).await;

    assert!(alice.url.as_str().starts_with("ws://udpm.bcast:1/"));

    alice
        .transport
        .send_space_notify(
            bob.url.clone(),
            test_space(),
            bytes::Bytes::from_static(b"over real multicast"),
        )
        .await
        .unwrap();
    let (from, _, data) = recv_timeout(&mut bob.notify).await;
    assert_eq!(from, alice.url);
    assert_eq!(&data[..], b"over real multicast");

    bob.transport
        .send_space_notify(
            alice.url.clone(),
            test_space(),
            bytes::Bytes::from_static(b"right back"),
        )
        .await
        .unwrap();
    let (from, _, data) = recv_timeout(&mut alice.notify).await;
    assert_eq!(from, bob.url);
    assert_eq!(&data[..], b"right back");

    let stats = alice.transport.dump_network_stats().await.unwrap();
    assert_eq!(stats.transport_stats.backend, "broadcast-udpm");
}

#[tokio::test(flavor = "multi_thread")]
async fn multicast_large_payload_chunked() {
    if gated() {
        return;
    }
    // Offset from the other test's port: both run in one process.
    let port = test_port() + 1;
    let alice = test_node(port).await;
    let mut bob = test_node(port).await;

    let payload: Vec<u8> = (0..=255_u8).cycle().take(100_000).collect();
    alice
        .transport
        .send_space_notify(
            bob.url.clone(),
            test_space(),
            bytes::Bytes::from(payload.clone()),
        )
        .await
        .unwrap();
    let (_, _, data) = recv_timeout(&mut bob.notify).await;
    assert_eq!(&data[..], &payload[..]);
}
