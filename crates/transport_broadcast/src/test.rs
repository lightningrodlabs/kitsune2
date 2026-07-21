use super::*;
use std::time::Duration;

/// Test handler recording transport events.
#[derive(Debug)]
struct TestHandler {
    name: &'static str,
    notify: tokio::sync::mpsc::UnboundedSender<(Url, SpaceId, bytes::Bytes)>,
    preflight_in: tokio::sync::mpsc::UnboundedSender<(Url, bytes::Bytes)>,
    disconnect: tokio::sync::mpsc::UnboundedSender<(Url, Option<String>)>,
}

struct TestNode {
    transport: DynTransport,
    url: Url,
    notify: tokio::sync::mpsc::UnboundedReceiver<(Url, SpaceId, bytes::Bytes)>,
    preflight_in: tokio::sync::mpsc::UnboundedReceiver<(Url, bytes::Bytes)>,
    disconnect: tokio::sync::mpsc::UnboundedReceiver<(Url, Option<String>)>,
}

impl TxBaseHandler for TestHandler {
    fn peer_disconnect(&self, peer: Url, reason: Option<String>) {
        let _ = self.disconnect.send((peer, reason));
    }
}

impl TxHandler for TestHandler {
    fn preflight_gather_outgoing(
        &self,
        _peer_url: Url,
    ) -> BoxFut<'_, K2Result<bytes::Bytes>> {
        let name = self.name;
        Box::pin(async move {
            Ok(bytes::Bytes::from(format!("preflight-from-{name}")))
        })
    }

    fn preflight_validate_incoming(
        &self,
        peer_url: Url,
        data: bytes::Bytes,
    ) -> BoxFut<'_, K2Result<()>> {
        let _ = self.preflight_in.send((peer_url, data));
        Box::pin(async { Ok(()) })
    }
}

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
    SpaceId::from(bytes::Bytes::from_static(b"test-space"))
}

/// Build a transport on the given air with the full DefaultTransport
/// wrapping, exactly as the factory does.
async fn test_node(
    name: &'static str,
    air: Arc<MemAir>,
    idle_timeout_ms: u32,
) -> TestNode {
    let (notify_tx, notify_rx) = tokio::sync::mpsc::unbounded_channel();
    let (preflight_tx, preflight_rx) = tokio::sync::mpsc::unbounded_channel();
    let (disconnect_tx, disconnect_rx) = tokio::sync::mpsc::unbounded_channel();
    let handler = Arc::new(TestHandler {
        name,
        notify: notify_tx,
        preflight_in: preflight_tx,
        disconnect: disconnect_tx,
    });

    let hnd = TxImpHnd::new(handler.clone());
    let imp = BroadcastTransport::create(
        BroadcastTransportConfig {
            medium: "mem".into(),
            idle_timeout_ms,
            ..Default::default()
        },
        air,
        hnd.clone(),
    )
    .await;
    let transport = DefaultTransport::create(&hnd, imp);
    let url = transport
        .register_space_handler(test_space(), handler)
        .expect("transport should know its url");

    TestNode {
        transport,
        url,
        notify: notify_rx,
        preflight_in: preflight_rx,
        disconnect: disconnect_rx,
    }
}

async fn recv_timeout<T>(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<T>,
) -> T {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for transport event")
        .expect("event channel closed")
}

#[tokio::test(flavor = "multi_thread")]
async fn notify_round_trip_with_preflight() {
    let air = MemAir::create(MemAirConfig::default());
    let mut alice = test_node("alice", air.clone(), 30_000).await;
    let mut bob = test_node("bob", air, 30_000).await;

    alice
        .transport
        .send_space_notify(
            bob.url.clone(),
            test_space(),
            bytes::Bytes::from_static(b"hello bob"),
        )
        .await
        .unwrap();

    // Bob hears the notify.
    let (from, space, data) = recv_timeout(&mut bob.notify).await;
    assert_eq!(from, alice.url);
    assert_eq!(space, test_space());
    assert_eq!(&data[..], b"hello bob");

    // Both sides saw each other's preflight.
    let (_, data) = recv_timeout(&mut bob.preflight_in).await;
    assert_eq!(&data[..], b"preflight-from-alice");
    let (_, data) = recv_timeout(&mut alice.preflight_in).await;
    assert_eq!(&data[..], b"preflight-from-bob");

    // And the reply direction works over the now-open virtual
    // connection.
    bob.transport
        .send_space_notify(
            alice.url.clone(),
            test_space(),
            bytes::Bytes::from_static(b"hello alice"),
        )
        .await
        .unwrap();
    let (from, _, data) = recv_timeout(&mut alice.notify).await;
    assert_eq!(from, bob.url);
    assert_eq!(&data[..], b"hello alice");

    // Both report the virtual connection.
    let peers = alice.transport.get_connected_peers().await.unwrap();
    assert_eq!(peers, vec![bob.url.clone()]);
    let peers = bob.transport.get_connected_peers().await.unwrap();
    assert_eq!(peers, vec![alice.url.clone()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn large_payload_chunked_over_small_mtu() {
    // Small enough that both the notify payload and the preflight-era
    // K2Proto envelope overhead force multi-frame chunking.
    let air = MemAir::create(MemAirConfig {
        mtu: 96,
        ..Default::default()
    });
    let alice = test_node("alice", air.clone(), 30_000).await;
    let mut bob = test_node("bob", air, 30_000).await;

    let payload: Vec<u8> = (0..=255_u8).cycle().take(10_000).collect();
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

#[tokio::test(flavor = "multi_thread")]
async fn graceful_disconnect_reaches_peer() {
    let air = MemAir::create(MemAirConfig::default());
    let mut alice = test_node("alice", air.clone(), 30_000).await;
    let mut bob = test_node("bob", air, 30_000).await;

    alice
        .transport
        .send_space_notify(
            bob.url.clone(),
            test_space(),
            bytes::Bytes::from_static(b"hi"),
        )
        .await
        .unwrap();
    let _ = recv_timeout(&mut bob.notify).await;

    alice
        .transport
        .disconnect(bob.url.clone(), Some("done with you".into()))
        .await;

    // Bob's handler processes the K2Proto Disconnect payload, errors,
    // and the virtual connection closes with the remote reason.
    let (peer, reason) = recv_timeout(&mut bob.disconnect).await;
    assert_eq!(peer, alice.url);
    assert!(
        reason
            .as_deref()
            .unwrap_or_default()
            .contains("done with you"),
        "unexpected disconnect reason: {reason:?}"
    );

    // Alice's side also closed.
    let (peer, _) = recv_timeout(&mut alice.disconnect).await;
    assert_eq!(peer, bob.url);
    assert!(
        alice
            .transport
            .get_connected_peers()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_virtual_connections_reaped() {
    let air = MemAir::create(MemAirConfig::default());
    let mut alice = test_node("alice", air.clone(), 300).await;
    let mut bob = test_node("bob", air, 300).await;

    alice
        .transport
        .send_space_notify(
            bob.url.clone(),
            test_space(),
            bytes::Bytes::from_static(b"hi"),
        )
        .await
        .unwrap();
    let _ = recv_timeout(&mut bob.notify).await;

    // No further traffic: both sides should reap the idle connection.
    let (_, reason) = recv_timeout(&mut alice.disconnect).await;
    assert_eq!(reason.as_deref(), Some("idle timeout"));
    let (_, reason) = recv_timeout(&mut bob.disconnect).await;
    assert_eq!(reason.as_deref(), Some("idle timeout"));
    assert!(
        alice
            .transport
            .get_connected_peers()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn network_stats_report_backend_and_counters() {
    let air = MemAir::create(MemAirConfig::default());
    let alice = test_node("alice", air.clone(), 30_000).await;
    let mut bob = test_node("bob", air, 30_000).await;

    alice
        .transport
        .send_space_notify(
            bob.url.clone(),
            test_space(),
            bytes::Bytes::from_static(b"stats"),
        )
        .await
        .unwrap();
    let _ = recv_timeout(&mut bob.notify).await;

    let stats = alice.transport.dump_network_stats().await.unwrap();
    let stats = stats.transport_stats;
    assert_eq!(stats.backend, "broadcast-mem");
    assert_eq!(stats.peer_urls, vec![alice.url.clone()]);
    let conn = stats
        .connections
        .iter()
        .find(|c| c.pub_key == bob.url.as_str())
        .expect("connection to bob in stats");
    assert!(conn.send_message_count >= 1);
    assert!(conn.send_bytes > 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn transports_on_different_airs_do_not_hear_each_other() {
    let mut alice =
        test_node("alice", MemAir::create(MemAirConfig::default()), 30_000)
            .await;
    let bob =
        test_node("bob", MemAir::create(MemAirConfig::default()), 30_000).await;

    // The send itself succeeds (fire and forget into alice's air), but
    // bob never hears it and no reply preflight ever arrives.
    alice
        .transport
        .send_space_notify(
            bob.url.clone(),
            test_space(),
            bytes::Bytes::from_static(b"into the void"),
        )
        .await
        .unwrap();

    assert!(
        tokio::time::timeout(
            Duration::from_millis(500),
            alice.preflight_in.recv()
        )
        .await
        .is_err(),
        "no preflight should cross isolated airs"
    );
}
