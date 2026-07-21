use super::*;
use kitsune2_transport_broadcast::{
    BroadcastTransportFactory, MemAir, MemAirConfig,
};
use std::time::Duration;

/// Top-level transport handler.
///
/// Its `new_listening_address` fires during a backend's own create —
/// i.e. for the switch transport, *before* a runtime switch has
/// replayed handlers and swapped the backend in. Kept on a separate
/// channel from [`SpaceHandler`] so tests can tell the two apart.
#[derive(Debug)]
struct TopHandler {
    address: tokio::sync::mpsc::UnboundedSender<Url>,
}

impl TxBaseHandler for TopHandler {
    fn new_listening_address(&self, this_url: Url) -> BoxFut<'static, ()> {
        let _ = self.address.send(this_url);
        Box::pin(async {})
    }
}

impl TxHandler for TopHandler {}

/// Space handler. Its `new_listening_address` only fires once a
/// runtime switch has completed (handlers replayed, backend swapped),
/// so it is the authoritative "the switch is done" signal.
#[derive(Debug)]
struct SpaceHandler {
    notify: tokio::sync::mpsc::UnboundedSender<(Url, SpaceId, bytes::Bytes)>,
    address: tokio::sync::mpsc::UnboundedSender<Url>,
}

impl TxBaseHandler for SpaceHandler {
    fn new_listening_address(&self, this_url: Url) -> BoxFut<'static, ()> {
        let _ = self.address.send(this_url);
        Box::pin(async {})
    }
}

impl TxSpaceHandler for SpaceHandler {
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

struct TestNode {
    builder: Arc<Builder>,
    transport: DynTransport,
    url: Url,
    notify: tokio::sync::mpsc::UnboundedReceiver<(Url, SpaceId, bytes::Bytes)>,
    /// Announcements to the top-level [`TopHandler`].
    top_address: tokio::sync::mpsc::UnboundedReceiver<Url>,
    /// Announcements to the [`SpaceHandler`] (post-switch only).
    space_address: tokio::sync::mpsc::UnboundedReceiver<Url>,
}

fn test_space() -> SpaceId {
    SpaceId::from(bytes::Bytes::from_static(b"switch-test-space"))
}

/// A full Builder around the given transport factory, mirroring
/// `kitsune2::default_builder` without depending on any particular
/// transport backend.
fn test_builder(transport: DynTransportFactory) -> K2Result<Builder> {
    use kitsune2_core::factories;
    Builder {
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
        transport,
        op_store: factories::MemOpStoreFactory::create(),
        peer_meta_store: factories::MemPeerMetaStoreFactory::create(),
        gossip: kitsune2_gossip::K2GossipFactory::create(),
        local_agent_store: factories::CoreLocalAgentStoreFactory::create(),
        publish: factories::CorePublishFactory::create(),
        blocks: factories::MemBlocksFactory::create(),
        known_peers: factories::CoreKnownPeersFactory::create(),
    }
    .with_default_config()
}

async fn test_node(transport_factory: DynTransportFactory) -> TestNode {
    let (notify_tx, notify_rx) = tokio::sync::mpsc::unbounded_channel();
    let (top_address_tx, top_address_rx) =
        tokio::sync::mpsc::unbounded_channel();
    let (space_address_tx, space_address_rx) =
        tokio::sync::mpsc::unbounded_channel();

    let builder = Arc::new(test_builder(transport_factory.clone()).unwrap());
    let transport = builder
        .transport
        .create(
            builder.clone(),
            Arc::new(TopHandler {
                address: top_address_tx,
            }),
        )
        .await
        .unwrap();
    let url = transport
        .register_space_handler(
            test_space(),
            Arc::new(SpaceHandler {
                notify: notify_tx,
                address: space_address_tx,
            }),
        )
        .expect("transport should know its url");

    TestNode {
        builder,
        transport,
        url,
        notify: notify_rx,
        top_address: top_address_rx,
        space_address: space_address_rx,
    }
}

async fn recv_timeout<T>(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<T>,
) -> T {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for event")
        .expect("event channel closed")
}

#[tokio::test(flavor = "multi_thread")]
async fn selects_first_backend_by_default_and_carries_traffic() {
    let air = MemAir::create(MemAirConfig::default());
    let factory = || {
        SwitchableTransportFactory::create(vec![
            (
                "bcast-a".into(),
                BroadcastTransportFactory::create_with_medium(air.clone()),
            ),
            (
                "bcast-b".into(),
                BroadcastTransportFactory::create_with_medium(MemAir::create(
                    MemAirConfig::default(),
                )),
            ),
        ])
    };
    let alice = test_node(factory()).await;
    let mut bob = test_node(factory()).await;

    alice
        .transport
        .send_space_notify(
            bob.url.clone(),
            test_space(),
            bytes::Bytes::from_static(b"via switch"),
        )
        .await
        .unwrap();
    let (from, _, data) = recv_timeout(&mut bob.notify).await;
    assert_eq!(from, alice.url);
    assert_eq!(&data[..], b"via switch");
}

#[tokio::test(flavor = "multi_thread")]
async fn runtime_switch_replays_handlers_and_reannounces() {
    let air_a = MemAir::create(MemAirConfig::default());
    let air_b = MemAir::create(MemAirConfig::default());

    // Alice runs the switch over backends on two isolated airs.
    let mut alice = test_node(SwitchableTransportFactory::create(vec![
        (
            "bcast-a".into(),
            BroadcastTransportFactory::create_with_medium(air_a.clone()),
        ),
        (
            "bcast-b".into(),
            BroadcastTransportFactory::create_with_medium(air_b.clone()),
        ),
    ]))
    .await;
    // The initial backend announced to the top-level handler during
    // create; the space handler learned the url via registration, not
    // an announcement.
    let initial_url = recv_timeout(&mut alice.top_address).await;
    assert_eq!(initial_url, alice.url);

    // Bob lives on air B only.
    let mut bob =
        test_node(BroadcastTransportFactory::create_with_medium(air_b.clone()))
            .await;

    // Alice cannot reach bob while on backend A: the airs are
    // isolated. (The send itself is fire-and-forget and succeeds.)
    alice
        .transport
        .send_space_notify(
            bob.url.clone(),
            test_space(),
            bytes::Bytes::from_static(b"lost in air a"),
        )
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(300), bob.notify.recv())
            .await
            .is_err()
    );

    // Runtime switch via config update — the same path a conductor
    // admin call would use.
    alice
        .builder
        .config
        .set_module_config(&serde_json::json!({
            "switchTransport": { "active": "bcast-b" }
        }))
        .unwrap();

    // The space handler is re-announced with the new backend's url
    // only after the swap has completed — this is the authoritative
    // "switch done" signal, so traffic sent below cannot race the
    // switch.
    let new_url = recv_timeout(&mut alice.space_address).await;
    assert_ne!(new_url, alice.url, "switching backends must change the url");

    // Now traffic flows on air B, both directions.
    alice
        .transport
        .send_space_notify(
            bob.url.clone(),
            test_space(),
            bytes::Bytes::from_static(b"found you on air b"),
        )
        .await
        .unwrap();
    let (from, _, data) = recv_timeout(&mut bob.notify).await;
    assert_eq!(from, new_url);
    assert_eq!(&data[..], b"found you on air b");

    bob.transport
        .send_space_notify(
            new_url.clone(),
            test_space(),
            bytes::Bytes::from_static(b"hello switched alice"),
        )
        .await
        .unwrap();
    let (from, _, data) = recv_timeout(&mut alice.notify).await;
    assert_eq!(from, bob.url);
    assert_eq!(&data[..], b"hello switched alice");
}

#[tokio::test(flavor = "multi_thread")]
async fn switch_to_unknown_backend_keeps_current() {
    let air = MemAir::create(MemAirConfig::default());
    let factory = || {
        SwitchableTransportFactory::create(vec![(
            "bcast-a".into(),
            BroadcastTransportFactory::create_with_medium(air.clone()),
        )])
    };
    let alice = test_node(factory()).await;
    let mut bob = test_node(factory()).await;

    alice
        .builder
        .config
        .set_module_config(&serde_json::json!({
            "switchTransport": { "active": "no-such-backend" }
        }))
        .unwrap();

    // Give the (failing) switch task a moment, then confirm the old
    // backend still carries traffic.
    tokio::time::sleep(Duration::from_millis(200)).await;
    alice
        .transport
        .send_space_notify(
            bob.url.clone(),
            test_space(),
            bytes::Bytes::from_static(b"still here"),
        )
        .await
        .unwrap();
    let (_, _, data) = recv_timeout(&mut bob.notify).await;
    assert_eq!(&data[..], b"still here");
}

#[tokio::test(flavor = "multi_thread")]
async fn validate_config_rejects_unknown_active() {
    let factory = SwitchableTransportFactory::create(vec![(
        "bcast-a".into(),
        BroadcastTransportFactory::create_with_medium(MemAir::create(
            MemAirConfig::default(),
        )),
    )]);
    let builder = test_builder(factory.clone()).unwrap();
    builder
        .config
        .set_module_config(&serde_json::json!({
            "switchTransport": { "active": "nope" }
        }))
        .unwrap();
    assert!(factory.validate_config(&builder.config).is_err());
}
