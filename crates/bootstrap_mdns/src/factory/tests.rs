//! Factory-level tests: what one factory does across several spaces,
//! driven through a fake daemon so no multicast is involved.

use super::*;
use crate::discovery::test_support::{FakeDaemon, resolved_peer};
use kitsune2_api::MockTransport;
use kitsune2_test_utils::agent::{AgentBuilder, TestLocalAgent};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

const PEER_A: &str = "ws://a.test:80/peera";
const PEER_B: &str = "ws://b.test:80/peerb";

fn url(s: &str) -> Url {
    Url::from_str(s).unwrap()
}

/// A transport recording its dials, reporting `connected` as its open
/// connections.
fn transport(connected: Vec<Url>) -> (DynTransport, Arc<Mutex<Vec<Url>>>) {
    let dials: Arc<Mutex<Vec<Url>>> = Arc::new(Mutex::new(Vec::new()));
    let record = dials.clone();
    let mut mock = MockTransport::new();
    mock.expect_dial().returning(move |_space, url| {
        record.lock().unwrap().push(url);
        Box::pin(async { Ok(()) })
    });
    mock.expect_get_connected_peers().returning(move || {
        let connected = connected.clone();
        Box::pin(async move { Ok(connected) })
    });
    (Arc::new(mock), dials)
}

struct Harness {
    factory: Arc<MdnsBootstrapFactory>,
    daemon: Arc<FakeDaemon>,
    /// How many times the daemon-start hook ran.
    starts: Arc<AtomicUsize>,
    builder: Arc<Builder>,
}

fn harness(cfg: MdnsBootstrapConfig) -> Harness {
    let daemon = FakeDaemon::new();
    let starts = Arc::new(AtomicUsize::new(0));
    let factory = {
        let daemon = daemon.clone();
        let starts = starts.clone();
        Arc::new(MdnsBootstrapFactory::with_daemon_start(Arc::new(
            move |_service_type| {
                starts.fetch_add(1, Ordering::SeqCst);
                Ok(daemon.clone() as DynDaemon)
            },
        )))
    };
    let builder = Builder {
        bootstrap: factory.clone(),
        ..kitsune2_core::default_test_builder()
    }
    .with_default_config()
    .unwrap();
    builder
        .config
        .set_module_config(&MdnsBootstrapModConfig {
            mdns_bootstrap: cfg,
        })
        .unwrap();
    Harness {
        factory,
        daemon,
        starts,
        builder: Arc::new(builder),
    }
}

fn enabled() -> MdnsBootstrapConfig {
    MdnsBootstrapConfig {
        enabled: true,
        service_type: crate::discovery::test_support::SERVICE_TYPE.into(),
        ..Default::default()
    }
}

async fn peer_store(h: &Harness, space_id: &SpaceId) -> DynPeerStore {
    let b = &h.builder;
    let blocks = b.blocks.create(b.clone(), space_id.clone()).await.unwrap();
    let known_peers = b
        .known_peers
        .create(b.clone(), space_id.clone())
        .await
        .unwrap();
    b.peer_store
        .create(b.clone(), space_id.clone(), blocks, known_peers)
        .await
        .unwrap()
}

/// Create a space's bootstrap through the factory, returning it with the
/// dials its transport recorded.
async fn create_space(
    h: &Harness,
    space: &[u8],
    connected: Vec<Url>,
) -> (DynBootstrap, SpaceId, Arc<Mutex<Vec<Url>>>) {
    let space_id = SpaceId::from(bytes::Bytes::copy_from_slice(space));
    let (tx, dials) = transport(connected);
    let peer_store = peer_store(h, &space_id).await;
    let boot = h
        .factory
        .create(h.builder.clone(), peer_store, space_id.clone(), tx)
        .await
        .unwrap();
    (boot, space_id, dials)
}

async fn settle() {
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn spaces_share_one_daemon_and_announce_their_own_records() {
    let h = harness(enabled());
    let (boot_a, space_a, _) = create_space(&h, b"space-a", vec![]).await;
    let (boot_b, space_b, _) = create_space(&h, b"space-b", vec![]).await;
    assert_eq!(h.starts.load(Ordering::SeqCst), 1);
    assert_eq!(h.factory.shared_if_started().unwrap().space_count(), 2);

    let put = |boot: &DynBootstrap, space: &SpaceId| {
        boot.put(
            AgentBuilder::default()
                .with_space(space.clone())
                .with_url(Some(url(PEER_A)))
                .build(TestLocalAgent::default()),
        );
    };
    put(&boot_a, &space_a);
    put(&boot_b, &space_b);

    let registered = h.daemon.registered.lock().unwrap();
    assert_eq!(registered.len(), 2, "one record per space");
    assert_ne!(registered[0].0, registered[1].0, "distinct instance names");
    let fps: Vec<&String> = registered
        .iter()
        .map(|(_, txt)| &txt.iter().find(|(k, _)| k == "spacefp").unwrap().1)
        .collect();
    assert_ne!(fps[0], fps[1], "each record commits to its own space");
}

#[tokio::test]
async fn a_disabled_factory_starts_no_daemon() {
    let h = harness(MdnsBootstrapConfig::default());
    let (_boot, _, _) = create_space(&h, b"space-a", vec![]).await;
    assert_eq!(h.starts.load(Ordering::SeqCst), 0);
    assert!(h.factory.shared_if_started().is_none());
}

#[tokio::test]
async fn a_record_for_one_space_never_dials_for_another() {
    let h = harness(enabled());
    let (_boot_a, space_a, dials_a) =
        create_space(&h, b"space-a", vec![]).await;
    let (_boot_b, _space_b, dials_b) =
        create_space(&h, b"space-b", vec![]).await;

    h.daemon.deliver(resolved_peer(
        "peer-1",
        &fingerprint::space_fingerprint(&space_a),
        PEER_A,
    ));
    settle().await;

    assert_eq!(*dials_a.lock().unwrap(), vec![url(PEER_A)]);
    assert!(dials_b.lock().unwrap().is_empty());
}

#[tokio::test]
async fn dropping_one_space_leaves_the_other_browsing() {
    let h = harness(enabled());
    let (boot_a, space_a, dials_a) = create_space(&h, b"space-a", vec![]).await;
    let (_boot_b, space_b, dials_b) =
        create_space(&h, b"space-b", vec![]).await;
    boot_a.put(
        AgentBuilder::default()
            .with_space(space_a.clone())
            .with_url(Some(url(PEER_A)))
            .build(TestLocalAgent::default()),
    );

    drop(boot_a);
    assert_eq!(h.factory.shared_if_started().unwrap().space_count(), 1);
    assert_eq!(
        h.daemon.unregistered.lock().unwrap().len(),
        1,
        "the dropped space's record is withdrawn"
    );

    h.daemon.deliver(resolved_peer(
        "peer-1",
        &fingerprint::space_fingerprint(&space_a),
        PEER_A,
    ));
    h.daemon.deliver(resolved_peer(
        "peer-2",
        &fingerprint::space_fingerprint(&space_b),
        PEER_B,
    ));
    settle().await;

    assert!(dials_a.lock().unwrap().is_empty());
    assert_eq!(*dials_b.lock().unwrap(), vec![url(PEER_B)]);
}

#[tokio::test]
async fn unconnected_peers_are_redialled_on_the_interval() {
    let h = harness(MdnsBootstrapConfig {
        redial_interval_ms: 100,
        ..enabled()
    });
    let (_boot, space_a, dials) = create_space(&h, b"space-a", vec![]).await;

    h.daemon.deliver(resolved_peer(
        "peer-1",
        &fingerprint::space_fingerprint(&space_a),
        PEER_A,
    ));
    settle().await;
    assert_eq!(dials.lock().unwrap().len(), 1);

    tokio::time::sleep(Duration::from_millis(250)).await;
    let count = dials.lock().unwrap().len();
    assert!(count >= 2, "expected redials, saw {count} dials");
    assert!(dials.lock().unwrap().iter().all(|u| u == &url(PEER_A)));
}
