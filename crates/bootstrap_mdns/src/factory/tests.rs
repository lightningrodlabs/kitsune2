//! Factory-level tests: what one factory does across several spaces,
//! driven through a fake daemon so no multicast is involved.

use super::*;
use crate::test_support::*;
use kitsune2_test_utils::agent::{AgentBuilder, TestLocalAgent};
use std::sync::atomic::{AtomicUsize, Ordering};

const PEER_A: &str = "ws://a.test:80/peera";
const PEER_B: &str = "ws://b.test:80/peerb";

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
        service_type: SERVICE_TYPE.into(),
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
) -> (DynBootstrap, SpaceId, Dials) {
    let space_id = space_id(space);
    let (tx, dials) = recording_transport(connected);
    let peer_store = peer_store(h, &space_id).await;
    let boot = h
        .factory
        .create(h.builder.clone(), peer_store, space_id.clone(), tx)
        .await
        .unwrap();
    (boot, space_id, dials)
}

/// Hand the bootstrap a local agent info carrying `url`, which is what
/// gives the space something to announce and makes it dial.
fn put(boot: &DynBootstrap, space: &SpaceId, url: &Url) {
    boot.put(
        AgentBuilder::default()
            .with_space(space.clone())
            .with_url(Some(url.clone()))
            .build(TestLocalAgent::default()),
    );
}

const SELF_URL: &str = "ws://self.test:80/selfpeer";

#[tokio::test]
async fn spaces_share_one_daemon_and_announce_their_own_records() {
    let h = harness(enabled());
    let (boot_a, space_a, _) = create_space(&h, b"space-a", vec![]).await;
    let (boot_b, space_b, _) = create_space(&h, b"space-b", vec![]).await;
    assert_eq!(h.starts.load(Ordering::SeqCst), 1);
    assert_eq!(h.factory.shared_if_started().unwrap().space_count(), 2);

    put(&boot_a, &space_a, &url(PEER_A));
    put(&boot_b, &space_b, &url(PEER_A));

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
    let (boot_a, space_a, dials_a) = create_space(&h, b"space-a", vec![]).await;
    let (boot_b, space_b, dials_b) = create_space(&h, b"space-b", vec![]).await;
    put(&boot_a, &space_a, &url(SELF_URL));
    put(&boot_b, &space_b, &url(SELF_URL));
    // The first put reconciles what was heard so far (nothing yet); let
    // that run before the LAN speaks, so each record is dialled once.
    settle().await;

    h.daemon.deliver(resolved_peer(
        "peer-1",
        &SpaceFingerprint::derive(&h.builder, &space_a)
            .await
            .unwrap(),
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
    let (boot_b, space_b, dials_b) = create_space(&h, b"space-b", vec![]).await;
    put(&boot_a, &space_a, &url(SELF_URL));
    put(&boot_b, &space_b, &url(SELF_URL));
    settle().await;

    drop(boot_a);
    assert_eq!(h.factory.shared_if_started().unwrap().space_count(), 1);
    assert_eq!(
        h.daemon.unregistered.lock().unwrap().len(),
        1,
        "the dropped space's record is withdrawn"
    );

    h.daemon.deliver(resolved_peer(
        "peer-1",
        &SpaceFingerprint::derive(&h.builder, &space_a)
            .await
            .unwrap(),
        PEER_A,
    ));
    h.daemon.deliver(resolved_peer(
        "peer-2",
        &SpaceFingerprint::derive(&h.builder, &space_b)
            .await
            .unwrap(),
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
    let (boot, space_a, dials) = create_space(&h, b"space-a", vec![]).await;
    put(&boot, &space_a, &url(SELF_URL));
    settle().await;

    h.daemon.deliver(resolved_peer(
        "peer-1",
        &SpaceFingerprint::derive(&h.builder, &space_a)
            .await
            .unwrap(),
        PEER_A,
    ));
    settle().await;
    let first = dials.lock().unwrap().len();
    assert!(first >= 1, "the announcement is dialled on arrival");

    tokio::time::sleep(Duration::from_millis(250)).await;
    let count = dials.lock().unwrap().len();
    assert!(count > first, "expected redials, saw {count} dials");
    assert!(dials.lock().unwrap().iter().all(|u| u == &url(PEER_A)));
}

/// A daemon that cannot start is reported as the error it is; making the
/// LAN path optional is the wrapping factory's job.
#[tokio::test]
async fn a_failing_daemon_start_fails_create() {
    let starts = Arc::new(AtomicUsize::new(0));
    let factory = {
        let starts = starts.clone();
        Arc::new(MdnsBootstrapFactory::with_daemon_start(Arc::new(
            move |_service_type| {
                starts.fetch_add(1, Ordering::SeqCst);
                Err(K2Error::other("no multicast on this host"))
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
            mdns_bootstrap: enabled(),
        })
        .unwrap();
    let h = Harness {
        factory,
        daemon: FakeDaemon::new(),
        starts,
        builder: Arc::new(builder),
    };

    let space_id = space_id(b"space-a");
    let (tx, _) = recording_transport(vec![]);
    let peer_store = peer_store(&h, &space_id).await;
    let err = h
        .factory
        .create(h.builder.clone(), peer_store, space_id, tx)
        .await
        .expect_err("a daemon that cannot start fails the create");
    assert!(err.to_string().contains("no multicast"), "{err}");
    assert_eq!(h.starts.load(Ordering::SeqCst), 1);
    assert!(h.factory.shared_if_started().is_none());
}

/// A record heard before the space's first put is dialled by that put:
/// the URL is what makes the space dialable, and the LAN does not repeat
/// itself.
#[tokio::test]
async fn the_first_put_dials_what_was_heard_before_it() {
    let h = harness(enabled());
    let (boot, space_a, dials) = create_space(&h, b"space-a", vec![]).await;

    h.daemon.deliver(resolved_peer(
        "peer-1",
        &SpaceFingerprint::derive(&h.builder, &space_a)
            .await
            .unwrap(),
        PEER_A,
    ));
    settle().await;
    assert!(dials.lock().unwrap().is_empty(), "not dialable yet");

    put(&boot, &space_a, &url(SELF_URL));
    settle().await;
    assert_eq!(*dials.lock().unwrap(), vec![url(PEER_A)]);
}

/// A space created after the LAN peer's record was resolved still hears
/// it: the shared browse replays it on join.
#[tokio::test]
async fn a_space_created_later_hears_records_resolved_before_it() {
    let h = harness(enabled());
    let (_boot_a, _, _) = create_space(&h, b"space-a", vec![]).await;
    let space_b = space_id(b"space-b");
    h.daemon.deliver(resolved_peer(
        "peer-1",
        &SpaceFingerprint::derive(&h.builder, &space_b)
            .await
            .unwrap(),
        PEER_B,
    ));
    settle().await;

    let (boot_b, space_b, dials_b) = create_space(&h, b"space-b", vec![]).await;
    put(&boot_b, &space_b, &url(SELF_URL));
    settle().await;
    assert_eq!(*dials_b.lock().unwrap(), vec![url(PEER_B)]);
}
