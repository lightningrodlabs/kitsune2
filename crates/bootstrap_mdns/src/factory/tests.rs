//! Factory-level tests: what one factory does across several spaces,
//! driven through a fake daemon so no multicast is involved.

use super::*;
use crate::test_support::*;
use kitsune2_test_utils::agent::{AgentBuilder, TestLocalAgent};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

const PEER_A: &str = "ws://a.test:80/peera";
const PEER_B: &str = "ws://b.test:80/peerb";
const SELF_URL: &str = "ws://self.test:80/selfpeer";
const OTHER_TYPE: &str = "_k2other._udp.local.";

struct Harness {
    factory: Arc<MdnsBootstrapFactory>,
    daemon: Arc<FakeDaemon>,
    /// The service type of every daemon-start hook run, in order.
    starts: Arc<Mutex<Vec<String>>>,
    builder: Arc<Builder>,
}

impl Harness {
    fn start_count(&self) -> usize {
        self.starts.lock().unwrap().len()
    }
}

/// A builder over `factory` whose mDNS config is `cfg`.
fn builder_with(
    factory: &Arc<MdnsBootstrapFactory>,
    cfg: MdnsBootstrapConfig,
) -> Arc<Builder> {
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
    Arc::new(builder)
}

/// A harness whose daemon-start hook runs `start`, and whose failed
/// starts are retried after `retry`.
fn harness_with_retry(
    cfg: MdnsBootstrapConfig,
    retry: Duration,
    start: impl Fn(&str, &Arc<FakeDaemon>) -> K2Result<DynDaemon>
    + Send
    + Sync
    + 'static,
) -> Harness {
    let daemon = FakeDaemon::new();
    let starts: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let factory = {
        let daemon = daemon.clone();
        let starts = starts.clone();
        Arc::new(MdnsBootstrapFactory::with_daemon_start_and_retry(
            Arc::new(move |service_type| {
                starts.lock().unwrap().push(service_type.to_string());
                start(service_type, &daemon)
            }),
            retry,
        ))
    };
    let builder = builder_with(&factory, cfg);
    Harness {
        factory,
        daemon,
        starts,
        builder,
    }
}

/// A harness whose daemon-start hook runs `start`.
fn harness_with(
    cfg: MdnsBootstrapConfig,
    start: impl Fn(&str, &Arc<FakeDaemon>) -> K2Result<DynDaemon>
    + Send
    + Sync
    + 'static,
) -> Harness {
    harness_with_retry(cfg, FAILED_START_RETRY, start)
}

fn harness(cfg: MdnsBootstrapConfig) -> Harness {
    harness_with(cfg, |_, daemon| Ok(daemon.clone() as DynDaemon))
}

/// A harness whose daemon never starts.
fn failing_harness() -> Harness {
    harness_with(enabled(), |_, _| {
        Err(K2Error::other("no multicast on this host"))
    })
}

/// A harness whose daemon starts only once `ready` is set, and which
/// retries a failed start on every attempt.
fn harness_ready_when(ready: Arc<AtomicBool>) -> Harness {
    harness_with_retry(enabled(), Duration::ZERO, move |_, daemon| {
        if ready.load(Ordering::Relaxed) {
            Ok(daemon.clone() as DynDaemon)
        } else {
            Err(K2Error::other("no network yet"))
        }
    })
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
    let (boot, space_id, dials) =
        try_create_space(h, &h.builder, space, connected).await;
    (boot.unwrap(), space_id, dials)
}

/// Create a space's bootstrap through `builder`'s factory, returning the
/// result with the dials its transport recorded.
async fn try_create_space(
    h: &Harness,
    builder: &Arc<Builder>,
    space: &[u8],
    connected: Vec<Url>,
) -> (K2Result<DynBootstrap>, SpaceId, Dials) {
    let space_id = space_id(space);
    let (tx, dials) = recording_transport(connected);
    let peer_store = peer_store(h, &space_id).await;
    let boot = h
        .factory
        .create(builder.clone(), peer_store, space_id.clone(), tx)
        .await;
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

async fn fp_of(h: &Harness, space: &SpaceId) -> SpaceFingerprint {
    SpaceFingerprint::derive(&h.builder, space).await.unwrap()
}

#[tokio::test]
async fn spaces_share_one_daemon_and_announce_their_own_records() {
    let h = harness(enabled());
    let (boot_a, space_a, _) = create_space(&h, b"space-a", vec![]).await;
    let (boot_b, space_b, _) = create_space(&h, b"space-b", vec![]).await;
    assert_eq!(h.start_count(), 1);
    assert_eq!(h.factory.shared_for(SERVICE_TYPE).unwrap().space_count(), 2);

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
    assert_eq!(h.start_count(), 0);
    assert!(h.factory.shared_for(SERVICE_TYPE).is_none());
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
        &fp_of(&h, &space_a).await,
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
    assert_eq!(h.factory.shared_for(SERVICE_TYPE).unwrap().space_count(), 1);
    assert_eq!(
        h.daemon.unregistered.lock().unwrap().len(),
        1,
        "the dropped space's record is withdrawn"
    );

    h.daemon.deliver(resolved_peer(
        "peer-1",
        &fp_of(&h, &space_a).await,
        PEER_A,
    ));
    h.daemon.deliver(resolved_peer(
        "peer-2",
        &fp_of(&h, &space_b).await,
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
        &fp_of(&h, &space_a).await,
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

/// A record heard before the space's first put is dialled by that put:
/// the URL is what makes the space dialable, and the LAN does not repeat
/// itself.
#[tokio::test]
async fn the_first_put_dials_what_was_heard_before_it() {
    let h = harness(enabled());
    let (boot, space_a, dials) = create_space(&h, b"space-a", vec![]).await;

    h.daemon.deliver(resolved_peer(
        "peer-1",
        &fp_of(&h, &space_a).await,
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
        &fp_of(&h, &space_b).await,
        PEER_B,
    ));
    settle().await;

    let (boot_b, space_b, dials_b) = create_space(&h, b"space-b", vec![]).await;
    put(&boot_b, &space_b, &url(SELF_URL));
    settle().await;
    assert_eq!(*dials_b.lock().unwrap(), vec![url(PEER_B)]);
}

/// A daemon that cannot start is the host's condition, not the space's
/// misconfiguration: the space still gets a bootstrap, one that has not
/// joined anything yet.
#[tokio::test]
async fn a_failing_daemon_start_still_yields_a_bootstrap() {
    let h = failing_harness();

    let (boot, space_a, _) =
        try_create_space(&h, &h.builder, b"space-a", vec![]).await;
    let boot =
        boot.expect("a daemon that cannot start does not fail the space");
    assert_eq!(h.start_count(), 1);
    assert!(h.factory.shared_for(SERVICE_TYPE).is_none());

    // Nothing to announce on: the put is remembered as a reason to retry,
    // not as a record.
    put(&boot, &space_a, &url(SELF_URL));
    assert!(h.daemon.registered.lock().unwrap().is_empty());
}

/// Fifty spaces created on a host without multicast must not cost fifty
/// daemon start attempts, and neither must their puts: the failure is
/// remembered for a while.
#[tokio::test]
async fn a_failed_start_is_not_retried_within_the_window() {
    let h = failing_harness();

    let (first, space_a, _) =
        try_create_space(&h, &h.builder, b"space-a", vec![]).await;
    let (second, _, _) =
        try_create_space(&h, &h.builder, b"space-b", vec![]).await;
    let first = first.unwrap();
    second.unwrap();
    assert_eq!(h.start_count(), 1, "one start attempt for both spaces");

    put(&first, &space_a, &url(SELF_URL));
    tokio::task::yield_now().await;
    assert_eq!(h.start_count(), 1, "a put inside the window does not retry");
    assert!(h.factory.shared_for(SERVICE_TYPE).is_none());
}

/// A space created while the daemon could not start joins it from a
/// later put once it can: agent infos are re-signed periodically, so the
/// puts keep coming, and the join announces the URL from that put and
/// replays what the LAN said meanwhile.
#[tokio::test]
async fn a_detached_space_joins_on_a_later_put_once_the_daemon_starts() {
    let ready = Arc::new(AtomicBool::new(false));
    let h = harness_ready_when(ready.clone());

    let (boot, space_a, dials) = create_space(&h, b"space-a", vec![]).await;
    assert_eq!(h.start_count(), 1);
    assert!(h.factory.shared_for(SERVICE_TYPE).is_none());

    put(&boot, &space_a, &url(SELF_URL));
    wait_until(|| h.start_count() == 2).await;
    tokio::task::yield_now().await;
    assert!(
        h.factory.shared_for(SERVICE_TYPE).is_none(),
        "still failing"
    );
    assert!(h.daemon.registered.lock().unwrap().is_empty());

    ready.store(true, Ordering::Relaxed);
    h.daemon.deliver(resolved_peer(
        "peer-1",
        &fp_of(&h, &space_a).await,
        PEER_A,
    ));
    put(&boot, &space_a, &url(SELF_URL));
    wait_until(|| h.factory.shared_for(SERVICE_TYPE).is_some()).await;
    assert_eq!(h.start_count(), 3);
    assert_eq!(h.factory.shared_for(SERVICE_TYPE).unwrap().space_count(), 1);
    wait_until(|| h.daemon.registered.lock().unwrap().len() == 1).await;
    // The record arrives around the first put, so it may be dialled both
    // on arrival and by that put's reconciliation; what matters is that
    // it is dialled at all, and nothing else is.
    let dials = wait_for_dials(&dials, 1).await;
    assert!(dials.iter().all(|u| u == &url(PEER_A)), "{dials:?}");

    // The joined space leaves like any other.
    drop(boot);
    assert_eq!(h.factory.shared_for(SERVICE_TYPE).unwrap().space_count(), 0);
    assert_eq!(h.daemon.unregistered.lock().unwrap().len(), 1);
}

/// A space whose config names another service type gets a daemon of its
/// own rather than silently joining the first one.
#[tokio::test]
async fn each_service_type_gets_its_own_daemon() {
    let h = harness(enabled());
    let other = builder_with(
        &h.factory,
        MdnsBootstrapConfig {
            service_type: OTHER_TYPE.into(),
            ..enabled()
        },
    );

    let (_boot_a, _, _) = create_space(&h, b"space-a", vec![]).await;
    let (boot_b, _, _) = try_create_space(&h, &other, b"space-b", vec![]).await;
    let _boot_b = boot_b.unwrap();
    let (_boot_c, _, _) = create_space(&h, b"space-c", vec![]).await;

    assert_eq!(
        *h.starts.lock().unwrap(),
        vec![SERVICE_TYPE.to_string(), OTHER_TYPE.to_string()]
    );
    assert_eq!(h.factory.shared_for(SERVICE_TYPE).unwrap().space_count(), 2);
    assert_eq!(h.factory.shared_for(OTHER_TYPE).unwrap().space_count(), 1);
}
