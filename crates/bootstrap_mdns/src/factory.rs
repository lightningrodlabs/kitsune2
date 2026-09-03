//! [`BootstrapFactory`] backed by mDNS LAN discovery.

use crate::config::{
    MdnsBootstrapConfig, MdnsBootstrapModConfig, validate_service_type,
};
use crate::discovery::{DynDaemon, MdnsService};
use crate::fingerprint::SpaceFingerprint;
use crate::shared::SharedMdns;
use crate::space::SpaceEntry;
use kitsune2_api::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, trace, warn};

/// How the factory starts its mDNS daemon for a service type. Injectable
/// so that the factory can be exercised without multicast.
pub(crate) type DaemonStart =
    Arc<dyn Fn(&str) -> K2Result<DynDaemon> + Send + Sync + 'static>;

/// How long a daemon that failed to start is left alone before the next
/// space creation or put tries again. Long enough that fifty spaces
/// starting at once cost one attempt, short enough that a node which
/// gains a network later recovers.
pub(crate) const FAILED_START_RETRY: Duration = Duration::from_secs(60);

/// The [`BootstrapFactory`] behind mDNS LAN discovery.
///
/// One factory owns one mDNS daemon per service type, started the first
/// time an enabled space asks for that type and kept for the factory's
/// life; every space on the same type shares that daemon, its single
/// browse and its reconciliation ticker.
pub struct MdnsBootstrapFactory {
    inner: Arc<FactoryInner>,
}

/// The state a factory's `create` futures and bootstraps share with it:
/// they outlive the borrow of the factory, so it lives behind an `Arc`.
struct FactoryInner {
    /// One slot per service type, held across a start so that spaces
    /// created together do not race to start the same daemon.
    daemons: tokio::sync::Mutex<HashMap<String, DaemonSlot>>,
    daemon_start: DaemonStart,
    /// How long a failed start is remembered before it is tried again.
    failed_start_retry: Duration,
    /// Whether a failed start has been reported at warning level yet. A
    /// host without multicast fails every space the same way, and one
    /// warning says it; the rest is debug noise.
    start_failure_warned: AtomicBool,
}

/// What a service type's daemon is doing.
enum DaemonSlot {
    Running(Arc<SharedMdns>),
    /// The last start failed at `at`; retried after the factory's
    /// `failed_start_retry`.
    Failed {
        at: Instant,
        err: String,
    },
}

impl std::fmt::Debug for FactoryInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FactoryInner").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for MdnsBootstrapFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MdnsBootstrapFactory")
            .finish_non_exhaustive()
    }
}

impl MdnsBootstrapFactory {
    /// Construct a new factory.
    pub fn create() -> DynBootstrapFactory {
        Arc::new(Self::with_daemon_start(Arc::new(start_mdns_sd_daemon)))
    }

    /// A factory whose daemons come from `daemon_start`.
    pub(crate) fn with_daemon_start(daemon_start: DaemonStart) -> Self {
        Self::with_daemon_start_and_retry(daemon_start, FAILED_START_RETRY)
    }

    /// A factory whose daemons come from `daemon_start` and whose failed
    /// starts are retried after `failed_start_retry`.
    pub(crate) fn with_daemon_start_and_retry(
        daemon_start: DaemonStart,
        failed_start_retry: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(FactoryInner {
                daemons: tokio::sync::Mutex::new(HashMap::new()),
                daemon_start,
                failed_start_retry,
                start_failure_warned: AtomicBool::new(false),
            }),
        }
    }

    /// The shared presence for `service_type`, if a space has started it.
    #[cfg(test)]
    pub(crate) fn shared_for(
        &self,
        service_type: &str,
    ) -> Option<Arc<SharedMdns>> {
        match self
            .inner
            .daemons
            .try_lock()
            .expect("unlocked")
            .get(service_type)
        {
            Some(DaemonSlot::Running(shared)) => Some(shared.clone()),
            _ => None,
        }
    }
}

impl FactoryInner {
    /// The shared mDNS presence for the space's service type, started on
    /// first use. The reconciliation interval is that of the space which
    /// starts the daemon; later spaces on the same service type share it.
    ///
    /// Starting the daemon binds sockets and spawns a thread, so it runs on
    /// the blocking pool.
    async fn shared(
        &self,
        cfg: &MdnsBootstrapConfig,
        tx: DynTransport,
    ) -> K2Result<Arc<SharedMdns>> {
        let mut daemons = self.daemons.lock().await;
        match daemons.get(&cfg.service_type) {
            Some(DaemonSlot::Running(shared)) => return Ok(shared.clone()),
            Some(DaemonSlot::Failed { at, err })
                if at.elapsed() < self.failed_start_retry =>
            {
                return Err(K2Error::other(format!(
                    "mdns daemon start failed {:?} ago, not retrying yet: {err}",
                    at.elapsed()
                )));
            }
            _ => {}
        }
        let start = self.daemon_start.clone();
        let service_type = cfg.service_type.clone();
        let started = tokio::task::spawn_blocking(move || start(&service_type))
            .await
            .map_err(|e| K2Error::other_src("mdns daemon start task", e))
            .and_then(|daemon| {
                SharedMdns::start(
                    daemon?,
                    tx,
                    Duration::from_millis(cfg.redial_interval_ms as u64),
                )
            });
        let slot = match &started {
            Ok(shared) => DaemonSlot::Running(shared.clone()),
            Err(err) => {
                self.report_start_failure(err);
                DaemonSlot::Failed {
                    at: Instant::now(),
                    err: err.to_string(),
                }
            }
        };
        daemons.insert(cfg.service_type.clone(), slot);
        started
    }

    /// Say that the daemon could not be started: loudly the first time
    /// this factory sees it, quietly afterwards.
    fn report_start_failure(&self, err: &K2Error) {
        if !self.start_failure_warned.swap(true, Ordering::Relaxed) {
            warn!(
                ?err,
                "mdns: the LAN discovery daemon could not start; spaces will retry as their agent infos are re-signed"
            );
        } else {
            debug!(?err, "mdns: the LAN discovery daemon still cannot start");
        }
    }
}

/// Start a real `mdns-sd` daemon.
fn start_mdns_sd_daemon(service_type: &str) -> K2Result<DynDaemon> {
    Ok(Arc::new(MdnsService::start(service_type)?))
}

impl BootstrapFactory for MdnsBootstrapFactory {
    fn default_config(&self, config: &mut Config) -> K2Result<()> {
        config.set_module_config(&MdnsBootstrapModConfig::default())
    }

    fn validate_config(&self, config: &Config) -> K2Result<()> {
        let cfg: MdnsBootstrapModConfig = config.get_module_config()?;
        let cfg = cfg.mdns_bootstrap;
        if cfg.max_concurrent_dials == 0 {
            return Err(K2Error::other(
                "mdnsBootstrap.maxConcurrentDials must be at least 1",
            ));
        }
        if cfg.redial_interval_ms == 0 {
            return Err(K2Error::other(
                "mdnsBootstrap.redialIntervalMs must be at least 1",
            ));
        }
        validate_service_type(&cfg.service_type).map_err(K2Error::other)
    }

    fn create(
        &self,
        builder: Arc<Builder>,
        _peer_store: DynPeerStore,
        space_id: SpaceId,
        tx: DynTransport,
    ) -> BoxFut<'static, K2Result<DynBootstrap>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let cfg: MdnsBootstrapModConfig =
                builder.config.get_module_config()?;
            let cfg = cfg.mdns_bootstrap;
            if !cfg.enabled {
                return Ok(noop());
            }
            // A fingerprint that cannot be derived is a misconfiguration
            // of the space, and the space should know. A daemon that
            // cannot start is a property of the host right now — no
            // network yet, no multicast on this interface — so the space
            // gets a bootstrap that keeps trying to join.
            let fp = SpaceFingerprint::derive(&builder, &space_id).await?;
            let boot = MdnsBootstrap {
                space: Arc::new(SpaceState {
                    factory: inner,
                    cfg,
                    space_id,
                    fp,
                    tx,
                    membership: Mutex::new(Membership::Detached),
                }),
            };
            boot.space.try_join().await;
            let out: DynBootstrap = Arc::new(boot);
            Ok(out)
        })
    }
}

/// What a space gets while mDNS discovery is switched off in config: a
/// bootstrap that accepts puts and does nothing, so the builder stack
/// stays uniform.
fn noop() -> DynBootstrap {
    #[derive(Debug)]
    struct DisabledMdnsBootstrap;

    impl Bootstrap for DisabledMdnsBootstrap {
        fn put(&self, _info: Arc<AgentInfoSigned>) {}
    }

    Arc::new(DisabledMdnsBootstrap)
}

/// One space's membership of the shared mDNS presence.
///
/// The space joins the daemon as soon as one is running. Until then it is
/// detached, and every put — local agent infos are re-signed on a timer,
/// so puts keep coming — is another chance to join: a host that had no
/// network when its spaces were created recovers LAN discovery when the
/// network arrives, without anyone recreating the spaces. Announcing waits
/// for the first local agent info that carries a URL, because the URL is
/// the whole payload: a peer that hears us needs something to dial.
/// Dropping the handle withdraws the space's record, stops routing records
/// to it and aborts its dials in flight.
#[derive(Debug)]
pub(crate) struct MdnsBootstrap {
    space: Arc<SpaceState>,
}

/// What a space's bootstrap and its join attempts share.
#[derive(Debug)]
struct SpaceState {
    factory: Arc<FactoryInner>,
    cfg: MdnsBootstrapConfig,
    space_id: SpaceId,
    fp: SpaceFingerprint,
    tx: DynTransport,
    membership: Mutex<Membership>,
}

/// Where a space stands with the shared daemon.
#[derive(Debug)]
enum Membership {
    /// No daemon to join yet; the next put tries again.
    Detached,
    /// A put is trying to join right now; later puts wait for its result.
    Joining,
    /// Registered on the daemon.
    Joined {
        shared: Arc<SharedMdns>,
        entry: Arc<SpaceEntry>,
    },
    /// The handle was dropped; a join still in flight must not land.
    Left,
}

impl Drop for MdnsBootstrap {
    fn drop(&mut self) {
        let membership = std::mem::replace(
            &mut *self.space.membership.lock().expect("poison"),
            Membership::Left,
        );
        if let Membership::Joined { shared, entry } = membership {
            shared.leave(&entry);
        }
    }
}

impl Bootstrap for MdnsBootstrap {
    fn put(&self, info: Arc<AgentInfoSigned>) {
        if info.space != self.space.space_id {
            tracing::error!(
                ?info,
                "mdns bootstrap received put for wrong space"
            );
            return;
        }
        // A tombstone has no URL and withdraws nothing: other local agents
        // may still be reachable at the URL we announce, and if none are,
        // the announcement dies with this instance.
        let Some(url) = info.url.clone().filter(|_| !info.is_tombstone) else {
            trace!("mdns: ignoring put without a url");
            return;
        };
        let mut membership = self.space.membership.lock().expect("poison");
        match &*membership {
            Membership::Joined { entry, .. } => advertise(entry, &url),
            Membership::Detached => {
                *membership = Membership::Joining;
                drop(membership);
                let space = self.space.clone();
                // `put` cannot await, and a start may block on the network.
                tokio::spawn(async move {
                    if space.try_join().await {
                        space.advertise(&url);
                    }
                });
            }
            Membership::Joining => {
                trace!(%url, "mdns: a join is in flight, the url will be announced once it lands");
            }
            Membership::Left => {}
        }
    }
}

/// Announce `url` on `entry`, dialling what the LAN announced so far when
/// this is the space's first URL.
fn advertise(entry: &Arc<SpaceEntry>, url: &Url) {
    match entry.advertise(url) {
        Ok(true) => {
            trace!(%url, "mdns: advertising peer url, dialling what the LAN announced so far");
            entry.reconcile_soon();
        }
        Ok(false) => trace!(%url, "mdns: advertising peer url"),
        Err(err) => warn!(?err, %url, "mdns: failed to advertise peer url"),
    }
}

impl SpaceState {
    /// Try to join the shared daemon, starting it if need be. Returns
    /// whether the space is registered on a daemon afterwards.
    async fn try_join(&self) -> bool {
        let result = self.factory.shared(&self.cfg, self.tx.clone()).await;
        let mut membership = self.membership.lock().expect("poison");
        match &*membership {
            Membership::Detached | Membership::Joining => {}
            Membership::Joined { .. } => return true,
            Membership::Left => return false,
        }
        let shared = match result {
            Ok(shared) => shared,
            Err(err) => {
                debug!(?err, space_id = ?self.space_id, "mdns: no daemon to join yet");
                *membership = Membership::Detached;
                return false;
            }
        };
        let entry = shared.join(
            self.space_id.clone(),
            self.fp.clone(),
            self.tx.clone(),
            self.cfg.max_concurrent_dials as usize,
        );
        debug!(
            space_id = ?self.space_id,
            fullname = %entry.fullname(),
            "mdns bootstrap joined the shared daemon"
        );
        *membership = Membership::Joined { shared, entry };
        true
    }

    /// Announce `url`, if the space is registered on a daemon.
    fn advertise(&self, url: &Url) {
        if let Membership::Joined { entry, .. } =
            &*self.membership.lock().expect("poison")
        {
            advertise(entry, url);
        }
    }
}

#[cfg(test)]
mod tests;
