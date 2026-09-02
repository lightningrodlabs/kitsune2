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
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, trace, warn};

/// How the factory starts its mDNS daemon for a service type. Injectable
/// so that the factory can be exercised without multicast.
pub(crate) type DaemonStart =
    Arc<dyn Fn(&str) -> K2Result<DynDaemon> + Send + Sync + 'static>;

/// How long a daemon that failed to start is left alone before a space
/// creation tries again. Long enough that fifty spaces starting at once
/// cost one attempt, short enough that a node which gains a network later
/// recovers.
pub const FAILED_START_RETRY: Duration = Duration::from_secs(60);

/// The [`BootstrapFactory`] that produces [`MdnsBootstrap`] instances.
///
/// One factory owns one mDNS daemon per service type, started the first
/// time an enabled space asks for that type and kept for the factory's
/// life; every space on the same type shares that daemon, its single
/// browse and its reconciliation ticker.
pub struct MdnsBootstrapFactory {
    inner: Arc<FactoryInner>,
}

/// The state a factory's `create` futures share with it: they outlive the
/// borrow of the factory, so it lives behind an `Arc`.
struct FactoryInner {
    /// One slot per service type, held across a start so that spaces
    /// created together do not race to start the same daemon.
    daemons: tokio::sync::Mutex<HashMap<String, DaemonSlot>>,
    daemon_start: DaemonStart,
}

/// What a service type's daemon is doing.
enum DaemonSlot {
    Running(Arc<SharedMdns>),
    /// The last start failed at `at`; retried after [`FAILED_START_RETRY`].
    Failed {
        at: Instant,
        err: String,
    },
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
        Self {
            inner: Arc::new(FactoryInner {
                daemons: tokio::sync::Mutex::new(HashMap::new()),
                daemon_start,
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
                if at.elapsed() < FAILED_START_RETRY =>
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
            Err(err) => DaemonSlot::Failed {
                at: Instant::now(),
                err: err.to_string(),
            },
        };
        daemons.insert(cfg.service_type.clone(), slot);
        started
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
                // Disabled: produce a no-op bootstrap so the builder stack
                // stays uniform.
                let out: DynBootstrap = Arc::new(DisabledMdnsBootstrap);
                return Ok(out);
            }
            // The fingerprint comes first so that the space is ready to
            // join the moment the daemon is up and replays what the LAN
            // already knows. A daemon that cannot start is this factory's
            // failure to report; whether the space may run without LAN
            // discovery is decided by whoever assembles the bootstrap
            // stack.
            let fp = SpaceFingerprint::derive(&builder, &space_id).await?;
            let shared = inner.shared(&cfg, tx.clone()).await?;
            let boot = MdnsBootstrap::join(shared, &cfg, space_id, fp, tx);
            let out: DynBootstrap = Arc::new(boot);
            Ok(out)
        })
    }
}

/// What a space gets while mDNS discovery is switched off in config.
#[derive(Debug)]
struct DisabledMdnsBootstrap;

impl Bootstrap for DisabledMdnsBootstrap {
    fn put(&self, _info: Arc<AgentInfoSigned>) {}
}

/// One space's membership of the shared mDNS presence.
///
/// Browsing is already running when the space joins. Announcing waits for
/// the first local agent info that carries a URL, because the URL is the
/// whole payload: a peer that hears us needs something to dial. Dropping
/// the handle withdraws the space's record, stops routing records to it
/// and aborts its dials in flight.
#[derive(Debug)]
pub struct MdnsBootstrap {
    shared: Arc<SharedMdns>,
    entry: Arc<SpaceEntry>,
}

impl Drop for MdnsBootstrap {
    fn drop(&mut self) {
        self.shared.leave(&self.entry);
    }
}

impl Bootstrap for MdnsBootstrap {
    fn put(&self, info: Arc<AgentInfoSigned>) {
        if &info.space != self.entry.space_id() {
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
        match self.entry.advertise(&url) {
            Ok(true) => {
                trace!(%url, "mdns: advertising peer url, dialling what the LAN announced so far");
                self.entry.reconcile_soon();
            }
            Ok(false) => trace!(%url, "mdns: advertising peer url"),
            Err(err) => warn!(?err, %url, "mdns: failed to advertise peer url"),
        }
    }
}

impl MdnsBootstrap {
    fn join(
        shared: Arc<SharedMdns>,
        cfg: &MdnsBootstrapConfig,
        space_id: SpaceId,
        fp: SpaceFingerprint,
        tx: DynTransport,
    ) -> Self {
        let entry = shared.join(
            space_id.clone(),
            fp,
            tx,
            cfg.max_concurrent_dials as usize,
        );
        debug!(
            ?space_id,
            fullname = %entry.fullname(),
            "mdns bootstrap joined the shared daemon"
        );
        Self { shared, entry }
    }
}

#[cfg(test)]
mod tests;
