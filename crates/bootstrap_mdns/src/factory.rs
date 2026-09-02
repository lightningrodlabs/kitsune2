//! [`BootstrapFactory`] backed by mDNS LAN discovery.

use crate::config::{
    MdnsBootstrapConfig, MdnsBootstrapModConfig, validate_service_type,
};
use crate::discovery::{DynDaemon, MdnsService};
use crate::fingerprint::SpaceFingerprint;
use crate::shared::SharedMdns;
use crate::space::SpaceEntry;
use kitsune2_api::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, trace, warn};

/// How the factory starts its mDNS daemon for a service type. Injectable
/// so that the factory can be exercised without multicast.
pub type DaemonStart =
    Arc<dyn Fn(&str) -> K2Result<DynDaemon> + Send + Sync + 'static>;

/// The [`BootstrapFactory`] that produces [`MdnsBootstrap`] instances.
///
/// One factory owns one mDNS daemon, started the first time an enabled
/// space is created and kept for the factory's life; every space created
/// through it shares that daemon and its single browse.
pub struct MdnsBootstrapFactory {
    inner: Arc<FactoryInner>,
}

/// The state a factory's `create` futures share with it: they outlive the
/// borrow of the factory, so it lives behind an `Arc`.
struct FactoryInner {
    shared: tokio::sync::OnceCell<Arc<SharedMdns>>,
    daemon_start: DaemonStart,
}

impl std::fmt::Debug for MdnsBootstrapFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MdnsBootstrapFactory")
            .field("shared", &self.inner.shared.get())
            .finish()
    }
}

impl MdnsBootstrapFactory {
    /// Construct a new factory.
    pub fn create() -> DynBootstrapFactory {
        Arc::new(Self::with_daemon_start(Arc::new(start_mdns_sd_daemon)))
    }

    /// A factory whose daemon comes from `daemon_start`.
    pub(crate) fn with_daemon_start(daemon_start: DaemonStart) -> Self {
        Self {
            inner: Arc::new(FactoryInner {
                shared: tokio::sync::OnceCell::new(),
                daemon_start,
            }),
        }
    }

    /// The shared presence, if a space has started it.
    #[cfg(test)]
    pub(crate) fn shared_if_started(&self) -> Option<Arc<SharedMdns>> {
        self.inner.shared.get().cloned()
    }
}

impl FactoryInner {
    /// The process-wide mDNS presence, started on first use.
    ///
    /// Starting the daemon binds sockets and spawns a thread, so it runs on
    /// the blocking pool.
    async fn shared(&self, service_type: &str) -> K2Result<Arc<SharedMdns>> {
        self.shared
            .get_or_try_init(|| async {
                let start = self.daemon_start.clone();
                let service_type = service_type.to_string();
                let daemon =
                    tokio::task::spawn_blocking(move || start(&service_type))
                        .await
                        .map_err(|e| {
                            K2Error::other_src("mdns daemon start task", e)
                        })??;
                SharedMdns::start(daemon)
            })
            .await
            .cloned()
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
            let shared = inner.shared(&cfg.service_type).await?;
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
    redial_task: JoinHandle<()>,
}

impl Drop for MdnsBootstrap {
    fn drop(&mut self) {
        self.redial_task.abort();
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
        let redial_task = tokio::spawn(redial_loop(
            entry.clone(),
            Duration::from_millis(cfg.redial_interval_ms as u64),
        ));
        debug!(
            ?space_id,
            fullname = %entry.fullname(),
            "mdns bootstrap joined the shared daemon"
        );
        Self {
            shared,
            entry,
            redial_task,
        }
    }
}

/// Every `interval`, dial the announced peers the transport is not
/// connected to.
async fn redial_loop(entry: Arc<SpaceEntry>, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first tick completes immediately; announcements dial themselves
    // on arrival, so the first round of reconciliation waits an interval.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        entry.reconcile_from_transport().await;
    }
}

#[cfg(test)]
mod tests;
