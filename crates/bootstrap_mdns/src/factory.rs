//! [`BootstrapFactory`] backed by mDNS LAN discovery.

use crate::browse::{LocalIdentity, browse_loop};
use crate::config::{MdnsBootstrapConfig, MdnsBootstrapModConfig};
use crate::dial_policy::DialPolicy;
use crate::discovery::{self, MdnsService};
use crate::fingerprint;
use kitsune2_api::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, trace, warn};

/// The [`BootstrapFactory`] that produces [`MdnsBootstrap`] instances.
#[derive(Debug)]
pub struct MdnsBootstrapFactory;

impl MdnsBootstrapFactory {
    /// Construct a new factory.
    pub fn create() -> DynBootstrapFactory {
        Arc::new(Self)
    }
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
        if !cfg.service_type.ends_with("._udp.local.")
            && !cfg.service_type.ends_with("._tcp.local.")
        {
            return Err(K2Error::other(
                "mdnsBootstrap.serviceType must be of the form _name._udp.local.",
            ));
        }
        Ok(())
    }

    fn create(
        &self,
        builder: Arc<Builder>,
        _peer_store: DynPeerStore,
        space_id: SpaceId,
        tx: DynTransport,
    ) -> BoxFut<'static, K2Result<DynBootstrap>> {
        Box::pin(async move {
            let cfg: MdnsBootstrapModConfig =
                builder.config.get_module_config()?;
            if !cfg.mdns_bootstrap.enabled {
                // Disabled: produce a no-op bootstrap so the builder stack
                // stays uniform.
                let out: DynBootstrap = Arc::new(NoopMdnsBootstrap);
                return Ok(out);
            }
            let boot = MdnsBootstrap::start(cfg.mdns_bootstrap, space_id, tx)?;
            let out: DynBootstrap = Arc::new(boot);
            Ok(out)
        })
    }
}

#[derive(Debug)]
struct NoopMdnsBootstrap;

impl Bootstrap for NoopMdnsBootstrap {
    fn put(&self, _info: Arc<AgentInfoSigned>) {}
}

/// The live mDNS discovery for one space.
///
/// Browsing starts immediately. Announcing waits for the first local agent
/// info that carries a URL, because the URL is the whole payload: a peer
/// that hears us needs something to dial.
#[derive(Debug)]
pub struct MdnsBootstrap {
    service: MdnsService,
    identity: Arc<LocalIdentity>,
    space_id: SpaceId,
    browse_task: JoinHandle<()>,
}

impl Drop for MdnsBootstrap {
    fn drop(&mut self) {
        self.browse_task.abort();
    }
}

impl Bootstrap for MdnsBootstrap {
    fn put(&self, info: Arc<AgentInfoSigned>) {
        if info.space != self.space_id {
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
        self.identity.set_url(url.clone());
        match self.service.advertise(&url) {
            Ok(()) => trace!(%url, "mdns: advertising peer url"),
            Err(err) => warn!(?err, %url, "mdns: failed to advertise peer url"),
        }
    }
}

impl MdnsBootstrap {
    fn start(
        cfg: MdnsBootstrapConfig,
        space_id: SpaceId,
        tx: DynTransport,
    ) -> K2Result<Self> {
        let addrs = discovery::local_addrs()?;
        let service = MdnsService::start(&cfg.service_type, &space_id, addrs)?;
        let identity =
            Arc::new(LocalIdentity::new(service.fullname().to_string()));
        let policy = Arc::new(DialPolicy::new(
            Duration::from_millis(cfg.dial_cooldown_ms as u64),
            cfg.max_concurrent_dials as usize,
        ));

        let browse_rx = service.browse()?;
        let browse_task = tokio::spawn(browse_loop(
            browse_rx,
            fingerprint::space_fingerprint(&space_id),
            identity.clone(),
            tx,
            policy,
        ));

        debug!(
            ?space_id,
            fullname = service.fullname(),
            "mdns bootstrap started"
        );

        Ok(Self {
            service,
            identity,
            space_id,
            browse_task,
        })
    }
}
