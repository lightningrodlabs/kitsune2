//! [`BootstrapFactory`] backed by mDNS LAN discovery + a lightweight
//! peer-info-exchange protocol.

use crate::config::{MdnsBootstrapConfig, MdnsBootstrapModConfig};
use crate::discovery::{self, MdnsService};
use crate::session;
use kitsune2_api::*;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
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

    fn validate_config(&self, _config: &Config) -> K2Result<()> {
        Ok(())
    }

    fn create(
        &self,
        builder: Arc<Builder>,
        peer_store: DynPeerStore,
        space_id: SpaceId,
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
            let boot = MdnsBootstrap::start(
                cfg.mdns_bootstrap,
                builder,
                peer_store,
                space_id,
            )
            .await?;
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

/// Shared mutable cache of the local agent infos this node is willing to
/// serve to discovered peers. [`Bootstrap::put`] updates this; the session
/// accept task reads it.
type InfoCache = Arc<Mutex<Vec<Arc<AgentInfoSigned>>>>;

/// The live mDNS bootstrap for one space.
#[derive(Debug)]
pub struct MdnsBootstrap {
    _service: Arc<MdnsService>,
    infos: InfoCache,
    space_id: SpaceId,
    accept_task: JoinHandle<()>,
    browse_task: JoinHandle<()>,
}

impl Drop for MdnsBootstrap {
    fn drop(&mut self) {
        self.accept_task.abort();
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
        let mut guard = self.infos.lock().expect("infos lock");
        // Replace any existing entry for this agent; prune expired.
        let now = Timestamp::now();
        guard.retain(|i| i.agent != info.agent && i.expires_at > now);
        guard.push(info);
    }
}

impl MdnsBootstrap {
    async fn start(
        cfg: MdnsBootstrapConfig,
        builder: Arc<Builder>,
        peer_store: DynPeerStore,
        space_id: SpaceId,
    ) -> K2Result<Self> {
        let listener = TcpListener::bind("0.0.0.0:0").await.map_err(|e| {
            K2Error::other_src("mdns listener bind", e)
        })?;
        let port = listener
            .local_addr()
            .map_err(|e| K2Error::other_src("mdns listener addr", e))?
            .port();

        let addrs = discovery::local_addrs()?;
        let service = Arc::new(MdnsService::register(
            &cfg.service_type,
            &space_id,
            port,
            addrs,
        )?);

        let infos: InfoCache = Arc::new(Mutex::new(Vec::new()));

        let accept_task = tokio::spawn(accept_loop(
            listener,
            space_id.clone(),
            infos.clone(),
            peer_store.clone(),
            builder.verifier.clone(),
        ));

        let browse_rx = service.browse(&cfg.service_type)?;
        let browse_task = tokio::spawn(browse_loop(
            browse_rx,
            service.clone(),
            space_id.clone(),
            infos.clone(),
            peer_store.clone(),
            builder.verifier.clone(),
        ));

        debug!(?space_id, port, "mdns bootstrap started");

        Ok(Self {
            _service: service,
            infos,
            space_id,
            accept_task,
            browse_task,
        })
    }
}

async fn accept_loop(
    listener: TcpListener,
    space_id: SpaceId,
    infos: InfoCache,
    peer_store: DynPeerStore,
    verifier: DynVerifier,
) {
    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                trace!(%peer_addr, "mdns: accepted incoming session");
                let space_id = space_id.clone();
                let infos = infos.clone();
                let peer_store = peer_store.clone();
                let verifier = verifier.clone();
                tokio::spawn(async move {
                    run_session_and_insert(
                        stream, space_id, infos, peer_store, verifier,
                    )
                    .await;
                });
            }
            Err(err) => {
                warn!(?err, "mdns: accept error");
                tokio::time::sleep(std::time::Duration::from_millis(100))
                    .await;
            }
        }
    }
}

async fn browse_loop(
    rx: flume::Receiver<mdns_sd::ServiceEvent>,
    service: Arc<MdnsService>,
    space_id: SpaceId,
    infos: InfoCache,
    peer_store: DynPeerStore,
    verifier: DynVerifier,
) {
    let fp = crate::proto::space_fingerprint(&space_id);
    while let Ok(event) = rx.recv_async().await {
        let Some(peer) =
            discovery::resolved_to_peer(&event, &fp, service.fullname())
        else {
            continue;
        };
        trace!(addr = %peer.addr, fullname = %peer.fullname, "mdns: discovered peer");

        let space_id = space_id.clone();
        let infos = infos.clone();
        let peer_store = peer_store.clone();
        let verifier = verifier.clone();
        tokio::spawn(async move {
            match connect_with_timeout(peer.addr).await {
                Ok(stream) => {
                    run_session_and_insert(
                        stream, space_id, infos, peer_store, verifier,
                    )
                    .await;
                }
                Err(err) => {
                    debug!(?err, addr = %peer.addr, "mdns: dial failed");
                }
            }
        });
    }
}

async fn connect_with_timeout(addr: SocketAddr) -> K2Result<TcpStream> {
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        TcpStream::connect(addr),
    )
    .await
    .map_err(|_| K2Error::other("mdns dial timeout"))?
    .map_err(|e| K2Error::other_src("mdns dial", e))
}

async fn run_session_and_insert(
    stream: TcpStream,
    space_id: SpaceId,
    infos: InfoCache,
    peer_store: DynPeerStore,
    verifier: DynVerifier,
) {
    let local = infos.lock().expect("infos lock").clone();
    match session::run(stream, space_id.clone(), local, verifier).await {
        Ok(discovered) if !discovered.is_empty() => {
            debug!(
                n = discovered.len(),
                "mdns: inserting discovered agent infos"
            );
            if let Err(err) = peer_store.insert(discovered).await {
                warn!(?err, "mdns: peer_store insert failed");
            }
        }
        Ok(_) => {}
        Err(err) => {
            debug!(?err, "mdns: session failed");
        }
    }
}
