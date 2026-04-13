#![deny(missing_docs)]
// Many internal symbols are scaffolded for the upcoming real rns_transport
// backend wiring and are temporarily unused. Silence the warnings for this
// module while the backend is stubbed.
#![allow(dead_code)]

//! Kitsune2 transport implementation backed by Reticulum.
//!
//! This transport carries kitsune2 gossip, fetch, publish, and module traffic
//! over a [Reticulum](https://reticulum.network/) network using the
//! [`reticulum-rs-transport`](https://crates.io/crates/reticulum-rs-transport)
//! crate (module name `rns_transport`).
//!
//! # Architecture
//!
//! Each kitsune2 space gets its own Reticulum `Destination` (aspect
//! `kitsune2/<space_hash>`), announced independently on the network.
//! Peer discovery is driven by Reticulum announces rather than an HTTP
//! bootstrap server: the crate exposes both a `ReticulumTransportFactory`
//! and a `ReticulumBootstrapFactory` that share state through a
//! `ReticulumNode`.
//!
//! The URL scheme is `ret://reticulum:1/<identity-hash-hex>`, where the
//! host and port are constants (Reticulum routes by destination hash, not
//! by IP) and the path carries the peer's stable Identity hash.
//!
//! # Trait abstraction
//!
//! All I/O operations are behind traits in the [`destination`] module,
//! mirroring the Iroh transport's endpoint abstraction. This allows unit
//! tests to swap in fakes without a real Reticulum network.

mod backend;
mod config;
mod destination;
mod frame;
mod url;

mod announce;
mod bootstrap;
mod link;
mod node;
mod peer_state;
mod routers;

#[cfg(test)]
mod test_utils;

#[cfg(test)]
mod tests;

use crate::peer_state::*;
use crate::routers::RouterState;
use crate::url::*;

use bytes::Bytes;
use kitsune2_api::*;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};
use tokio::task::AbortHandle;
use tracing::{debug, info, warn};

pub use config::{
    ReticulumInterfaceConfig, ReticulumTransportConfig,
    ReticulumTransportModConfig,
};
pub use node::ReticulumNode;

/// Internals re-exported for this crate's `tests/` integration tests.
///
/// This module is **not** part of the stable public API. It exists so
/// the crate's own integration tests can wire the bootstrap pipeline
/// by hand — consumers should never depend on these symbols.
#[doc(hidden)]
pub mod internal_testing {
    use super::*;

    pub use crate::bootstrap::ReticulumBootstrap;

    /// Spawn the announce listener + bootstrap drain for a standalone
    /// `ReticulumNode` that isn't running under a full
    /// `ReticulumTransport`. Mirrors the wiring done inside
    /// `ReticulumTransport::create`. Returns handles the caller must
    /// retain until the test finishes.
    pub async fn wire_bootstrap_pipeline(
        node: Arc<ReticulumNode>,
    ) -> K2Result<Vec<tokio::task::AbortHandle>> {
        let ann_rx = node.endpoint().recv_announces().await?;
        let listener = crate::announce::spawn_announce_listener(
            ann_rx,
            node.identity_cache().clone(),
            node.space_name_hashes().clone(),
            node.peer_discovered_tx().clone(),
        );
        let drain_rx =
            node.take_peer_discovered_rx().await.ok_or_else(|| {
                K2Error::other("peer_discovered rx already taken")
            })?;
        let drain = crate::bootstrap::spawn_bootstrap_drain(
            drain_rx,
            node.clone(),
        );
        Ok(vec![listener, drain])
    }
}

/// Kitsune2 transport factory backed by Reticulum.
#[derive(Debug)]
pub struct ReticulumTransportFactory {
    node: Arc<ReticulumNode>,
}

impl ReticulumTransportFactory {
    /// Create a new factory instance sharing state with the given node.
    pub fn create(node: Arc<ReticulumNode>) -> DynTransportFactory {
        Arc::new(Self { node })
    }
}

impl TransportFactory for ReticulumTransportFactory {
    fn default_config(&self, config: &mut Config) -> K2Result<()> {
        config.set_module_config(&ReticulumTransportModConfig::default())
    }

    fn validate_config(&self, config: &Config) -> K2Result<()> {
        let c: ReticulumTransportModConfig = config.get_module_config()?;
        c.reticulum_transport.validate()
    }

    fn create(
        &self,
        builder: Arc<Builder>,
        handler: DynTxHandler,
    ) -> BoxFut<'static, K2Result<DynTransport>> {
        let node = self.node.clone();
        Box::pin(async move {
            let handler = TxImpHnd::new(handler);
            let config: ReticulumTransportModConfig =
                builder.config.get_module_config()?;

            let imp = ReticulumTransport::create(
                config.reticulum_transport,
                handler.clone(),
                node,
            )
            .await?;
            Ok(DefaultTransport::create(&handler, imp))
        })
    }
}

/// Kitsune2 bootstrap factory backed by Reticulum announces.
#[derive(Debug)]
pub struct ReticulumBootstrapFactory {
    node: Arc<ReticulumNode>,
}

impl ReticulumBootstrapFactory {
    /// Create a new factory instance sharing state with the given node.
    pub fn create(node: Arc<ReticulumNode>) -> DynBootstrapFactory {
        Arc::new(Self { node })
    }
}

impl BootstrapFactory for ReticulumBootstrapFactory {
    fn default_config(&self, _config: &mut Config) -> K2Result<()> {
        // No additional config beyond what ReticulumTransportFactory sets.
        Ok(())
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
        let node = self.node.clone();
        Box::pin(async move {
            let verifier = builder.verifier.clone();
            let bootstrap = bootstrap::ReticulumBootstrap::new(
                node, peer_store, verifier, space_id,
            );
            Ok(Arc::new(bootstrap) as DynBootstrap)
        })
    }
}

/// Reticulum-based transport implementation.
#[derive(Debug)]
struct ReticulumTransport {
    node: Arc<ReticulumNode>,
    handler: Arc<TxImpHnd>,
    local_url: Arc<RwLock<Option<Url>>>,
    config: ReticulumTransportConfig,
    /// Routers' shared state: dest-hash→space map, peer-state map,
    /// link registry.
    router_state: RouterState,
    /// Per-space task abort handles, cleaned up on drop.
    space_tasks: Arc<Mutex<HashMap<SpaceId, Vec<AbortHandle>>>>,
    /// Global (transport-scoped) task abort handles.
    global_tasks: Mutex<Vec<AbortHandle>>,
}

impl Drop for ReticulumTransport {
    fn drop(&mut self) {
        info!(local_url = ?self.local_url, "Dropping Reticulum transport");
        if let Ok(tasks) = self.space_tasks.lock() {
            for (_, handles) in tasks.iter() {
                for h in handles {
                    h.abort();
                }
            }
        }
        if let Ok(tasks) = self.global_tasks.lock() {
            for h in tasks.iter() {
                h.abort();
            }
        }
    }
}

impl ReticulumTransport {
    async fn create(
        config: ReticulumTransportConfig,
        handler: Arc<TxImpHnd>,
        node: Arc<ReticulumNode>,
    ) -> K2Result<DynTxImp> {
        let identity_hash = node.local_identity_hash();
        let local_url = identity_hash_to_url(&identity_hash)?;

        let url_holder = Arc::new(RwLock::new(Some(local_url.clone())));

        // Emit our listening address immediately -- it is deterministic.
        handler.new_listening_address(local_url).await;

        // Build the shared RouterState that both routers and TxImp::send
        // consult.
        let router_state = RouterState::new(config.max_frame_bytes);

        // Spawn the global announce listener (identity cache + bootstrap
        // candidate queue), the inbound-link router, and the data router.
        let announce_rx = node.endpoint().recv_announces().await?;
        let announce_listener_handle = announce::spawn_announce_listener(
            announce_rx,
            node.identity_cache().clone(),
            node.space_name_hashes().clone(),
            node.peer_discovered_tx().clone(),
        );

        let links_rx = node.endpoint().recv_links().await?;
        let links_router_handle = routers::spawn_links_router(
            links_rx,
            router_state.clone(),
            handler.clone(),
            node.endpoint().clone(),
        );

        let data_rx = node.endpoint().recv_resource_data().await?;
        let data_router_handle = routers::spawn_data_router(
            data_rx,
            router_state.clone(),
            handler.clone(),
        );

        let close_rx = node.endpoint().recv_link_closures().await?;
        let close_router_handle = routers::spawn_close_router(
            close_rx,
            router_state.clone(),
            handler.clone(),
        );

        // Spawn the bootstrap drain that decodes incoming announce
        // app_data into AgentInfoSigned records and inserts them into
        // each space's peer store (via the PeerBinding registered by
        // its ReticulumBootstrap instance).
        let drain_rx =
            node.take_peer_discovered_rx().await.ok_or_else(|| {
                K2Error::other(
                    "peer_discovered receiver already taken",
                )
            })?;
        let bootstrap_drain_handle =
            bootstrap::spawn_bootstrap_drain(drain_rx, node.clone());

        let out: DynTxImp = Arc::new(Self {
            node,
            handler,
            local_url: url_holder,
            config,
            router_state,
            space_tasks: Arc::new(Mutex::new(HashMap::new())),
            global_tasks: Mutex::new(vec![
                announce_listener_handle,
                links_router_handle,
                data_router_handle,
                close_router_handle,
                bootstrap_drain_handle,
            ]),
        });
        Ok(out)
    }
}

impl TxImp for ReticulumTransport {
    fn url(&self) -> Option<Url> {
        self.local_url.read().expect("poisoned").clone()
    }

    fn disconnect(
        &self,
        peer: Url,
        _payload: Option<(String, Bytes)>,
    ) -> BoxFut<'_, ()> {
        if let Some(state) = self
            .router_state
            .peer_states
            .write()
            .expect("poisoned")
            .remove(&peer)
        {
            state.teardown_all_links();
        }
        // Drop any link registry entries pointing at this peer.
        self.router_state
            .link_registry
            .write()
            .expect("poisoned")
            .retain(|_, (url, _)| url != &peer);
        Box::pin(async {})
    }

    fn send(&self, remote_url: Url, data: Bytes) -> BoxFut<'_, K2Result<()>> {
        let node = self.node.clone();
        let handler = self.handler.clone();
        let router_state = self.router_state.clone();
        let config = self.config.clone();

        Box::pin(async move {
            // Extract space_id from the encoded K2Proto so we know which
            // per-space link to route over.
            let space_id = match extract_space_id(&data)? {
                Some(id) => SpaceId::from(id),
                None => {
                    // Preflight messages carry space_id=None. They are
                    // emitted by TxImpHnd::peer_connect as a response to
                    // an inbound peer_connect trigger -- but here in
                    // `send`, kitsune2's high-level callers only emit
                    // Notify/Module frames, which always carry space_id.
                    // If we hit this branch it's a bug in the caller.
                    return Err(K2Error::other(
                        "TxImp::send called with no space_id (bug in caller)",
                    ));
                }
            };

            // Resolve the remote identity from our announce cache.
            let identity_hash = url_to_identity_hash(&remote_url)?;
            let peer_identity = match node.get_peer_identity(&identity_hash) {
                Some(id) => id,
                None => {
                    // Peer not yet discovered via announce. Mark unresponsive
                    // so kitsune2 doesn't keep retrying on the same URL.
                    let _ = handler
                        .set_unresponsive(remote_url.clone(), Timestamp::now())
                        .await;
                    return Err(K2Error::other(format!(
                        "No known identity for peer {remote_url}"
                    )));
                }
            };

            // Get (or create) an outbound link for this (peer, space).
            let peer_state = {
                let mut states =
                    router_state.peer_states.write().expect("poisoned");
                states
                    .entry(remote_url.clone())
                    .or_insert_with(PeerState::new)
                    .clone()
            };

            let link = match peer_state.get_link(&space_id) {
                Some(l) => l,
                None => {
                    // Open a new link to this peer's per-space destination.
                    let space_hash = hex_encode_space(&space_id);
                    let link = node
                        .endpoint()
                        .link_to(
                            peer_identity,
                            "kitsune2".to_string(),
                            space_hash,
                        )
                        .await?;

                    let first_link = peer_state
                        .insert_link(space_id.clone(), link.clone());
                    router_state
                        .link_registry
                        .write()
                        .expect("poisoned")
                        .insert(
                            link.id(),
                            (remote_url.clone(), space_id.clone()),
                        );

                    if first_link {
                        routers::start_preflight(
                            &remote_url,
                            &link,
                            &peer_state,
                            &handler,
                            node.endpoint(),
                            config.max_frame_bytes,
                        )
                        .await?;
                    }
                    link
                }
            };

            // Encode as a Data frame and send. The data router on the
            // remote side will gate on preflight readiness; locally we
            // fire-and-forget -- any queued data before preflight
            // completes will be sent now and the remote may drop it.
            // This matches the plan's §3 shape: kitsune2's send layer
            // doesn't retry, the sender is expected to know the link
            // is ready before calling.
            let frame = frame::ReticulumFrame::Data(data);
            let encoded =
                frame::encode_frame(&frame, config.max_frame_bytes)?;
            routers::send_over_link(
                &link,
                &encoded,
                node.endpoint(),
                config.max_frame_bytes,
            )
            .await
        })
    }

    fn get_connected_peers(&self) -> BoxFut<'_, K2Result<Vec<Url>>> {
        Box::pin(async {
            Ok(self
                .router_state
                .peer_states
                .read()
                .expect("poisoned")
                .keys()
                .cloned()
                .collect())
        })
    }

    fn dump_network_stats(&self) -> BoxFut<'_, K2Result<TransportStats>> {
        Box::pin(async move {
            let mut peer_urls = Vec::new();
            if let Some(url) = self.local_url.read().expect("poisoned").clone()
            {
                peer_urls.push(url);
            }
            Ok(TransportStats {
                backend: "reticulum".to_string(),
                peer_urls,
                connections: Vec::new(),
            })
        })
    }

    fn register_space(&self, space_id: SpaceId) {
        // The hook is synchronous, but destination creation is async.
        // Clone fields and spawn a detached task that creates the
        // destination, registers its address hash with the router
        // state (so inbound links for this space can be matched), and
        // starts the per-space announce publisher.
        let node = self.node.clone();
        let space_tasks = self.space_tasks.clone();
        let router_state = self.router_state.clone();
        let interval = self.config.announce_interval_s;
        tokio::spawn(async move {
            match node.register_space(&space_id).await {
                Ok(dest) => {
                    router_state
                        .register_dest(dest.address_hash(), space_id.clone());
                    // Publisher pulls the current app_data for the
                    // space from the node on each tick; empty until
                    // ReticulumBootstrap::put stages an AgentInfoSigned.
                    let pub_node = node.clone();
                    let pub_space = space_id.clone();
                    let h = announce::spawn_announce_publisher(
                        dest,
                        interval,
                        move || pub_node.get_my_agent_info(&pub_space),
                    );
                    space_tasks
                        .lock()
                        .expect("poisoned")
                        .entry(space_id.clone())
                        .or_default()
                        .push(h);
                    debug!(
                        ?space_id,
                        "Started Reticulum per-space tasks"
                    );
                }
                Err(e) => {
                    warn!(
                        ?e,
                        ?space_id,
                        "Failed to register Reticulum destination for space"
                    );
                }
            }
        });
    }

    fn unregister_space(&self, space_id: SpaceId) {
        let node = self.node.clone();
        // Abort any per-space tasks we spawned.
        if let Some(tasks) = self
            .space_tasks
            .lock()
            .expect("poisoned")
            .remove(&space_id)
        {
            for h in tasks {
                h.abort();
            }
        }
        self.router_state.unregister_space(&space_id);
        node.unregister_space(&space_id);
    }
}

/// Hex-encode a SpaceId for use as a Reticulum aspect string
/// (matches the encoding used in `ReticulumNode::register_space`).
fn hex_encode_space(space_id: &SpaceId) -> String {
    let bytes: &[u8] = space_id.as_ref();
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Extract the `space_id` field from an encoded `K2Proto` message
/// using a partial protobuf decode.
///
/// Field 3 (space_id) is `optional bytes`, wire type 2 (length-delimited),
/// so its tag byte is `(3 << 3) | 2 = 0x1a`.
fn extract_space_id(data: &[u8]) -> K2Result<Option<Bytes>> {
    let proto = kitsune2_api::K2Proto::decode(data)?;
    Ok(proto.space_id)
}
