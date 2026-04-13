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

#[cfg(test)]
mod test_utils;

#[cfg(test)]
mod tests;

use crate::peer_state::*;
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
        _builder: Arc<Builder>,
        peer_store: DynPeerStore,
        space_id: SpaceId,
    ) -> BoxFut<'static, K2Result<DynBootstrap>> {
        let node = self.node.clone();
        Box::pin(async move {
            let bootstrap =
                bootstrap::ReticulumBootstrap::new(node, peer_store, space_id);
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
    peer_states: Arc<RwLock<HashMap<Url, Arc<PeerState>>>>,
    config: ReticulumTransportConfig,
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

        // Spawn the global announce listener. This runs for the lifetime of
        // the transport, populating the identity cache and pushing matches
        // into the per-space peer-discovered queue for the bootstrap layer.
        let announce_rx = node.endpoint().recv_announces().await?;
        let announce_listener_handle = announce::spawn_announce_listener(
            announce_rx,
            node.identity_cache().clone(),
            node.space_name_hashes().clone(),
            node.peer_discovered_tx().clone(),
        );

        let out: DynTxImp = Arc::new(Self {
            node,
            handler,
            local_url: url_holder,
            peer_states: Arc::new(RwLock::new(HashMap::new())),
            config,
            space_tasks: Arc::new(Mutex::new(HashMap::new())),
            global_tasks: Mutex::new(vec![announce_listener_handle]),
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
        if let Some(state) =
            self.peer_states.write().expect("poisoned").remove(&peer)
        {
            state.teardown_all_links();
        }
        Box::pin(async {})
    }

    fn send(&self, remote_url: Url, data: Bytes) -> BoxFut<'_, K2Result<()>> {
        let node = self.node.clone();
        let handler = self.handler.clone();
        let peer_states = self.peer_states.clone();
        let config = self.config.clone();

        Box::pin(async move {
            // Extract space_id from the K2Proto payload for per-space link routing.
            let space_id_bytes = extract_space_id(&data)?;

            let space_id = match space_id_bytes {
                Some(id) => SpaceId::from(id),
                None => {
                    // Preflight messages have no space_id.
                    // Route over the first available per-space link.
                    return Err(K2Error::other(
                        "Cannot route message without space_id and no existing link",
                    ));
                }
            };

            // Resolve the remote identity from our announce cache.
            let identity_hash = url_to_identity_hash(&remote_url)?;

            let peer_identity =
                node.get_peer_identity(&identity_hash).ok_or_else(|| {
                    K2Error::other(format!(
                        "No known identity for peer {remote_url}"
                    ))
                })?;

            // Suppress unused warnings while send path is scaffolded.
            let _ = (peer_states, handler, config.max_frame_bytes);

            node.send_to_peer(
                &peer_identity,
                &space_id,
                &data,
                config.max_frame_bytes,
            )
            .await
        })
    }

    fn get_connected_peers(&self) -> BoxFut<'_, K2Result<Vec<Url>>> {
        Box::pin(async {
            Ok(self
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
        // We need `Arc<Self>` so our spawned task can call
        // `spawn_space_tasks`. The hook is called from
        // `DefaultTransport::register_space_handler`, which holds us as
        // `DynTxImp = Arc<dyn TxImp>` already; reaching back to a concrete
        // `Arc<Self>` from here isn't possible through the trait. Instead,
        // clone the fields we actually need and spawn a detached task.
        let node = self.node.clone();
        let space_tasks = self.space_tasks.clone();
        let interval = self.config.announce_interval_s;
        tokio::spawn(async move {
            match node.register_space(&space_id).await {
                Ok(dest) => {
                    let h =
                        announce::spawn_announce_publisher(dest, interval);
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
        node.unregister_space(&space_id);
    }
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
