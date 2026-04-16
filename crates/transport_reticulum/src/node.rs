//! Shared state for the Reticulum transport and bootstrap factories.
//!
//! `ReticulumNode` owns the `rns_transport::Transport` instance, the
//! identity cache, per-space destination map, and announce queues.
//! Both `ReticulumTransportFactory` and `ReticulumBootstrapFactory`
//! hold an `Arc<ReticulumNode>`.

use crate::announce::{self, IdentityCache};
use crate::backend::RealEndpoint;
use crate::config::{ReticulumInterfaceConfig, ReticulumTransportConfig};
use crate::destination::{DynDestination, DynEndpoint};
use bytes::Bytes;
use kitsune2_api::{DynPeerStore, DynVerifier, K2Error, K2Result, SpaceId};
use rand_core::OsRng;
use rns_transport::destination::DestinationName;
use rns_transport::hash::AddressHash;
use rns_transport::identity::{Identity, PrivateIdentity};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tracing::{debug, info};

/// Discovery event pushed from the announce listener to the bootstrap
/// drain task: the space the announce is for, the announcer's
/// Identity, and whatever app_data was attached (expected to be a
/// canonical-JSON `AgentInfoSigned` under the current protocol).
pub(crate) type PeerDiscovery = (SpaceId, Identity, Bytes);

/// Per-space binding registered by a `ReticulumBootstrap` instance:
/// where to `insert` discovered peers, and which `Verifier` to use
/// when decoding the app_data.
#[derive(Clone)]
pub(crate) struct PeerBinding {
    pub peer_store: DynPeerStore,
    pub verifier: DynVerifier,
}

/// Shared state between the transport and bootstrap factories.
pub struct ReticulumNode {
    /// The abstracted Reticulum transport endpoint.
    endpoint: DynEndpoint,
    /// Our local private identity's address hash.
    local_identity_hash: AddressHash,
    /// Cache of peer identities learned from announces.
    identity_cache: IdentityCache,
    /// Map of space ID -> per-space destination.
    space_destinations: RwLock<HashMap<SpaceId, DynDestination>>,
    /// Map of name_hash -> space ID, for announce filtering.
    space_name_hashes: Arc<RwLock<HashMap<[u8; 10], Bytes>>>,
    /// Channel for notifying the bootstrap layer about discovered peers.
    peer_discovered_tx: tokio::sync::mpsc::Sender<PeerDiscovery>,
    /// Receiver side, consumed by the bootstrap drain task.
    peer_discovered_rx:
        tokio::sync::Mutex<Option<tokio::sync::mpsc::Receiver<PeerDiscovery>>>,
    /// Per-space local agent info to include in outbound announces
    /// (canonical-JSON-encoded `AgentInfoSigned` bytes).
    my_agent_infos: RwLock<HashMap<SpaceId, Bytes>>,
    /// Per-space binding registered by each `ReticulumBootstrap`
    /// instance — where discovered peers get inserted, and the
    /// `Verifier` used to decode announce app_data.
    peer_space_bindings: RwLock<HashMap<SpaceId, PeerBinding>>,
}

impl std::fmt::Debug for ReticulumNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReticulumNode")
            .field("local_identity_hash", &self.local_identity_hash)
            .field(
                "num_spaces",
                &self.space_destinations.read().map(|d| d.len()).unwrap_or(0),
            )
            .finish()
    }
}

impl ReticulumNode {
    /// Create a new ReticulumNode with the given endpoint and local identity hash.
    pub(crate) fn new(
        endpoint: DynEndpoint,
        local_identity_hash: AddressHash,
    ) -> Arc<Self> {
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        Arc::new(Self {
            endpoint,
            local_identity_hash,
            identity_cache: announce::new_identity_cache(),
            space_destinations: RwLock::new(HashMap::new()),
            space_name_hashes: Arc::new(RwLock::new(HashMap::new())),
            peer_discovered_tx: tx,
            peer_discovered_rx: tokio::sync::Mutex::new(Some(rx)),
            my_agent_infos: RwLock::new(HashMap::new()),
            peer_space_bindings: RwLock::new(HashMap::new()),
        })
    }

    /// Construct a `ReticulumNode` from a [`ReticulumTransportConfig`].
    ///
    /// This is the normal entry point for applications embedding the
    /// Reticulum transport (e.g. Holochain). It mirrors the iroh
    /// transport's factory flow: identity, runtime, interfaces, and
    /// announce plumbing are all brought up before the Node is handed
    /// to [`crate::ReticulumTransportFactory`] and
    /// [`crate::ReticulumBootstrapFactory`].
    ///
    /// Steps performed:
    /// 1. Validate the config.
    /// 2. Load an existing `PrivateIdentity` from `config.identity_path`,
    ///    or generate a fresh one when the file is absent / no path was
    ///    provided. When a path is supplied and the file does not exist,
    ///    a new identity is written to that path so subsequent runs
    ///    reuse it.
    /// 3. Build and tune a `TransportConfig` (link idle timeout, etc.).
    /// 4. Instantiate `rns_transport::Transport`.
    /// 5. Spawn every configured interface on the Transport's
    ///    `InterfaceManager`.
    /// 6. Wrap the live Transport in a `RealEndpoint` and return a
    ///    shared `ReticulumNode`.
    pub async fn from_config(
        config: ReticulumTransportConfig,
    ) -> K2Result<Arc<Self>> {
        config.validate()?;

        let identity = load_or_generate_identity(&config).await?;
        let identity_hash = identity.as_identity().address_hash;

        // Tune the rns transport with config values we care about.
        //
        // `broadcast: true` is load-bearing. rns's internal
        // `path_table` only populates routes for Link IDs once an
        // announce has been observed for that destination; link
        // establishment alone does not add a route. With
        // `broadcast: false`, `Transport::send_packet_with_outcome`
        // hits `DroppedNoRoute` (surfaced to callers as
        // `RnsError::ConnectionError`) whenever it's asked to send a
        // Data packet to a Link ID that hasn't been advertised by
        // announce yet — which is the normal case for resource-manager
        // traffic like our preflight frames on a freshly-Active link.
        //
        // Setting `broadcast: true` makes the fallback branch send the
        // packet on all interfaces, which for a point-to-point TCP
        // interface just means "deliver to the one peer on the other
        // end." It matches the pattern the in-process loopback
        // integration tests use (see `two_node_data.rs`) and is what
        // unblocks real-TCP deployments. See
        // `tests/two_node_tcp_preflight.rs` for the regression target.
        let mut transport_config =
            rns_transport::transport::TransportConfig::new(
                format!("kitsune2-{}", identity_hash.to_hex_string()),
                &identity,
                true,
            );
        transport_config
            .set_link_idle_timeout_secs(config.link_idle_timeout_s as u64);
        transport_config
            .set_link_proof_timeout_secs(config.connect_timeout_s as u64);

        let transport =
            rns_transport::transport::Transport::new(transport_config);
        let transport = Arc::new(tokio::sync::Mutex::new(transport));

        // Bring up each configured interface.
        start_interfaces(&transport, &config.interfaces).await?;

        let endpoint: DynEndpoint =
            Arc::new(RealEndpoint::new(transport, identity).await);

        info!(
            ?identity_hash,
            num_interfaces = config.interfaces.len(),
            "ReticulumNode ready"
        );

        Ok(Self::new(endpoint, identity_hash))
    }

    /// Build a `ReticulumNode` around a caller-owned
    /// `rns_transport::Transport`.
    ///
    /// This bypasses the interface startup done by
    /// [`Self::from_config`] and is useful when the rns `Transport`
    /// is managed externally — either to share it with other
    /// rns-stack tooling, or to wire a test harness (see the
    /// `tests/` integration tests).
    pub async fn from_rns_transport(
        transport: Arc<tokio::sync::Mutex<rns_transport::transport::Transport>>,
        identity: PrivateIdentity,
    ) -> K2Result<Arc<Self>> {
        let identity_hash = identity.as_identity().address_hash;
        let endpoint: DynEndpoint = Arc::new(
            crate::backend::RealEndpoint::new(transport, identity).await,
        );
        info!(
            ?identity_hash,
            "ReticulumNode built from caller-owned rns_transport"
        );
        Ok(Self::new(endpoint, identity_hash))
    }

    /// Get our local identity address hash.
    pub fn local_identity_hash(&self) -> AddressHash {
        self.local_identity_hash
    }

    /// Look up a peer's full Identity from the cache.
    pub(crate) fn get_peer_identity(
        &self,
        hash: &AddressHash,
    ) -> Option<Identity> {
        self.identity_cache
            .read()
            .expect("poisoned")
            .get(hash)
            .copied()
    }

    /// Get the identity cache (shared reference).
    #[doc(hidden)]
    pub fn identity_cache(&self) -> &IdentityCache {
        &self.identity_cache
    }

    /// Get the space name hashes map (for announce filtering).
    #[doc(hidden)]
    pub fn space_name_hashes(&self) -> &Arc<RwLock<HashMap<[u8; 10], Bytes>>> {
        &self.space_name_hashes
    }

    /// Get a sender for peer discovery notifications.
    #[doc(hidden)]
    pub fn peer_discovered_tx(
        &self,
    ) -> &tokio::sync::mpsc::Sender<PeerDiscovery> {
        &self.peer_discovered_tx
    }

    /// Take the peer discovery receiver (can only be called once).
    /// Consumed by the transport's bootstrap drain task.
    #[doc(hidden)]
    pub async fn take_peer_discovered_rx(
        &self,
    ) -> Option<tokio::sync::mpsc::Receiver<PeerDiscovery>> {
        self.peer_discovered_rx.lock().await.take()
    }

    /// Set the `AgentInfoSigned` bytes that the per-space announce
    /// publisher should include as `app_data`.
    ///
    /// Called by `ReticulumBootstrap::put`.
    pub(crate) fn set_my_agent_info(&self, space_id: SpaceId, bytes: Bytes) {
        self.my_agent_infos
            .write()
            .expect("poisoned")
            .insert(space_id, bytes);
    }

    /// Get the current `AgentInfoSigned` bytes to include as `app_data`
    /// in announces for the given space, if one has been set.
    #[doc(hidden)]
    pub fn get_my_agent_info(&self, space_id: &SpaceId) -> Option<Bytes> {
        self.my_agent_infos
            .read()
            .expect("poisoned")
            .get(space_id)
            .cloned()
    }

    /// Register a `ReticulumBootstrap` instance's binding for a space.
    /// Called when the bootstrap factory creates a new instance.
    pub(crate) fn bind_space(&self, space_id: SpaceId, binding: PeerBinding) {
        self.peer_space_bindings
            .write()
            .expect("poisoned")
            .insert(space_id, binding);
    }

    /// Look up the `PeerBinding` registered for a space.
    pub(crate) fn get_space_binding(
        &self,
        space_id: &SpaceId,
    ) -> Option<PeerBinding> {
        self.peer_space_bindings
            .read()
            .expect("poisoned")
            .get(space_id)
            .cloned()
    }

    /// Unbind a space — drop any stored agent info and peer-store binding.
    pub(crate) fn unbind_space(&self, space_id: &SpaceId) {
        self.my_agent_infos
            .write()
            .expect("poisoned")
            .remove(space_id);
        self.peer_space_bindings
            .write()
            .expect("poisoned")
            .remove(space_id);
    }

    /// Get a reference to the endpoint.
    pub(crate) fn endpoint(&self) -> &DynEndpoint {
        &self.endpoint
    }

    /// Public: register a space without exposing the internal
    /// `Destination` trait. Returns the destination's `AddressHash`
    /// so the caller can correlate it back to a SpaceId.
    #[doc(hidden)]
    pub async fn register_space_for_test(
        &self,
        space_id: &SpaceId,
    ) -> K2Result<AddressHash> {
        let dest = self.register_space(space_id).await?;
        Ok(dest.address_hash())
    }

    /// Register a space: create a Reticulum destination for it
    /// and register the name hash for announce filtering.
    pub(crate) async fn register_space(
        &self,
        space_id: &SpaceId,
    ) -> K2Result<DynDestination> {
        let space_hash = hex::encode_to_string(space_id);
        let name = DestinationName::new("kitsune2", &space_hash);
        // as_name_hash_slice returns a slice; take first 10 bytes.
        let name_hash_slice = name.as_name_hash_slice();
        let mut name_hash = [0u8; 10];
        name_hash.copy_from_slice(&name_hash_slice[..10]);

        let dest = self.endpoint.add_destination(name).await?;

        // Register in our maps.
        {
            let mut dests = self.space_destinations.write().expect("poisoned");
            dests.insert(space_id.clone(), dest.clone());
        }
        {
            let mut hashes = self.space_name_hashes.write().expect("poisoned");
            hashes.insert(name_hash, Bytes::copy_from_slice(space_id));
        }

        info!(
            space_hash = %space_hash,
            dest_hash = ?dest.address_hash(),
            "Registered Reticulum destination for space"
        );

        Ok(dest)
    }

    /// Unregister a space.
    pub(crate) fn unregister_space(&self, space_id: &SpaceId) {
        let mut dests = self.space_destinations.write().expect("poisoned");
        dests.remove(space_id);
        // Also remove from name hash map.
        let space_bytes = Bytes::copy_from_slice(space_id);
        let mut hashes = self.space_name_hashes.write().expect("poisoned");
        hashes.retain(|_, v| *v != space_bytes);
        debug!(?space_id, "Unregistered Reticulum destination for space");
    }
}

/// Load a `PrivateIdentity` from the configured path, generating (and
/// persisting) a fresh one when no file is present.
async fn load_or_generate_identity(
    config: &ReticulumTransportConfig,
) -> K2Result<PrivateIdentity> {
    match &config.identity_path {
        Some(path) => {
            if path.exists() {
                let bytes = tokio::fs::read(path).await.map_err(|e| {
                    K2Error::other_src(
                        format!(
                            "Failed to read identity file: {}",
                            path.display()
                        ),
                        e,
                    )
                })?;
                PrivateIdentity::from_private_key_bytes(&bytes).map_err(|e| {
                    K2Error::other(format!(
                        "Invalid identity file {}: {e:?}",
                        path.display()
                    ))
                })
            } else {
                let identity = PrivateIdentity::new_from_rand(OsRng);
                let bytes = identity.to_private_key_bytes();
                tokio::fs::write(path, bytes).await.map_err(|e| {
                    K2Error::other_src(
                        format!(
                            "Failed to write identity file: {}",
                            path.display()
                        ),
                        e,
                    )
                })?;
                info!(path = %path.display(), "Generated and persisted new Reticulum identity");
                Ok(identity)
            }
        }
        None => {
            info!("No identity_path configured; generating ephemeral identity");
            Ok(PrivateIdentity::new_from_rand(OsRng))
        }
    }
}

/// Spawn each configured interface on the Transport's `InterfaceManager`.
async fn start_interfaces(
    transport: &Arc<tokio::sync::Mutex<rns_transport::transport::Transport>>,
    interfaces: &[ReticulumInterfaceConfig],
) -> K2Result<()> {
    let iface_manager = {
        let t = transport.lock().await;
        t.iface_manager()
    };
    let mut mgr = iface_manager.lock().await;
    for iface in interfaces {
        match iface {
            ReticulumInterfaceConfig::TcpClient { target } => {
                let client = rns_transport::iface::tcp_client::TcpClient::new(
                    target.clone(),
                );
                mgr.spawn(
                    client,
                    rns_transport::iface::tcp_client::TcpClient::spawn,
                );
                info!(%target, "Started Reticulum TCP client interface");
            }
            ReticulumInterfaceConfig::TcpServer { bind } => {
                let server = rns_transport::iface::tcp_server::TcpServer::new(
                    bind.clone(),
                    iface_manager.clone(),
                );
                mgr.spawn(
                    server,
                    rns_transport::iface::tcp_server::TcpServer::spawn,
                );
                info!(%bind, "Started Reticulum TCP server interface");
            }
            ReticulumInterfaceConfig::Udp { bind, group } => {
                let udp = rns_transport::iface::udp::UdpInterface::new(
                    bind.clone(),
                    group.clone(),
                );
                mgr.spawn(udp, rns_transport::iface::udp::UdpInterface::spawn);
                info!(%bind, ?group, "Started Reticulum UDP interface");
            }
        }
    }
    Ok(())
}

/// Helper to hex-encode a space ID for use as a Reticulum aspect.
mod hex {
    pub fn encode_to_string(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}
