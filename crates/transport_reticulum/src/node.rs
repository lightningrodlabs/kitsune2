//! Shared state for the Reticulum transport and bootstrap factories.
//!
//! `ReticulumNode` owns the `rns_transport::Transport` instance, the
//! identity cache, per-space destination map, and announce queues.
//! Both `ReticulumTransportFactory` and `ReticulumBootstrapFactory`
//! hold an `Arc<ReticulumNode>`.

use crate::announce::{self, IdentityCache};
use crate::destination::{DynDestination, DynEndpoint};
use bytes::Bytes;
use kitsune2_api::{K2Error, K2Result, SpaceId};
use rns_transport::destination::DestinationName;
use rns_transport::hash::AddressHash;
use rns_transport::identity::Identity;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tracing::{debug, info};

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
    peer_discovered_tx: tokio::sync::mpsc::Sender<(Bytes, Identity)>,
    /// Receiver side, consumed by bootstrap instances.
    peer_discovered_rx: tokio::sync::Mutex<
        Option<tokio::sync::mpsc::Receiver<(Bytes, Identity)>>,
    >,
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
        })
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
    pub(crate) fn identity_cache(&self) -> &IdentityCache {
        &self.identity_cache
    }

    /// Get the space name hashes map (for announce filtering).
    pub(crate) fn space_name_hashes(
        &self,
    ) -> &Arc<RwLock<HashMap<[u8; 10], Bytes>>> {
        &self.space_name_hashes
    }

    /// Get a sender for peer discovery notifications.
    pub(crate) fn peer_discovered_tx(
        &self,
    ) -> &tokio::sync::mpsc::Sender<(Bytes, Identity)> {
        &self.peer_discovered_tx
    }

    /// Take the peer discovery receiver (can only be called once).
    pub(crate) async fn take_peer_discovered_rx(
        &self,
    ) -> Option<tokio::sync::mpsc::Receiver<(Bytes, Identity)>> {
        self.peer_discovered_rx.lock().await.take()
    }

    /// Get a reference to the endpoint.
    pub(crate) fn endpoint(&self) -> &DynEndpoint {
        &self.endpoint
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

    /// Send data to a peer on a specific space's link.
    pub(crate) async fn send_to_peer(
        &self,
        _peer_identity: &Identity,
        _space_id: &SpaceId,
        _data: &[u8],
        _max_frame_bytes: usize,
    ) -> K2Result<()> {
        // TODO: implement link management + send
        Err(K2Error::other("send_to_peer not yet implemented"))
    }
}

/// Helper to hex-encode a space ID for use as a Reticulum aspect.
mod hex {
    pub fn encode_to_string(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}
