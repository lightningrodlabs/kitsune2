//! Per-space announce publisher and listener tasks.
//!
//! Each joined space gets:
//! - A publisher task that periodically calls `dest.announce()`.
//! - A listener that filters incoming `AnnounceEvent`s by `name_hash`.

use crate::destination::{AnnounceInfo, DynDestination};
use crate::node::PeerDiscovery;
use bytes::Bytes;
use kitsune2_api::SpaceId;
use rns_transport::hash::AddressHash;
use rns_transport::identity::Identity;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;
use tracing::{debug, trace, warn};

/// Identity cache: maps peer address hash -> full Identity.
///
/// Populated from validated announces. Shared across the transport
/// and bootstrap factories via `ReticulumNode`.
pub(crate) type IdentityCache = Arc<RwLock<HashMap<AddressHash, Identity>>>;

/// Create a new empty identity cache.
pub(crate) fn new_identity_cache() -> IdentityCache {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Spawn a task that periodically announces a destination, fetching the
/// current `app_data` via a user-supplied callback on each tick. The
/// callback typically looks up the node's stored `AgentInfoSigned`
/// bytes for the space.
pub(crate) fn spawn_announce_publisher<F>(
    dest: DynDestination,
    interval_s: u32,
    get_app_data: F,
) -> tokio::task::AbortHandle
where
    F: Fn() -> Option<Bytes> + Send + 'static,
{
    tokio::spawn(async move {
        let interval = std::time::Duration::from_secs(interval_s as u64);
        loop {
            let app_data = get_app_data();
            match dest.announce(app_data.as_deref()).await {
                Ok(_packet) => {
                    debug!(
                        dest_hash = ?dest.address_hash(),
                        has_app_data = app_data.is_some(),
                        "Published announce for destination"
                    );
                }
                Err(e) => {
                    warn!(?e, "Failed to announce destination");
                }
            }
            tokio::time::sleep(interval).await;
        }
    })
    .abort_handle()
}

/// Filter an announce event by comparing its `name_hash` against a set
/// of known per-space name hashes.
///
/// Returns the matching space ID if the announce is for one of our spaces.
pub(crate) fn filter_announce_by_space(
    announce: &AnnounceInfo,
    space_name_hashes: &HashMap<[u8; 10], bytes::Bytes>,
) -> Option<bytes::Bytes> {
    space_name_hashes.get(&announce.name_hash).cloned()
}

/// Spawn a task that consumes announces from the broadcast channel,
/// filters by name_hash, updates the identity cache, and pushes
/// matching announces (with their `app_data`) to the peer-discovered
/// drain.
pub fn spawn_announce_listener(
    mut rx: broadcast::Receiver<AnnounceInfo>,
    identity_cache: IdentityCache,
    space_name_hashes: Arc<RwLock<HashMap<[u8; 10], Bytes>>>,
    peer_discovered_tx: tokio::sync::mpsc::Sender<PeerDiscovery>,
) -> tokio::task::AbortHandle {
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(announce) => {
                    // Update the identity cache with every announce.
                    let addr_hash = announce.identity.address_hash;
                    {
                        let mut cache =
                            identity_cache.write().expect("poisoned");
                        cache.insert(addr_hash, announce.identity);
                    }

                    // Check if this announce is for a space we've joined.
                    // Drop the read guard before awaiting.
                    let matched_space = {
                        let hashes =
                            space_name_hashes.read().expect("poisoned");
                        filter_announce_by_space(&announce, &hashes)
                    };
                    if let Some(space_bytes) = matched_space {
                        let space_id = SpaceId::from(space_bytes);
                        debug!(
                            ?addr_hash,
                            ?space_id,
                            "Received announce for joined space"
                        );
                        let _ = peer_discovered_tx
                            .send((
                                space_id,
                                announce.identity,
                                announce.app_data,
                            ))
                            .await;
                    } else {
                        trace!(
                            ?addr_hash,
                            "Ignoring announce for unknown space"
                        );
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(n, "Announce listener lagged, dropped {n} events");
                }
                Err(broadcast::error::RecvError::Closed) => {
                    debug!("Announce broadcast channel closed");
                    break;
                }
            }
        }
    })
    .abort_handle()
}
