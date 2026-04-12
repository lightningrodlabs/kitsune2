//! Link lifecycle management and preflight handshake.
//!
//! Each `LinkContext` wraps a Reticulum `Link` (via the trait layer)
//! and manages the preflight exchange for a specific (peer, space) pair.
//! Preflight is per-peer, not per-link -- tracked in `PeerState`.

use crate::destination::DynLink;
use kitsune2_api::SpaceId;

/// Context for a single per-space link to a peer.
///
/// This is a lightweight wrapper that associates a link with its space
/// and provides convenience methods. The heavier preflight and refcount
/// logic lives in `PeerState`.
#[derive(Debug)]
pub(crate) struct ManagedLink {
    /// The underlying Reticulum link.
    pub link: DynLink,
    /// The space this link is associated with.
    pub space_id: SpaceId,
}

impl ManagedLink {
    /// Create a new managed link.
    pub fn new(link: DynLink, space_id: SpaceId) -> Self {
        Self { link, space_id }
    }
}
