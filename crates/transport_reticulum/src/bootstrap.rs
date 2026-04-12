//! Reticulum bootstrap implementation.
//!
//! Uses Reticulum's announce system for peer discovery instead of
//! an HTTP bootstrap server.

use kitsune2_api::*;
use std::sync::Arc;
use tracing::debug;

use crate::node::ReticulumNode;

/// Bootstrap implementation backed by Reticulum announces.
#[derive(Debug)]
pub(crate) struct ReticulumBootstrap {
    node: Arc<ReticulumNode>,
    peer_store: DynPeerStore,
    space_id: SpaceId,
}

impl ReticulumBootstrap {
    /// Create a new bootstrap instance for a specific space.
    pub fn new(
        node: Arc<ReticulumNode>,
        peer_store: DynPeerStore,
        space_id: SpaceId,
    ) -> Self {
        Self {
            node,
            peer_store,
            space_id,
        }
    }
}

impl Bootstrap for ReticulumBootstrap {
    fn put(&self, info: Arc<AgentInfoSigned>) {
        debug!(
            agent = ?info.agent,
            space = ?self.space_id,
            "ReticulumBootstrap::put (announce-driven, info cached locally)"
        );
        // In the Reticulum model, `put` is a no-op for outbound announces:
        // our announces are driven by the periodic publisher task in the
        // transport layer. This method is called by kitsune2 core when it
        // wants to "publish" an agent info to the bootstrap service --
        // for Reticulum, that already happens via destination announces.
    }
}
