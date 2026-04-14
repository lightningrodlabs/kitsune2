//! Reticulum bootstrap implementation.
//!
//! Uses Reticulum's announce system for peer discovery instead of
//! an HTTP bootstrap server.
//!
//! # Protocol
//!
//! - **Outbound.** `Bootstrap::put(info)` packs the `AgentInfoSigned`
//!   into the compact announce wire format (see [`crate::announce_wire`])
//!   and stores the bytes on the node as the per-space `app_data` to
//!   include in the next announce. The per-space announce publisher
//!   picks up the latest value on each tick — one announce per interval
//!   carries whatever info was most recently put.
//! - **Inbound.** The transport's bootstrap drain task consumes
//!   discovery events pushed by the announce listener, looks up the
//!   `PeerBinding` for the announcing space, and decodes the app_data
//!   through the registered `Verifier`. On success the
//!   `AgentInfoSigned` is inserted into the space's peer store.

use crate::announce_wire::{decode_announce_wire, encode_announce_wire};
use crate::node::{PeerBinding, ReticulumNode};
use kitsune2_api::*;
use std::sync::Arc;
use tracing::{debug, warn};

/// Bootstrap implementation backed by Reticulum announces.
#[derive(Debug)]
pub struct ReticulumBootstrap {
    node: Arc<ReticulumNode>,
    space_id: SpaceId,
}

impl ReticulumBootstrap {
    /// Create a new bootstrap instance for a specific space.
    ///
    /// Registers the given peer store + verifier with the node under
    /// the space id so the drain task can route inbound discoveries.
    pub fn new(
        node: Arc<ReticulumNode>,
        peer_store: DynPeerStore,
        verifier: DynVerifier,
        space_id: SpaceId,
    ) -> Self {
        node.bind_space(
            space_id.clone(),
            PeerBinding {
                peer_store,
                verifier,
            },
        );
        Self { node, space_id }
    }
}

impl Bootstrap for ReticulumBootstrap {
    fn put(&self, info: Arc<AgentInfoSigned>) {
        match encode_announce_wire(&info) {
            Ok(encoded) => {
                debug!(
                    agent = ?info.agent,
                    space = ?self.space_id,
                    encoded_size = encoded.len(),
                    "ReticulumBootstrap: staged AgentInfoSigned for next announce"
                );
                self.node.set_my_agent_info(self.space_id.clone(), encoded);
            }
            Err(e) => {
                warn!(
                    ?e,
                    space = ?self.space_id,
                    "ReticulumBootstrap::put: announce wire encode failed",
                );
            }
        }
    }
}

impl Drop for ReticulumBootstrap {
    fn drop(&mut self) {
        self.node.unbind_space(&self.space_id);
    }
}

/// Spawn the bootstrap drain. Consumes `(space_id, identity, app_data)`
/// events and, for each space that has a [`PeerBinding`] registered,
/// decodes the app_data as an `AgentInfoSigned` via the binding's
/// `Verifier` and inserts it into the space's peer store.
///
/// Events for unbound spaces, or with empty / invalid app_data, are
/// logged and dropped.
pub fn spawn_bootstrap_drain(
    mut rx: tokio::sync::mpsc::Receiver<crate::node::PeerDiscovery>,
    node: Arc<ReticulumNode>,
) -> tokio::task::AbortHandle {
    tokio::spawn(async move {
        while let Some((space_id, identity, app_data)) = rx.recv().await {
            if app_data.is_empty() {
                debug!(
                    ?space_id,
                    identity_hash = ?identity.address_hash,
                    "bootstrap drain: empty app_data, skipping",
                );
                continue;
            }
            let binding = match node.get_space_binding(&space_id) {
                Some(b) => b,
                None => {
                    debug!(
                        ?space_id,
                        "bootstrap drain: no binding for space (yet?)",
                    );
                    continue;
                }
            };

            let signed = match decode_announce_wire(
                &binding.verifier,
                &app_data,
            ) {
                Ok(s) => s,
                Err(e) => {
                    warn!(
                        ?e,
                        ?space_id,
                        identity_hash = ?identity.address_hash,
                        "bootstrap drain: failed to decode announce app_data",
                    );
                    continue;
                }
            };

            if let Err(e) = binding.peer_store.insert(vec![signed]).await {
                warn!(
                    ?e,
                    ?space_id,
                    "bootstrap drain: peer_store.insert failed",
                );
            }
        }
        debug!("bootstrap drain: channel closed");
    })
    .abort_handle()
}
