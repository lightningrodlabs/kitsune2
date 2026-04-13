//! Reticulum bootstrap implementation.
//!
//! Uses Reticulum's announce system for peer discovery instead of
//! an HTTP bootstrap server.
//!
//! # Protocol
//!
//! - **Outbound.** `Bootstrap::put(info)` encodes the `AgentInfoSigned`
//!   to canonical JSON and stores the bytes on the node as the
//!   per-space `app_data` to include in the next announce. The
//!   per-space announce publisher picks up the latest value on each
//!   tick — one announce per interval carries whatever info was most
//!   recently put.
//! - **Inbound.** The transport's bootstrap drain task consumes
//!   discovery events pushed by the announce listener, looks up the
//!   `PeerBinding` for the announcing space, and decodes the app_data
//!   through the registered `Verifier`. On success the
//!   `AgentInfoSigned` is inserted into the space's peer store.

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
        let json = match info.encode() {
            Ok(j) => j,
            Err(e) => {
                warn!(
                    ?e,
                    space = ?self.space_id,
                    "ReticulumBootstrap::put: failed to encode AgentInfoSigned",
                );
                return;
            }
        };
        match compress_app_data(json.as_bytes()) {
            Ok(compressed) => {
                debug!(
                    agent = ?info.agent,
                    space = ?self.space_id,
                    json_size = json.len(),
                    compressed_size = compressed.len(),
                    "ReticulumBootstrap: staged compressed AgentInfoSigned for next announce"
                );
                self.node
                    .set_my_agent_info(self.space_id.clone(), compressed);
            }
            Err(e) => {
                warn!(
                    ?e,
                    space = ?self.space_id,
                    "ReticulumBootstrap::put: compression failed",
                );
            }
        }
    }
}

/// Deflate-compress the AgentInfoSigned JSON so it fits in a single
/// Reticulum announce packet.
///
/// An rns announce packet has ~316 bytes for app_data (after the
/// pub_key / verifying_key / name_hash / rand_hash / signature /
/// header overhead). A canonical-JSON `AgentInfoSigned` with real
/// Ed25519 signatures is 400-600 bytes and doesn't fit raw.
pub fn compress_app_data(input: &[u8]) -> std::io::Result<bytes::Bytes> {
    use flate2::{write::DeflateEncoder, Compression};
    use std::io::Write;
    let mut enc = DeflateEncoder::new(Vec::new(), Compression::best());
    enc.write_all(input)?;
    Ok(bytes::Bytes::from(enc.finish()?))
}

/// Inverse of [`compress_app_data`].
fn decompress_app_data(input: &[u8]) -> std::io::Result<Vec<u8>> {
    use flate2::read::DeflateDecoder;
    use std::io::Read;
    let mut dec = DeflateDecoder::new(input);
    let mut out = Vec::new();
    dec.read_to_end(&mut out)?;
    Ok(out)
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

            // Inverse of `compress_app_data` in `Bootstrap::put`.
            let decompressed = match decompress_app_data(&app_data) {
                Ok(d) => d,
                Err(e) => {
                    warn!(
                        ?e,
                        ?space_id,
                        "bootstrap drain: failed to decompress app_data",
                    );
                    continue;
                }
            };
            let decoded =
                AgentInfoSigned::decode(&binding.verifier, &decompressed);
            let signed = match decoded {
                Ok(s) => s,
                Err(e) => {
                    warn!(
                        ?e,
                        ?space_id,
                        identity_hash = ?identity.address_hash,
                        "bootstrap drain: failed to decode AgentInfoSigned",
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
