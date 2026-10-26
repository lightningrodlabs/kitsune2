//! Kitsune2 peer access related types.

use crate::*;
use bytes::Bytes;
use std::sync::Arc;

/// The module id of the access module.
///
/// The access module performs the "hello" proof-of-knowledge handshake that
/// decides whether a peer is granted access to a space. Because that handshake
/// is what produces an access decision in the first place, messages addressed
/// to this module id are exempt from the access gate, in the same way that
/// preflight messages are.
pub const HELLO_MOD_NAME: &str = "hello";

/// The decision made about access for a peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessDecision {
    /// Access is granted to the peer.
    Granted,
    /// Access is blocked for the peer.
    Blocked,
}

/// The access information for a peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerAccess {
    /// The access decision for the peer.
    pub decision: AccessDecision,

    /// The timestamp when the decision was made.
    pub decided_at: Timestamp,
}

/// Trait for tracking access state of peers.
///
/// Peers are named by URL here because URLs are what the transport passes
/// around, but a decision is *about* the peer, not about the URL it was
/// reached at. Implementations must therefore key decisions by the
/// [`Url::peer_id`] segment, which is the part the transport authenticates and
/// the part that survives a relay failover, and not by the full URL. A URL
/// with no peer id in it names no peer, so it can carry no decision.
pub trait PeerAccessState: 'static + Send + Sync + std::fmt::Debug {
    /// Get a previously made access decision for the peer at the given URL.
    ///
    /// Returns `Ok(None)` for a URL with no peer id, which is unknown rather
    /// than an error.
    fn get_access_decision(
        &self,
        peer_url: Url,
    ) -> K2Result<Option<PeerAccess>>;

    /// Record an access decision for the peer at the given URL.
    ///
    /// This is how the access module records the outcome of a successful
    /// proof-of-knowledge exchange.
    ///
    /// Implementations must treat an explicit [`AccessDecision::Blocked`]
    /// entry as final with respect to this method: a denylist entry always
    /// wins, so a later [`AccessDecision::Granted`] passed to this method must
    /// not overwrite it.
    ///
    /// A URL with no peer id records nothing, and is not an error.
    fn set_access_decision(
        &self,
        peer_url: Url,
        access: PeerAccess,
    ) -> K2Result<()>;

    /// Remove any access decision for the peer at the given URL.
    ///
    /// After this call the peer is "unknown" again, which is the primitive
    /// behind decision pruning. It is not an error to remove a decision that
    /// does not exist, or to pass a URL with no peer id.
    fn remove_access_decision(&self, peer_url: Url) -> K2Result<()>;
}

/// Trait-object version of kitsune2 [`PeerAccessState`] trait.
pub type DynPeerAccessState = std::sync::Arc<dyn PeerAccessState>;

/// Provider of purpose-scoped key material derived from a host-held space
/// secret.
///
/// The host never hands kitsune2 the root space secret. Instead it hands over
/// key material derived from it for a named purpose, so that an accidentally
/// disclosed derived key reveals neither the root secret nor any other derived
/// key.
///
/// Kitsune2 calls this once per `(space_id, purpose)` pair and caches the
/// result, then runs all protocol crypto itself keyed by the derived material.
/// Purposes currently requested by kitsune2:
///
/// - `"k2-hello-v1"` — the HMAC key for the hello proof-of-knowledge exchange.
pub trait SpaceSecret: 'static + Send + Sync + std::fmt::Debug {
    /// Derive purpose-scoped key material from the host-held space secret.
    ///
    /// Called once per `(space_id, purpose)`; kitsune2 caches the result.
    fn derive_key(
        &self,
        space_id: SpaceId,
        purpose: &str,
    ) -> BoxFut<'static, K2Result<Bytes>>;
}

/// Trait-object version of the kitsune2 [`SpaceSecret`] trait.
pub type DynSpaceSecret = Arc<dyn SpaceSecret>;

/// A factory for constructing [`SpaceSecret`] instances.
pub trait SpaceSecretFactory: 'static + Send + Sync + std::fmt::Debug {
    /// Help the builder construct a default config from the chosen
    /// module factories.
    fn default_config(&self, config: &mut Config) -> K2Result<()>;

    /// Validate configuration.
    fn validate_config(&self, config: &Config) -> K2Result<()>;

    /// Construct a space secret instance for a single space.
    fn create(
        &self,
        builder: Arc<Builder>,
        space_id: SpaceId,
    ) -> BoxFut<'static, K2Result<DynSpaceSecret>>;
}

/// Trait-object version of the kitsune2 [`SpaceSecretFactory`] trait.
pub type DynSpaceSecretFactory = Arc<dyn SpaceSecretFactory>;
