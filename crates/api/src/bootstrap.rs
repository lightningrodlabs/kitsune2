//! Kitsune2 bootstrap related types.

use crate::*;
use std::sync::Arc;

/// Method for bootstrapping WAN discovery of peers.
///
/// The internal implementation will take care of whatever polling
/// or managing of message queues is required to be notified of
/// remote peers both on initialization and over runtime.
pub trait Bootstrap: 'static + Send + Sync + std::fmt::Debug {
    /// Put an agent info onto a bootstrap server.
    ///
    /// This method takes responsibility for retrying the send operation in the case
    /// of server error until such time as:
    /// - the Put succeeds
    /// - we receive a new info that supersedes the previous
    /// - or the info expires
    fn put(&self, info: Arc<AgentInfoSigned>);
}

/// Trait-object [Bootstrap].
pub type DynBootstrap = Arc<dyn Bootstrap>;

/// A [`Bootstrap`] that accepts puts and does nothing with them: the
/// stand-in for a bootstrap that is switched off, could not start, or is
/// not wanted in a test, so that a builder stack stays uniform.
#[derive(Debug)]
pub struct NoopBootstrap;

impl Bootstrap for NoopBootstrap {
    fn put(&self, _info: Arc<AgentInfoSigned>) {}
}

/// A factory for constructing Bootstrap instances.
pub trait BootstrapFactory: 'static + Send + Sync + std::fmt::Debug {
    /// Help the builder construct a default config from the chosen
    /// module factories.
    fn default_config(&self, config: &mut Config) -> K2Result<()>;

    /// Validate configuration.
    fn validate_config(&self, config: &Config) -> K2Result<()>;

    /// Construct a bootstrap instance.
    ///
    /// `tx` is the space's transport, so that a bootstrap which learns peer
    /// URLs by some out-of-band means can have them dialled directly.
    fn create(
        &self,
        builder: Arc<Builder>,
        peer_store: DynPeerStore,
        space_id: SpaceId,
        tx: DynTransport,
    ) -> BoxFut<'static, K2Result<DynBootstrap>>;
}

/// Trait-object [BootstrapFactory].
pub type DynBootstrapFactory = Arc<dyn BootstrapFactory>;
