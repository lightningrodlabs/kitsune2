//! A no-op bootstrap implementation.
//!
//! This is useful for testing or for cases where you don't want bootstrap to discover peers.

use kitsune2_api::*;
use kitsune2_api::{BoxFut, K2Result, SpaceId};
use std::sync::Arc;

pub use kitsune2_api::NoopBootstrap;

/// A factory for constructing [NoopBootstrap] instances.
#[derive(Debug)]
pub struct NoopBootstrapFactory;

impl BootstrapFactory for NoopBootstrapFactory {
    fn default_config(&self, _config: &mut Config) -> K2Result<()> {
        Ok(())
    }

    fn validate_config(&self, _config: &Config) -> K2Result<()> {
        Ok(())
    }

    fn create(
        &self,
        _builder: Arc<Builder>,
        _peer_store: DynPeerStore,
        _space_id: SpaceId,
        _tx: DynTransport,
    ) -> BoxFut<'static, K2Result<DynBootstrap>> {
        Box::pin(async move {
            let bootstrap: DynBootstrap = Arc::new(NoopBootstrap);
            Ok(bootstrap)
        })
    }
}
