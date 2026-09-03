//! A [`BootstrapFactory`] whose inner factory is allowed to fail.
//!
//! Some bootstraps are optional by nature: LAN discovery needs multicast
//! the host may not have, and a space must not lose its other bootstraps
//! over that. Rather than having every such factory decide for itself to
//! swallow its own errors, the decision is made where the stack is
//! assembled, by wrapping the factory in this one. An inner `create` error
//! is replaced by a bootstrap that does nothing; the first such error a
//! factory sees is a warning, since every space on a host without the
//! facility fails the same way, and the rest are debug noise.

use kitsune2_api::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Factory that turns an inner factory's `create` error into a no-op
/// bootstrap for that space.
#[derive(Debug)]
pub struct OptionalBootstrapFactory {
    inner: DynBootstrapFactory,
    /// Whether an inner failure has been reported at warning level yet.
    warned: Arc<AtomicBool>,
}

impl OptionalBootstrapFactory {
    /// Wrap `inner` so that a space it cannot serve runs without it.
    pub fn create(inner: DynBootstrapFactory) -> DynBootstrapFactory {
        Arc::new(Self {
            inner,
            warned: Arc::new(AtomicBool::new(false)),
        })
    }
}

impl BootstrapFactory for OptionalBootstrapFactory {
    fn default_config(&self, config: &mut Config) -> K2Result<()> {
        self.inner.default_config(config)
    }

    fn validate_config(&self, config: &Config) -> K2Result<()> {
        self.inner.validate_config(config)
    }

    fn create(
        &self,
        builder: Arc<Builder>,
        peer_store: DynPeerStore,
        space_id: SpaceId,
        tx: DynTransport,
    ) -> BoxFut<'static, K2Result<DynBootstrap>> {
        let inner = self.inner.clone();
        let warned = self.warned.clone();
        Box::pin(async move {
            match inner
                .create(builder, peer_store, space_id.clone(), tx)
                .await
            {
                Ok(bootstrap) => Ok(bootstrap),
                Err(err) => {
                    if !warned.swap(true, Ordering::Relaxed) {
                        tracing::warn!(
                            ?err,
                            ?space_id,
                            "optional bootstrap could not start, the space runs without it"
                        );
                    } else {
                        tracing::debug!(
                            ?err,
                            ?space_id,
                            "optional bootstrap could not start, the space runs without it"
                        );
                    }
                    let out: DynBootstrap = Arc::new(NoopBootstrap);
                    Ok(out)
                }
            }
        })
    }
}

/// What a space gets in place of the bootstrap that could not start.
#[derive(Debug)]
struct NoopBootstrap;

impl Bootstrap for NoopBootstrap {
    fn put(&self, _info: Arc<AgentInfoSigned>) {}
}

#[cfg(test)]
mod test;
