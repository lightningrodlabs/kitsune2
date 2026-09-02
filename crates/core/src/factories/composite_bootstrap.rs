//! A [`BootstrapFactory`] that stacks multiple underlying factories.
//!
//! All configured factories `create` a bootstrap for the same space sharing
//! one peer store, and `put` is fanned out to every one of them. This is the
//! primary way to run WAN (`CoreBootstrap`) and LAN (`MdnsBootstrap`)
//! simultaneously.

use kitsune2_api::*;
use std::sync::Arc;

/// Factory that wraps a list of inner [`BootstrapFactory`] instances.
#[derive(Debug)]
pub struct CompositeBootstrapFactory {
    inner: Vec<DynBootstrapFactory>,
}

impl CompositeBootstrapFactory {
    /// Construct a new composite factory over the given inner factories.
    /// Order is irrelevant for `put` fan-out; config init/validation runs
    /// in order.
    pub fn create(inner: Vec<DynBootstrapFactory>) -> DynBootstrapFactory {
        Arc::new(Self { inner })
    }
}

impl BootstrapFactory for CompositeBootstrapFactory {
    fn default_config(&self, config: &mut Config) -> K2Result<()> {
        for f in &self.inner {
            f.default_config(config)?;
        }
        Ok(())
    }

    fn validate_config(&self, config: &Config) -> K2Result<()> {
        for f in &self.inner {
            f.validate_config(config)?;
        }
        Ok(())
    }

    fn create(
        &self,
        builder: Arc<Builder>,
        peer_store: DynPeerStore,
        space_id: SpaceId,
    ) -> BoxFut<'static, K2Result<DynBootstrap>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let mut instances = Vec::with_capacity(inner.len());
            for f in inner {
                instances.push(
                    f.create(
                        builder.clone(),
                        peer_store.clone(),
                        space_id.clone(),
                    )
                    .await?,
                );
            }
            let out: DynBootstrap =
                Arc::new(CompositeBootstrap { inner: instances });
            Ok(out)
        })
    }
}

#[derive(Debug)]
struct CompositeBootstrap {
    inner: Vec<DynBootstrap>,
}

impl Bootstrap for CompositeBootstrap {
    fn put(&self, info: Arc<AgentInfoSigned>) {
        for b in &self.inner {
            b.put(info.clone());
        }
    }
}

#[cfg(test)]
mod test;
