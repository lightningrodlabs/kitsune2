//! Doubles for exercising the factories that wrap other bootstrap
//! factories: a bootstrap that records its puts, a factory that yields a
//! fixed result, and a way to create a bootstrap for a fresh space.

use kitsune2_api::*;
use std::sync::{Arc, Mutex};

/// A bootstrap that remembers every agent info it is handed.
#[derive(Debug, Default)]
pub(crate) struct RecordingBootstrap {
    pub puts: Mutex<Vec<Arc<AgentInfoSigned>>>,
}

impl Bootstrap for RecordingBootstrap {
    fn put(&self, info: Arc<AgentInfoSigned>) {
        self.puts.lock().unwrap().push(info);
    }
}

/// A factory whose `create` yields whatever it was built with: a recording
/// bootstrap, or the error of a bootstrap that cannot start on this host.
#[derive(Debug)]
pub(crate) struct StubBootstrapFactory(
    Result<Arc<RecordingBootstrap>, &'static str>,
);

impl StubBootstrapFactory {
    pub fn recording(
        instance: &Arc<RecordingBootstrap>,
    ) -> DynBootstrapFactory {
        Arc::new(Self(Ok(instance.clone())))
    }

    pub fn failing(msg: &'static str) -> DynBootstrapFactory {
        Arc::new(Self(Err(msg)))
    }
}

impl BootstrapFactory for StubBootstrapFactory {
    fn default_config(&self, _: &mut Config) -> K2Result<()> {
        Ok(())
    }
    fn validate_config(&self, _: &Config) -> K2Result<()> {
        Ok(())
    }
    fn create(
        &self,
        _: Arc<Builder>,
        _: DynPeerStore,
        _: SpaceId,
        _: DynTransport,
    ) -> BoxFut<'static, K2Result<DynBootstrap>> {
        let result = self
            .0
            .clone()
            .map(|inst| inst as DynBootstrap)
            .map_err(K2Error::other);
        Box::pin(async move { result })
    }
}

/// Create a bootstrap for a fresh space through `factory`, with the
/// minimal values a wrapping factory passes through to its inner ones.
pub(crate) async fn create_bootstrap(
    factory: DynBootstrapFactory,
) -> (K2Result<DynBootstrap>, SpaceId) {
    let builder =
        Arc::new(crate::default_test_builder().with_default_config().unwrap());
    let space_id = SpaceId::from(bytes::Bytes::from_static(b"s"));
    let blocks = builder
        .blocks
        .create(builder.clone(), space_id.clone())
        .await
        .unwrap();
    let known_peers = builder
        .known_peers
        .create(builder.clone(), space_id.clone())
        .await
        .unwrap();
    let peer_store = builder
        .peer_store
        .create(builder.clone(), space_id.clone(), blocks, known_peers)
        .await
        .unwrap();
    let tx: DynTransport = Arc::new(MockTransport::new());
    (
        factory
            .create(builder, peer_store, space_id.clone(), tx)
            .await,
        space_id,
    )
}
