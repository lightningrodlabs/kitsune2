use super::super::CompositeBootstrapFactory;
use kitsune2_api::*;
use kitsune2_test_utils::agent::{AgentBuilder, TestLocalAgent};
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
struct RecordingBootstrap {
    puts: Mutex<Vec<Arc<AgentInfoSigned>>>,
}

impl Bootstrap for RecordingBootstrap {
    fn put(&self, info: Arc<AgentInfoSigned>) {
        self.puts.lock().unwrap().push(info);
    }
}

#[derive(Debug)]
struct RecordingBootstrapFactory {
    instance: Arc<RecordingBootstrap>,
}

impl BootstrapFactory for RecordingBootstrapFactory {
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
        let inst = self.instance.clone();
        Box::pin(async move { Ok(inst as DynBootstrap) })
    }
}

#[tokio::test]
async fn fans_put_to_all_inner() {
    let a = Arc::new(RecordingBootstrap::default());
    let b = Arc::new(RecordingBootstrap::default());
    let composite = CompositeBootstrapFactory::create(vec![
        Arc::new(RecordingBootstrapFactory {
            instance: a.clone(),
        }),
        Arc::new(RecordingBootstrapFactory {
            instance: b.clone(),
        }),
    ]);

    // Composite itself doesn't touch builder/peer_store/space beyond passing
    // them through to its inner factories, so we can invent minimal values.
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
    let bootstrap = composite
        .create(builder, peer_store, space_id.clone(), tx)
        .await
        .unwrap();

    let agent = AgentBuilder::default()
        .with_space(space_id)
        .build(TestLocalAgent::default());

    bootstrap.put(agent);

    assert_eq!(a.puts.lock().unwrap().len(), 1);
    assert_eq!(b.puts.lock().unwrap().len(), 1);
}

/// A factory whose `create` always fails, standing in for a LAN discovery
/// that cannot start on this host.
#[derive(Debug)]
struct FailingBootstrapFactory;

impl BootstrapFactory for FailingBootstrapFactory {
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
        Box::pin(async { Err(K2Error::other("this bootstrap cannot start")) })
    }
}

async fn create_composite(
    composite: DynBootstrapFactory,
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
        composite
            .create(builder, peer_store, space_id.clone(), tx)
            .await,
        space_id,
    )
}

#[tokio::test]
async fn a_failing_inner_factory_fails_the_composite() {
    let survivor = Arc::new(RecordingBootstrap::default());
    let composite = CompositeBootstrapFactory::create(vec![
        Arc::new(RecordingBootstrapFactory {
            instance: survivor.clone(),
        }),
        Arc::new(FailingBootstrapFactory),
    ]);

    let (bootstrap, _) = create_composite(composite).await;
    let err = bootstrap.expect_err("a failing inner factory is an error");
    assert!(
        err.to_string().contains("this bootstrap cannot start"),
        "the inner error is propagated: {err}"
    );
}
