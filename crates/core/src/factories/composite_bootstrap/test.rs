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

/// A factory whose `create` yields whatever it was built with: a recording
/// bootstrap, or the error of a bootstrap that cannot start on this host.
#[derive(Debug)]
struct StubBootstrapFactory(Result<Arc<RecordingBootstrap>, &'static str>);

impl StubBootstrapFactory {
    fn recording(instance: &Arc<RecordingBootstrap>) -> DynBootstrapFactory {
        Arc::new(Self(Ok(instance.clone())))
    }

    fn failing(msg: &'static str) -> DynBootstrapFactory {
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

/// Create a composite for a fresh space with the minimal values its inner
/// factories are passed through to.
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
async fn fans_put_to_all_inner() {
    let a = Arc::new(RecordingBootstrap::default());
    let b = Arc::new(RecordingBootstrap::default());
    let composite = CompositeBootstrapFactory::create(vec![
        StubBootstrapFactory::recording(&a),
        StubBootstrapFactory::recording(&b),
    ]);

    let (bootstrap, space_id) = create_composite(composite).await;
    let bootstrap = bootstrap.unwrap();

    let agent = AgentBuilder::default()
        .with_space(space_id)
        .build(TestLocalAgent::default());
    bootstrap.put(agent);

    assert_eq!(a.puts.lock().unwrap().len(), 1);
    assert_eq!(b.puts.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_failing_inner_factory_fails_the_composite() {
    let survivor = Arc::new(RecordingBootstrap::default());
    let composite = CompositeBootstrapFactory::create(vec![
        StubBootstrapFactory::recording(&survivor),
        StubBootstrapFactory::failing("this bootstrap cannot start"),
    ]);

    let (bootstrap, _) = create_composite(composite).await;
    let err = bootstrap.expect_err("a failing inner factory is an error");
    assert!(
        err.to_string().contains("this bootstrap cannot start"),
        "the inner error is propagated: {err}"
    );
}
