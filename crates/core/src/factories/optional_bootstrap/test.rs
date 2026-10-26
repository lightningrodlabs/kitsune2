use super::OptionalBootstrapFactory;
use crate::factories::bootstrap_test_support::*;
use kitsune2_test_utils::agent::{AgentBuilder, TestLocalAgent};
use std::sync::Arc;

#[tokio::test]
async fn a_failing_inner_factory_yields_a_bootstrap_that_does_nothing() {
    let optional = OptionalBootstrapFactory::create(
        StubBootstrapFactory::failing("no multicast on this host"),
    );

    let (bootstrap, space_id) = create_bootstrap(optional).await;
    let bootstrap = bootstrap.expect("an optional bootstrap never fails");

    // The stand-in accepts puts and has nowhere to send them.
    bootstrap.put(
        AgentBuilder::default()
            .with_space(space_id)
            .build(TestLocalAgent::default()),
    );
}

#[tokio::test]
async fn a_working_inner_factory_is_used_as_is() {
    let inner = Arc::new(RecordingBootstrap::default());
    let optional = OptionalBootstrapFactory::create(
        StubBootstrapFactory::recording(&inner),
    );

    let (bootstrap, space_id) = create_bootstrap(optional).await;
    let bootstrap = bootstrap.unwrap();

    bootstrap.put(
        AgentBuilder::default()
            .with_space(space_id)
            .build(TestLocalAgent::default()),
    );
    assert_eq!(inner.puts.lock().unwrap().len(), 1);
}
