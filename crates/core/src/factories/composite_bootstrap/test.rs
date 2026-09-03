use super::super::CompositeBootstrapFactory;
use crate::factories::bootstrap_test_support::*;
use kitsune2_test_utils::agent::{AgentBuilder, TestLocalAgent};
use std::sync::Arc;

#[tokio::test]
async fn fans_put_to_all_inner() {
    let a = Arc::new(RecordingBootstrap::default());
    let b = Arc::new(RecordingBootstrap::default());
    let composite = CompositeBootstrapFactory::create(vec![
        StubBootstrapFactory::recording(&a),
        StubBootstrapFactory::recording(&b),
    ]);

    let (bootstrap, space_id) = create_bootstrap(composite).await;
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

    let (bootstrap, _) = create_bootstrap(composite).await;
    let err = bootstrap.expect_err("a failing inner factory is an error");
    assert!(
        err.to_string().contains("this bootstrap cannot start"),
        "the inner error is propagated: {err}"
    );
}

/// A composite over nothing would silently leave the space with no
/// bootstrap at all, which is never what a configuration meant.
#[tokio::test]
async fn a_composite_with_no_inner_factories_is_an_error() {
    let composite = CompositeBootstrapFactory::create(vec![]);

    let (bootstrap, _) = create_bootstrap(composite).await;
    let err = bootstrap.expect_err("an empty composite is an error");
    assert!(
        err.to_string().contains("no inner factories"),
        "the error names the problem: {err}"
    );
}
