//! Unit tests for connection establishment failures.
//!
//! These tests use a fake [`Endpoint`](crate::endpoint::Endpoint) to
//! deterministically exercise error paths in
//! [`IrohTransport::create_connection_and_context`](crate::IrohTransport::create_connection_and_context)
//! that are difficult to trigger reproducibly via the real iroh stack.

use super::fakes::*;
use super::support::{build_recording_handler, remote_url};
use crate::IrohTransportConfig;
use crate::url::endpoint_from_url;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

fn config() -> IrohTransportConfig {
    IrohTransportConfig {
        // Make the outer wrapper effectively unreachable so the test
        // observes the *inner* error path, not the outer tokio timeout.
        connect_timeout_s: 60,
        ..Default::default()
    }
}

/// When `iroh::Endpoint::connect` returns an error (the production case-B
/// path: quinn `ConnectionError::TimedOut` after the relay has nothing to
/// say), `create_connection_and_context` must mark the peer unresponsive
/// and surface a clear error.
#[tokio::test]
async fn marks_unresponsive_when_iroh_connect_returns_error() {
    let recorder = build_recording_handler();
    let calls = recorder.unresponsive_calls.clone();
    let handler = recorder.handler;

    let endpoint = Arc::new(FakeEndpoint {
        connect: connect_fails("timed out"),
        ..Default::default()
    });

    let remote_url = remote_url();
    let target = endpoint_from_url(&remote_url).unwrap();

    let connections = Arc::new(RwLock::new(HashMap::new()));
    let local_url = Arc::new(RwLock::new(Some(remote_url.clone())));

    let transport = build_transport(
        endpoint,
        handler,
        connections.clone(),
        local_url,
        config(),
    );

    let result = transport
        .create_connection_and_context(target, remote_url.clone())
        .await;

    let err_str = result.expect_err("connect should fail").to_string();
    assert!(
        err_str.contains("iroh connect error"),
        "expected wrapped 'iroh connect error', got: {err_str}"
    );
    assert!(
        err_str.contains("timed out"),
        "expected inner 'timed out' source to be preserved, got: {err_str}"
    );

    let recorded = calls.lock().unwrap();
    assert_eq!(
        recorded.len(),
        1,
        "set_unresponsive should be called exactly once"
    );
    assert_eq!(recorded[0].0, remote_url);

    // The connections map must not have been mutated for a failed connect.
    assert!(connections.read().unwrap().is_empty());
}

/// When the *outer* `tokio::time::timeout` wrapper fires (i.e. iroh's connect
/// hangs longer than `connect_timeout_s`), the same set_unresponsive
/// guarantee must hold.
#[tokio::test]
async fn marks_unresponsive_when_outer_connect_timeout_fires() {
    let recorder = build_recording_handler();
    let calls = recorder.unresponsive_calls.clone();
    let handler = recorder.handler;

    let endpoint = Arc::new(FakeEndpoint {
        connect: connect_hangs(),
        ..Default::default()
    });

    let remote_url = remote_url();
    let target = endpoint_from_url(&remote_url).unwrap();

    let connections = Arc::new(RwLock::new(HashMap::new()));
    let local_url = Arc::new(RwLock::new(Some(remote_url.clone())));

    let cfg = IrohTransportConfig {
        // Use the smallest unit (1 second) so the test runs quickly while
        // still going through the real `tokio::time::timeout` codepath.
        connect_timeout_s: 1,
        ..Default::default()
    };

    let transport =
        build_transport(endpoint, handler, connections.clone(), local_url, cfg);

    let start = std::time::Instant::now();
    let result = transport
        .create_connection_and_context(target, remote_url.clone())
        .await;
    let elapsed = start.elapsed();

    let err_str = result.expect_err("connect should time out").to_string();
    assert!(
        err_str.contains("iroh connect timed out"),
        "expected 'iroh connect timed out', got: {err_str}"
    );

    // Sanity check: we did wait for the timeout, but not much longer.
    assert!(
        elapsed >= Duration::from_secs(1),
        "should have waited at least connect_timeout_s, was {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "outer timeout should fire promptly, was {elapsed:?}"
    );

    let recorded = calls.lock().unwrap();
    assert_eq!(
        recorded.len(),
        1,
        "set_unresponsive should be called exactly once"
    );
    assert_eq!(recorded[0].0, remote_url);
}
