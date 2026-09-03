//! Unit tests for [`TxImp::dial`]: connection establishment without a
//! payload, and reuse of the connection it establishes.

use super::fakes::*;
use super::support::{build_recording_handler, remote_url};
use crate::url::endpoint_from_url;
use crate::{FRAME_HEADER_LEN, FrameType, IrohTransportConfig};
use kitsune2_api::TxImp;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Dialling the same peer twice must not open a second connection: the
/// second call finds the context the first one registered and returns it.
#[tokio::test]
async fn dial_twice_opens_one_connection() {
    let remote_url = remote_url();
    let remote_id = endpoint_from_url(&remote_url).unwrap().id;
    let conn = DialableConnection::new(remote_id);
    let endpoint = Arc::new(FakeEndpoint {
        connect: connect_yields(conn.clone()),
        ..Default::default()
    });
    let connections = Arc::new(RwLock::new(HashMap::new()));
    let transport = build_transport(
        endpoint.clone(),
        build_recording_handler().handler,
        connections.clone(),
        Arc::new(RwLock::new(Some(fake_local_url()))),
        IrohTransportConfig::default(),
    );

    transport.dial(remote_url.clone()).await.unwrap();
    transport.dial(remote_url.clone()).await.unwrap();

    assert_eq!(
        endpoint.connect_targets.lock().unwrap().len(),
        1,
        "second dial must reuse the first connection"
    );
    assert_eq!(connections.read().unwrap().len(), 1);
    assert!(connections.read().unwrap().contains_key(&remote_url));
}

/// A dial sends the preflight and nothing else.
#[tokio::test]
async fn dial_sends_only_the_preflight() {
    let remote_url = remote_url();
    let remote_id = endpoint_from_url(&remote_url).unwrap().id;
    let conn = DialableConnection::new(remote_id);
    let endpoint = Arc::new(FakeEndpoint {
        connect: connect_yields(conn.clone()),
        ..Default::default()
    });
    let transport = build_transport(
        endpoint,
        build_recording_handler().handler,
        Arc::new(RwLock::new(HashMap::new())),
        Arc::new(RwLock::new(Some(fake_local_url()))),
        IrohTransportConfig::default(),
    );

    transport.dial(remote_url).await.unwrap();

    let written = conn.send_stream.get_written_data();
    assert_eq!(written.len(), 1, "exactly one frame must be written");
    assert!(written[0].len() >= FRAME_HEADER_LEN);
    assert_eq!(written[0][0], FrameType::Preflight as u8);
}
