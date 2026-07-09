//! Tests for transport behavior when the configured relay is unreachable.
//!
//! Offline LAN operation requires two things: a node must have a peer URL
//! even though the relay handshake never completes (the URL is derived from
//! the configured relay), and dialing a peer must succeed over the
//! mDNS-discovered direct path instead of blocking on the dead relay.
#![cfg(feature = "mdns")]

use bytes::Bytes;
use kitsune2_api::{Builder, DynTransport, DynTxHandler};
use kitsune2_test_utils::{
    enable_tracing, retry_fn_until_timeout, space::TEST_SPACE_ID,
};
use kitsune2_transport_iroh::test_utils::{MockTxHandler, dummy_url};
use kitsune2_transport_iroh::{
    IrohTransportConfig, IrohTransportFactory, IrohTransportModConfig,
};
use std::sync::Arc;

/// A relay URL that accepts no connections (TCP discard port).
const UNREACHABLE_RELAY: &str = "https://127.0.0.1:9/relay";

async fn build_offline_transport(handler: DynTxHandler) -> DynTransport {
    let builder = Builder {
        transport: IrohTransportFactory::create(),
        ..kitsune2_core::default_test_builder()
    }
    .with_default_config()
    .unwrap();
    builder
        .config
        .set_module_config(&IrohTransportModConfig {
            iroh_transport: IrohTransportConfig {
                relay_url: Some(UNREACHABLE_RELAY.to_string()),
                enable_lan_discovery: true,
                connect_timeout_s: 5,
                ..Default::default()
            },
        })
        .unwrap();
    let builder = Arc::new(builder);
    builder
        .transport
        .create(builder.clone(), handler)
        .await
        .unwrap()
}

/// The peer URL must be announced from the configured relay URL alone; a
/// node that can never reach its relay still needs to be addressable so
/// that LAN peers can dial it.
#[tokio::test]
async fn peer_url_announced_when_relay_unreachable() {
    enable_tracing();

    let handler = Arc::new(MockTxHandler::default());
    let _ep = build_offline_transport(handler.clone()).await;

    retry_fn_until_timeout(
        || async { handler.current_url.lock().unwrap().clone() != dummy_url() },
        Some(5000),
        Some(100),
    )
    .await
    .expect("peer URL should be announced without a relay connection");

    let url = handler.current_url.lock().unwrap().clone();
    assert!(
        url.as_str().starts_with("https://127.0.0.1:9/relay/"),
        "peer URL should be derived from the configured relay, got {url}"
    );
}

/// Full offline-LAN smoke test: two nodes whose relay is unreachable must
/// still exchange messages in both directions via mDNS-discovered direct
/// paths.
///
/// mDNS requires a real multicast-capable network interface, which CI
/// runners often lack, so this test only runs when KITSUNE2_LAN_TEST is
/// set.
#[tokio::test(flavor = "multi_thread")]
async fn offline_lan_send_without_relay() {
    if std::env::var("KITSUNE2_LAN_TEST").is_err() {
        eprintln!(
            "skipping offline_lan_send_without_relay: set KITSUNE2_LAN_TEST=1 to run"
        );
        return;
    }
    enable_tracing();

    let (notify_sender_1, mut notify_receiver_1) =
        tokio::sync::mpsc::unbounded_channel();
    let handler_1 = Arc::new(MockTxHandler {
        recv_space_notify: Arc::new(move |_peer, _space_id, data| {
            notify_sender_1.send(data).unwrap();
            Ok(())
        }),
        ..Default::default()
    });
    let ep_1 = build_offline_transport(handler_1.clone()).await;
    ep_1.register_space_handler(TEST_SPACE_ID, handler_1.clone());

    let (notify_sender_2, mut notify_receiver_2) =
        tokio::sync::mpsc::unbounded_channel();
    let handler_2 = Arc::new(MockTxHandler {
        recv_space_notify: Arc::new(move |_peer, _space_id, data| {
            notify_sender_2.send(data).unwrap();
            Ok(())
        }),
        ..Default::default()
    });
    let ep_2 = build_offline_transport(handler_2.clone()).await;
    ep_2.register_space_handler(TEST_SPACE_ID, handler_2.clone());

    // Both peer URLs derive from the configured relay, no connection needed.
    retry_fn_until_timeout(
        || async {
            handler_1.current_url.lock().unwrap().clone() != dummy_url()
                && handler_2.current_url.lock().unwrap().clone() != dummy_url()
        },
        Some(5000),
        Some(100),
    )
    .await
    .expect("peer URLs should be announced");

    let url_1 = handler_1.current_url.lock().unwrap().clone();
    let url_2 = handler_2.current_url.lock().unwrap().clone();

    // mDNS discovery needs a moment to advertise and browse; retry the
    // send until the direct path is dialable.
    retry_fn_until_timeout(
        || async {
            ep_1.send_space_notify(
                url_2.clone(),
                TEST_SPACE_ID,
                Bytes::from_static(b"ping"),
            )
            .await
            .is_ok()
        },
        Some(60_000),
        Some(1000),
    )
    .await
    .expect("send over LAN should succeed with the relay unreachable");

    let received = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        notify_receiver_2.recv(),
    )
    .await
    .expect("ep_2 should receive the notify")
    .unwrap();
    assert_eq!(received, Bytes::from_static(b"ping"));

    // Reverse direction: the receiving node must also be able to
    // originate connections.
    retry_fn_until_timeout(
        || async {
            ep_2.send_space_notify(
                url_1.clone(),
                TEST_SPACE_ID,
                Bytes::from_static(b"pong"),
            )
            .await
            .is_ok()
        },
        Some(60_000),
        Some(1000),
    )
    .await
    .expect("reverse send over LAN should succeed");

    let received = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        notify_receiver_1.recv(),
    )
    .await
    .expect("ep_1 should receive the notify")
    .unwrap();
    assert_eq!(received, Bytes::from_static(b"pong"));
}
