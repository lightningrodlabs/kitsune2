//! QUIC address discovery (QAD) must survive a relay being (re-)inserted.
//!
//! A relay configured at endpoint creation gets QAD enabled by
//! `RelayMap::from_iter`. `configure_for_space` later inserts the relay a
//! second time through `do_insert_relay`, replacing the map entry. If that
//! entry carries no QAD config, iroh's net_report can no longer learn the
//! endpoint's public address, so every NAT-traversal round advertises LAN
//! candidates only and peers behind different NATs never go direct.

use super::*;

fn relay_url() -> RelayUrl {
    RelayUrl::from_str("http://127.0.0.1:1/relay/").unwrap()
}

#[test]
fn relay_config_enables_qad_with_and_without_token() {
    let url = relay_url();
    for token in [None, Some("bearer-token")] {
        let config = IrohTransport::relay_config_with_token(&url, token);
        assert!(
            config.quic.is_some(),
            "QAD must be enabled (token={token:?})"
        );
        assert_eq!(config.quic, RelayConfig::from(url.clone()).quic);
        assert_eq!(config.auth_token.as_deref(), token);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn do_insert_relay_keeps_qad_on_existing_relay() {
    let url = relay_url();
    let raw = Endpoint::builder(Minimal)
        .relay_mode(RelayMode::Custom(RelayMap::from_iter([url.clone()])))
        .bind()
        .await
        .unwrap();
    let endpoint: DynIrohEndpoint = Arc::new(IrohEndpoint::new(raw));

    IrohTransport::do_insert_relay(endpoint.clone(), url.to_string(), None)
        .await
        .unwrap();

    let config = endpoint
        .remove_relay(&url)
        .await
        .expect("relay should still be in the relay map");
    assert!(
        config.quic.is_some(),
        "re-inserting the relay must not disable QAD"
    );
    assert_eq!(config.quic, RelayConfig::from(url.clone()).quic);

    endpoint.close().await;
}
