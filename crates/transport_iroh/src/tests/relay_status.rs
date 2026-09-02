//! How the real iroh endpoint reports home relay state through the
//! [`Endpoint`](crate::endpoint::Endpoint) abstraction.
//!
//! iroh only selects a home relay it has managed to reach, so a relay that
//! refuses every connection never appears in `home_relay_status()` at all.
//! These tests pin down what the transport's two relay-state queries make
//! of that, since the unresponsive reconciliation depends on it.

use crate::endpoint::{Endpoint as _, IrohEndpoint};
use iroh::endpoint::presets::Minimal;
use iroh::{Endpoint, RelayMap, RelayMode, RelayUrl};
use kitsune2_test_utils::retry_fn_until_timeout;
use std::str::FromStr;
use std::time::Duration;

async fn bind_with_relay(relay_url: &str) -> IrohEndpoint {
    let relay = RelayUrl::from_str(relay_url).unwrap();
    let endpoint = Endpoint::builder(Minimal)
        .relay_mode(RelayMode::Custom(RelayMap::from_iter([relay])))
        .ca_tls_config(iroh_relay::tls::CaTlsConfig::insecure_skip_verify())
        .bind()
        .await
        .unwrap();
    IrohEndpoint::new(endpoint)
}

/// A relay that refuses connections is never selected, so it is neither
/// "connected" nor "known down": a dial goes ahead, and its failure is not
/// held against the peer.
#[tokio::test]
async fn unreachable_relay_is_neither_connected_nor_known_down() {
    let endpoint = bind_with_relay("https://127.0.0.1:9/relay/").await;

    // Give iroh time to fail a few connection attempts.
    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(!endpoint.is_home_relay_connected());
    assert!(!endpoint.is_home_relay_known_down());
}

/// A reachable relay becomes the connected home relay.
#[tokio::test(flavor = "multi_thread")]
async fn reachable_relay_reports_connected() {
    use kitsune2_test_utils::bootstrap::TestBootstrapSrv;

    let bootstrap_server = TestBootstrapSrv::new(false).await;
    let relay_url = format!("{}/relay/", bootstrap_server.addr());
    let endpoint = bind_with_relay(&relay_url).await;

    retry_fn_until_timeout(
        || async { endpoint.is_home_relay_connected() },
        Some(10_000),
        Some(100),
    )
    .await
    .expect("home relay should connect");
    assert!(!endpoint.is_home_relay_known_down());
}
