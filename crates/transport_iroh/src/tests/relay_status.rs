//! How the real iroh endpoint reports home relay state through the
//! [`Endpoint`](crate::endpoint::Endpoint) abstraction.
//!
//! iroh only selects a home relay it has managed to reach, so a relay that
//! refuses every connection never appears in `home_relay_status()` at all.
//! The relay-down guard on the dial path depends on that: an unreachable
//! relay must not read as "known down", or every dial from a node whose
//! relay is unreachable would be skipped before LAN discovery gets a say.

use crate::endpoint::{Endpoint as _, IrohEndpoint};
use iroh::endpoint::presets::Minimal;
use iroh::{Endpoint, RelayMap, RelayMode, RelayUrl};
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

/// A relay that refuses connections is never selected, so it is not "known
/// down" either: dials go ahead.
#[tokio::test]
async fn unreachable_relay_is_not_known_down() {
    let endpoint = bind_with_relay("https://127.0.0.1:9/relay/").await;

    // Give iroh time to fail a few connection attempts.
    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(!endpoint.is_home_relay_known_down());
}
