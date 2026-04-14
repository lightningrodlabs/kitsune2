//! LAN-local peer discovery wrapper.
//!
//! Single-file iroh surface for mDNS-based local discovery. All iroh API
//! churn related to local discovery should live here so a future iroh bump
//! touches one file.
//!
//! In iroh 0.95.1 this uses `iroh::discovery::mdns::MdnsDiscovery` (backed by
//! the `swarm-discovery` crate). The `discovery-local-network` feature on the
//! `iroh` dep must be enabled; in this crate that is gated behind the `mdns`
//! cargo feature.

/// Attach an mDNS-based LAN discovery service to the given iroh endpoint
/// builder. Returns the builder unchanged if the `mdns` feature is off or if
/// `enabled` is false.
#[cfg(feature = "mdns")]
pub(crate) fn maybe_enable_lan_discovery(
    builder: iroh::endpoint::Builder,
    enabled: bool,
) -> iroh::endpoint::Builder {
    if enabled {
        builder.discovery(iroh::discovery::mdns::MdnsDiscovery::builder())
    } else {
        builder
    }
}

/// Stub used when the `mdns` cargo feature is disabled.
#[cfg(not(feature = "mdns"))]
pub(crate) fn maybe_enable_lan_discovery(
    builder: iroh::endpoint::Builder,
    _enabled: bool,
) -> iroh::endpoint::Builder {
    builder
}

/// Validate that the given config is consistent with the compiled-in
/// feature set. Call from config validation.
pub(crate) fn validate_lan_discovery_config(
    enable_lan_discovery: bool,
) -> Result<(), &'static str> {
    #[cfg(not(feature = "mdns"))]
    if enable_lan_discovery {
        return Err(
            "enable_lan_discovery requires the `mdns` cargo feature on kitsune2_transport_iroh",
        );
    }
    let _ = enable_lan_discovery;
    Ok(())
}

#[cfg(all(test, feature = "mdns"))]
mod tests {
    use super::*;
    use iroh::{Endpoint, RelayMode};

    // Sanity: with the `mdns` feature on, an endpoint binds with discovery
    // registered. We do not assert cross-node discovery here — that is the
    // job of the LAN integration smoke test, which is gated on a network
    // environment variable because mDNS requires a real interface.
    #[tokio::test]
    async fn mdns_discovery_attaches() {
        let builder = Endpoint::empty_builder(RelayMode::Disabled);
        let builder = maybe_enable_lan_discovery(builder, true);
        let ep = builder.bind().await.expect("bind");
        assert!(!ep.discovery().is_empty(), "discovery should be registered");
        ep.close().await;
    }
}
