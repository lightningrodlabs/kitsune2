//! LAN-local peer discovery wrapper.
//!
//! Single-file iroh surface for mDNS-based local discovery, and the only
//! place in this crate that names the `iroh_mdns_address_lookup` crate, so
//! that an iroh bump touches one file.
//!
//! With the `mdns` cargo feature, an `MdnsAddressLookup` is attached to the
//! endpoint as one of its address lookup services. iroh consults those
//! services by itself while a connect is in flight and no path has been
//! validated yet, so a dial to a peer that is only known by its relay URL
//! still finds the LAN path without any help from this crate.

/// Attach an mDNS-based LAN discovery service to the given iroh endpoint
/// builder. Returns the builder unchanged if the `mdns` feature is off or if
/// `enabled` is false.
#[cfg(feature = "mdns")]
pub(crate) fn maybe_enable_lan_discovery(
    builder: iroh::endpoint::Builder,
    enabled: bool,
) -> iroh::endpoint::Builder {
    if enabled {
        builder.address_lookup(
            iroh_mdns_address_lookup::MdnsAddressLookup::builder(),
        )
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
    use iroh::endpoint::presets::Minimal;
    use iroh::{Endpoint, RelayMode};

    // Sanity: with the `mdns` feature on, an endpoint binds with the lookup
    // service registered. Cross-node discovery is not asserted here — that
    // is the job of the LAN integration test, which is gated on a network
    // environment variable because mDNS requires a real interface.
    #[tokio::test]
    async fn mdns_discovery_attaches() {
        let builder =
            Endpoint::builder(Minimal).relay_mode(RelayMode::Disabled);
        let builder = maybe_enable_lan_discovery(builder, true);
        let ep = builder.bind().await.expect("bind");
        assert!(
            !ep.address_lookup().expect("endpoint open").is_empty(),
            "address lookup should be registered"
        );
        ep.close().await;
    }
}
