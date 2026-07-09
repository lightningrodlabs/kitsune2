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

/// How long a dial waits for the LAN discovery cache to answer before
/// proceeding with whatever addresses are already in the dial target. The
/// mDNS service answers from its in-memory cache, so a hit arrives almost
/// immediately; this bound only limits the no-answer case (peer not
/// present on this LAN).
pub(crate) const RESOLVE_DIRECT_ADDRS_TIMEOUT: std::time::Duration =
    std::time::Duration::from_millis(500);

/// Ask the endpoint's discovery services (mDNS) for direct IP addresses of
/// the given peer.
///
/// Returns the addresses from the first discovery item that contains at
/// least one IP address, or an empty list if no discovery service reports
/// one within `timeout`.
#[cfg(feature = "mdns")]
pub(crate) async fn resolve_direct_addrs(
    endpoint: &iroh::Endpoint,
    endpoint_id: iroh::EndpointId,
    timeout: std::time::Duration,
) -> Vec<iroh::TransportAddr> {
    use futures::StreamExt;
    use iroh::discovery::Discovery;

    let Some(mut stream) = endpoint.discovery().resolve(endpoint_id) else {
        return Vec::new();
    };

    let first_ip_addrs = async {
        while let Some(item) = stream.next().await {
            match item {
                Ok(item) => {
                    let addrs: Vec<iroh::TransportAddr> = item
                        .into_endpoint_addr()
                        .addrs
                        .into_iter()
                        .filter(|addr| {
                            matches!(addr, iroh::TransportAddr::Ip(_))
                        })
                        .collect();
                    if !addrs.is_empty() {
                        return addrs;
                    }
                }
                Err(err) => {
                    tracing::debug!(
                        ?err,
                        %endpoint_id,
                        "LAN discovery resolve error"
                    );
                }
            }
        }
        Vec::new()
    };

    tokio::time::timeout(timeout, first_ip_addrs)
        .await
        .unwrap_or_default()
}

/// Stub used when the `mdns` cargo feature is disabled.
#[cfg(not(feature = "mdns"))]
pub(crate) async fn resolve_direct_addrs(
    _endpoint: &iroh::Endpoint,
    _endpoint_id: iroh::EndpointId,
    _timeout: std::time::Duration,
) -> Vec<iroh::TransportAddr> {
    Vec::new()
}

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

    // Resolving a peer that no discovery service knows must come back
    // empty within the timeout bound rather than hanging the dial path.
    #[tokio::test]
    async fn resolve_unknown_peer_returns_empty_within_timeout() {
        let builder = Endpoint::empty_builder(RelayMode::Disabled);
        let builder = maybe_enable_lan_discovery(builder, true);
        let ep = builder.bind().await.expect("bind");

        let unknown_peer = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
        let timeout = std::time::Duration::from_millis(300);
        let start = std::time::Instant::now();
        let addrs = resolve_direct_addrs(&ep, unknown_peer, timeout).await;

        assert!(addrs.is_empty(), "unknown peer should yield no addresses");
        assert!(
            start.elapsed() < std::time::Duration::from_secs(3),
            "resolve must be bounded by the timeout"
        );
        ep.close().await;
    }
}
