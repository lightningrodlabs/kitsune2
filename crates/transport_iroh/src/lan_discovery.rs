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
//! still finds the LAN path without any help from this crate. The explicit
//! [`resolve_direct_addrs`] exists for the one case where the transport
//! wants to know *before* dialling whether a LAN path exists: when the home
//! relay is known to be down and the dial would otherwise be skipped.

use std::time::Duration;

/// How long a dial that would otherwise be skipped waits for the LAN
/// discovery cache before concluding there is no LAN path. The mDNS service
/// answers from its in-memory cache, so a hit arrives almost immediately;
/// this bound only limits the no-answer case (peer not present on this
/// LAN), where the service stays silent for up to ten seconds.
pub(crate) const LAN_LOOKUP_TIMEOUT: Duration = Duration::from_millis(300);

/// Ask the endpoint's address lookup services (mDNS) for direct IP
/// addresses of the given peer.
///
/// Returns the addresses from the first lookup item that carries at least
/// one IP address, or an empty list if none arrives within `timeout`. The
/// mDNS service answers from its passive cache, so a known LAN peer resolves
/// almost immediately; for an unknown peer it stays silent, which is what
/// the timeout bounds.
#[cfg(feature = "mdns")]
pub(crate) async fn resolve_direct_addrs(
    endpoint: &iroh::Endpoint,
    endpoint_id: iroh::EndpointId,
    timeout: Duration,
) -> Vec<iroh::TransportAddr> {
    use futures::StreamExt;

    let Ok(services) = endpoint.address_lookup() else {
        return Vec::new();
    };
    let mut stream = std::pin::pin!(services.resolve(endpoint_id));

    let first_ip_addrs = async {
        while let Some(next) = stream.next().await {
            match next {
                Ok(Ok(item)) => {
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
                Ok(Err(err)) => {
                    tracing::debug!(
                        ?err,
                        %endpoint_id,
                        "LAN discovery service reported an error"
                    );
                }
                Err(err) => {
                    tracing::debug!(
                        ?err,
                        %endpoint_id,
                        "LAN discovery lookup failed"
                    );
                    return Vec::new();
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
    _timeout: Duration,
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

    // The mDNS service stays silent for a peer it has never seen, so the
    // resolve must come back empty at the timeout bound rather than hanging.
    #[tokio::test]
    async fn resolve_unknown_peer_returns_empty_within_timeout() {
        let builder =
            Endpoint::builder(Minimal).relay_mode(RelayMode::Disabled);
        let builder = maybe_enable_lan_discovery(builder, true);
        let ep = builder.bind().await.expect("bind");

        let unknown_peer = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
        let start = std::time::Instant::now();
        let addrs =
            resolve_direct_addrs(&ep, unknown_peer, LAN_LOOKUP_TIMEOUT).await;

        assert!(addrs.is_empty(), "unknown peer should yield no addresses");
        assert!(
            start.elapsed() < Duration::from_secs(3),
            "resolve must be bounded by the timeout"
        );
        ep.close().await;
    }
}
