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
//!
//! mDNS answers are unauthenticated, so the transport keeps only addresses
//! a LAN peer could actually have from the pre-resolve ([`is_lan_scoped`]);
//! an answer must not be able to steer a dial at an arbitrary public
//! address. iroh's own in-connect lookup applies no such filter. In both
//! cases the QUIC handshake pins the peer's `EndpointId`, so a spoofed
//! address can only waste a connect attempt or bounce traffic off a third
//! party — a DoS/reflection concern, not an impersonation one.
//!
//! Two IPv6 cases are deliberately outside the filter. Link-local
//! (`fe80::/10`) addresses arrive from the lookup without a scope id and
//! cannot be dialled, and a failed dial would mark the peer unresponsive,
//! so they are dropped. A LAN numbered with global-unicast addresses
//! (SLAAC from a delegated prefix) is not recognised as a LAN, so the
//! relay-down bypass does not serve it; the relay-up path, where iroh's
//! in-connect lookup is unfiltered, is unaffected. Both are known
//! limitations.

use std::time::Duration;

/// How long a dial that would otherwise be skipped waits for the LAN
/// discovery cache before concluding there is no LAN path. The mDNS service
/// answers from its in-memory cache, so a hit arrives almost immediately;
/// this bound only limits the no-answer case (peer not present on this
/// LAN), where the service stays silent for up to ten seconds.
pub(crate) const LAN_LOOKUP_TIMEOUT: Duration = Duration::from_millis(300);

/// Ask the endpoint's address lookup services (mDNS) for direct IP
/// addresses of the given peer, as answered.
///
/// Returns the addresses from the first lookup item that carries at least
/// one IP address, or an empty list if none arrives within `timeout`. The
/// mDNS service answers from its passive cache, so a known LAN peer resolves
/// almost immediately; for an unknown peer it stays silent, which is what
/// the timeout bounds. Which of the answers to trust is the caller's call.
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
                        .filter(|addr| addr.is_ip())
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

/// Whether `ip` is one a peer on the same LAN could hold and that this
/// node can dial: RFC 1918 private, link-local or carrier-grade NAT
/// (`100.64.0.0/10`) for IPv4, unique-local (`fc00::/7`) for IPv6.
/// IPv4-mapped IPv6 addresses are judged by the IPv4 they carry. IPv6
/// link-local is excluded on purpose: without a scope id it is not
/// dialable (see the module doc).
pub(crate) fn is_lan_scoped(ip: std::net::IpAddr) -> bool {
    use std::net::{IpAddr, Ipv4Addr};
    fn v4_lan(v4: Ipv4Addr) -> bool {
        let [a, b, _, _] = v4.octets();
        let cgnat = a == 100 && (64..128).contains(&b);
        v4.is_private() || v4.is_link_local() || cgnat
    }
    match ip {
        IpAddr::V4(v4) => v4_lan(v4),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => v4_lan(v4),
            None => v6.is_unique_local(),
        },
    }
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

#[cfg(test)]
mod scope_tests {
    use super::is_lan_scoped;

    #[test]
    fn lan_scoped_admits_only_dialable_local_addresses() {
        let cases: &[(&str, bool)] = &[
            ("10.0.0.1", true),
            ("172.16.0.1", true),
            ("172.31.255.254", true),
            ("192.168.1.20", true),
            ("169.254.7.7", true),
            ("100.64.0.1", true),
            ("100.127.255.254", true),
            ("fd00::20", true),
            ("fc00::1", true),
            ("::ffff:192.168.1.20", true),
            ("::ffff:100.100.1.1", true),
            ("100.63.255.255", false),
            ("100.128.0.0", false),
            ("172.32.0.1", false),
            ("8.8.8.8", false),
            ("203.0.113.9", false),
            ("127.0.0.1", false),
            ("0.0.0.0", false),
            ("fe80::1", false),
            ("2001:db8::20", false),
            ("::1", false),
            ("::ffff:8.8.8.8", false),
        ];
        for (ip, expected) in cases {
            let ip: std::net::IpAddr = ip.parse().unwrap();
            assert_eq!(is_lan_scoped(ip), *expected, "{ip}");
        }
    }
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
