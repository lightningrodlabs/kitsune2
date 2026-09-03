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
//! a LAN peer could actually have from the pre-resolve ([`is_on_link`]);
//! an answer must not be able to steer a dial at an arbitrary public
//! address. iroh's own in-connect lookup applies no such filter. In both
//! cases the QUIC handshake pins the peer's `EndpointId`, so a spoofed
//! address can only waste a connect attempt or bounce traffic off a third
//! party — a DoS/reflection concern, not an impersonation one.
//!
//! The filter admits two kinds of address. The first are the ranges that
//! are on-link by definition ([`is_lan_scoped`]): RFC 1918, IPv4
//! link-local and IPv6 unique-local. The second are IPv6 global-unicast
//! addresses that share a `/64` with one of this node's own global-unicast
//! addresses: a LAN numbered by SLAAC from a delegated prefix has no
//! private range to recognise, and the only thing that separates its
//! hosts from the rest of the internet is the prefix this node was itself
//! configured with. `/64` is the SLAAC subnet size and the only prefix
//! length available without reading interface configuration; other
//! subnets of the same delegation (a `/56` split into several `/64`s)
//! are therefore not treated as on-link even though they may be one
//! router hop away. IPv4 has no such rule: a public-v4 LAN cannot be
//! told apart from the internet without prefix lengths, and RFC 1918
//! covers the LANs that exist in practice.
//!
//! Two cases are deliberately outside the filter. IPv6 link-local
//! (`fe80::/10`) addresses arrive from the lookup without a scope id and
//! cannot be dialled, and a failed dial would mark the peer unresponsive,
//! so they are dropped even when the local set holds a matching one.
//! Carrier-grade NAT space (`100.64.0.0/10`) is shared by an ISP's
//! customers, not on-link, so a forged record could steer a dial at any
//! host behind the same carrier; a LAN numbered from it is not
//! recognised. Neither is served by the relay-down bypass; the relay-up
//! path, where iroh's in-connect lookup is unfiltered, is unaffected.
//! Both are known limitations.
//!
//! The mDNS service joins the multicast group once, on the interfaces
//! that exist when it is built, and never follows interface changes. A
//! node that starts without a usable interface and gains one later would
//! therefore never announce or hear iroh records on it. To cover that,
//! [`maybe_spawn_lan_rebind_task`] watches the endpoint's local IP set and,
//! when it changes, rebuilds the lookup service after a short debounce
//! (`LAN_REBIND_DEBOUNCE`); the fresh service joins the group on the
//! interfaces present now and iroh republishes the current addresses to
//! it. The rebuild replaces the endpoint's whole service list, which is
//! sound because the transport builds its endpoint from the `Minimal`
//! preset and the mDNS lookup is the only service it ever attaches.

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
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

/// Whether `ip` lies in a range that is on-link by definition and that
/// this node can dial: RFC 1918 private or link-local for IPv4,
/// unique-local (`fc00::/7`) for IPv6. IPv4-mapped IPv6 addresses are
/// judged by the IPv4 they carry. IPv6 link-local and carrier-grade NAT
/// space are excluded on purpose (see the module doc).
pub(crate) fn is_lan_scoped(ip: IpAddr) -> bool {
    fn v4_lan(v4: Ipv4Addr) -> bool {
        v4.is_private() || v4.is_link_local()
    }
    match ip {
        IpAddr::V4(v4) => v4_lan(v4),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => v4_lan(v4),
            None => v6.is_unique_local(),
        },
    }
}

/// Whether `v6` is an IPv6 global-unicast address (`2000::/3`). The block
/// excludes loopback, unspecified, IPv4-mapped, unique-local, link-local
/// and multicast by construction, so the top three bits are the whole
/// test.
fn is_global_unicast_v6(v6: Ipv6Addr) -> bool {
    v6.segments()[0] & 0xe000 == 0x2000
}

/// Whether two IPv6 addresses share their first 64 bits, the SLAAC
/// subnet size.
fn same_slaac_subnet(a: Ipv6Addr, b: Ipv6Addr) -> bool {
    a.segments()[..4] == b.segments()[..4]
}

/// Whether `candidate` is an address a peer on one of this node's LANs
/// could hold, given the node's own addresses `local_ips`.
///
/// True for anything [`is_lan_scoped`] accepts, and for an IPv6
/// global-unicast candidate that shares a `/64` with one of this node's
/// global-unicast addresses. The `/64` is the SLAAC assumption spelled out
/// in the module doc: a wider delegation's other subnets do not match.
/// IPv4 gets no on-link rule; `local_ips` only ever widens the IPv6 case.
pub(crate) fn is_on_link(
    candidate: IpAddr,
    local_ips: &BTreeSet<IpAddr>,
) -> bool {
    if is_lan_scoped(candidate) {
        return true;
    }
    let IpAddr::V6(candidate) = candidate else {
        return false;
    };
    if !is_global_unicast_v6(candidate) {
        return false;
    }
    local_ips.iter().any(|local| match local {
        IpAddr::V6(local) => {
            is_global_unicast_v6(*local) && same_slaac_subnet(*local, candidate)
        }
        IpAddr::V4(_) => false,
    })
}

/// How long the local IP set has to stay unchanged before the LAN
/// discovery service is rebuilt. An interface coming up typically fires
/// several address updates in quick succession (v4, v6, temporary
/// addresses); the window folds them into one rebuild.
#[cfg(feature = "mdns")]
pub(crate) const LAN_REBIND_DEBOUNCE: Duration = Duration::from_secs(1);

/// The set of local IP addresses in an endpoint address, ignoring ports
/// and relay entries. Ports change with every socket rebind while the
/// interfaces stay the same, and relays say nothing about the LAN, so
/// neither is part of what decides a rebind or what counts as on-link.
pub(crate) fn local_ip_set(addr: &iroh::EndpointAddr) -> BTreeSet<IpAddr> {
    addr.ip_addrs().map(|sock| sock.ip()).collect()
}

/// Whether the local IP set changed in a way that calls for rebuilding
/// the LAN discovery service: any difference counts, because a lost
/// address may mean an interface went down and lost its multicast
/// membership, and a new one is an interface the service never joined.
#[cfg(any(test, feature = "mdns"))]
pub(crate) fn rebind_needed(
    prev: &BTreeSet<IpAddr>,
    next: &BTreeSet<IpAddr>,
) -> bool {
    prev != next
}

/// Replace the endpoint's mDNS lookup service with a freshly built one.
///
/// The new service is built before the old one is removed, so a build
/// failure leaves the endpoint as it was. iroh publishes the last known
/// endpoint data to the new service as soon as it is added, so the
/// current addresses are announced on the interfaces present now.
#[cfg(feature = "mdns")]
pub(crate) fn rebind_lan_discovery(
    endpoint: &iroh::Endpoint,
) -> kitsune2_api::K2Result<()> {
    use kitsune2_api::K2Error;

    let services = endpoint.address_lookup().map_err(|err| {
        K2Error::other_src("endpoint closed, cannot rebind LAN discovery", err)
    })?;
    let fresh = iroh_mdns_address_lookup::MdnsAddressLookup::builder()
        .build(endpoint.id())
        .map_err(|err| {
            K2Error::other_src("failed to build the LAN discovery service", err)
        })?;
    debug_assert_eq!(
        services.len(),
        1,
        "the mDNS lookup must be the endpoint's only address lookup service"
    );
    services.clear();
    services.add(fresh);
    Ok(())
}

/// Watch the endpoint's local IP set and call `rebind` once per debounced
/// change. The first observed value never triggers a rebind: the service
/// built at bind time already covers the interfaces present then.
///
/// `rebind` is injectable so that tests can count rebuilds; production
/// passes [`rebind_lan_discovery`]. A failed rebind is logged and the next
/// change retries it. The task runs until it is aborted; the watcher only
/// disconnects when the last endpoint clone is dropped, and this task
/// holds one.
#[cfg(feature = "mdns")]
pub(crate) fn spawn_lan_rebind_task(
    endpoint: iroh::Endpoint,
    debounce: Duration,
    rebind: impl Fn(&iroh::Endpoint) -> kitsune2_api::K2Result<()> + Send + 'static,
) -> tokio::task::AbortHandle {
    use n0_watcher::Watcher;
    use tokio::time::{Instant, sleep_until};

    let mut watcher = endpoint.watch_addr();
    tokio::spawn(async move {
        let mut prev = local_ip_set(&watcher.get());
        loop {
            let mut next = match watcher.updated().await {
                Ok(addr) => local_ip_set(&addr),
                Err(_) => return,
            };
            if !rebind_needed(&prev, &next) {
                continue;
            }
            // Absorb the burst: every further change restarts the window.
            let mut deadline = Instant::now() + debounce;
            loop {
                tokio::select! {
                    _ = sleep_until(deadline) => break,
                    updated = watcher.updated() => match updated {
                        Ok(addr) => {
                            let seen = local_ip_set(&addr);
                            if seen != next {
                                next = seen;
                                deadline = Instant::now() + debounce;
                            }
                        }
                        Err(_) => return,
                    },
                }
            }
            match rebind(&endpoint) {
                Ok(()) => tracing::info!(
                    local_ips = ?next,
                    "LAN discovery rebound after a local address change"
                ),
                Err(err) => tracing::warn!(
                    ?err,
                    local_ips = ?next,
                    "LAN discovery rebind failed, will retry on the next address change"
                ),
            }
            prev = next;
        }
    })
    .abort_handle()
}

/// Start the address-change rebind task for an endpoint that has LAN
/// discovery enabled. Returns `None` when LAN discovery is off.
#[cfg(feature = "mdns")]
pub(crate) fn maybe_spawn_lan_rebind_task(
    endpoint: &iroh::Endpoint,
    enabled: bool,
) -> Option<tokio::task::AbortHandle> {
    enabled.then(|| {
        spawn_lan_rebind_task(
            endpoint.clone(),
            LAN_REBIND_DEBOUNCE,
            rebind_lan_discovery,
        )
    })
}

/// Stub used when the `mdns` cargo feature is disabled.
#[cfg(not(feature = "mdns"))]
pub(crate) fn maybe_spawn_lan_rebind_task(
    _endpoint: &iroh::Endpoint,
    _enabled: bool,
) -> Option<tokio::task::AbortHandle> {
    None
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
    use super::{is_lan_scoped, is_on_link};
    use std::collections::BTreeSet;
    use std::net::IpAddr;

    fn ips(list: &[&str]) -> BTreeSet<IpAddr> {
        list.iter().map(|ip| ip.parse().unwrap()).collect()
    }

    #[test]
    fn lan_scoped_admits_only_dialable_local_addresses() {
        let cases: &[(&str, bool)] = &[
            ("10.0.0.1", true),
            ("172.16.0.1", true),
            ("172.31.255.254", true),
            ("192.168.1.20", true),
            ("169.254.7.7", true),
            ("fd00::20", true),
            ("fc00::1", true),
            ("::ffff:192.168.1.20", true),
            ("100.64.0.1", false),
            ("100.127.255.254", false),
            ("::ffff:100.100.1.1", false),
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

    // The on-link rule widens the filter only for a global-unicast IPv6
    // candidate on one of our own /64s; everything else is decided as by
    // `is_lan_scoped`, whatever the local set holds.
    #[test]
    fn on_link_admits_global_ipv6_on_our_slaac_subnet() {
        let local = ips(&["2001:db8:1:2::10", "192.168.1.20"]);
        let no_v6 = ips(&["192.168.1.20"]);
        let link_local_only = ips(&["fe80::1"]);
        let empty = ips(&[]);
        let cases: &[(&str, &BTreeSet<IpAddr>, bool)] = &[
            // Same /64 as our global address.
            ("2001:db8:1:2::20", &local, true),
            ("2001:db8:1:2:abcd:ef01:2345:6789", &local, true),
            // Another /64, including a sibling subnet of the same /56.
            ("2001:db8:1:3::20", &local, false),
            ("2001:db8:9:2::20", &local, false),
            // Global candidate with no local global v6 to match against.
            ("2001:db8:1:2::20", &no_v6, false),
            ("2001:db8:1:2::20", &empty, false),
            // Ranges on-link by definition need no local match.
            ("fd00::20", &empty, true),
            ("192.168.1.20", &empty, true),
            ("10.0.0.1", &no_v6, true),
            // IPv6 link-local stays out even when it matches a local one.
            ("fe80::1", &link_local_only, false),
            ("fe80::20", &link_local_only, false),
            // Carrier-grade NAT stays out.
            ("100.64.0.1", &local, false),
            // An IPv4-mapped candidate is judged as its IPv4.
            ("::ffff:192.168.1.20", &empty, true),
            ("::ffff:8.8.8.8", &local, false),
            // Public IPv4 has no on-link rule.
            ("203.0.113.9", &local, false),
            // Non-global v6 that is not ULA never matches.
            ("::1", &local, false),
            ("ff02::1", &local, false),
        ];
        for (ip, local, expected) in cases {
            let ip: IpAddr = ip.parse().unwrap();
            assert_eq!(is_on_link(ip, local), *expected, "{ip} vs {local:?}");
        }
    }
}

#[cfg(test)]
mod rebind_tests {
    use super::{local_ip_set, rebind_needed};
    use iroh::{EndpointAddr, RelayUrl, TransportAddr};
    use std::collections::BTreeSet;
    use std::net::IpAddr;

    fn ips(list: &[&str]) -> BTreeSet<IpAddr> {
        list.iter().map(|ip| ip.parse().unwrap()).collect()
    }

    #[test]
    fn local_ip_set_keeps_ips_only() {
        let id = iroh::SecretKey::from_bytes(&[3u8; 32]).public();
        let relay: RelayUrl = "https://relay.example/".parse().unwrap();
        let addr = EndpointAddr::from_parts(
            id,
            [
                TransportAddr::Relay(relay),
                TransportAddr::Ip("192.168.1.20:4433".parse().unwrap()),
                TransportAddr::Ip("192.168.1.20:5000".parse().unwrap()),
                TransportAddr::Ip("[fd00::20]:4433".parse().unwrap()),
            ],
        );
        assert_eq!(local_ip_set(&addr), ips(&["192.168.1.20", "fd00::20"]));
        assert!(local_ip_set(&EndpointAddr::new(id)).is_empty());
    }

    #[test]
    fn rebind_needed_on_any_set_difference() {
        let a = ips(&["192.168.1.20"]);
        let b = ips(&["192.168.1.20", "10.0.0.5"]);
        let c = ips(&[]);
        assert!(!rebind_needed(&a, &a));
        assert!(!rebind_needed(&c, &c));
        assert!(rebind_needed(&a, &b));
        assert!(rebind_needed(&b, &a));
        assert!(rebind_needed(&c, &a));
        assert!(rebind_needed(&a, &c));
    }
}

#[cfg(all(test, feature = "mdns"))]
mod tests {
    use super::*;
    use iroh::endpoint::presets::Minimal;
    use iroh::{Endpoint, RelayMode};
    use n0_watcher::Watcher;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    async fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !cond() {
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for {what}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    // The rebind task must ignore the initial address, rebuild the lookup
    // once per debounced change, and leave exactly one service behind. The
    // address change is driven with `add_external_addr`, which feeds the
    // same `ip_addrs` watcher that an interface change does.
    #[tokio::test]
    async fn rebinds_once_per_debounced_address_change() {
        let builder =
            Endpoint::builder(Minimal).relay_mode(RelayMode::Disabled);
        let builder = maybe_enable_lan_discovery(builder, true);
        let ep = builder.bind().await.expect("bind");

        let debounce = Duration::from_millis(200);
        let rebinds = Arc::new(AtomicUsize::new(0));
        let counter = rebinds.clone();
        let task = spawn_lan_rebind_task(ep.clone(), debounce, move |ep| {
            counter.fetch_add(1, Ordering::SeqCst);
            rebind_lan_discovery(ep)
        });

        tokio::time::sleep(debounce * 3).await;
        assert_eq!(
            rebinds.load(Ordering::SeqCst),
            0,
            "the initial address must not trigger a rebind"
        );

        let first: std::net::SocketAddr = "192.0.2.9:4433".parse().unwrap();
        ep.add_external_addr(first).await;
        wait_for("the external addr to surface in watch_addr", || {
            ep.watch_addr().get().ip_addrs().any(|sock| *sock == first)
        })
        .await;
        wait_for("the first rebind", || rebinds.load(Ordering::SeqCst) == 1)
            .await;
        tokio::time::sleep(debounce * 2).await;
        assert_eq!(rebinds.load(Ordering::SeqCst), 1);
        assert_eq!(
            ep.address_lookup().expect("endpoint open").len(),
            1,
            "the rebuild must leave exactly one lookup service"
        );

        // Two changes inside one debounce window fold into one rebind.
        ep.add_external_addr("192.0.2.10:4433".parse().unwrap())
            .await;
        tokio::time::sleep(debounce / 4).await;
        ep.add_external_addr("192.0.2.11:4433".parse().unwrap())
            .await;
        wait_for("the second rebind", || rebinds.load(Ordering::SeqCst) == 2)
            .await;
        tokio::time::sleep(debounce * 2).await;
        assert_eq!(
            rebinds.load(Ordering::SeqCst),
            2,
            "rapid changes must be debounced into one rebind"
        );
        assert_eq!(ep.address_lookup().expect("endpoint open").len(), 1);

        task.abort();
        ep.close().await;
    }
}
