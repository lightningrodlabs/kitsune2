//! mDNS announce and browse glue around `mdns-sd`.
//!
//! An announcement carries two TXT fields: `spacefp`, the hex-encoded
//! [space fingerprint](crate::fingerprint), and `url`, this node's kitsune2
//! peer URL. The raw space id is never sent. The instance name is a random
//! token so that announcements do not correlate across sessions or spaces,
//! and the port is zero because nothing listens for this crate: the URL is
//! all a peer needs to dial us through the transport.
//!
//! Browsing yields [`DiscoveredPeer`]s: announcements that match our
//! fingerprint and are not our own. What to do with them is the browse
//! loop's business.

use crate::fingerprint::{self, SpaceFingerprint};
use kitsune2_api::{K2Error, K2Result, SpaceId, Url};
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use rand::Rng;
use std::net::IpAddr;
use std::sync::Mutex;

/// TXT record key carrying the hex-encoded space fingerprint.
pub const TXT_KEY_SPACE_FP: &str = "spacefp";

/// TXT record key carrying the announcing node's kitsune2 peer URL.
pub const TXT_KEY_URL: &str = "url";

/// A peer announced on the LAN that claims to be in our space.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredPeer {
    /// The peer URL to dial.
    pub url: Url,
    /// Full mDNS instance name, for de-duplication and logging.
    pub fullname: String,
}

/// One node's presence on the LAN for one space: an mDNS daemon, a fixed
/// instance name, and whichever peer URL is currently advertised under it.
///
/// Dropping the service withdraws the announcement and shuts the daemon
/// down.
pub struct MdnsService {
    daemon: ServiceDaemon,
    service_type: String,
    instance: String,
    fullname: String,
    fp_hex: String,
    addrs: Vec<IpAddr>,
    advertised: Mutex<Option<Url>>,
}

impl std::fmt::Debug for MdnsService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MdnsService")
            .field("fullname", &self.fullname)
            .field("advertised", &self.advertised.lock().ok().as_deref())
            .finish()
    }
}

impl Drop for MdnsService {
    fn drop(&mut self) {
        // Best effort: the daemon shutdown withdraws the record too.
        let _ = self.daemon.unregister(&self.fullname);
        let _ = self.daemon.shutdown();
    }
}

impl MdnsService {
    /// Start an mDNS daemon for `service_type`, announcing nothing yet.
    ///
    /// `addrs` are the local addresses the record will name once a URL is
    /// advertised; they are fixed for the life of the service.
    pub fn start(
        service_type: &str,
        space_id: &SpaceId,
        addrs: Vec<IpAddr>,
    ) -> K2Result<Self> {
        if addrs.is_empty() {
            return Err(K2Error::other("mdns: no local addresses to announce"));
        }
        let daemon = ServiceDaemon::new()
            .map_err(|e| K2Error::other_src("mdns daemon start", e))?;
        let instance = instance_name();
        let fullname = format!("{instance}.{service_type}");
        Ok(Self {
            daemon,
            service_type: service_type.to_string(),
            instance,
            fullname,
            fp_hex: hex::encode(fingerprint::space_fingerprint(space_id)),
            addrs,
            advertised: Mutex::new(None),
        })
    }

    /// Subscribe to announcements of our service type.
    pub fn browse(&self) -> K2Result<flume::Receiver<ServiceEvent>> {
        self.daemon
            .browse(&self.service_type)
            .map_err(|e| K2Error::other_src("mdns browse", e))
    }

    /// Announce `url` as this node's peer URL, replacing any earlier
    /// announcement. Announcing the URL already advertised is a no-op.
    pub fn advertise(&self, url: &Url) -> K2Result<()> {
        let mut advertised =
            self.advertised.lock().expect("mdns advertised poisoned");
        if advertised.as_ref() == Some(url) {
            return Ok(());
        }
        if advertised.is_some() {
            // Commands are handled by the daemon in order, so the fresh
            // record is registered only after the stale one is gone.
            self.daemon
                .unregister(&self.fullname)
                .map_err(|e| K2Error::other_src("mdns unregister", e))?;
            *advertised = None;
        }

        let host = format!("{}.local.", self.instance);
        let props = [
            (TXT_KEY_SPACE_FP, self.fp_hex.as_str()),
            (TXT_KEY_URL, url.as_str()),
        ];
        let info = ServiceInfo::new(
            &self.service_type,
            &self.instance,
            &host,
            &self.addrs[..],
            0,
            &props[..],
        )
        .map_err(|e| K2Error::other_src("mdns ServiceInfo::new", e))?;
        debug_assert_eq!(info.get_fullname(), self.fullname);

        self.daemon
            .register(info)
            .map_err(|e| K2Error::other_src("mdns register", e))?;
        *advertised = Some(url.clone());
        Ok(())
    }

    /// The full mDNS instance name of our own announcement.
    pub fn fullname(&self) -> &str {
        &self.fullname
    }
}

/// Extract a [`DiscoveredPeer`] from a browse event.
///
/// Only a resolved service that carries `expected_fp` under `spacefp` and a
/// parseable peer URL under `url` qualifies, and never our own announcement,
/// whether recognised by instance name or by the URL it names.
pub fn resolved_to_peer(
    event: &ServiceEvent,
    expected_fp: &SpaceFingerprint,
    self_fullname: &str,
    self_url: Option<&Url>,
) -> Option<DiscoveredPeer> {
    let ServiceEvent::ServiceResolved(svc) = event else {
        return None;
    };
    if svc.fullname == self_fullname {
        return None;
    }
    let fp_hex = svc.txt_properties.get_property_val_str(TXT_KEY_SPACE_FP)?;
    let fp = hex::decode(fp_hex).ok()?;
    if fp != expected_fp {
        return None;
    }
    let url = svc.txt_properties.get_property_val_str(TXT_KEY_URL)?;
    let url = Url::from_str(url).ok()?;
    if !url.is_peer() || Some(&url) == self_url {
        return None;
    }
    Some(DiscoveredPeer {
        url,
        fullname: svc.fullname.clone(),
    })
}

fn instance_name() -> String {
    let mut bytes = [0u8; 16];
    rand::rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// Collect the local addresses to name in our announcement, skipping
/// loopback and unspecified ones. An empty result is an error: a node
/// nobody on the LAN can address has nothing to announce.
pub fn local_addrs() -> K2Result<Vec<IpAddr>> {
    // One "best" address per family, found by asking the kernel which
    // source address it would route a packet through.
    let mut out = Vec::new();
    if let Ok(v4) = primary_v4() {
        out.push(IpAddr::V4(v4));
    }
    if let Ok(v6) = primary_v6() {
        out.push(IpAddr::V6(v6));
    }
    if out.is_empty() {
        return Err(K2Error::other("mdns: no usable local IP addresses found"));
    }
    Ok(out)
}

fn primary_v4() -> std::io::Result<std::net::Ipv4Addr> {
    use std::net::{SocketAddrV4, UdpSocket};
    let s =
        UdpSocket::bind(SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, 0))?;
    s.connect("8.8.8.8:80")?;
    match s.local_addr()? {
        std::net::SocketAddr::V4(a) => Ok(*a.ip()),
        _ => Err(std::io::Error::other("expected v4 local addr")),
    }
}

fn primary_v6() -> std::io::Result<std::net::Ipv6Addr> {
    use std::net::{SocketAddrV6, UdpSocket};
    let s = UdpSocket::bind(SocketAddrV6::new(
        std::net::Ipv6Addr::UNSPECIFIED,
        0,
        0,
        0,
    ))?;
    s.connect("[2001:4860:4860::8888]:80")?;
    match s.local_addr()? {
        std::net::SocketAddr::V6(a) => Ok(*a.ip()),
        _ => Err(std::io::Error::other("expected v6 local addr")),
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    //! Builders for the browse events the unit tests feed in, so that no
    //! test needs a multicast-capable interface.

    use super::*;

    pub const SERVICE_TYPE: &str = "_k2test._udp.local.";

    /// A resolved-service event as `mdns-sd` would deliver it for an
    /// announcement with the given instance name and TXT fields.
    pub fn resolved(instance: &str, txt: &[(&str, &str)]) -> ServiceEvent {
        let info = ServiceInfo::new(
            SERVICE_TYPE,
            instance,
            &format!("{instance}.local."),
            "192.0.2.10",
            0,
            txt,
        )
        .unwrap();
        ServiceEvent::ServiceResolved(Box::new(info.as_resolved_service()))
    }

    pub fn fullname(instance: &str) -> String {
        format!("{instance}.{SERVICE_TYPE}")
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;

    const SELF: &str = "self-instance";
    const OTHER: &str = "other-instance";
    const SELF_URL: &str = "ws://self.test:80/selfpeer";
    const OTHER_URL: &str = "ws://other.test:80/otherpeer";

    fn space() -> SpaceId {
        SpaceId::from(bytes::Bytes::from_static(b"space"))
    }

    fn fp_hex() -> String {
        hex::encode(fingerprint::space_fingerprint(&space()))
    }

    fn classify(event: &ServiceEvent) -> Option<DiscoveredPeer> {
        let self_url = Url::from_str(SELF_URL).unwrap();
        resolved_to_peer(
            event,
            &fingerprint::space_fingerprint(&space()),
            &fullname(SELF),
            Some(&self_url),
        )
    }

    #[test]
    fn matching_announcement_yields_a_peer() {
        let fp = fp_hex();
        let ev = resolved(OTHER, &[("spacefp", &fp), ("url", OTHER_URL)]);
        let peer = classify(&ev).expect("peer");
        assert_eq!(peer.url, Url::from_str(OTHER_URL).unwrap());
        assert_eq!(peer.fullname, fullname(OTHER));
    }

    #[test]
    fn fingerprint_mismatch_is_ignored() {
        let other_fp = hex::encode([7u8; 32]);
        let ev = resolved(OTHER, &[("spacefp", &other_fp), ("url", OTHER_URL)]);
        assert!(classify(&ev).is_none());
    }

    #[test]
    fn malformed_fingerprint_is_ignored() {
        let ev = resolved(OTHER, &[("spacefp", "zz"), ("url", OTHER_URL)]);
        assert!(classify(&ev).is_none());
        let ev = resolved(OTHER, &[("url", OTHER_URL)]);
        assert!(classify(&ev).is_none());
    }

    #[test]
    fn our_own_instance_is_ignored() {
        let fp = fp_hex();
        let ev = resolved(SELF, &[("spacefp", &fp), ("url", OTHER_URL)]);
        assert!(classify(&ev).is_none());
    }

    #[test]
    fn our_own_url_is_ignored() {
        let fp = fp_hex();
        let ev = resolved(OTHER, &[("spacefp", &fp), ("url", SELF_URL)]);
        assert!(classify(&ev).is_none());
    }

    #[test]
    fn missing_or_bad_url_is_ignored() {
        let fp = fp_hex();
        let ev = resolved(OTHER, &[("spacefp", &fp)]);
        assert!(classify(&ev).is_none());
        let ev = resolved(OTHER, &[("spacefp", &fp), ("url", "not a url")]);
        assert!(classify(&ev).is_none());
        // A URL without a peer id names nobody to dial.
        let ev =
            resolved(OTHER, &[("spacefp", &fp), ("url", "ws://other.test:80")]);
        assert!(classify(&ev).is_none());
    }

    #[test]
    fn non_resolved_events_are_ignored() {
        let ev = ServiceEvent::ServiceFound(
            SERVICE_TYPE.to_string(),
            fullname(OTHER),
        );
        assert!(classify(&ev).is_none());
    }
}
