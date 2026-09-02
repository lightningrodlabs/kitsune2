//! The mDNS daemon behind LAN discovery, and the records it carries.
//!
//! An announcement carries two TXT fields: `spacefp`, the hex-encoded
//! [space fingerprint](crate::fingerprint), and `url`, this node's kitsune2
//! peer URL. The raw space id is never sent. Each space announces its own
//! record under a random instance name, so that announcements do not
//! correlate across sessions or spaces; all of a node's records share one
//! hostname, because they all name the same machine. The port is zero:
//! nothing listens for this crate, the URL is all a peer needs to dial us
//! through the transport.
//!
//! [`Daemon`] is the narrow surface this crate needs from an mDNS
//! implementation, so that the browse and announce logic can be exercised
//! without multicast. [`MdnsService`] is the real thing, over `mdns-sd`.

use crate::fingerprint::SpaceFingerprint;
use kitsune2_api::{K2Error, K2Result, Url};
use mdns_sd::{ResolvedService, ServiceDaemon, ServiceEvent, ServiceInfo};
use rand::Rng;
use std::net::IpAddr;
use std::sync::Arc;

/// TXT record key carrying the hex-encoded space fingerprint.
pub const TXT_KEY_SPACE_FP: &str = "spacefp";

/// TXT record key carrying the announcing node's kitsune2 peer URL.
pub const TXT_KEY_URL: &str = "url";

/// What this crate needs from an mDNS daemon: one browse of its service
/// type, and records announced and withdrawn by instance name.
pub trait Daemon: 'static + Send + Sync + std::fmt::Debug {
    /// The service type every record lives under.
    fn service_type(&self) -> &str;

    /// Subscribe to announcements of the service type.
    fn browse(&self) -> K2Result<flume::Receiver<ServiceEvent>>;

    /// Announce a record under `instance` with the given TXT fields,
    /// replacing any record already announced under that name.
    fn register(&self, instance: &str, txt: &[(&str, &str)]) -> K2Result<()>;

    /// Withdraw the record announced under `instance`.
    fn unregister(&self, instance: &str) -> K2Result<()>;
}

/// Trait-object [`Daemon`].
pub type DynDaemon = Arc<dyn Daemon>;

/// A random token for an instance or host name.
pub fn random_name() -> String {
    let mut bytes = [0u8; 16];
    rand::rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// Fully qualified name of the record announced under `instance`.
pub fn fullname(daemon: &dyn Daemon, instance: &str) -> String {
    format!("{instance}.{}", daemon.service_type())
}

/// This process's presence on the LAN: an `mdns-sd` daemon, the service
/// type it browses and announces, and the hostname all records share.
///
/// Dropping the service shuts the daemon down, which withdraws every
/// record still announced.
pub struct MdnsService {
    daemon: ServiceDaemon,
    service_type: String,
    hostname: String,
    addrs: Vec<IpAddr>,
}

impl std::fmt::Debug for MdnsService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MdnsService")
            .field("service_type", &self.service_type)
            .field("hostname", &self.hostname)
            .finish()
    }
}

impl Drop for MdnsService {
    fn drop(&mut self) {
        let _ = self.daemon.shutdown();
    }
}

impl MdnsService {
    /// Start an mDNS daemon for `service_type`, announcing nothing yet.
    ///
    /// `addrs` are the local addresses every record will name; they are
    /// fixed for the life of the service. Starting the daemon binds
    /// multicast sockets and spawns a thread, so call this off the async
    /// runtime.
    pub fn start(service_type: &str, addrs: Vec<IpAddr>) -> K2Result<Self> {
        if addrs.is_empty() {
            return Err(K2Error::other("mdns: no local addresses to announce"));
        }
        let daemon = ServiceDaemon::new()
            .map_err(|e| K2Error::other_src("mdns daemon start", e))?;
        Ok(Self {
            daemon,
            service_type: service_type.to_string(),
            hostname: format!("{}.local.", random_name()),
            addrs,
        })
    }
}

impl Daemon for MdnsService {
    fn service_type(&self) -> &str {
        &self.service_type
    }

    fn browse(&self) -> K2Result<flume::Receiver<ServiceEvent>> {
        self.daemon
            .browse(&self.service_type)
            .map_err(|e| K2Error::other_src("mdns browse", e))
    }

    fn register(&self, instance: &str, txt: &[(&str, &str)]) -> K2Result<()> {
        let info = ServiceInfo::new(
            &self.service_type,
            instance,
            &self.hostname,
            &self.addrs[..],
            0,
            txt,
        )
        .map_err(|e| K2Error::other_src("mdns ServiceInfo::new", e))?;
        self.daemon
            .register(info)
            .map_err(|e| K2Error::other_src("mdns register", e))
    }

    fn unregister(&self, instance: &str) -> K2Result<()> {
        self.daemon
            .unregister(&fullname(self, instance))
            .map(|_| ())
            .map_err(|e| K2Error::other_src("mdns unregister", e))
    }
}

/// The TXT fields a record for `fp` and `url` carries.
pub fn record_txt(fp: &SpaceFingerprint, url: &Url) -> [(String, String); 2] {
    [
        (TXT_KEY_SPACE_FP.to_string(), hex::encode(fp)),
        (TXT_KEY_URL.to_string(), url.to_string()),
    ]
}

/// The space commitment and peer URL a resolved record carries, if it is
/// one of ours: a decodable `spacefp` and a peer URL that names someone to
/// dial. Whose space it is, and whether it is our own record, is for the
/// caller to decide.
pub fn parse_record(svc: &ResolvedService) -> Option<(SpaceFingerprint, Url)> {
    let fp_hex = svc.txt_properties.get_property_val_str(TXT_KEY_SPACE_FP)?;
    let fp: SpaceFingerprint = hex::decode(fp_hex).ok()?.try_into().ok()?;
    let url = svc.txt_properties.get_property_val_str(TXT_KEY_URL)?;
    let url = Url::from_str(url).ok()?;
    if !url.is_peer() {
        return None;
    }
    Some((fp, url))
}

/// Collect the local addresses to name in our announcement: every
/// interface address a LAN peer could plausibly reach, which excludes
/// loopback, unspecified and IPv6 link-local addresses. An empty result is
/// an error: a node nobody on the LAN can address has nothing to announce.
pub fn local_addrs() -> K2Result<Vec<IpAddr>> {
    let ifaces = if_addrs::get_if_addrs()
        .map_err(|e| K2Error::other_src("mdns: enumerating interfaces", e))?;
    let out = announceable(ifaces.iter().map(|i| i.ip()));
    if out.is_empty() {
        return Err(K2Error::other("mdns: no usable local IP addresses found"));
    }
    Ok(out)
}

/// Keep the addresses worth announcing, in the order given, without
/// duplicates.
///
/// IPv6 link-local addresses are dropped because they are only meaningful
/// together with a scope id, which a TXT/A record cannot carry; IPv4
/// link-local addresses are kept, since on a LAN without DHCP they are the
/// only addresses there are.
fn announceable(addrs: impl Iterator<Item = IpAddr>) -> Vec<IpAddr> {
    let mut out: Vec<IpAddr> = Vec::new();
    for ip in addrs {
        let skip = match ip {
            IpAddr::V4(v4) => v4.is_loopback() || v4.is_unspecified(),
            IpAddr::V6(v6) => {
                v6.is_loopback()
                    || v6.is_unspecified()
                    || v6.is_unicast_link_local()
            }
        };
        if !skip && !out.contains(&ip) {
            out.push(ip);
        }
    }
    out
}

#[cfg(test)]
pub(crate) mod test_support {
    //! A [`Daemon`] that needs no multicast: the test feeds it the browse
    //! events it wants seen and reads back what was announced.

    use super::*;
    use std::sync::Mutex;

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

    /// A resolved-service event for a record of space `fp` naming `url`.
    pub fn resolved_peer(
        instance: &str,
        fp: &SpaceFingerprint,
        url: &str,
    ) -> ServiceEvent {
        let fp_hex = hex::encode(fp);
        resolved(instance, &[("spacefp", &fp_hex), ("url", url)])
    }

    /// The event `mdns-sd` delivers when a record goes away.
    pub fn removed(instance: &str) -> ServiceEvent {
        ServiceEvent::ServiceRemoved(
            SERVICE_TYPE.to_string(),
            fullname_for(instance),
        )
    }

    pub fn fullname_for(instance: &str) -> String {
        format!("{instance}.{SERVICE_TYPE}")
    }

    /// A `register` call as `(instance, txt)`.
    pub type Registered = (String, Vec<(String, String)>);

    #[derive(Debug)]
    pub struct FakeDaemon {
        events: flume::Sender<ServiceEvent>,
        browse_rx: flume::Receiver<ServiceEvent>,
        /// Every `register` call.
        pub registered: Mutex<Vec<Registered>>,
        /// Every `unregister` call.
        pub unregistered: Mutex<Vec<String>>,
    }

    impl FakeDaemon {
        pub fn new() -> Arc<Self> {
            let (events, browse_rx) = flume::unbounded();
            Arc::new(Self {
                events,
                browse_rx,
                registered: Mutex::new(Vec::new()),
                unregistered: Mutex::new(Vec::new()),
            })
        }

        /// Deliver a browse event as if the LAN had produced it.
        pub fn deliver(&self, event: ServiceEvent) {
            self.events.send(event).unwrap();
        }
    }

    impl Daemon for FakeDaemon {
        fn service_type(&self) -> &str {
            SERVICE_TYPE
        }

        fn browse(&self) -> K2Result<flume::Receiver<ServiceEvent>> {
            Ok(self.browse_rx.clone())
        }

        fn register(
            &self,
            instance: &str,
            txt: &[(&str, &str)],
        ) -> K2Result<()> {
            let txt = txt
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            self.registered
                .lock()
                .unwrap()
                .push((instance.to_string(), txt));
            Ok(())
        }

        fn unregister(&self, instance: &str) -> K2Result<()> {
            self.unregistered.lock().unwrap().push(instance.to_string());
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;
    use crate::fingerprint;
    use kitsune2_api::SpaceId;

    const OTHER: &str = "other-instance";
    const OTHER_URL: &str = "ws://other.test:80/otherpeer";

    fn fp() -> SpaceFingerprint {
        fingerprint::space_fingerprint(&SpaceId::from(
            bytes::Bytes::from_static(b"space"),
        ))
    }

    fn parse(event: &ServiceEvent) -> Option<(SpaceFingerprint, Url)> {
        match event {
            ServiceEvent::ServiceResolved(svc) => parse_record(svc),
            _ => None,
        }
    }

    #[test]
    fn a_well_formed_record_yields_its_fingerprint_and_url() {
        let ev = resolved_peer(OTHER, &fp(), OTHER_URL);
        let (got_fp, url) = parse(&ev).expect("record");
        assert_eq!(got_fp, fp());
        assert_eq!(url, Url::from_str(OTHER_URL).unwrap());
    }

    #[test]
    fn a_malformed_or_missing_fingerprint_is_ignored() {
        let ev = resolved(OTHER, &[("spacefp", "zz"), ("url", OTHER_URL)]);
        assert!(parse(&ev).is_none());
        let short = hex::encode([7u8; 8]);
        let ev = resolved(OTHER, &[("spacefp", &short), ("url", OTHER_URL)]);
        assert!(parse(&ev).is_none());
        let ev = resolved(OTHER, &[("url", OTHER_URL)]);
        assert!(parse(&ev).is_none());
    }

    #[test]
    fn a_missing_or_bad_url_is_ignored() {
        let fp_hex = hex::encode(fp());
        let ev = resolved(OTHER, &[("spacefp", &fp_hex)]);
        assert!(parse(&ev).is_none());
        let ev = resolved(OTHER, &[("spacefp", &fp_hex), ("url", "not a url")]);
        assert!(parse(&ev).is_none());
        // A URL without a peer id names nobody to dial.
        let ev = resolved(
            OTHER,
            &[("spacefp", &fp_hex), ("url", "ws://other.test:80")],
        );
        assert!(parse(&ev).is_none());
    }

    #[test]
    fn record_txt_round_trips_through_parse() {
        let url = Url::from_str(OTHER_URL).unwrap();
        let txt = record_txt(&fp(), &url);
        let txt: Vec<(&str, &str)> =
            txt.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let ev = resolved(OTHER, &txt);
        assert_eq!(parse(&ev), Some((fp(), url)));
    }

    #[test]
    fn announceable_drops_loopback_unspecified_and_v6_link_local() {
        let addrs: Vec<IpAddr> = [
            "127.0.0.1",
            "0.0.0.0",
            "::",
            "::1",
            "fe80::1",
            "169.254.7.7",
            "192.168.1.20",
            "192.168.1.20",
            "fd00::20",
            "2001:db8::20",
        ]
        .iter()
        .map(|s| s.parse().unwrap())
        .collect();
        let kept = announceable(addrs.into_iter());
        let expected: Vec<IpAddr> =
            ["169.254.7.7", "192.168.1.20", "fd00::20", "2001:db8::20"]
                .iter()
                .map(|s| s.parse().unwrap())
                .collect();
        assert_eq!(kept, expected);
    }
}
