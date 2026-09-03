//! The mDNS daemon behind LAN discovery, and the records it carries.
//!
//! An announcement carries two TXT fields: `spacefp`, the base64-encoded
//! [space fingerprint](crate::fingerprint), and `url`, this node's kitsune2
//! peer URL. The raw space id is never sent. Each space announces its own
//! record under a random instance name, so that announcements do not
//! correlate across sessions or spaces; all of a node's records share one
//! hostname, because they all name the same machine, and the daemon fills
//! in and maintains that host's addresses itself. The port is zero: nothing
//! listens for this crate, the URL is all a peer needs to dial us through
//! the transport.
//!
//! [`Daemon`] is the narrow surface this crate needs from an mDNS
//! implementation, so that the browse and announce logic can be exercised
//! without multicast. [`MdnsService`] is the real thing, over `mdns-sd`.

use crate::fingerprint::SpaceFingerprint;
use base64::prelude::*;
use kitsune2_api::{K2Error, K2Result, Url};
use mdns_sd::{ResolvedService, ServiceDaemon, ServiceEvent, ServiceInfo};
use rand::{Rng, RngExt};
use std::sync::Arc;

/// TXT record key carrying the base64-encoded space fingerprint.
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

/// A random token for an instance name.
pub fn random_name() -> String {
    let mut bytes = [0u8; 16];
    rand::rng().fill_bytes(&mut bytes);
    BASE64_URL_SAFE_NO_PAD.encode(bytes)
}

/// A random hostname. Hostnames are DNS labels, so this stays within
/// lowercase hex.
fn random_hostname() -> String {
    format!("{:032x}.local.", rand::rng().random::<u128>())
}

/// Fully qualified name of the record announced under `instance` within
/// `service_type`.
pub fn fullname(service_type: &str, instance: &str) -> String {
    format!("{instance}.{service_type}")
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
    /// Starting the daemon binds multicast sockets and spawns a thread, so
    /// call this off the async runtime.
    pub fn start(service_type: &str) -> K2Result<Self> {
        let daemon = ServiceDaemon::new()
            .map_err(|e| K2Error::other_src("mdns daemon start", e))?;
        Ok(Self {
            daemon,
            service_type: service_type.to_string(),
            hostname: random_hostname(),
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
        // The daemon enumerates the host's interface addresses itself and
        // re-announces when they change.
        let info = ServiceInfo::new(
            &self.service_type,
            instance,
            &self.hostname,
            (),
            0,
            txt,
        )
        .map_err(|e| K2Error::other_src("mdns ServiceInfo::new", e))?
        .enable_addr_auto();
        self.daemon
            .register(info)
            .map_err(|e| K2Error::other_src("mdns register", e))
    }

    fn unregister(&self, instance: &str) -> K2Result<()> {
        self.daemon
            .unregister(&fullname(&self.service_type, instance))
            .map(|_| ())
            .map_err(|e| K2Error::other_src("mdns unregister", e))
    }
}

/// The TXT fields a record for `fp` and `url` carries.
pub fn record_txt(fp: &SpaceFingerprint, url: &Url) -> [(String, String); 2] {
    [
        (TXT_KEY_SPACE_FP.to_string(), fp.encode()),
        (TXT_KEY_URL.to_string(), url.to_string()),
    ]
}

/// The `spacefp` and `url` TXT values of a resolved record, as announced.
/// Neither is decoded here: a record for a space this node is not in
/// should cost one string lookup and nothing more.
pub fn record_fields(svc: &ResolvedService) -> Option<(&str, &str)> {
    let fp = svc.txt_properties.get_property_val_str(TXT_KEY_SPACE_FP)?;
    let url = svc.txt_properties.get_property_val_str(TXT_KEY_URL)?;
    Some((fp, url))
}

/// The peer URL an announced `url` value names, if it names someone to
/// dial.
pub fn parse_peer_url(url: &str) -> Option<Url> {
    let url = Url::from_str(url).ok()?;
    url.is_peer().then_some(url)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;

    const OTHER: &str = "other-instance";
    const OTHER_URL: &str = "ws://other.test:80/otherpeer";

    fn fp() -> SpaceFingerprint {
        test_fp(&[7u8; 32])
    }

    /// The fingerprint and peer URL a record carries, decoded.
    fn parse(event: &ServiceEvent) -> Option<(SpaceFingerprint, Url)> {
        let ServiceEvent::ServiceResolved(svc) = event else {
            return None;
        };
        let (fp, url) = record_fields(svc)?;
        Some((SpaceFingerprint::decode(fp)?, parse_peer_url(url)?))
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
        let ev = resolved(OTHER, &[("spacefp", "z!z"), ("url", OTHER_URL)]);
        assert!(parse(&ev).is_none());
        let ev = resolved(OTHER, &[("url", OTHER_URL)]);
        assert!(parse(&ev).is_none());
    }

    #[test]
    fn a_missing_or_bad_url_is_ignored() {
        let fp_txt = fp().encode();
        let ev = resolved(OTHER, &[("spacefp", &fp_txt)]);
        assert!(parse(&ev).is_none());
        let ev = resolved(OTHER, &[("spacefp", &fp_txt), ("url", "not a url")]);
        assert!(parse(&ev).is_none());
        // A URL without a peer id names nobody to dial.
        let ev = resolved(
            OTHER,
            &[("spacefp", &fp_txt), ("url", "ws://other.test:80")],
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
}
