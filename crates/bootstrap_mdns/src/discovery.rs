//! mDNS service announce and browse glue around `mdns-sd`.
//!
//! Announcement publishes a single TXT record field — `spacefp` — carrying
//! the hex-encoded space fingerprint. The raw `SpaceId` is never sent over
//! mDNS. The instance name is a random 16-byte token, so it does not
//! correlate across sessions or spaces.
//!
//! Browsing emits [`DiscoveredPeer`]s that match our local fingerprint; the
//! rest is the job of the session layer.

use crate::proto::{self, FP_LEN};
use kitsune2_api::{K2Error, K2Result, SpaceId};
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use rand::RngCore;
use std::net::{IpAddr, SocketAddr};

/// TXT record key under which we store the hex-encoded space fingerprint.
pub const TXT_KEY_SPACE_FP: &str = "spacefp";

/// A peer resolved via mDNS that claims to match our space fingerprint.
/// The session layer connects to `addr` to complete the handshake.
#[derive(Debug, Clone)]
pub struct DiscoveredPeer {
    /// The socket address to connect to.
    pub addr: SocketAddr,
    /// Full mDNS instance name, useful for de-duplication and logging.
    pub fullname: String,
}

/// An mDNS service handle. While this value is alive, the service is
/// published; dropping it unregisters and shuts down the daemon.
pub struct MdnsService {
    daemon: ServiceDaemon,
    fullname: String,
}

impl std::fmt::Debug for MdnsService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MdnsService")
            .field("fullname", &self.fullname)
            .finish()
    }
}

impl Drop for MdnsService {
    fn drop(&mut self) {
        // Best-effort unregister; daemon will also shut down when dropped.
        let _ = self.daemon.unregister(&self.fullname);
        let _ = self.daemon.shutdown();
    }
}

impl MdnsService {
    /// Register an mDNS service advertising the given space fingerprint.
    ///
    /// `port` is the TCP port of the local info-exchange listener. `addrs`
    /// is the list of local IP addresses to advertise.
    pub fn register(
        service_type: &str,
        space_id: &SpaceId,
        port: u16,
        addrs: Vec<IpAddr>,
    ) -> K2Result<Self> {
        let daemon = ServiceDaemon::new()
            .map_err(|e| K2Error::other_src("mdns daemon start", e))?;

        let fp = proto::space_fingerprint(space_id);
        let fp_hex = hex::encode(fp);

        let instance = instance_name();
        let host = format!("{instance}.local.");

        let props = [(TXT_KEY_SPACE_FP, fp_hex.as_str())];

        let info = ServiceInfo::new(
            service_type,
            &instance,
            &host,
            &addrs[..],
            port,
            &props[..],
        )
        .map_err(|e| K2Error::other_src("mdns ServiceInfo::new", e))?;

        let fullname = info.get_fullname().to_string();
        daemon
            .register(info)
            .map_err(|e| K2Error::other_src("mdns register", e))?;

        Ok(Self { daemon, fullname })
    }

    /// Get a browse receiver for this service type.
    pub fn browse(
        &self,
        service_type: &str,
    ) -> K2Result<flume::Receiver<ServiceEvent>> {
        self.daemon
            .browse(service_type)
            .map_err(|e| K2Error::other_src("mdns browse", e))
    }

    /// The full mDNS instance name of our registered service. Useful to
    /// filter out our own announcements from the browse stream.
    pub fn fullname(&self) -> &str {
        &self.fullname
    }
}

/// Extract a [`DiscoveredPeer`] from a resolved mDNS service event, but
/// only if:
///
/// - The TXT `spacefp` field is present and parses to 32 bytes,
/// - It matches `expected_fp`,
/// - The event is not our own announcement (filtered by `self_fullname`).
///
/// Returns `None` when any of those conditions fail. Logs at `trace` to
/// aid debugging.
pub fn resolved_to_peer(
    event: &ServiceEvent,
    expected_fp: &[u8; FP_LEN],
    self_fullname: &str,
) -> Option<DiscoveredPeer> {
    let svc = match event {
        ServiceEvent::ServiceResolved(svc) => svc,
        _ => return None,
    };
    if svc.fullname == self_fullname {
        return None;
    }
    let fp_hex = svc.txt_properties.get_property_val_str(TXT_KEY_SPACE_FP)?;
    let fp = hex::decode(fp_hex).ok()?;
    if fp.len() != FP_LEN || fp != expected_fp {
        return None;
    }
    let ip = svc.addresses.iter().next()?;
    let addr = SocketAddr::new(scoped_ip_to_std(ip)?, svc.port);
    Some(DiscoveredPeer {
        addr,
        fullname: svc.fullname.clone(),
    })
}

fn scoped_ip_to_std(ip: &mdns_sd::ScopedIp) -> Option<IpAddr> {
    match ip {
        mdns_sd::ScopedIp::V4(v) => Some(IpAddr::V4(*v.addr())),
        mdns_sd::ScopedIp::V6(v) => Some(IpAddr::V6(*v.addr())),
        _ => None,
    }
}

fn instance_name() -> String {
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// Collect local IPv4/IPv6 addresses suitable for advertising over mDNS.
/// Skips loopback and unspecified. Returns an error if none are found —
/// in that case we can't be reached on the LAN so there's no point
/// registering.
pub fn local_addrs() -> K2Result<Vec<IpAddr>> {
    // `if_addrs` is a small focused crate, but we avoid adding a dep and
    // instead query via `local-ip-address`. To keep dep count low here we
    // fall back to the standard approach: enumerate via a UDP socket
    // connect-to-sentinel trick to find the primary interface IP.
    //
    // This yields one "best" address per family and is adequate for v1.
    let mut out = Vec::new();
    if let Ok(v4) = primary_v4() {
        out.push(IpAddr::V4(v4));
    }
    if let Ok(v6) = primary_v6() {
        out.push(IpAddr::V6(v6));
    }
    if out.is_empty() {
        return Err(K2Error::other(
            "mdns: no usable local IP addresses found",
        ));
    }
    Ok(out)
}

fn primary_v4() -> std::io::Result<std::net::Ipv4Addr> {
    use std::net::{SocketAddrV4, UdpSocket};
    let s = UdpSocket::bind(SocketAddrV4::new(
        std::net::Ipv4Addr::UNSPECIFIED,
        0,
    ))?;
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
