//! A UDP multicast broadcast medium.
//!
//! Every node binds the same port (with `SO_REUSEADDR`/`SO_REUSEPORT`,
//! so several nodes can share one host), joins the same multicast
//! group, and transmits frames to `group:port`. Multicast loopback is
//! enabled so same-host nodes hear each other; the transport filters
//! our own frames by sender id.
//!
//! This is a real, shippable medium on LANs and multicast-capable mesh
//! networks (e.g. batman-adv), and it is fast enough that protocol bugs
//! in the layers above are not hidden by bitrate.

use crate::medium::{BroadcastMedium, DynBroadcastMedium};
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use kitsune2_api::{BoxFut, K2Error, K2Result};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

/// Configuration for the udp multicast medium.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct UdpMulticastConfig {
    /// The IPv4 multicast group to join. Must be within the
    /// administratively-scoped range `239.0.0.0/8`.
    ///
    /// Default: `239.19.42.7`.
    pub group: String,

    /// The UDP port shared by all nodes on the medium.
    ///
    /// Default: 24842.
    pub port: u16,

    /// Largest frame to transmit. Keep under the path MTU minus
    /// IP/UDP overhead to avoid fragmentation.
    ///
    /// Default: 1400.
    pub mtu: usize,
}

impl Default for UdpMulticastConfig {
    fn default() -> Self {
        Self {
            group: "239.19.42.7".into(),
            port: 24842,
            mtu: 1400,
        }
    }
}

/// The udp multicast medium. See the module docs.
pub struct UdpMulticastMedium {
    socket: Arc<tokio::net::UdpSocket>,
    target: SocketAddr,
    mtu: usize,
}

impl std::fmt::Debug for UdpMulticastMedium {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdpMulticastMedium")
            .field("target", &self.target)
            .field("mtu", &self.mtu)
            .finish()
    }
}

impl UdpMulticastMedium {
    /// Bind the shared port, join the group and return the medium.
    pub async fn create(
        config: &UdpMulticastConfig,
    ) -> K2Result<DynBroadcastMedium> {
        let group: Ipv4Addr = config.group.parse().map_err(|err| {
            K2Error::other_src(
                format!("invalid multicast group: {}", config.group),
                err,
            )
        })?;
        if !group.is_multicast() {
            return Err(K2Error::other(format!(
                "{group} is not a multicast address"
            )));
        }

        let socket = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::DGRAM,
            Some(socket2::Protocol::UDP),
        )
        .map_err(|err| K2Error::other_src("create udp socket", err))?;
        socket
            .set_reuse_address(true)
            .map_err(|err| K2Error::other_src("set SO_REUSEADDR", err))?;
        #[cfg(all(
            unix,
            not(any(target_os = "solaris", target_os = "illumos"))
        ))]
        socket
            .set_reuse_port(true)
            .map_err(|err| K2Error::other_src("set SO_REUSEPORT", err))?;
        socket
            .bind(
                &SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::UNSPECIFIED,
                    config.port,
                ))
                .into(),
            )
            .map_err(|err| K2Error::other_src("bind multicast port", err))?;
        socket
            .join_multicast_v4(&group, &Ipv4Addr::UNSPECIFIED)
            .map_err(|err| K2Error::other_src("join multicast group", err))?;
        socket
            .set_multicast_loop_v4(true)
            .map_err(|err| K2Error::other_src("set multicast loopback", err))?;
        socket
            .set_nonblocking(true)
            .map_err(|err| K2Error::other_src("set nonblocking", err))?;

        let socket = tokio::net::UdpSocket::from_std(socket.into())
            .map_err(|err| K2Error::other_src("register udp socket", err))?;

        Ok(Arc::new(Self {
            socket: Arc::new(socket),
            target: SocketAddr::V4(SocketAddrV4::new(group, config.port)),
            mtu: config.mtu,
        }))
    }
}

impl BroadcastMedium for UdpMulticastMedium {
    fn kind(&self) -> &'static str {
        "udpm"
    }

    fn mtu(&self) -> usize {
        self.mtu
    }

    fn est_bytes_per_sec(&self) -> u32 {
        // Conservative LAN multicast estimate; only used for timer
        // scaling.
        1024 * 1024
    }

    fn half_duplex(&self) -> bool {
        false
    }

    fn transmit(&self, frame: Bytes) -> BoxFut<'_, K2Result<()>> {
        Box::pin(async move {
            if frame.len() > self.mtu {
                return Err(K2Error::other(format!(
                    "frame of {} bytes exceeds udp multicast mtu {}",
                    frame.len(),
                    self.mtu
                )));
            }
            self.socket
                .send_to(&frame, self.target)
                .await
                .map_err(|err| K2Error::other_src("multicast send", err))?;
            Ok(())
        })
    }

    fn frames(&self) -> BoxStream<'static, Bytes> {
        let socket = self.socket.clone();
        let mtu = self.mtu;
        futures::stream::unfold(
            (socket, vec![0_u8; mtu.max(2048)]),
            |(socket, mut buf)| async move {
                loop {
                    match socket.recv_from(&mut buf).await {
                        Ok((len, _addr)) => {
                            let frame = Bytes::copy_from_slice(&buf[..len]);
                            return Some((frame, (socket, buf)));
                        }
                        Err(err) => {
                            // Transient errors (e.g. ICMP-induced) are
                            // routine on an open medium; keep listening.
                            tracing::debug!(?err, "udp multicast recv error");
                            tokio::time::sleep(
                                std::time::Duration::from_millis(10),
                            )
                            .await;
                        }
                    }
                }
            },
        )
        .boxed()
    }
}
