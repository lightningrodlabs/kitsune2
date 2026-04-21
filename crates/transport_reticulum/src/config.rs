//! Configuration types for the Reticulum transport.
//!
//! Unlike [`kitsune2_transport_iroh`] and [`kitsune2_transport_tx5`],
//! `schemars::JsonSchema` is derived unconditionally on these types
//! (not behind a `schema` feature). Reticulum's configuration is
//! structural — a list of interface variants, identity path, and
//! tuning — so consumers that expose the conductor config in their
//! own `JsonSchema`-deriving structs (e.g. holochain's `NetworkConfig`)
//! need this derive always-on. iroh/tx5 can stay gated because their
//! typed configs never leak into user-facing config structs; they're
//! only ever seen through URL primitives or internal schema-gen.

use kitsune2_api::{K2Error, K2Result};

/// Configuration for the Reticulum transport.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "camelCase")]
pub struct ReticulumTransportConfig {
    /// Reticulum interfaces to bring up on startup.
    ///
    /// Each entry describes one interface (e.g. `TCPClient`, `TCPServer`,
    /// `AutoInterface`). At least one must be specified.
    pub interfaces: Vec<ReticulumInterfaceConfig>,

    /// Path to the Reticulum identity file on disk.
    ///
    /// If `None`, a fresh identity is generated on every startup,
    /// meaning the node URL changes each run. Persisting the identity
    /// is strongly recommended for anything beyond ephemeral tests.
    ///
    /// Defaults to `None`.
    #[serde(default)]
    #[schemars(default)]
    pub identity_path: Option<std::path::PathBuf>,

    /// Maximum kitsune2 frame size in bytes.
    ///
    /// Payloads exceeding the Reticulum packet MDU (~464 bytes) are
    /// sent via the Resource abstraction (automatic chunking). This
    /// cap limits what we hand to `send_resource()`.
    ///
    /// Default: 1 MiB.
    #[serde(default = "default_max_frame_bytes")]
    #[schemars(default)]
    pub max_frame_bytes: usize,

    /// Link-establishment timeout in seconds.
    ///
    /// Covers the Reticulum 1-RTT handshake plus the kitsune2 preflight
    /// round-trip.
    ///
    /// Default: 30 seconds.
    #[serde(default = "default_connect_timeout_s")]
    #[schemars(default)]
    pub connect_timeout_s: u32,

    /// How often (seconds) to re-announce each joined-space destination.
    ///
    /// Applied per-space: a node with N joined spaces emits N announces
    /// every interval.
    ///
    /// Default: 300 seconds.
    #[serde(default = "default_announce_interval_s")]
    #[schemars(default)]
    pub announce_interval_s: u32,

    /// Idle timeout for a per-space Link in seconds.
    ///
    /// After this many seconds of inactivity, a Link is torn down.
    /// Note that one idle Link does not imply the peer is idle --
    /// other per-space Links may still be active.
    ///
    /// Default: 600 seconds.
    ///
    /// Note: this is honoured by the LXMF-rs backend. The Beechat
    /// backend uses compile-time constants for link timing and
    /// silently ignores this value.
    #[serde(default = "default_link_idle_timeout_s")]
    #[schemars(default)]
    pub link_idle_timeout_s: u32,

    /// Beechat-backend-specific tuning.
    ///
    /// Fields here only take effect when the `backend-beechat` feature
    /// is enabled; the LXMF-rs backend ignores them. Each flag is
    /// `Option<bool>`: `None` leaves the Beechat default in place.
    #[serde(default)]
    #[schemars(default)]
    pub beechat: ReticulumBeechatConfig,
}

/// Beechat-backend-only `TransportConfig` extras.
///
/// The Beechat crate's `TransportConfig` exposes a handful of knobs
/// — retransmit/broadcast/reroute/restart/announce-forever — that
/// have no equivalent in the LXMF-rs backend. This struct carries
/// them through `ReticulumTransportConfig`; `None` means "leave the
/// Beechat default in place" (all default to `false` upstream).
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "camelCase")]
pub struct ReticulumBeechatConfig {
    /// Act as a transport node, forwarding packets for others.
    #[schemars(default)]
    pub retransmit: Option<bool>,
    /// Enable broadcast mode.
    #[schemars(default)]
    pub broadcast: Option<bool>,
    /// Replace known routes to distant destinations with equally-long
    /// newer routes (not just shorter ones). Prefers newer over older.
    #[schemars(default)]
    pub reroute_eager: Option<bool>,
    /// Auto-restart closed outbound links.
    #[schemars(default)]
    pub restart_outlinks: Option<bool>,
    /// Keep retransmitting announces indefinitely at a slower pace
    /// after the initial round.
    #[schemars(default)]
    pub announce_forever: Option<bool>,
}

fn default_max_frame_bytes() -> usize {
    1024 * 1024
}
fn default_connect_timeout_s() -> u32 {
    30
}
fn default_announce_interval_s() -> u32 {
    300
}
fn default_link_idle_timeout_s() -> u32 {
    600
}

/// Configuration for a single Reticulum interface.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ReticulumInterfaceConfig {
    /// TCP client interface -- connects to a remote Reticulum node.
    TcpClient {
        /// Target address, e.g. `"127.0.0.1:4242"`.
        target: String,
    },
    /// TCP server interface -- listens for incoming connections.
    TcpServer {
        /// Bind address, e.g. `"0.0.0.0:4242"`.
        bind: String,
    },
    /// UDP interface.
    ///
    /// Two usage patterns:
    ///
    /// - **Multicast** (LAN discovery). Set `group` to a multicast
    ///   `IP:PORT` like `"224.0.0.224:4242"`. The backend joins that
    ///   group on receive and uses it as the forward target on send,
    ///   giving bidirectional multicast. The `bind` field is ignored
    ///   in this mode — you can pass `"0.0.0.0:0"`.
    ///
    /// - **Point-to-point unicast**. Set `bind` to a local address
    ///   and `group` to the peer's unicast address (e.g.
    ///   `bind: "0.0.0.0:8000", group: Some("10.0.0.2:8000")`). The
    ///   backend binds locally and forwards outbound packets to the
    ///   unicast target.
    ///
    /// With `group: None` the interface is inert (no forward target
    /// means no tx task; no group means no multicast receive) — use
    /// `TcpClient` / `TcpServer` for point-to-point TCP instead.
    ///
    /// Multicast support depends on the backend:
    /// - `backend-lxmf` honors multicast fully.
    /// - `backend-beechat`'s underlying `reticulum-rs` crate does not
    ///   currently join multicast groups at the socket layer. A
    ///   multicast config compiles and runs but won't receive
    ///   multicast traffic. Use LXMF for LAN discovery, or fix the
    ///   upstream `reticulum-rs::iface::udp::UdpInterface`.
    Udp {
        /// Local bind address. Ignored when `group` is a multicast
        /// address (the backend uses `group` as the bind).
        bind: String,
        /// Multicast group to join (`"224.x.y.z:PORT"` or
        /// `"[ffXX::Y]:PORT"`), or a unicast peer address for
        /// point-to-point forwarding.
        group: Option<String>,
    },
}

impl Default for ReticulumTransportConfig {
    fn default() -> Self {
        Self {
            interfaces: Vec::new(),
            identity_path: None,
            max_frame_bytes: 1024 * 1024, // 1 MiB
            connect_timeout_s: 30,
            announce_interval_s: 300,
            link_idle_timeout_s: 600,
            beechat: ReticulumBeechatConfig::default(),
        }
    }
}

impl ReticulumTransportConfig {
    /// Validate the configuration, returning an error for invalid combinations.
    pub fn validate(&self) -> K2Result<()> {
        if self.interfaces.is_empty() {
            return Err(K2Error::other(
                "ReticulumTransportConfig: at least one interface must be specified",
            ));
        }

        const MAX_FRAME_CAP: usize = 16 * 1024 * 1024; // 16 MiB
        if self.max_frame_bytes > MAX_FRAME_CAP {
            return Err(K2Error::other(format!(
                "ReticulumTransportConfig: max_frame_bytes ({}) exceeds sanity cap ({MAX_FRAME_CAP})",
                self.max_frame_bytes,
            )));
        }

        if let Some(ref path) = self.identity_path
            && let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
            && !parent.exists()
        {
            return Err(K2Error::other(format!(
                "ReticulumTransportConfig: identity_path parent directory does not exist: {}",
                parent.display(),
            )));
        }

        Ok(())
    }
}

/// Resolve the `(bind, group)` user config for a UDP interface into the
/// `(bind_addr, forward_addr)` pair that the underlying
/// `rns_transport` / `reticulum-rs` `UdpInterface::new` wants.
///
/// When `group` is a multicast address, override the user's `bind`:
/// LXMF's socket layer only joins the multicast group if the bind
/// address itself is multicast, and you can't simultaneously bind to a
/// multicast group and a unicast local address on the same socket.
/// The multicast join is the load-bearing part.
///
/// When `group` is unicast, keep the user's `bind` (where to listen
/// locally) and pass `group` as `forward_addr` (where outbound goes).
pub(crate) fn resolve_udp_addrs(
    bind: &str,
    group: Option<&str>,
) -> (String, Option<String>) {
    match group {
        Some(g) if is_multicast_addr(g) => (g.to_string(), Some(g.to_string())),
        Some(g) => (bind.to_string(), Some(g.to_string())),
        None => (bind.to_string(), None),
    }
}

/// Returns `true` if `addr` parses as a `SocketAddr` whose IP is in
/// the IPv4 (`224.0.0.0/4`) or IPv6 (`ff00::/8`) multicast range.
pub(crate) fn is_multicast_addr(addr: &str) -> bool {
    addr.parse::<std::net::SocketAddr>()
        .ok()
        .map(|sa| sa.ip().is_multicast())
        .unwrap_or(false)
}

/// Module-level config wrapper, matching the `ModConfig` pattern.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Default,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "camelCase")]
pub struct ReticulumTransportModConfig {
    /// The Reticulum transport configuration.
    pub reticulum_transport: ReticulumTransportConfig,
}
