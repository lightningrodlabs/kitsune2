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
    #[serde(default = "default_link_idle_timeout_s")]
    #[schemars(default)]
    pub link_idle_timeout_s: u32,
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
    /// UDP interface for local-network discovery.
    Udp {
        /// Bind address, e.g. `"0.0.0.0:0"`.
        bind: String,
        /// Multicast group, e.g. `"ff02::1"`.
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
