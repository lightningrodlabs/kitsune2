//! Configuration for the mDNS bootstrap factory.

use serde::{Deserialize, Serialize};

/// Configuration parameters for [`MdnsBootstrapFactory`](crate::MdnsBootstrapFactory).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct MdnsBootstrapConfig {
    /// Enable mDNS discovery. When false, the factory produces a no-op
    /// bootstrap that accepts `put`s and discards them.
    ///
    /// Default: `false`. This makes the factory safe to include in any
    /// builder stack without unexpectedly starting an mDNS service.
    #[cfg_attr(feature = "schema", schemars(default))]
    pub enabled: bool,

    /// mDNS service type. Nodes only discover peers publishing the same
    /// service type. Must be of the form `_name._udp.local.`.
    ///
    /// Default: `_kitsune2._udp.local.`.
    #[cfg_attr(feature = "schema", schemars(default))]
    pub service_type: String,

    /// Minimum time, in milliseconds, between two dial attempts toward the
    /// same peer URL. mDNS resolves the same record repeatedly (on every
    /// re-announce and on every interface it is heard on) and a peer that
    /// could not be reached a moment ago is unlikely to be reachable now.
    ///
    /// Default: 60 seconds.
    #[cfg_attr(feature = "schema", schemars(default))]
    pub dial_cooldown_ms: u32,

    /// Maximum number of dials this module has in flight at once. Bounds
    /// the work a burst of announcements — or a hostile flood of them — can
    /// push into the transport.
    ///
    /// Default: 4.
    #[cfg_attr(feature = "schema", schemars(default))]
    pub max_concurrent_dials: u32,
}

impl Default for MdnsBootstrapConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            service_type: "_kitsune2._udp.local.".to_string(),
            dial_cooldown_ms: 60_000,
            max_concurrent_dials: 4,
        }
    }
}

/// Module-level configuration for [`MdnsBootstrapFactory`](crate::MdnsBootstrapFactory).
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct MdnsBootstrapModConfig {
    /// mDNS bootstrap configuration.
    pub mdns_bootstrap: MdnsBootstrapConfig,
}
