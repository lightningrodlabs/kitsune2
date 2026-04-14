//! Configuration for the mDNS bootstrap factory.

use serde::{Deserialize, Serialize};

/// Configuration parameters for [`MdnsBootstrapFactory`](crate::MdnsBootstrapFactory).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct MdnsBootstrapConfig {
    /// Enable mDNS bootstrap. When false, the factory produces a no-op
    /// bootstrap that accepts `put`s and discards them.
    ///
    /// Default: `false`. This makes the factory safe to include in any
    /// builder stack without unexpectedly starting an mDNS service.
    #[cfg_attr(feature = "schema", schemars(default))]
    pub enabled: bool,

    /// mDNS service type. Clients only discover peers publishing the same
    /// service type. Must be of the form `_name._udp.local.`.
    ///
    /// Default: `_kitsune2._udp.local.`.
    #[cfg_attr(feature = "schema", schemars(default))]
    pub service_type: String,

    /// How often, in milliseconds, to re-announce our presence on mDNS.
    /// `mdns-sd` handles periodic announcements internally; this knob
    /// controls how often we refresh the advertised record when our agent
    /// info changes (new URL, new expiry).
    ///
    /// Default: 30 seconds.
    #[cfg_attr(feature = "schema", schemars(default))]
    pub refresh_interval_ms: u32,
}

impl Default for MdnsBootstrapConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            service_type: "_kitsune2._udp.local.".to_string(),
            refresh_interval_ms: 30_000,
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
