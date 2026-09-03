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

    /// How often, in milliseconds, announced peers the transport is not
    /// connected to are considered for another dial. A peer is dialled
    /// once when its announcement is first heard; if that dial fails —
    /// commonly because the peer's transport record has not reached this
    /// node yet — it is retried after one interval, then two, four, up to
    /// sixteen intervals between dials, for as long as the LAN keeps
    /// announcing the peer. A fresh announcement restarts the short
    /// schedule.
    ///
    /// All spaces of one factory share one reconciliation ticker, whose
    /// interval is that of the space that started the daemon; a
    /// per-space override of this value is not honoured by later spaces.
    ///
    /// Default: 30 seconds.
    #[cfg_attr(feature = "schema", schemars(default))]
    pub redial_interval_ms: u32,

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
            redial_interval_ms: 30_000,
            max_concurrent_dials: 4,
        }
    }
}

/// The longest service label mDNS-SD allows, in bytes, not counting the
/// leading underscore. The daemon enforces this inside its own thread after
/// `register` has already returned success, so a longer label would browse
/// but never announce; checking it here turns that silence into a config
/// error.
pub const SERVICE_LABEL_MAX_LEN: usize = 15;

/// Check that `service_type` is a service type this crate can announce
/// under: `_<label>._udp.local.` or `_<label>._tcp.local.` with a label of
/// 1 to [`SERVICE_LABEL_MAX_LEN`] bytes.
pub fn validate_service_type(service_type: &str) -> Result<(), String> {
    let label = service_type
        .strip_suffix("._udp.local.")
        .or_else(|| service_type.strip_suffix("._tcp.local."))
        .and_then(|name| name.strip_prefix('_'))
        .filter(|label| !label.is_empty() && !label.contains('.'))
        .ok_or_else(|| {
            format!(
                "mdnsBootstrap.serviceType must be of the form _name._udp.local., got {service_type:?}"
            )
        })?;
    if label.len() > SERVICE_LABEL_MAX_LEN {
        return Err(format!(
            "mdnsBootstrap.serviceType label {label:?} is {} bytes, the mDNS limit is {SERVICE_LABEL_MAX_LEN}",
            label.len()
        ));
    }
    Ok(())
}

/// Module-level configuration for [`MdnsBootstrapFactory`](crate::MdnsBootstrapFactory).
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct MdnsBootstrapModConfig {
    /// mDNS bootstrap configuration.
    pub mdns_bootstrap: MdnsBootstrapConfig,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_service_type_is_valid() {
        validate_service_type(&MdnsBootstrapConfig::default().service_type)
            .unwrap();
    }

    #[test]
    fn a_fifteen_byte_label_is_the_longest_allowed() {
        validate_service_type("_abcdefghijklmno._udp.local.").unwrap();
        validate_service_type("_abcdefghijklmno._tcp.local.").unwrap();
        let err = validate_service_type("_abcdefghijklmnop._udp.local.")
            .expect_err("16-byte label");
        assert!(err.contains("16 bytes"), "{err}");
    }

    #[test]
    fn malformed_service_types_are_rejected() {
        for bad in [
            "",
            "_udp.local.",
            "kitsune2._udp.local.",
            "_._udp.local.",
            "_kitsune2._udp.local",
            "_kitsune2.local.",
            "_a._sub._kitsune2._udp.local.",
        ] {
            assert!(validate_service_type(bad).is_err(), "{bad:?}");
        }
    }
}
