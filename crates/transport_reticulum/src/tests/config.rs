//! Config validation tests.

use crate::config::*;

#[test]
fn empty_interfaces_rejected() {
    let config = ReticulumTransportConfig::default();
    assert!(config.validate().is_err());
}

#[test]
fn valid_config_accepted() {
    let config = ReticulumTransportConfig {
        interfaces: vec![ReticulumInterfaceConfig::TcpClient {
            target: "127.0.0.1:4242".to_string(),
        }],
        ..Default::default()
    };
    assert!(config.validate().is_ok());
}

#[test]
fn max_frame_bytes_too_large() {
    let config = ReticulumTransportConfig {
        interfaces: vec![ReticulumInterfaceConfig::TcpClient {
            target: "127.0.0.1:4242".to_string(),
        }],
        max_frame_bytes: 17 * 1024 * 1024,
        ..Default::default()
    };
    assert!(config.validate().is_err());
}

#[test]
fn identity_path_missing_parent() {
    let config = ReticulumTransportConfig {
        interfaces: vec![ReticulumInterfaceConfig::TcpClient {
            target: "127.0.0.1:4242".to_string(),
        }],
        identity_path: Some(std::path::PathBuf::from(
            "/nonexistent/path/identity",
        )),
        ..Default::default()
    };
    assert!(config.validate().is_err());
}
