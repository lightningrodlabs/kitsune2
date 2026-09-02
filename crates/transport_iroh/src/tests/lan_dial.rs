//! Unit tests for how LAN discovery changes outbound connection attempts:
//! the relay-down guard lets a known LAN path through, and a failed dial
//! only counts against the peer when our own relay was up for it.

use super::fakes::*;
use crate::url::endpoint_from_url;
use crate::{IrohTransportConfig, RELAY_NOT_CONNECTED_ERR};
use iroh::TransportAddr;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};

fn lan_config() -> IrohTransportConfig {
    IrohTransportConfig {
        enable_lan_discovery: true,
        // Keep the outer timeout out of the way so the fake's own outcome is
        // what the tests observe.
        connect_timeout_s: 60,
        ..Default::default()
    }
}

fn lan_addr() -> SocketAddr {
    "192.168.1.20:4433".parse().unwrap()
}

/// Run one connection attempt against `endpoint`, returning the error it
/// produced and the `set_unresponsive` calls it made.
async fn attempt(
    endpoint: Arc<FakeEndpoint>,
    config: IrohTransportConfig,
) -> (String, UnresponsiveCalls) {
    let calls: UnresponsiveCalls = Arc::new(Mutex::new(Vec::new()));
    let remote_url = fake_remote_url();
    let target = endpoint_from_url(&remote_url).unwrap();
    let transport = build_transport(
        endpoint,
        build_handler_with_space(calls.clone()),
        crate::Connections::new(),
        Arc::new(RwLock::new(Some(fake_local_url()))),
        config,
    );
    let err = transport
        .create_connection_and_context(target, remote_url)
        .await
        .expect_err("the fake endpoint never yields a connection")
        .to_string();
    (err, calls)
}

/// With the relay known down, a peer that LAN discovery knows is dialled
/// with the discovered address added to the relay-only target.
#[tokio::test]
async fn relay_down_dials_lan_addresses_when_discovery_knows_them() {
    let endpoint = Arc::new(FakeEndpoint {
        connect: connect_fails("no route"),
        relay_known_down: true,
        relay_connected: false,
        direct_addrs: vec![TransportAddr::Ip(lan_addr())],
        ..Default::default()
    });

    let (err, _) = attempt(endpoint.clone(), lan_config()).await;

    assert!(err.contains("iroh connect error"), "got: {err}");
    let targets = endpoint.connect_targets.lock().unwrap();
    assert_eq!(targets.len(), 1, "connect must be attempted once");
    assert!(
        targets[0].addrs.contains(&TransportAddr::Ip(lan_addr())),
        "dial target must carry the LAN address: {:?}",
        targets[0].addrs
    );
    assert!(
        targets[0].addrs.iter().any(|a| a.is_relay()),
        "the relay address from the peer URL must be kept"
    );
}

/// With the relay known down and nothing known on the LAN, the attempt is
/// skipped without touching the peer's standing.
#[tokio::test]
async fn relay_down_without_lan_addresses_skips_the_dial() {
    let endpoint = Arc::new(FakeEndpoint {
        relay_known_down: true,
        relay_connected: false,
        ..Default::default()
    });

    let (err, calls) = attempt(endpoint.clone(), lan_config()).await;

    assert!(err.contains(RELAY_NOT_CONNECTED_ERR), "got: {err}");
    assert!(endpoint.connect_targets.lock().unwrap().is_empty());
    assert!(calls.lock().unwrap().is_empty());
}

/// Without LAN discovery enabled the guard does not consult discovery at
/// all: a dial with the relay down is skipped even if a lookup would have
/// answered.
#[tokio::test]
async fn relay_down_guard_ignores_discovery_when_lan_discovery_is_off() {
    let endpoint = Arc::new(FakeEndpoint {
        relay_known_down: true,
        relay_connected: false,
        direct_addrs: vec![TransportAddr::Ip(lan_addr())],
        ..Default::default()
    });

    let (err, _) =
        attempt(endpoint.clone(), IrohTransportConfig::default()).await;

    assert!(err.contains(RELAY_NOT_CONNECTED_ERR), "got: {err}");
    assert!(endpoint.connect_targets.lock().unwrap().is_empty());
}

/// A failed dial while our relay is not connected says nothing about the
/// peer, so it is not marked unresponsive.
#[tokio::test]
async fn connect_failure_does_not_blame_peer_while_relay_is_disconnected() {
    let endpoint = Arc::new(FakeEndpoint {
        connect: connect_fails("timed out"),
        relay_connected: false,
        ..Default::default()
    });

    let (err, calls) = attempt(endpoint, lan_config()).await;

    assert!(err.contains("iroh connect error"), "got: {err}");
    assert!(
        calls.lock().unwrap().is_empty(),
        "peer must not be marked unresponsive while our relay is down"
    );
}

/// A failed dial while our relay is connected is the peer's problem.
#[tokio::test]
async fn connect_failure_blames_peer_while_relay_is_connected() {
    let endpoint = Arc::new(FakeEndpoint {
        connect: connect_fails("timed out"),
        relay_connected: true,
        ..Default::default()
    });

    let (err, calls) = attempt(endpoint, lan_config()).await;

    assert!(err.contains("iroh connect error"), "got: {err}");
    assert_eq!(calls.lock().unwrap().len(), 1);
    assert_eq!(calls.lock().unwrap()[0].0, fake_remote_url());
}
