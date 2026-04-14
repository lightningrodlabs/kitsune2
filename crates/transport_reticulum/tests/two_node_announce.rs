//! Functional test: two `rns_transport::Transport` instances wired
//! together via an in-process loopback bridge.
//!
//! The goal here is to exercise the real `RealEndpoint` bridges
//! (announce, in_link_events, received_data, resource, link closures)
//! against actual `rns_transport` state — not the in-memory fake —
//! so we learn which of the risks called out in
//! `PLAN-transport-reticulum.md` §14 / discussion actually bite.
//!
//! # Harness
//!
//! ```text
//!    Transport A                       Transport B
//!   ┌──────────┐   TxMessage          ┌──────────┐
//!   │ iface_a  │──────────────────┐   │          │
//!   │          │                  ▼   │          │
//!   │          │              ┌─────────────┐    │
//!   │          │              │    bridge   │    │
//!   │          │              └─────────────┘    │
//!   │          │                  │              │
//!   │          │◄─────────────────┘  RxMessage   │
//!   │          │                        etc.     │
//!   └──────────┘                              ────┘
//! ```
//!
//! Each transport exposes an `InterfaceChannel` via
//! `InterfaceManager::new_channel`. The bridge pulls `TxMessage`s off
//! one side's tx-receiver and pushes `RxMessage`s (with the peer's
//! interface address) into the other side's rx-sender. No network, no
//! TCP — just tokio mpsc.

use rand_core::OsRng;
use rns_transport::destination::DestinationName;
use rns_transport::hash::AddressHash;
use rns_transport::identity::PrivateIdentity;
use rns_transport::iface::{RxMessage, TxMessage};
use rns_transport::transport::{Transport, TransportConfig};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as TokioMutex;

/// Wire two transports together with a loopback bridge that forwards
/// each TxMessage on one side to an RxMessage on the other.
async fn wire_loopback(
    tp_a: Arc<TokioMutex<Transport>>,
    tp_b: Arc<TokioMutex<Transport>>,
) {
    let (a_iface_addr, mut a_tx_recv, a_rx_send) = {
        let tp = tp_a.lock().await;
        let mgr = tp.iface_manager();
        let mut mgr = mgr.lock().await;
        let ch = mgr.new_channel(128);
        (ch.address, ch.tx_channel, ch.rx_channel)
    };
    let (b_iface_addr, mut b_tx_recv, b_rx_send) = {
        let tp = tp_b.lock().await;
        let mgr = tp.iface_manager();
        let mut mgr = mgr.lock().await;
        let ch = mgr.new_channel(128);
        (ch.address, ch.tx_channel, ch.rx_channel)
    };

    // A → B
    let b_rx_send_clone = b_rx_send.clone();
    tokio::spawn(async move {
        while let Some(TxMessage { packet, .. }) = a_tx_recv.recv().await {
            let _ = b_rx_send_clone
                .send(RxMessage {
                    address: b_iface_addr,
                    packet,
                })
                .await;
        }
    });
    // B → A
    let a_rx_send_clone = a_rx_send.clone();
    tokio::spawn(async move {
        while let Some(TxMessage { packet, .. }) = b_tx_recv.recv().await {
            let _ = a_rx_send_clone
                .send(RxMessage {
                    address: a_iface_addr,
                    packet,
                })
                .await;
        }
    });
}

fn make_transport(name: &str) -> (Arc<TokioMutex<Transport>>, PrivateIdentity) {
    let identity = PrivateIdentity::new_from_rand(OsRng);
    let mut cfg = TransportConfig::new(name, &identity, true);
    cfg.set_link_proof_timeout_secs(5);
    cfg.set_link_idle_timeout_secs(30);
    let tp = Transport::new(cfg);
    (Arc::new(TokioMutex::new(tp)), identity)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_transports_exchange_announces() {
    let (tp_a, id_a) = make_transport("node-a");
    let (tp_b, _id_b) = make_transport("node-b");

    wire_loopback(tp_a.clone(), tp_b.clone()).await;

    // Give the bridge tasks a moment to come up.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // B subscribes to announces.
    let mut b_announces = {
        let tp = tp_b.lock().await;
        tp.recv_announces().await
    };

    // A registers a destination and announces.
    let name = DestinationName::new("kitsune2", "test-space");
    let dest = {
        let mut tp = tp_a.lock().await;
        tp.add_destination(id_a.clone(), name).await
    };
    let expected_hash = dest.lock().await.desc.address_hash;

    let announce_packet = {
        let mut d = dest.lock().await;
        d.announce(OsRng, Some(b"hello from A")).unwrap()
    };
    {
        let tp = tp_a.lock().await;
        tp.send_packet(announce_packet).await;
    }

    // B should observe the announce within a reasonable window.
    let ev = tokio::time::timeout(Duration::from_secs(2), b_announces.recv())
        .await
        .expect("timed out waiting for announce on B")
        .expect("announce broadcast closed");

    let got_hash = ev.destination.lock().await.desc.address_hash;
    assert_eq!(
        got_hash, expected_hash,
        "B saw the announce but destination hash didn't match A's"
    );
    assert_eq!(ev.app_data.as_slice(), b"hello from A");

    // Let the runtime drain.
    let _ = (tp_a, tp_b);
}

/// Minimal smoke test: can a destination address hash be derived
/// offline from a peer's Identity (using `new_out`) and match the one
/// that the peer's `add_destination` produced? This is plan §1 /
/// spike Q2; regressing it would break per-space link establishment.
#[tokio::test(flavor = "current_thread")]
async fn destination_hash_matches_offline_derivation() {
    use rns_transport::destination::new_out;

    let identity = PrivateIdentity::new_from_rand(OsRng);
    let name = DestinationName::new("kitsune2", "some-space");

    // Hash produced when the owner actually adds the destination.
    let mut cfg = TransportConfig::new("owner", &identity, true);
    cfg.set_link_proof_timeout_secs(5);
    let tp = Transport::new(cfg);
    let dest = {
        let mut tp_guard =
            Transport::new(TransportConfig::new("owner", &identity, true));
        tp_guard.add_destination(identity.clone(), name).await
    };
    let actual = dest.lock().await.desc.address_hash;
    let _ = tp;

    // Hash computed offline from only the public Identity + name.
    let out_dest = new_out(*identity.as_identity(), "kitsune2", "some-space");
    let computed = out_dest.desc.address_hash;

    assert_eq!(
        computed, actual,
        "offline-derived destination hash must equal add_destination's"
    );

    // Make sure it isn't trivially all-zero.
    assert_ne!(actual, AddressHash::new([0u8; 16]));
}
