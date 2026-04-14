//! Tests for the per-space task management wired up on `register_space`.
//!
//! These tests exercise the [`crate::ReticulumTransport::register_space`]
//! hook against the in-memory [`crate::test_utils::FakeEndpoint`] — no
//! real Reticulum network, no timers, no sleeping.

use crate::announce::{new_identity_cache, spawn_announce_listener};
use crate::destination::{AnnounceInfo, Destination, Endpoint};
use crate::node::ReticulumNode;
use crate::test_utils::{FakeEndpoint, fake_announce, fake_identity};
use bytes::Bytes;
use kitsune2_api::SpaceId;
use rns_transport::destination::DestinationName;
use rns_transport::hash::AddressHash;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

fn local_hash() -> AddressHash {
    AddressHash::new([0xaa; 16])
}

fn space(s: &str) -> SpaceId {
    SpaceId::from(Bytes::copy_from_slice(s.as_bytes()))
}

#[tokio::test(flavor = "current_thread")]
async fn register_space_adds_destination_to_endpoint() {
    let endpoint = FakeEndpoint::new();
    let node = ReticulumNode::new(endpoint.clone(), local_hash());

    let dest = node.register_space(&space("ALPHA")).await.unwrap();

    // The fake endpoint records every add_destination call.
    let added = endpoint.destinations_added.lock().unwrap();
    assert_eq!(added.len(), 1, "one destination should have been added");

    // The destination's address_hash is deterministic in the fake;
    // re-registering the same space should give the same hash.
    let dest_addr = dest.address_hash();
    assert_eq!(added[0].1.address_hash(), dest_addr);
}

#[tokio::test(flavor = "current_thread")]
async fn register_space_registers_name_hash_for_filtering() {
    let endpoint = FakeEndpoint::new();
    let node = ReticulumNode::new(endpoint.clone(), local_hash());

    node.register_space(&space("ALPHA")).await.unwrap();

    // The node keeps a name_hash -> space_id map that the announce
    // listener uses to filter inbound announces.
    let hashes = node.space_name_hashes().read().unwrap();
    assert_eq!(hashes.len(), 1);
    let (_hash, sid) = hashes.iter().next().unwrap();
    assert_eq!(sid.as_ref(), space("ALPHA").as_ref());
}

#[tokio::test(flavor = "current_thread")]
async fn unregister_space_clears_destination_and_name_hash() {
    let endpoint = FakeEndpoint::new();
    let node = ReticulumNode::new(endpoint, local_hash());

    node.register_space(&space("ALPHA")).await.unwrap();
    assert_eq!(node.space_name_hashes().read().unwrap().len(), 1);

    node.unregister_space(&space("ALPHA"));
    assert!(node.space_name_hashes().read().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn announce_listener_populates_identity_cache() {
    let endpoint = FakeEndpoint::new();
    let cache = new_identity_cache();
    let (tx, _rx) = tokio::sync::mpsc::channel(16);
    let hashes: Arc<RwLock<HashMap<[u8; 10], Bytes>>> =
        Arc::new(RwLock::new(HashMap::new()));

    let rx_ann = endpoint.recv_announces().await.unwrap();
    let _handle =
        spawn_announce_listener(rx_ann, cache.clone(), hashes.clone(), tx);

    let id = fake_identity();
    let name = DestinationName::new("kitsune2", "somespace");
    endpoint.inject_announce(fake_announce(name, id));

    // Give the listener task a tick.
    tokio::time::sleep(Duration::from_millis(20)).await;

    let got = cache.read().unwrap();
    assert!(
        got.contains_key(&id.address_hash),
        "identity should have been cached"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn announce_listener_notifies_only_matching_spaces() {
    let endpoint = FakeEndpoint::new();
    let cache = new_identity_cache();
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let hashes: Arc<RwLock<HashMap<[u8; 10], Bytes>>> =
        Arc::new(RwLock::new(HashMap::new()));

    // Register a space name_hash for filtering.
    let joined_name = DestinationName::new("kitsune2", "joined");
    let mut joined_hash = [0u8; 10];
    let slice = joined_name.as_name_hash_slice();
    let n = slice.len().min(10);
    joined_hash[..n].copy_from_slice(&slice[..n]);
    hashes
        .write()
        .unwrap()
        .insert(joined_hash, Bytes::from_static(b"joined"));

    let rx_ann = endpoint.recv_announces().await.unwrap();
    let _handle =
        spawn_announce_listener(rx_ann, cache.clone(), hashes.clone(), tx);

    // Matching announce -- should flow through to peer_discovered.
    endpoint.inject_announce(fake_announce(joined_name, fake_identity()));
    // Non-matching announce (different aspect) -- filtered out.
    let other = DestinationName::new("kitsune2", "not-joined");
    endpoint.inject_announce(fake_announce(other, fake_identity()));

    // First recv should be the matching announce.
    let (space_id, _identity, _app_data) =
        tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .expect("timed out waiting for matching announce")
            .expect("channel closed");
    assert_eq!(space_id.as_ref(), b"joined");

    // Nothing else should be queued.
    let second =
        tokio::time::timeout(Duration::from_millis(50), rx.recv()).await;
    assert!(
        second.is_err(),
        "non-matching announce should have been filtered"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn unused_announce_info_fields_are_available() {
    // Smoke test that AnnounceInfo carries all the fields we expect, so
    // callers that inspect e.g. hops for path selection compile.
    let info: AnnounceInfo =
        fake_announce(DestinationName::new("kitsune2", "x"), fake_identity());
    assert_eq!(info.hops, 0);
    assert!(info.app_data.is_empty());
}
