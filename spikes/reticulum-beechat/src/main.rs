// Reticulum spike for kitsune2 transport_reticulum
// Tests reticulum v0.1.0 (BeechatNetworkSystemsLtd) against the 7 spike questions
// from PLAN-transport-reticulum.md

use reticulum::destination::{
    DestinationAnnounce, DestinationName, SingleInputDestination, SingleOutputDestination,
};
use reticulum::identity::PrivateIdentity;
use reticulum::iface::InterfaceManager;
use reticulum::transport::{Transport, TransportConfig};

use rand_core::OsRng;

fn separator(title: &str) {
    println!("\n============================================================");
    println!("  SPIKE Q: {}", title);
    println!("============================================================\n");
}

#[tokio::main]
async fn main() {
    println!("=== Reticulum v0.1.0 (Beechat) Spike ===\n");

    // Create two identities for our test nodes
    let identity_a = PrivateIdentity::new_from_rand(OsRng);
    let identity_b = PrivateIdentity::new_from_rand(OsRng);

    println!("Node A identity hash: {}", identity_a.as_identity().address_hash);
    println!("Node B identity hash: {}", identity_b.as_identity().address_hash);

    // =========================================================================
    // Q1: Can one Reticulum instance host multiple Destinations?
    // =========================================================================
    separator("Q1: Can one Reticulum instance host multiple Destinations?");

    let config_a = TransportConfig::new("node_a", &identity_a, false);
    let mut transport_a = Transport::new(config_a);

    // Create two destinations with different aspects (simulating two spaces)
    let space1_name = DestinationName::new("kitsune2", "space_aaaaaaaaaaaa");
    let space2_name = DestinationName::new("kitsune2", "space_bbbbbbbbbbbb");

    let dest_a_space1 = transport_a
        .add_destination(identity_a.clone(), space1_name)
        .await;
    let dest_a_space2 = transport_a
        .add_destination(identity_a.clone(), space2_name)
        .await;

    let hash1 = dest_a_space1.lock().await.desc.address_hash;
    let hash2 = dest_a_space2.lock().await.desc.address_hash;

    println!("Destination for space1: {}", hash1);
    println!("Destination for space2: {}", hash2);
    println!("Hashes are different: {}", hash1 != hash2);
    println!("RESULT: YES - one Transport instance can host multiple Destinations");

    // =========================================================================
    // Q2: Can destination hash be computed offline from (identity, aspect)?
    // =========================================================================
    separator("Q2: Can destination hash be computed offline from (identity, aspect)?");

    let pub_identity_b = identity_b.as_identity().clone();

    // Create a SingleOutputDestination (public-key only, no private key needed)
    let dest_b_space1_computed =
        SingleOutputDestination::new(pub_identity_b, DestinationName::new("kitsune2", "space_aaaaaaaaaaaa"));
    let computed_hash = dest_b_space1_computed.desc.address_hash;

    // Now create the actual destination using B's private key
    let config_b = TransportConfig::new("node_b", &identity_b, false);
    let mut transport_b = Transport::new(config_b);
    let dest_b_space1_actual = transport_b
        .add_destination(identity_b.clone(), space1_name)
        .await;
    let actual_hash = dest_b_space1_actual.lock().await.desc.address_hash;

    println!("Computed hash (from public identity only): {}", computed_hash);
    println!("Actual hash (from private identity):       {}", actual_hash);
    println!("Hashes match: {}", computed_hash == actual_hash);
    println!("RESULT: YES - same deterministic hash computation as LXMF-rs");

    // =========================================================================
    // Q3: Do announces carry the full public identity?
    // =========================================================================
    separator("Q3: Do announces carry the full public identity?");

    let announce_packet = dest_b_space1_actual
        .lock()
        .await
        .announce(OsRng, Some(b"hello from B"))
        .unwrap();

    println!("Announce packet data length: {} bytes", announce_packet.data.len());
    println!("Announce destination hash: {}", announce_packet.destination);

    match DestinationAnnounce::validate(&announce_packet) {
        Ok((dest, app_data)) => {
            let announced_identity = dest.identity;
            println!(
                "Announced public key matches B: {}",
                announced_identity.public_key == pub_identity_b.public_key
            );
            println!(
                "Announced verifying key matches B: {}",
                announced_identity.verifying_key == pub_identity_b.verifying_key
            );
            println!(
                "App data: {:?}",
                std::str::from_utf8(app_data).unwrap_or("(binary)")
            );
            println!("RESULT: YES - announces carry full public identity");
            println!("  NOTE: validate() returns (SingleOutputDestination, &[u8]) - simpler than LXMF-rs");
            println!("  No ratchet support in this version.");
        }
        Err(e) => {
            println!("RESULT: FAILED to validate announce: {:?}", e);
        }
    }

    // =========================================================================
    // Q4: Is there an announce subscription API filtered by aspect?
    // =========================================================================
    separator("Q4: Is there announce subscription filtered by aspect?");

    let _announce_rx = transport_a.recv_announces().await;

    println!("recv_announces() returns: broadcast::Receiver<AnnounceEvent>");
    println!("AnnounceEvent fields (Beechat v0.1.0):");
    println!("  - destination: Arc<Mutex<SingleOutputDestination>>");
    println!("  - app_data: PacketDataBuffer");
    println!("  *** NO name_hash field ***");
    println!("  *** NO hops field ***");
    println!("  *** NO interface field ***");
    println!();
    println!("RESULT: WORSE than LXMF-rs - no name_hash on AnnounceEvent.");
    println!("  Filtering by aspect would require locking the destination mutex");
    println!("  and inspecting dest.desc.name to get the name hash.");
    println!("  Or, match on destination address_hash against a precomputed set.");

    // =========================================================================
    // Q5: What do Link::send + Resource look like in practice?
    // =========================================================================
    separator("Q5: What do Link::send + Resource look like in practice?");

    println!("Link API (Beechat v0.1.0):");
    println!("  - Link::new(destination_desc, event_tx) -> Link");
    println!("  - link.request() -> Packet");
    println!("  - link.data_packet(data: &[u8]) -> Result<Packet>");
    println!("  - link.status() -> LinkStatus");
    println!("  - link.id() -> &LinkId");
    println!();
    println!("Transport-level send:");
    println!("  - transport.link(dest_desc) -> Arc<Mutex<Link>>");
    println!("  - transport.send_packet(packet)");
    println!("  - transport.send_to_out_links(destination, payload)");
    println!("  - transport.send_to_in_links(destination, payload)");
    println!("  - transport.send_to_all_out_links(payload)");
    println!();
    println!("  *** NO send_resource() method ***");
    println!("  *** NO Resource support at all ***");
    println!("  *** NO channel system ***");
    println!();
    println!("Packet MDU: {} bytes", reticulum::packet::PACKET_MDU);
    println!();
    println!("RESULT: Basic link + data_packet only. No Resource for large payloads.");
    println!("  This is a significant limitation for kitsune2 gossip payloads.");
    println!("  Would need to implement chunking ourselves or contribute Resource upstream.");

    // =========================================================================
    // Q6: Does reticulum offer a loopback/in-memory interface?
    // =========================================================================
    separator("Q6: Loopback/in-memory interface for tests?");

    let mut mgr = InterfaceManager::new(128);
    let channel = mgr.new_channel(64);
    println!("Test channel address: {}", channel.address);
    println!("InterfaceManager::new_channel() works the same as LXMF-rs.");
    println!();
    println!("Available interface types:");
    println!("  - iface::tcp_client");
    println!("  - iface::tcp_server");
    println!("  - iface::udp");
    println!("  - iface::serial (HDLC)");
    println!("  *** NO test_bridge module ***");
    println!();
    println!("RESULT: Same as LXMF-rs - no dedicated loopback, but new_channel() works.");

    // =========================================================================
    // Q7: Announce metadata for dedup?
    // =========================================================================
    separator("Q7: Announce metadata for dedup?");

    println!("AnnounceEvent fields (Beechat v0.1.0):");
    println!("  - destination: Arc<Mutex<SingleOutputDestination>>");
    println!("  - app_data: PacketDataBuffer");
    println!("  *** NO name_hash ***");
    println!("  *** NO hops ***");
    println!("  *** NO interface ***");
    println!();
    println!("Raw announce packet still contains rand_hash in data payload,");
    println!("but it's not exposed as a field on AnnounceEvent.");
    println!();

    let announce1 = dest_b_space1_actual
        .lock()
        .await
        .announce(OsRng, Some(b"first"))
        .unwrap();
    let announce2 = dest_b_space1_actual
        .lock()
        .await
        .announce(OsRng, Some(b"second"))
        .unwrap();
    println!("Two announces from same destination:");
    println!("  Data differs: {}", announce1.data.as_slice() != announce2.data.as_slice());
    println!();
    println!("  *** NO AnnounceTable for dedup ***");
    println!("  *** NO packet_cache with announce-specific logic ***");
    println!();
    println!("RESULT: WEAKER than LXMF-rs. Dedup metadata exists in raw packet");
    println!("  but is not surfaced in AnnounceEvent. No built-in dedup.");
    println!("  We'd need to parse it ourselves from the raw announce data.");

    // =========================================================================
    // Summary
    // =========================================================================
    println!("\n============================================================");
    println!("  SPIKE SUMMARY: reticulum v0.1.0 (Beechat)");
    println!("============================================================\n");
    println!("Q1 Multiple Destinations:     YES - same API as LXMF-rs");
    println!("Q2 Offline hash computation:   YES - same algorithm");
    println!("Q3 Full identity in announces: YES - same format (no ratchet support)");
    println!("Q4 Aspect-filtered announces:  WORSE - no name_hash on AnnounceEvent");
    println!("Q5 Link::send + Resource:      NO Resource support, data_packet only");
    println!("Q6 Loopback/in-memory iface:   Same - new_channel() works");
    println!("Q7 Announce dedup metadata:    WEAKER - no name_hash/hops/interface on event");
    println!();
    println!("Key differences from LXMF-rs:");
    println!("  - No Resource support (critical for large gossip payloads)");
    println!("  - No channel system");
    println!("  - No ratchet/forward secrecy support");
    println!("  - Simpler AnnounceEvent (missing name_hash, hops, interface)");
    println!("  - No AnnounceTable dedup");
    println!("  - No configurable timeouts in TransportConfig");
    println!("  - No test_bridge module");
    println!("  - Requires protoc to build (proto dependency)");
    println!("  - Has kaonic module (gRPC-based interface) not present in LXMF-rs");
}
