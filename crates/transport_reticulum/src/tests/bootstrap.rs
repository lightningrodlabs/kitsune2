//! Tests for the bootstrap drain pipeline.
//!
//! Covers the round-trip from `Bootstrap::put(info)` → staged
//! `app_data` on the node → announce → listener → drain →
//! `PeerStore::insert`.

use crate::announce::{new_identity_cache, spawn_announce_listener};
use crate::bootstrap::{spawn_bootstrap_drain, ReticulumBootstrap};
use crate::destination::Endpoint;
use crate::node::ReticulumNode;
use crate::test_utils::{fake_announce, FakeEndpoint};
use bytes::Bytes;
use kitsune2_api::{
    AgentInfo, AgentInfoSigned, AgentId, Bootstrap, DhtArc, DynPeerStore,
    DynVerifier, K2Result, SpaceId, Timestamp, Url, Verifier,
};
use rns_transport::destination::DestinationName;
use rns_transport::hash::AddressHash;
use rns_transport::identity::PrivateIdentity;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

/// Trivial always-accept verifier for tests.
#[derive(Debug)]
struct YesVerifier;
impl Verifier for YesVerifier {
    fn verify(
        &self,
        _agent_info: &AgentInfo,
        _message: &[u8],
        _signature: &[u8],
    ) -> bool {
        true
    }
}

/// Minimal in-memory PeerStore capturing inserted infos.
#[derive(Debug, Default)]
struct FakePeerStore {
    inserted: Mutex<Vec<Arc<AgentInfoSigned>>>,
}

impl kitsune2_api::PeerStore for FakePeerStore {
    fn insert(
        &self,
        agent_list: Vec<Arc<AgentInfoSigned>>,
    ) -> kitsune2_api::BoxFut<'_, K2Result<()>> {
        self.inserted.lock().unwrap().extend(agent_list);
        Box::pin(async move { Ok(()) })
    }
    fn remove(
        &self,
        _agent_id: AgentId,
    ) -> kitsune2_api::BoxFut<'_, K2Result<()>> {
        Box::pin(async move { Ok(()) })
    }
    fn get(
        &self,
        _agent: AgentId,
    ) -> kitsune2_api::BoxFut<'_, K2Result<Option<Arc<AgentInfoSigned>>>> {
        Box::pin(async move { Ok(None) })
    }
    fn get_all(
        &self,
    ) -> kitsune2_api::BoxFut<'_, K2Result<Vec<Arc<AgentInfoSigned>>>> {
        let all = self.inserted.lock().unwrap().clone();
        Box::pin(async move { Ok(all) })
    }
    fn get_by_overlapping_storage_arc(
        &self,
        _arc: DhtArc,
    ) -> kitsune2_api::BoxFut<'_, K2Result<Vec<Arc<AgentInfoSigned>>>> {
        Box::pin(async move { Ok(Vec::new()) })
    }
    fn get_near_location(
        &self,
        _loc: u32,
        _limit: usize,
    ) -> kitsune2_api::BoxFut<'_, K2Result<Vec<Arc<AgentInfoSigned>>>> {
        Box::pin(async move { Ok(Vec::new()) })
    }
    fn get_by_url(
        &self,
        _peer_url: Url,
    ) -> kitsune2_api::BoxFut<'_, K2Result<Vec<Arc<AgentInfoSigned>>>> {
        Box::pin(async move { Ok(Vec::new()) })
    }
}

/// Fabricate a signed AgentInfo for testing.
async fn mk_signed(agent: u8, space: SpaceId) -> Arc<AgentInfoSigned> {
    #[derive(Debug)]
    struct YesSigner;
    impl kitsune2_api::Signer for YesSigner {
        fn sign<'a, 'b: 'a, 'c: 'a>(
            &'a self,
            _agent_info: &AgentInfo,
            _message: &'c [u8],
        ) -> kitsune2_api::BoxFut<'a, K2Result<Bytes>> {
            Box::pin(async move { Ok(Bytes::from_static(b"sig")) })
        }
    }

    let signer = YesSigner;
    let info = AgentInfo {
        agent: AgentId::from(Bytes::from(vec![agent; 32])),
        space,
        created_at: Timestamp::now(),
        expires_at: Timestamp::now() + std::time::Duration::from_secs(3600),
        is_tombstone: false,
        url: Some(
            Url::from_str(
                "ret://reticulum:1/00112233445566778899aabbccddeeff",
            )
            .unwrap(),
        ),
        storage_arc: DhtArc::FULL,
    };
    AgentInfoSigned::sign(&signer, info).await.unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn put_stages_agent_info_for_announce() {
    let endpoint = FakeEndpoint::new();
    let node = ReticulumNode::new(endpoint, AddressHash::new([0; 16]));
    let store: DynPeerStore = Arc::new(FakePeerStore::default());
    let verifier: DynVerifier = Arc::new(YesVerifier);
    let space = SpaceId::from(Bytes::from_static(b"alpha"));

    let bs = ReticulumBootstrap::new(
        node.clone(),
        store,
        verifier,
        space.clone(),
    );

    let signed = mk_signed(1, space.clone()).await;
    bs.put(signed.clone());

    // The node now has compressed bytes staged for that space; the
    // drain decompresses before decoding, so mirror that here.
    let staged = node.get_my_agent_info(&space).expect("staged");
    let decompressed = {
        use flate2::read::DeflateDecoder;
        use std::io::Read;
        let mut dec = DeflateDecoder::new(&staged[..]);
        let mut out = Vec::new();
        dec.read_to_end(&mut out).unwrap();
        out
    };
    let decoded =
        AgentInfoSigned::decode(&YesVerifier, &decompressed).unwrap();
    assert_eq!(decoded.agent, signed.agent);
}

#[tokio::test(flavor = "current_thread")]
async fn drain_inserts_decoded_agent_info_into_peer_store() {
    let endpoint = FakeEndpoint::new();
    let node = ReticulumNode::new(endpoint.clone(), AddressHash::new([0; 16]));

    // Keep a concrete handle to assert on, plus a trait-object Arc.
    let concrete_store = Arc::new(FakePeerStore::default());
    let store: DynPeerStore = concrete_store.clone();
    let verifier: DynVerifier = Arc::new(YesVerifier);
    let space = SpaceId::from(Bytes::from_static(b"alpha"));

    // Bootstrap registers its peer_store + verifier on the node.
    let _bs = ReticulumBootstrap::new(
        node.clone(),
        store,
        verifier,
        space.clone(),
    );

    // Bind the space's announce name_hash so the listener's filter matches.
    let name = DestinationName::new("kitsune2", "somespace");
    let mut name_hash = [0u8; 10];
    let slice = name.as_name_hash_slice();
    let n = slice.len().min(10);
    name_hash[..n].copy_from_slice(&slice[..n]);
    let hashes: Arc<RwLock<HashMap<[u8; 10], Bytes>>> =
        node.space_name_hashes().clone();
    hashes
        .write()
        .unwrap()
        .insert(name_hash, Bytes::copy_from_slice(&space));

    // Spawn listener and drain.
    let rx_ann = endpoint.recv_announces().await.unwrap();
    let cache = new_identity_cache();
    let _hl = spawn_announce_listener(
        rx_ann,
        cache,
        hashes,
        node.peer_discovered_tx().clone(),
    );
    let drain_rx = node.take_peer_discovered_rx().await.unwrap();
    let _hd = spawn_bootstrap_drain(drain_rx, node.clone());

    // Fabricate a signed AgentInfo and encode it as the announce app_data.
    let signed = mk_signed(2, space.clone()).await;
    // Announce app_data is compressed -- the drain decompresses.
    let encoded_bytes: Bytes = crate::bootstrap::compress_app_data(
        signed.encode().unwrap().as_bytes(),
    )
    .unwrap();

    // Inject an announce carrying the signed AgentInfo in app_data.
    let identity =
        *PrivateIdentity::new_from_rand(rand_core::OsRng).as_identity();
    let mut info = fake_announce(name, identity);
    info.app_data = encoded_bytes;
    endpoint.inject_announce(info);

    // Give listener + drain a moment to process.
    tokio::time::sleep(Duration::from_millis(80)).await;

    // The peer_store should have our signed AgentInfo inserted.
    let inserted = concrete_store.inserted.lock().unwrap();
    assert_eq!(
        inserted.len(),
        1,
        "expected one AgentInfoSigned inserted"
    );
    assert_eq!(inserted[0].agent, signed.agent);
}

#[tokio::test(flavor = "current_thread")]
async fn drain_skips_empty_app_data() {
    let endpoint = FakeEndpoint::new();
    let node = ReticulumNode::new(endpoint.clone(), AddressHash::new([0; 16]));

    let concrete_store = Arc::new(FakePeerStore::default());
    let store: DynPeerStore = concrete_store.clone();
    let verifier: DynVerifier = Arc::new(YesVerifier);
    let space = SpaceId::from(Bytes::from_static(b"alpha"));

    let _bs = ReticulumBootstrap::new(
        node.clone(),
        store,
        verifier,
        space.clone(),
    );

    let name = DestinationName::new("kitsune2", "somespace");
    let mut name_hash = [0u8; 10];
    let slice = name.as_name_hash_slice();
    let n = slice.len().min(10);
    name_hash[..n].copy_from_slice(&slice[..n]);
    let hashes = node.space_name_hashes().clone();
    hashes
        .write()
        .unwrap()
        .insert(name_hash, Bytes::copy_from_slice(&space));

    let rx_ann = endpoint.recv_announces().await.unwrap();
    let _hl = spawn_announce_listener(
        rx_ann,
        new_identity_cache(),
        hashes,
        node.peer_discovered_tx().clone(),
    );
    let drain_rx = node.take_peer_discovered_rx().await.unwrap();
    let _hd = spawn_bootstrap_drain(drain_rx, node.clone());

    // Announce with empty app_data -- listener still forwards it but
    // the drain should skip the peer_store insert.
    let identity =
        *PrivateIdentity::new_from_rand(rand_core::OsRng).as_identity();
    endpoint.inject_announce(fake_announce(name, identity));

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(concrete_store.inserted.lock().unwrap().is_empty());
}
