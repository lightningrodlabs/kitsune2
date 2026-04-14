//! Functional test: two full `ReticulumNode` instances (including the
//! real `RealEndpoint` bridges) exchange an announce so that node B's
//! bootstrap drain inserts node A's `AgentInfoSigned` into node B's
//! peer store.
//!
//! This is the end-to-end smoke test for the announce + bootstrap
//! pipeline against real `rns_transport` state — wired through an
//! in-process interface loopback rather than over TCP.

use bytes::Bytes;
use kitsune2_api::{
    AgentId, AgentInfo, AgentInfoSigned, BoxFut, DhtArc, DynPeerStore,
    DynVerifier, K2Result, PeerStore, SpaceId, Timestamp, Url, Verifier,
};
use kitsune2_transport_reticulum::ReticulumNode;
use rand_core::OsRng;
use rns_transport::identity::PrivateIdentity;
use rns_transport::iface::{RxMessage, TxMessage};
use rns_transport::transport::{Transport, TransportConfig};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Mutex as TokioMutex;

// ---------------------------------------------------------------------------
// Minimal peer-store / verifier / signer stubs for the test.
// ---------------------------------------------------------------------------

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

#[derive(Debug)]
struct YesSigner;
impl kitsune2_api::Signer for YesSigner {
    fn sign<'a, 'b: 'a, 'c: 'a>(
        &'a self,
        _agent_info: &AgentInfo,
        _message: &'c [u8],
    ) -> BoxFut<'a, K2Result<Bytes>> {
        Box::pin(async move { Ok(Bytes::from_static(b"sig")) })
    }
}

#[derive(Debug, Default)]
struct FakePeerStore {
    inserted: Mutex<Vec<Arc<AgentInfoSigned>>>,
}

impl PeerStore for FakePeerStore {
    fn insert(
        &self,
        agent_list: Vec<Arc<AgentInfoSigned>>,
    ) -> BoxFut<'_, K2Result<()>> {
        self.inserted.lock().unwrap().extend(agent_list);
        Box::pin(async move { Ok(()) })
    }
    fn remove(&self, _a: AgentId) -> BoxFut<'_, K2Result<()>> {
        Box::pin(async move { Ok(()) })
    }
    fn get(
        &self,
        _a: AgentId,
    ) -> BoxFut<'_, K2Result<Option<Arc<AgentInfoSigned>>>> {
        Box::pin(async move { Ok(None) })
    }
    fn get_all(&self) -> BoxFut<'_, K2Result<Vec<Arc<AgentInfoSigned>>>> {
        let v = self.inserted.lock().unwrap().clone();
        Box::pin(async move { Ok(v) })
    }
    fn get_by_overlapping_storage_arc(
        &self,
        _arc: DhtArc,
    ) -> BoxFut<'_, K2Result<Vec<Arc<AgentInfoSigned>>>> {
        Box::pin(async move { Ok(Vec::new()) })
    }
    fn get_near_location(
        &self,
        _loc: u32,
        _limit: usize,
    ) -> BoxFut<'_, K2Result<Vec<Arc<AgentInfoSigned>>>> {
        Box::pin(async move { Ok(Vec::new()) })
    }
    fn get_by_url(
        &self,
        _u: Url,
    ) -> BoxFut<'_, K2Result<Vec<Arc<AgentInfoSigned>>>> {
        Box::pin(async move { Ok(Vec::new()) })
    }
}

// ---------------------------------------------------------------------------
// Loopback bridge: two Transports forward packets to each other over
// in-memory mpsc channels, no TCP involved.
// ---------------------------------------------------------------------------

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

async fn mk_signed(agent: u8, space: SpaceId) -> Arc<AgentInfoSigned> {
    let info = AgentInfo {
        agent: AgentId::from(Bytes::from(vec![agent; 32])),
        space,
        created_at: Timestamp::now(),
        expires_at: Timestamp::now() + Duration::from_secs(3600),
        is_tombstone: false,
        url: Some(
            Url::from_str("ret://reticulum:1/00112233445566778899aabbccddeeff")
                .unwrap(),
        ),
        storage_arc: DhtArc::FULL,
    };
    AgentInfoSigned::sign(&YesSigner, info).await.unwrap()
}

// ---------------------------------------------------------------------------
// The actual test.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn announce_flows_through_real_endpoint_and_bootstrap_drain() {
    use kitsune2_transport_reticulum::internal_testing::{
        ReticulumBootstrap, wire_bootstrap_pipeline,
    };
    use rns_transport::destination::DestinationName;

    // Two rns Transports wired via in-process loopback.
    let (tp_a, id_a) = make_transport("node-a");
    let (tp_b, id_b) = make_transport("node-b");
    wire_loopback(tp_a.clone(), tp_b.clone()).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Node B uses the full `ReticulumNode` stack (receive side).
    let node_b = ReticulumNode::from_rns_transport(tp_b.clone(), id_b.clone())
        .await
        .unwrap();

    let space = SpaceId::from(Bytes::from_static(b"alpha"));

    // B registers the space so its announce filter matches.
    let _dest_hash_b = node_b.register_space_for_test(&space).await.unwrap();

    // B's bootstrap drain + peer store binding.
    let b_store = Arc::new(FakePeerStore::default());
    let b_peer_store: DynPeerStore = b_store.clone();
    let b_verifier: DynVerifier = Arc::new(YesVerifier);
    let _bs_b = ReticulumBootstrap::new(
        node_b.clone(),
        b_peer_store,
        b_verifier,
        space.clone(),
    );

    // Spawn B's announce listener + bootstrap drain (normally done
    // inside `ReticulumTransport::create`; this test stands up only
    // the node, so we wire the pipeline explicitly).
    let _pipeline_handles = wire_bootstrap_pipeline(node_b.clone())
        .await
        .expect("wire_bootstrap_pipeline");

    // Node A: publish an announce directly via the raw rns API. We
    // don't need A to run the full node stack — we just want it to
    // emit an announce whose app_data carries a signed AgentInfo.
    let signed = mk_signed(1, space.clone()).await;
    // Pack into the compact announce wire format — the drain on B
    // decodes back via `announce_wire::decode_announce_wire`.
    let encoded: Vec<u8> =
        kitsune2_transport_reticulum::internal_testing::encode_announce_wire(
            &signed,
        )
        .unwrap()
        .to_vec();

    // A registers the same space destination name B is filtering on.
    // `register_space` uses the hex-encoded space id as the aspect,
    // so we mirror that here.
    let space_hash: String = space.iter().map(|b| format!("{b:02x}")).collect();
    let name = DestinationName::new("kitsune2", &space_hash);
    let dest_a = {
        let mut tp = tp_a.lock().await;
        tp.add_destination(id_a.clone(), name).await
    };

    // Publish announce carrying the AgentInfo JSON as app_data.
    let ann_packet = {
        let mut d = dest_a.lock().await;
        d.announce(OsRng, Some(&encoded)).unwrap()
    };
    {
        let tp = tp_a.lock().await;
        tp.send_packet(ann_packet).await;
    }

    // Wait for B's peer_store to receive A's AgentInfoSigned.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if !b_store.inserted.lock().unwrap().is_empty() {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "timed out waiting for B's peer_store to receive A's AgentInfoSigned"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let inserted = b_store.inserted.lock().unwrap();
    assert_eq!(inserted.len(), 1);
    assert_eq!(inserted[0].agent, signed.agent);

    // Sanity: the encoded bytes we're asking to ride inside a single
    // announce packet. If this is larger than PACKET_MDU the test
    // would have exercised fragmentation -- since rns announces are
    // single-packet, oversized app_data is a real risk. Record the
    // size so a future regression is visible.
    eprintln!(
        "announce app_data size = {} bytes (MDU = {})",
        encoded.len(),
        rns_transport::packet::PACKET_MDU,
    );
}
