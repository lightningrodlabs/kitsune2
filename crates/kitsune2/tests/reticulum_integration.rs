//! Step-16 integration test: two Kitsune instances, each built via
//! `kitsune2::reticulum_builder`, exchange ops through a real
//! `rns_transport`-backed gossip round.
//!
//! This is the single end-to-end integration test the plan calls for
//! in `crates/kitsune2/`. Unlike the unit tests in
//! `crates/transport_reticulum`, this wires everything — `Builder`,
//! default modules, gossip, bootstrap, both transport factories —
//! and drives gossip from one node to the other until the op stores
//! converge.
//!
//! Two rns Transports are wired via an in-process loopback bridge
//! (`InterfaceManager::new_channel` + a userspace forwarder, same
//! pattern as the `transport_reticulum` crate's integration tests).
//! There is no external bootstrap server — Reticulum's announce
//! mechanism handles peer discovery.

#![cfg(feature = "transport-reticulum")]

use bytes::Bytes;
use kitsune2::reticulum_builder;
use kitsune2_api::{
    BoxFut, Config, DhtArc, DynKitsune, DynSpace, DynSpaceHandler, K2Result,
    KitsuneHandler, LocalAgent, OpId, SpaceHandler, SpaceId, Timestamp,
};
use kitsune2_core::{Ed25519LocalAgent, factories::MemoryOp};
use kitsune2_gossip::{K2GossipConfig, K2GossipModConfig};
use kitsune2_test_utils::{
    enable_tracing, iter_check, random_bytes, space::TEST_SPACE_ID,
};
use kitsune2_transport_reticulum::{
    ReticulumInterfaceConfig, ReticulumNode, ReticulumTransportConfig,
    ReticulumTransportModConfig,
};
use rand_core::OsRng;
use rns_transport::identity::PrivateIdentity;
use rns_transport::iface::{RxMessage, TxMessage};
use rns_transport::transport::{
    Transport as RnsTransport, TransportConfig as RnsTransportConfig,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as TokioMutex;

// ---------------------------------------------------------------------------
// Minimal handlers.
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct TestKitsuneHandler;
impl KitsuneHandler for TestKitsuneHandler {
    fn create_space(
        &self,
        _space_id: SpaceId,
        _config_override: Option<&Config>,
    ) -> BoxFut<'_, K2Result<DynSpaceHandler>> {
        Box::pin(async { Ok(Arc::new(TestSpaceHandler) as DynSpaceHandler) })
    }
}

#[derive(Debug)]
struct TestSpaceHandler;
impl SpaceHandler for TestSpaceHandler {}

// ---------------------------------------------------------------------------
// Two-node loopback harness.
// ---------------------------------------------------------------------------

/// Build an rns Transport in a Mutex for sharing.
fn make_rns_transport(
    name: &str,
) -> (Arc<TokioMutex<RnsTransport>>, PrivateIdentity) {
    let identity = PrivateIdentity::new_from_rand(OsRng);
    let mut cfg = RnsTransportConfig::new(name, &identity, true);
    cfg.set_link_proof_timeout_secs(5);
    cfg.set_link_idle_timeout_secs(60);
    let tp = RnsTransport::new(cfg);
    (Arc::new(TokioMutex::new(tp)), identity)
}

/// Forward packets between two rns Transports' interface channels.
async fn wire_loopback(
    tp_a: Arc<TokioMutex<RnsTransport>>,
    tp_b: Arc<TokioMutex<RnsTransport>>,
) {
    let (a_iface_addr, mut a_tx_recv, a_rx_send) = {
        let tp = tp_a.lock().await;
        let mgr = tp.iface_manager();
        let mut mgr = mgr.lock().await;
        let ch = mgr.new_channel(512);
        (ch.address, ch.tx_channel, ch.rx_channel)
    };
    let (b_iface_addr, mut b_tx_recv, b_rx_send) = {
        let tp = tp_b.lock().await;
        let mgr = tp.iface_manager();
        let mut mgr = mgr.lock().await;
        let ch = mgr.new_channel(512);
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

/// Build a Kitsune instance on top of a `ReticulumNode`.
async fn make_kitsune_node(node: Arc<ReticulumNode>) -> DynKitsune {
    let builder = reticulum_builder(node)
        .with_default_config()
        .expect("with_default_config");

    // The config registered by `ReticulumTransportFactory::default_config`
    // is the `ReticulumTransportModConfig` defaults, which has an empty
    // `interfaces` list — fine for tests that never call
    // `from_config` (we feed a pre-built node), but the factory's
    // `validate_config` rejects empty interfaces. Override with a
    // placeholder that's never started.
    builder
        .config
        .set_module_config(&ReticulumTransportModConfig {
            reticulum_transport: ReticulumTransportConfig {
                interfaces: vec![ReticulumInterfaceConfig::TcpClient {
                    target: "0.0.0.0:0".to_string(),
                }],
                identity_path: None,
                max_frame_bytes: 1024 * 1024,
                connect_timeout_s: 10,
                // Tight so peers discover each other quickly in-test.
                announce_interval_s: 1,
                link_idle_timeout_s: 60,
                beechat: Default::default(),
            },
        })
        .unwrap();

    // Make gossip aggressive enough to finish quickly.
    builder
        .config
        .set_module_config(&K2GossipModConfig {
            k2_gossip: K2GossipConfig {
                initiate_interval_ms: 1000,
                min_initiate_interval_ms: 100,
                initiate_jitter_ms: 100,
                round_timeout_ms: 10_000,
                ..Default::default()
            },
        })
        .unwrap();

    let kitsune = builder.build().await.expect("builder.build");
    kitsune
        .register_handler(Arc::new(TestKitsuneHandler))
        .await
        .expect("register_handler");
    kitsune
}

async fn start_space(kitsune: &DynKitsune) -> DynSpace {
    let space = kitsune.space(TEST_SPACE_ID, None).await.unwrap();
    let local_agent = Arc::new(Ed25519LocalAgent::default());
    local_agent.set_tgt_storage_arc_hint(DhtArc::FULL);
    space.local_agent_join(local_agent.clone()).await.unwrap();

    // Wait for the local agent to land in this node's peer store
    // (the local `set_agent_info` path, not the inbound bootstrap
    // drain — that one fires on the peer's discoveries, not ours).
    iter_check!(5000, 100, {
        let agent = local_agent.agent().clone();
        if matches!(space.peer_store().get(agent).await, Ok(Some(_))) {
            break;
        }
    });

    space
}

fn create_op_list(num_ops: u16) -> (Vec<Bytes>, Vec<OpId>) {
    let mut ops = Vec::new();
    let mut op_ids = Vec::new();
    for _ in 0..num_ops {
        let op = MemoryOp::new(Timestamp::from_micros(0), random_bytes(256));
        let op_id = op.compute_op_id();
        ops.push(op.into());
        op_ids.push(op_id);
    }
    (ops, op_ids)
}

// ---------------------------------------------------------------------------
// The test.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_node_gossip_over_reticulum() {
    enable_tracing();

    // Two rns Transports, wired via loopback so packets flow between
    // them without TCP.
    let (tp_a, id_a) = make_rns_transport("kitsune-a");
    let (tp_b, id_b) = make_rns_transport("kitsune-b");
    wire_loopback(tp_a.clone(), tp_b.clone()).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Wrap each Transport in a ReticulumNode — this is what
    // `reticulum_builder` consumes.
    let node_a = ReticulumNode::from_rns_transport(tp_a, id_a)
        .await
        .expect("ReticulumNode A");
    let node_b = ReticulumNode::from_rns_transport(tp_b, id_b)
        .await
        .expect("ReticulumNode B");

    // Two full Kitsune instances.
    let kitsune_a = make_kitsune_node(node_a).await;
    let kitsune_b = make_kitsune_node(node_b).await;

    // One space per instance, agents join, announces flow.
    let space_a = start_space(&kitsune_a).await;
    let space_b = start_space(&kitsune_b).await;

    // Each side publishes a batch of ops.
    let (ops_a, op_ids_a) = create_op_list(50);
    space_a
        .op_store()
        .process_incoming_ops(ops_a.clone())
        .await
        .expect("A insert ops");
    let (ops_b, op_ids_b) = create_op_list(50);
    space_b
        .op_store()
        .process_incoming_ops(ops_b.clone())
        .await
        .expect("B insert ops");

    // Wait for gossip to exchange all ops in both directions.
    iter_check!(120_000, 1_000, {
        let got_a = space_a
            .op_store()
            .retrieve_ops(op_ids_b.clone())
            .await
            .unwrap();
        let got_b = space_b
            .op_store()
            .retrieve_ops(op_ids_a.clone())
            .await
            .unwrap();
        if got_a.len() == ops_b.len() && got_b.len() == ops_a.len() {
            break;
        } else {
            tracing::info!(
                "A has {}/{} of B's ops; B has {}/{} of A's ops",
                got_a.len(),
                ops_b.len(),
                got_b.len(),
                ops_a.len()
            );
        }
    });
}
