//! End-to-end LAN discovery on the production wiring.
//!
//! Two nodes with no bootstrap server and a relay that accepts no
//! connections must still find each other: the mDNS bootstrap hears the
//! other node's announcement and dials it, iroh's own address lookup turns
//! that relay-only dial into a direct LAN path, and the hello access module
//! picks the new connection up, exchanges proofs and stores agent infos.
//! The only observable this test asserts on is the last link of that chain:
//! each peer store ends up holding the other node's agent.
//!
//! mDNS needs a real multicast-capable interface, which CI runners rarely
//! have, so the test only runs when KITSUNE2_LAN_TEST is set.
#![cfg(all(feature = "mdns", feature = "transport-iroh"))]

use kitsune2::default_builder;
use kitsune2_api::{
    AgentId, BoxFut, Builder, Config, DhtArc, DynKitsune, DynSpace,
    DynSpaceHandler, K2Result, KitsuneHandler, LocalAgent, SpaceHandler,
    SpaceId,
};
use kitsune2_bootstrap_mdns::MdnsBootstrapFactory;
use kitsune2_bootstrap_mdns::config::{
    MdnsBootstrapConfig, MdnsBootstrapModConfig,
};
use kitsune2_core::Ed25519LocalAgent;
use kitsune2_core::factories::CompositeBootstrapFactory;
use kitsune2_gossip::{K2GossipConfig, K2GossipModConfig};
use kitsune2_test_utils::noop_bootstrap::NoopBootstrapFactory;
use kitsune2_test_utils::{enable_tracing, iter_check, space::TEST_SPACE_ID};
use kitsune2_transport_iroh::config::{
    IrohTransportConfig, IrohTransportModConfig,
};
use rand::RngExt;
use std::sync::Arc;

/// A relay URL that accepts no connections (TCP discard port).
const UNREACHABLE_RELAY: &str = "https://127.0.0.1:9/relay";

#[derive(Debug)]
struct TestSpaceHandler;
impl SpaceHandler for TestSpaceHandler {}

#[derive(Debug)]
struct TestKitsuneHandler;
impl KitsuneHandler for TestKitsuneHandler {
    fn create_space(
        &self,
        _space_id: SpaceId,
        _config_override: Option<&Config>,
    ) -> BoxFut<'_, K2Result<DynSpaceHandler>> {
        Box::pin(async {
            let out: DynSpaceHandler = Arc::new(TestSpaceHandler);
            Ok(out)
        })
    }
}

struct Node {
    space: DynSpace,
    agent: AgentId,
    _kitsune: DynKitsune,
}

/// A service type private to this test run, so that other kitsune2 nodes
/// on the same LAN — or a previous run's lingering records — cannot take
/// part. The label must stay within the 15-byte limit for service names.
fn per_run_service_type() -> String {
    let tag: u32 = rand::rng().random();
    format!("_k2t{tag:08x}._udp.local.")
}

async fn make_node(service_type: &str) -> Node {
    let builder = Builder {
        // The mDNS factory copes with a daemon that cannot start by
        // itself, retrying the join on later puts, so it is wired in
        // bare: a host on which the daemon never comes up shows as this
        // test failing, not as a silent no-op. `OptionalBootstrapFactory`
        // is for embedders that want a fingerprint-derive error tolerated
        // as well.
        bootstrap: CompositeBootstrapFactory::create(vec![
            Arc::new(NoopBootstrapFactory),
            MdnsBootstrapFactory::create(),
        ]),
        ..default_builder()
    }
    .with_default_config()
    .unwrap();

    builder
        .config
        .set_module_config(&MdnsBootstrapModConfig {
            mdns_bootstrap: MdnsBootstrapConfig {
                enabled: true,
                service_type: service_type.to_string(),
                ..Default::default()
            },
        })
        .unwrap();

    builder
        .config
        .set_module_config(&IrohTransportModConfig {
            iroh_transport: IrohTransportConfig {
                relay_url: Some(UNREACHABLE_RELAY.to_string()),
                enable_lan_discovery: true,
                connect_timeout_s: 5,
                ..Default::default()
            },
        })
        .unwrap();

    // On a bare kitsune2 node nothing enters the peer store when a
    // connection opens, so it is gossip's "nobody to gossip with" report
    // that makes the hello module challenge the freshly connected peer.
    // Keep that report coming often enough for the test's patience.
    builder
        .config
        .set_module_config(&K2GossipModConfig {
            k2_gossip: K2GossipConfig {
                initiate_interval_ms: 1000,
                min_initiate_interval_ms: 100,
                initiate_jitter_ms: 100,
                ..Default::default()
            },
        })
        .unwrap();

    let kitsune = builder.build().await.unwrap();
    kitsune
        .register_handler(Arc::new(TestKitsuneHandler))
        .await
        .unwrap();

    let space = kitsune.space(TEST_SPACE_ID, None).await.unwrap();

    let local_agent = Arc::new(Ed25519LocalAgent::default());
    local_agent.set_tgt_storage_arc_hint(DhtArc::FULL);
    space.local_agent_join(local_agent.clone()).await.unwrap();

    Node {
        space,
        agent: local_agent.agent().clone(),
        _kitsune: kitsune,
    }
}

async fn knows(node: &Node, agent: &AgentId) -> bool {
    node.space
        .peer_store()
        .get(agent.clone())
        .await
        .unwrap()
        .is_some()
}

#[tokio::test(flavor = "multi_thread")]
async fn two_nodes_find_each_other_over_mdns_without_relay_or_bootstrap() {
    if std::env::var("KITSUNE2_LAN_TEST").is_err() {
        eprintln!(
            "skipping two_nodes_find_each_other_over_mdns_without_relay_or_bootstrap: set KITSUNE2_LAN_TEST=1 to run"
        );
        return;
    }
    enable_tracing();

    let service_type = per_run_service_type();
    tracing::info!(%service_type, "mdns e2e service type");

    let node_1 = make_node(&service_type).await;
    let node_2 = make_node(&service_type).await;

    iter_check!(60_000, 500, {
        let one_knows_two = knows(&node_1, &node_2.agent).await;
        let two_knows_one = knows(&node_2, &node_1.agent).await;
        tracing::info!(one_knows_two, two_knows_one, "mdns e2e progress");
        if one_knows_two && two_knows_one {
            break;
        }
    });
}
