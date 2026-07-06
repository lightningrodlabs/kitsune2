//! Integration test: two in-process mDNS bootstraps discover each other
//! over loopback and exchange signed agent info.
//!
//! mDNS is a real-network protocol, so this test is gated behind the
//! `K2_MDNS_IT` env var. Without it, the test is skipped — running in
//! sandboxed CI environments that block multicast traffic would otherwise
//! cause spurious failures.

use kitsune2_api::*;
use kitsune2_bootstrap_mdns::config::{
    MdnsBootstrapConfig, MdnsBootstrapModConfig,
};
use kitsune2_bootstrap_mdns::MdnsBootstrapFactory;
use kitsune2_core::factories::{
    MemBlocksFactory, MemPeerStoreFactory,
};
use kitsune2_test_utils::agent::{
    AgentBuilder, TestLocalAgent, TestVerifier,
};
use std::sync::Arc;
use std::time::Duration;

fn should_run() -> bool {
    std::env::var("K2_MDNS_IT").is_ok()
}

async fn mk_node(
    service_type: &str,
    space_id: SpaceId,
    agent_space_id: SpaceId,
) -> (DynPeerStore, DynBootstrap, Arc<AgentInfoSigned>) {
    let mut config = Config::default();
    let factory = MdnsBootstrapFactory::create();
    factory.default_config(&mut config).unwrap();
    let peer_store_factory = MemPeerStoreFactory::create();
    peer_store_factory.default_config(&mut config).unwrap();
    let blocks_factory = MemBlocksFactory::create();
    blocks_factory.default_config(&mut config).unwrap();

    // Override mdns config for this test: enable, use the test-specific
    // service type so parallel test processes don't see each other.
    config
        .set_module_config(&MdnsBootstrapModConfig {
            mdns_bootstrap: MdnsBootstrapConfig {
                enabled: true,
                service_type: service_type.to_string(),
                ..Default::default()
            },
        })
        .unwrap();

    // Minimal builder — only the fields the bootstrap factory and peer
    // store touch (`config`, `verifier`, `peer_store`, `blocks`) need to be
    // real. Other factories are unused on this path.
    let builder = Arc::new(Builder {
        config,
        verifier: Arc::new(TestVerifier),
        auth_material_bootstrap: None,
        auth_material_relay: None,
        kitsune: kitsune2_core::factories::CoreKitsuneFactory::create(),
        space: kitsune2_core::factories::CoreSpaceFactory::create(),
        peer_store: peer_store_factory.clone(),
        bootstrap: factory.clone(),
        fetch: kitsune2_core::factories::CoreFetchFactory::create(),
        report: kitsune2_core::factories::CoreReportFactory::create(),
        transport: kitsune2_core::factories::MemTransportFactory::create(),
        op_store: kitsune2_core::factories::MemOpStoreFactory::create(),
        peer_meta_store:
            kitsune2_core::factories::MemPeerMetaStoreFactory::create(),
        gossip: kitsune2_core::factories::CoreGossipStubFactory::create(),
        local_agent_store:
            kitsune2_core::factories::CoreLocalAgentStoreFactory::create(),
        publish: kitsune2_core::factories::CorePublishFactory::create(),
        blocks: blocks_factory.clone(),
    });

    let blocks = blocks_factory
        .create(builder.clone(), space_id.clone())
        .await
        .unwrap();
    let peer_store = peer_store_factory
        .create(builder.clone(), space_id.clone(), blocks)
        .await
        .unwrap();

    let bootstrap = factory
        .create(builder.clone(), peer_store.clone(), space_id)
        .await
        .unwrap();

    let agent = AgentBuilder::default()
        .with_space(agent_space_id)
        .build(TestLocalAgent::default());

    (peer_store, bootstrap, agent)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_nodes_same_space_discover_each_other() {
    if !should_run() {
        eprintln!(
            "skipping lan mdns test (set K2_MDNS_IT=1 on a host with multicast)"
        );
        return;
    }

    // Randomise service type per test run so stale announcements from a
    // previous run don't poison the browse.
    let tag: u32 = rand::random();
    let service_type = format!("_k2test{tag}._udp.local.");

    let space_id =
        SpaceId::from(bytes::Bytes::copy_from_slice(b"shared-space"));

    let (store_a, boot_a, agent_a) =
        mk_node(&service_type, space_id.clone(), space_id.clone()).await;
    let (store_b, boot_b, agent_b) =
        mk_node(&service_type, space_id.clone(), space_id.clone()).await;

    boot_a.put(agent_a.clone());
    boot_b.put(agent_b.clone());

    // Poll both peer stores for the other node's agent. mDNS can take a
    // while to propagate the first advertisement: when several responders
    // share one host (e.g. alongside avahi, or two in-process daemons),
    // mdns-sd's probe tiebreaking can delay the announcement by ~20s.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let a_sees_b = store_a
            .get(agent_b.agent.clone())
            .await
            .unwrap()
            .is_some();
        let b_sees_a = store_b
            .get(agent_a.agent.clone())
            .await
            .unwrap()
            .is_some();
        if a_sees_b && b_sees_a {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "timeout: a_sees_b={a_sees_b}, b_sees_a={b_sees_a} within 60s"
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nodes_in_different_spaces_do_not_leak_infos() {
    if !should_run() {
        eprintln!(
            "skipping lan mdns test (set K2_MDNS_IT=1 on a host with multicast)"
        );
        return;
    }

    let tag: u32 = rand::random();
    let service_type = format!("_k2test{tag}._udp.local.");
    let space_a = SpaceId::from(bytes::Bytes::copy_from_slice(b"space-A"));
    let space_b = SpaceId::from(bytes::Bytes::copy_from_slice(b"space-B"));

    let (store_a, boot_a, agent_a) =
        mk_node(&service_type, space_a.clone(), space_a.clone()).await;
    let (store_b, boot_b, agent_b) =
        mk_node(&service_type, space_b.clone(), space_b.clone()).await;

    boot_a.put(agent_a.clone());
    boot_b.put(agent_b.clone());

    // Give mDNS time to run. If there's a cross-space leak it would be
    // visible here. 5 seconds is well past typical mDNS settle time.
    tokio::time::sleep(Duration::from_secs(5)).await;

    assert!(
        store_a.get(agent_b.agent.clone()).await.unwrap().is_none(),
        "store_a must not have learned about space-B agent"
    );
    assert!(
        store_b.get(agent_a.agent.clone()).await.unwrap().is_none(),
        "store_b must not have learned about space-A agent"
    );
}
