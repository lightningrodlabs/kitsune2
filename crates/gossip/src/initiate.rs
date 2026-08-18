use crate::K2GossipConfig;
use crate::gossip::K2Gossip;
use crate::peer_meta_store::K2PeerMetaStore;
use kitsune2_api::*;
use kitsune2_api::{AgentId, K2Result, Timestamp, Url};
use std::collections::HashSet;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::mpsc::error::TrySendError;
use tokio::task::AbortHandle;

pub fn spawn_initiate_task(
    config: Arc<K2GossipConfig>,
    gossip: Weak<K2Gossip>,
    hooks: GossipSpaceHooks,
) -> (tokio::sync::mpsc::Sender<()>, AbortHandle) {
    tracing::info!("Starting initiate task");

    let (force_initiate, mut force_initiate_rx) = tokio::sync::mpsc::channel(1);

    let initiate_jitter_ms = config.initiate_jitter_ms as u64;
    let compute_delay = move |base_delay: Duration| {
        if initiate_jitter_ms > 0 {
            let jitter = rand::random::<u64>() % initiate_jitter_ms;
            base_delay + Duration::from_millis(jitter)
        } else {
            base_delay
        }
    };

    let initial_initiate_interval = config.initial_initiate_interval();
    let initiate_interval = config.initiate_interval();
    let here_force_initiate = force_initiate.clone();
    let abort_handle = tokio::task::spawn(async move {
        loop {
            let Some(gossip) = gossip.upgrade() else {
                tracing::info!("Gossip instance dropped, stopping initiate task");
                break;
            };

            let Ok(local_agents) = gossip.local_agent_store.get_all().await else {
                tracing::warn!("Failed to get local agents, pausing and then retrying");
                // Wait a short amount of time before retrying to avoid busy looping
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            };

            if local_agents.is_empty() {
                tracing::warn!("No local agents available, skipping initiation");
                // Wait a short amount of time before retrying to avoid busy looping.
                //
                // This should be a short wait because it's expected to be a temporary state. The
                // space should not be running if there are no local agents. Either the space has
                // just been created and local agents haven't joined yet, or all local agents have
                // left and the space is about to be shut down.
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }

            // This logic isn't entirely sound with sharding. If a new local agent joins the space
            // with a different target arc that isn't completely contained within the existing
            // storage arc declared by the other local agents, then we might end up waiting a while
            // before we start the initial sync for the new local agent.
            // With full arc, if one local agent is in sync, then all local agents are and this isn't
            // a problem, and with zero arc, we don't gossip data at all so it also doesn't matter.
            // This is something to come back to when we implement sharding.
            if local_agents.iter().all(|a| a.get_cur_storage_arc() == a.get_tgt_storage_arc()) {
                // All agents are at their target arc, we should do the normal delay

                // Prepare the delay based on the configured initiate interval.
                let delay = tokio::time::sleep(compute_delay(initiate_interval));

                // Race the delay against the force initiate signal.
                tokio::select! {
                    _ = delay => {
                        // Continue to the next iteration
                    }
                    _ = force_initiate_rx.recv() => {
                        tracing::debug!("Force initiate received, skipping the remaining delay and attempting initiation");
                    }
                }
            } else {
                // At least one agent is still growing its arc, we should try to wait for the fetch
                // queue to be drained and then go ahead with the initiation.
                let (tx, rx) = futures::channel::oneshot::channel();
                gossip.fetch.notify_on_drained(tx);

                // Prepare the delay based on the configured initiate interval.
                let delay = tokio::time::sleep(compute_delay(initiate_interval));

                tokio::select! {
                    _ = delay => {
                        tracing::info!("Wanted the fetch queue to drain but the delay expired first, proceeding with gossip initiation");
                    }
                    r = rx => {
                        // If the fetch queue is empty and there's nobody to gossip with then this loop
                        // would just spin. So use the initial initiate interval to wait a short amount of
                        // time before trying to initiate again.
                        // For this reason, this configuration value should be set to a small value!
                        tokio::time::sleep(initial_initiate_interval).await;

                        tracing::debug!(?r, "Fetch queue drained, proceeding with gossip initiation");
                    }
                }
            }

            if gossip.initiated_round_state.lock().await.is_some() {
                tracing::debug!("Not initiating gossip because there is already an initiated round");
                continue;
            }

            match select_next_target(
                gossip.peer_store.clone(),
                &local_agents,
                gossip.peer_meta_store.clone(),
                &hooks.peer_eligible,
            )
            .await
            {
                Ok(Some(url)) => {
                    tracing::debug!("Selected target for gossip: {}", url);

                    match gossip.initiate_gossip(url.clone()).await {
                        Ok(true) => {
                            tracing::info!("Initiated gossip with {}", url);
                        }
                        Ok(false) => {
                            // Don't log here, will already have logged the reason
                        }
                        Err(e) => {
                            tracing::warn!("Failed to initiate gossip: {:?}", e);

                            match here_force_initiate.try_send(()) {
                                Ok(_) => {},
                                Err(TrySendError::Full(_)) => {
                                    tracing::debug!("Force initiate already pending");
                                }
                                Err(TrySendError::Closed(_)) => {
                                    tracing::error!("Force initiate channel closed, stopping initiate task");
                                    break;
                                }
                            }
                        }
                    }
                }
                Ok(None) => {
                    // Nobody to gossip with, expect `select_next_target` to have logged a reason.
                    //
                    // This covers both "we know of nobody" and "everybody we
                    // know of is ineligible", which is the shape symmetric
                    // grant loss takes: this timer is then the only thing
                    // still firing, so report the starvation. It covers the
                    // force-initiate path too, since forcing only skips the
                    // delay ahead of this same selection.
                    (hooks.on_no_target)();
                }
                Err(e) => {
                    tracing::error!("Error selecting target: {:?}", e);
                }
            }
        }
    })
    .abort_handle();

    (force_initiate, abort_handle)
}

async fn select_next_target(
    peer_store: DynPeerStore,
    local_agents: &[DynLocalAgent],
    peer_meta_store: Arc<K2PeerMetaStore>,
    peer_eligible: &GossipPeerEligible,
) -> K2Result<Option<Url>> {
    let current_time = Timestamp::now();

    // Get the TARGET storage arcs for all local agents
    //
    // We should gossip to try to gather data that is within our target storage arcs so
    // that we are working towards a complete set of data and can claim those sectors.
    let local_arcs = local_agents
        .iter()
        .map(|a| a.get_tgt_storage_arc())
        .collect::<HashSet<_>>();

    let local_agent_ids = local_agents
        .iter()
        .map(|a| a.agent().clone())
        .collect::<HashSet<_>>();

    // Discover remote agents whose arc overlaps with at least one of our local agents' arcs
    let mut all_agents = HashSet::new();
    for local_arc in local_arcs {
        let by_local_arc_agents =
            peer_store.get_by_overlapping_storage_arc(local_arc).await?;

        all_agents.extend(by_local_arc_agents);
    }

    // Filter local agents out of the list of all agents
    remove_local_agents(&mut all_agents, &local_agent_ids);

    let mut using_overlapping_agents = true;

    // There are no agents with an overlapping arc to gossip with. We should cast the net
    // wider and gossip with agents that might still be growing their arc and have some ops
    // or agent infos that we are missing
    if all_agents.is_empty() {
        tracing::debug!(
            "No agents with overlapping arcs available, selecting from all agents"
        );

        all_agents = peer_store.get_all().await?.into_iter().collect();
        remove_local_agents(&mut all_agents, &local_agent_ids);
        using_overlapping_agents = false;
    }

    let mut possible_target = select_responsive_and_least_recently_gossiped(
        all_agents,
        peer_meta_store.clone(),
        current_time,
        peer_eligible,
    )
    .await?;

    // We tried using overlapping agents, but they're all on timeout
    if possible_target.is_none() && using_overlapping_agents {
        tracing::debug!(
            "All agents with overlapping arcs are on timeout, selecting from all agents"
        );

        all_agents = peer_store.get_all().await?.into_iter().collect();
        remove_local_agents(&mut all_agents, &local_agent_ids);

        possible_target = select_responsive_and_least_recently_gossiped(
            all_agents,
            peer_meta_store.clone(),
            current_time,
            peer_eligible,
        )
        .await?;
    }

    match possible_target {
        None => {
            // All options exhausted, give up for now
            tracing::debug!("No agents to gossip with");
            Ok(None)
        }
        Some(target) => Ok(Some(target)),
    }
}

fn remove_local_agents(
    agents: &mut HashSet<Arc<AgentInfoSigned>>,
    local_agents: &HashSet<AgentId>,
) {
    agents.retain(|a| !local_agents.contains(&a.get_agent_info().agent));
}

async fn select_responsive_and_least_recently_gossiped(
    all_agents: HashSet<Arc<AgentInfoSigned>>,
    peer_meta_store: Arc<K2PeerMetaStore>,
    current_time: Timestamp,
    peer_eligible: &GossipPeerEligible,
) -> K2Result<Option<Url>> {
    let mut possible_targets = Vec::with_capacity(all_agents.len());
    for agent in all_agents {
        if agent.is_tombstone {
            // Skip tombstone agents, they are not valid gossip targets
            continue;
        }

        if agent.expires_at < current_time {
            // Skip agents that have expired, they are not valid gossip targets
            continue;
        }

        // Agent hasn't provided a URL, we won't be able to gossip with them.
        let Some(url) = agent.url.clone() else {
            continue;
        };

        // The space has not granted this peer access, so the transport would
        // refuse to send to it. Gossiping with it is not merely futile: the
        // initiate message is dropped locally while the round it starts stays
        // open, and no other peer is initiated with until that round times
        // out.
        if !peer_eligible(&url) {
            tracing::debug!(
                ?url,
                "Skipping a peer that is not eligible to gossip with"
            );
            continue;
        }

        // Agent has been marked as unresponsive, we won't be able to gossip
        // with them.
        if peer_meta_store
            .get_unresponsive(url.clone())
            .await?
            .is_some()
        {
            continue;
        }

        let timestamp =
            peer_meta_store.last_gossip_timestamp(url.clone()).await?;

        let duration_since_last = (current_time
            - timestamp.unwrap_or(UNIX_TIMESTAMP))
        .unwrap_or(Duration::ZERO);

        possible_targets.push((duration_since_last, url));
    }

    Ok(possible_targets
        .into_iter()
        .max_by_key(|t| t.0)
        .map(|t| t.1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use kitsune2_core::default_test_builder;
    use kitsune2_dht::SECTOR_SIZE;
    use kitsune2_test_utils::agent::{AgentBuilder, TestLocalAgent};
    use kitsune2_test_utils::enable_tracing;
    use kitsune2_test_utils::space::TEST_SPACE_ID;
    use std::sync::Arc;

    /// Access is not what these tests are about, so every peer is eligible.
    fn all_eligible() -> GossipPeerEligible {
        Arc::new(|_| true)
    }

    struct Harness {
        peer_store: DynPeerStore,
        local_agent_store: DynLocalAgentStore,
        peer_meta_store: Arc<K2PeerMetaStore>,
    }

    impl Harness {
        async fn create() -> Self {
            let builder =
                Arc::new(default_test_builder().with_default_config().unwrap());
            let blocks = builder
                .blocks
                .create(builder.clone(), TEST_SPACE_ID)
                .await
                .unwrap();
            let known_peers = builder
                .known_peers
                .create(builder.clone(), TEST_SPACE_ID)
                .await
                .unwrap();

            Harness {
                peer_store: builder
                    .peer_store
                    .create(builder.clone(), TEST_SPACE_ID, blocks, known_peers)
                    .await
                    .unwrap(),
                local_agent_store: builder
                    .local_agent_store
                    .create(builder.clone())
                    .await
                    .unwrap(),
                peer_meta_store: Arc::new(K2PeerMetaStore::new(
                    builder
                        .peer_meta_store
                        .create(builder.clone(), TEST_SPACE_ID.clone())
                        .await
                        .unwrap(),
                )),
            }
        }

        async fn new_local_agent(&self, arc: DhtArc) -> Arc<AgentInfoSigned> {
            let local_agent: DynLocalAgent =
                Arc::new(TestLocalAgent::default());
            local_agent.set_tgt_storage_arc_hint(arc);

            let agent_info_signed = AgentBuilder::default()
                .with_url(Some(Url::from_str("ws://test:80/local").unwrap()))
                .build(local_agent.clone());

            self.local_agent_store.add(local_agent).await.unwrap();
            self.peer_store
                .insert(vec![agent_info_signed.clone()])
                .await
                .unwrap();

            agent_info_signed
        }

        async fn new_remote_agent(
            &self,
            peer_url: Option<Url>,
            storage_arc: Option<DhtArc>,
            target_arc: Option<DhtArc>,
        ) -> (DynLocalAgent, Arc<AgentInfoSigned>) {
            let local_agent: DynLocalAgent =
                Arc::new(TestLocalAgent::default());
            if let Some(arc) = storage_arc {
                local_agent.set_cur_storage_arc(arc);
            }
            if let Some(arc) = target_arc {
                local_agent.set_tgt_storage_arc_hint(arc);
            }

            let mut agent_builder = AgentBuilder::default().with_url(peer_url);
            if let Some(arc) = storage_arc {
                agent_builder = agent_builder.with_storage_arc(arc);
            }
            let agent_info_signed = agent_builder.build(local_agent.clone());

            self.peer_store
                .insert(vec![agent_info_signed.clone()])
                .await
                .unwrap();

            (local_agent, agent_info_signed)
        }
    }

    /// A gossip instance wired to in-memory modules, with the hooks a test
    /// wants and nothing else in the peer store.
    struct StarvationHarness {
        gossip: Arc<crate::gossip::K2Gossip>,
        peer_store: DynPeerStore,
        peer_meta_store: Arc<K2PeerMetaStore>,
        starved: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl StarvationHarness {
        async fn create(peer_eligible: GossipPeerEligible) -> Self {
            #[derive(Debug)]
            struct NoopTxHandler;
            impl kitsune2_api::TxBaseHandler for NoopTxHandler {}
            impl kitsune2_api::TxHandler for NoopTxHandler {}

            let builder =
                Arc::new(default_test_builder().with_default_config().unwrap());
            let transport = builder
                .transport
                .create(builder.clone(), Arc::new(NoopTxHandler))
                .await
                .unwrap();
            let report = builder
                .report
                .create(builder.clone(), transport.clone())
                .await
                .unwrap();
            let blocks = builder
                .blocks
                .create(builder.clone(), TEST_SPACE_ID)
                .await
                .unwrap();
            let known_peers = builder
                .known_peers
                .create(builder.clone(), TEST_SPACE_ID)
                .await
                .unwrap();
            let peer_store = builder
                .peer_store
                .create(builder.clone(), TEST_SPACE_ID, blocks, known_peers)
                .await
                .unwrap();
            let local_agent_store = builder
                .local_agent_store
                .create(builder.clone())
                .await
                .unwrap();
            let peer_meta_store = builder
                .peer_meta_store
                .create(builder.clone(), TEST_SPACE_ID)
                .await
                .unwrap();
            let op_store = builder
                .op_store
                .create(builder.clone(), TEST_SPACE_ID)
                .await
                .unwrap();
            let fetch = builder
                .fetch
                .create(
                    builder.clone(),
                    TEST_SPACE_ID,
                    report,
                    op_store.clone(),
                    peer_meta_store.clone(),
                    transport.clone(),
                )
                .await
                .unwrap();

            // A local agent, so the initiate loop gets as far as looking for
            // a target rather than skipping the iteration.
            local_agent_store
                .add(Arc::new(TestLocalAgent::default()))
                .await
                .unwrap();

            let starved = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let gossip = crate::gossip::K2Gossip::create(
                K2GossipConfig {
                    initiate_interval_ms: 5,
                    min_initiate_interval_ms: 5,
                    initial_initiate_interval_ms: 5,
                    initiate_jitter_ms: 0,
                    ..Default::default()
                },
                TEST_SPACE_ID,
                peer_store.clone(),
                local_agent_store,
                peer_meta_store.clone(),
                op_store,
                transport,
                fetch,
                builder.verifier.clone(),
                GossipSpaceHooks {
                    peer_eligible,
                    on_no_target: {
                        let starved = starved.clone();
                        Arc::new(move || {
                            starved.fetch_add(
                                1,
                                std::sync::atomic::Ordering::SeqCst,
                            );
                        })
                    },
                },
            )
            .await
            .unwrap();

            StarvationHarness {
                gossip,
                peer_store,
                peer_meta_store: Arc::new(K2PeerMetaStore::new(
                    peer_meta_store,
                )),
                starved,
            }
        }

        fn starved(&self) -> usize {
            self.starved.load(std::sync::atomic::Ordering::SeqCst)
        }

        async fn await_starvation(&self) {
            tokio::time::timeout(Duration::from_secs(5), async {
                while self.starved() == 0 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("gossip never reported having no target");
        }

        /// Whether this peer has been picked as a gossip target.
        async fn was_selected(&self, url: &Url) -> bool {
            self.peer_meta_store
                .last_gossip_timestamp(url.clone())
                .await
                .unwrap()
                .is_some()
        }
    }

    /// Gossip reports a starved initiation, which is the only event still
    /// firing when two peers have symmetrically forgotten each other's access
    /// grant.
    #[tokio::test(flavor = "multi_thread")]
    async fn notify_when_there_is_no_target() {
        enable_tracing();

        let harness = StarvationHarness::create(Arc::new(|_| true)).await;
        harness.await_starvation().await;
    }

    /// A peer the space will not let us talk to is not a gossip target, and a
    /// peer store holding only such peers starves gossip exactly as an empty
    /// one does.
    ///
    /// Choosing an ineligible peer would be worse than useless: the transport
    /// drops the initiate locally while the round it opened stays in flight,
    /// and no other peer is initiated with until that round times out — which
    /// is also what would hide the starvation the access module needs to hear
    /// about.
    #[tokio::test(flavor = "multi_thread")]
    async fn ineligible_peers_are_not_gossip_targets() {
        enable_tracing();

        let peer_url = Url::from_str("ws://test:80/ineligible").unwrap();

        // The control: with everyone eligible, this peer is picked, so the
        // test below is about eligibility and not about some other filter.
        let permissive = StarvationHarness::create(Arc::new(|_| true)).await;
        insert_remote_agent(&permissive.peer_store, &peer_url).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while !permissive.was_selected(&peer_url).await {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("an eligible peer must be selected");

        let harness = StarvationHarness::create(Arc::new(|_| false)).await;
        insert_remote_agent(&harness.peer_store, &peer_url).await;

        harness.await_starvation().await;
        assert!(
            !harness.was_selected(&peer_url).await,
            "an ineligible peer must never be initiated with"
        );
        assert!(
            harness.gossip.initiated_round_state.lock().await.is_none(),
            "no round may be opened toward an ineligible peer"
        );
    }

    async fn insert_remote_agent(peer_store: &DynPeerStore, url: &Url) {
        let local_agent: DynLocalAgent = Arc::new(TestLocalAgent::default());
        local_agent.set_cur_storage_arc(DhtArc::FULL);
        local_agent.set_tgt_storage_arc_hint(DhtArc::FULL);
        let info = AgentBuilder::default()
            .with_url(Some(url.clone()))
            .with_storage_arc(DhtArc::FULL)
            .build(local_agent);
        peer_store.insert(vec![info]).await.unwrap();
    }

    #[tokio::test]
    async fn skip_when_no_local_agents() {
        enable_tracing();

        let harness = Harness::create().await;

        assert!(
            harness
                .local_agent_store
                .get_all()
                .await
                .unwrap()
                .is_empty(),
            "Expected no local agents"
        );

        let url = select_next_target(
            harness.peer_store.clone(),
            &harness.local_agent_store.get_all().await.unwrap(),
            harness.peer_meta_store.clone(),
            &all_eligible(),
        )
        .await
        .unwrap();

        assert_eq!(None, url);
    }

    #[tokio::test]
    async fn select_peer() {
        enable_tracing();

        let harness = Harness::create().await;

        harness.new_local_agent(DhtArc::FULL).await;

        let remote_agent = harness
            .new_remote_agent(
                Some(Url::from_str("ws://test:80/1").unwrap()),
                Some(DhtArc::FULL),
                Some(DhtArc::FULL),
            )
            .await
            .1;

        let url = select_next_target(
            harness.peer_store.clone(),
            &harness.local_agent_store.get_all().await.unwrap(),
            harness.peer_meta_store.clone(),
            &all_eligible(),
        )
        .await
        .unwrap();

        assert_eq!(remote_agent.url.clone().unwrap(), url.unwrap());
    }

    #[tokio::test]
    async fn select_by_last_gossip_timestamp() {
        enable_tracing();

        let harness = Harness::create().await;

        harness.new_local_agent(DhtArc::FULL).await;
        let remote_agent_1 = harness
            .new_remote_agent(
                Some(Url::from_str("ws://test:80/2").unwrap()),
                Some(DhtArc::FULL),
                Some(DhtArc::FULL),
            )
            .await
            .1;
        let remote_agent_2 = harness
            .new_remote_agent(
                Some(Url::from_str("ws://test:80/3").unwrap()),
                Some(DhtArc::FULL),
                Some(DhtArc::FULL),
            )
            .await
            .1;

        // Mark both of the remote agents as having gossiped previously.
        // They should be selected by how long ago they gossiped.
        harness
            .peer_meta_store
            .set_last_gossip_timestamp(
                remote_agent_1.url.clone().unwrap(),
                (Timestamp::now() - Duration::from_secs(30)).unwrap(),
            )
            .await
            .unwrap();
        harness
            .peer_meta_store
            .set_last_gossip_timestamp(
                remote_agent_2.url.clone().unwrap(),
                (Timestamp::now() - Duration::from_secs(60)).unwrap(),
            )
            .await
            .unwrap();

        let url = select_next_target(
            harness.peer_store.clone(),
            &harness.local_agent_store.get_all().await.unwrap(),
            harness.peer_meta_store.clone(),
            &all_eligible(),
        )
        .await
        .unwrap();

        assert_eq!(remote_agent_2.url.clone().unwrap(), url.unwrap());

        // Now that remote_agent_2 was selected for gossip, mark them as having gossiped recently
        // so that they shouldn't be selected on the next pass.
        harness
            .peer_meta_store
            .set_last_gossip_timestamp(
                remote_agent_2.url.clone().unwrap(),
                Timestamp::now(),
            )
            .await
            .unwrap();

        let url = select_next_target(
            harness.peer_store.clone(),
            &harness.local_agent_store.get_all().await.unwrap(),
            harness.peer_meta_store.clone(),
            &all_eligible(),
        )
        .await
        .unwrap();

        assert_eq!(remote_agent_1.url.clone().unwrap(), url.unwrap());

        // Do the same thing for remote_agent_1
        harness
            .peer_meta_store
            .set_last_gossip_timestamp(
                remote_agent_1.url.clone().unwrap(),
                Timestamp::now(),
            )
            .await
            .unwrap();

        // Those the next best option by last gossip timestamp. Though this time we'd be selecting
        // a remote agent that we've recently gossiped with.
        let url = select_next_target(
            harness.peer_store.clone(),
            &harness.local_agent_store.get_all().await.unwrap(),
            harness.peer_meta_store.clone(),
            &all_eligible(),
        )
        .await
        .unwrap();

        assert_eq!(remote_agent_2.url.clone().unwrap(), url.unwrap());
    }

    #[tokio::test]
    async fn skip_peer_with_missing_url() {
        enable_tracing();

        let harness = Harness::create().await;

        harness.new_local_agent(DhtArc::FULL).await;

        harness
            .new_remote_agent(None, Some(DhtArc::FULL), Some(DhtArc::FULL))
            .await;
        let agent_3 = harness
            .new_remote_agent(
                Some(Url::from_str("ws://test:80/3").unwrap()),
                Some(DhtArc::FULL),
                Some(DhtArc::FULL),
            )
            .await
            .1;

        let url = select_next_target(
            harness.peer_store.clone(),
            &harness.local_agent_store.get_all().await.unwrap(),
            harness.peer_meta_store.clone(),
            &all_eligible(),
        )
        .await
        .unwrap();

        assert_eq!(agent_3.url.clone().unwrap(), url.unwrap());

        harness
            .peer_meta_store
            .set_last_gossip_timestamp(
                agent_3.url.clone().unwrap(),
                Timestamp::now(),
            )
            .await
            .unwrap();

        let url = select_next_target(
            harness.peer_store.clone(),
            &harness.local_agent_store.get_all().await.unwrap(),
            harness.peer_meta_store.clone(),
            &all_eligible(),
        )
        .await
        .unwrap();

        // Must pick the same agent again, it's the only valid choice.
        assert_eq!(agent_3.url.clone().unwrap(), url.unwrap());
    }

    #[tokio::test]
    async fn prioritise_never_gossiped_peers() {
        enable_tracing();

        let harness = Harness::create().await;

        harness.new_local_agent(DhtArc::FULL).await;

        let remote_agent_1 = harness
            .new_remote_agent(
                Some(Url::from_str("ws://test:80/2").unwrap()),
                Some(DhtArc::FULL),
                Some(DhtArc::FULL),
            )
            .await
            .1;
        let remote_agent_2 = harness
            .new_remote_agent(
                Some(Url::from_str("ws://test:80/3").unwrap()),
                Some(DhtArc::FULL),
                Some(DhtArc::FULL),
            )
            .await
            .1;

        // Mark that we've gossiped with remote_agent_2, but not recently
        harness
            .peer_meta_store
            .set_last_gossip_timestamp(
                remote_agent_2.url.clone().unwrap(),
                (Timestamp::now() - Duration::from_secs(900)).unwrap(),
            )
            .await
            .unwrap();

        let url = select_next_target(
            harness.peer_store.clone(),
            &harness.local_agent_store.get_all().await.unwrap(),
            harness.peer_meta_store.clone(),
            &all_eligible(),
        )
        .await
        .unwrap();

        assert_eq!(remote_agent_1.url.clone().unwrap(), url.unwrap());
    }

    #[tokio::test]
    async fn selects_non_overlapping_when_no_overlapping_peers_available() {
        enable_tracing();

        let harness = Harness::create().await;

        harness
            .new_local_agent(DhtArc::Arc(0, SECTOR_SIZE - 1))
            .await;

        // Two agents where the arcs DO NOT overlap with our local agent
        harness
            .new_remote_agent(
                Some(Url::from_str("ws://test:80/2").unwrap()),
                Some(DhtArc::Arc(SECTOR_SIZE, SECTOR_SIZE * 2 - 1)),
                Some(DhtArc::Arc(SECTOR_SIZE, SECTOR_SIZE * 2 - 1)),
            )
            .await;
        harness
            .new_remote_agent(
                Some(Url::from_str("ws://test:80/3").unwrap()),
                Some(DhtArc::Arc(SECTOR_SIZE, SECTOR_SIZE * 2 - 1)),
                Some(DhtArc::Arc(SECTOR_SIZE, SECTOR_SIZE * 2 - 1)),
            )
            .await;

        let mut seen = HashSet::new();
        for _ in 0..2 {
            let Some(url) = select_next_target(
                harness.peer_store.clone(),
                &harness.local_agent_store.get_all().await.unwrap(),
                harness.peer_meta_store.clone(),
                &all_eligible(),
            )
            .await
            .unwrap() else {
                panic!("Expected to find a peer to gossip with");
            };

            harness
                .peer_meta_store
                .set_last_gossip_timestamp(url.clone(), Timestamp::now())
                .await
                .unwrap();

            seen.insert(url);
        }

        assert_eq!(2, seen.len());
    }

    #[tokio::test]
    async fn prioritises_overlapping_arcs() {
        enable_tracing();

        let harness = Harness::create().await;

        harness
            .new_local_agent(DhtArc::Arc(0, SECTOR_SIZE - 1))
            .await;

        // Overlapping
        let (remote_local_agent_1, remote_agent_1) = harness
            .new_remote_agent(
                Some(Url::from_str("ws://test:80/2").unwrap()),
                Some(DhtArc::Arc(0, SECTOR_SIZE * 10 - 1)),
                Some(DhtArc::Arc(0, SECTOR_SIZE * 10 - 1)),
            )
            .await;
        // Non-overlapping with our local agent
        let remote_agent_2 = harness
            .new_remote_agent(
                Some(Url::from_str("ws://test:80/3").unwrap()),
                Some(DhtArc::Arc(SECTOR_SIZE, SECTOR_SIZE * 2 - 1)),
                Some(DhtArc::Arc(SECTOR_SIZE, SECTOR_SIZE * 2 - 1)),
            )
            .await
            .1;

        // Should pick the overlapping agent first
        let Some(url) = select_next_target(
            harness.peer_store.clone(),
            &harness.local_agent_store.get_all().await.unwrap(),
            harness.peer_meta_store.clone(),
            &all_eligible(),
        )
        .await
        .unwrap() else {
            panic!("Expected to find a peer to gossip with");
        };

        assert_eq!(remote_agent_1.url.clone().unwrap(), url);

        // Repeating will pick the same agent because an overlapping arc is preferred
        let Some(url) = select_next_target(
            harness.peer_store.clone(),
            &harness.local_agent_store.get_all().await.unwrap(),
            harness.peer_meta_store.clone(),
            &all_eligible(),
        )
        .await
        .unwrap() else {
            panic!("Expected to find a peer to gossip with");
        };
        assert_eq!(remote_agent_1.url.clone().unwrap(), url);

        // Mark the overlapping agent as having gone offline
        let remote_agent_1 = AgentBuilder::update_for(remote_agent_1)
            .with_tombstone(true)
            .build(remote_local_agent_1);
        harness
            .peer_store
            .insert(vec![remote_agent_1])
            .await
            .unwrap();

        // Now we should pick the non-overlapping agent
        let Some(url) = select_next_target(
            harness.peer_store.clone(),
            &harness.local_agent_store.get_all().await.unwrap(),
            harness.peer_meta_store.clone(),
            &all_eligible(),
        )
        .await
        .unwrap() else {
            panic!("Expected to find a peer to gossip with");
        };
        assert_eq!(remote_agent_2.url.clone().unwrap(), url);
    }

    #[tokio::test]
    async fn skip_unresponsive_peer() {
        enable_tracing();

        let harness = Harness::create().await;

        harness.new_local_agent(DhtArc::FULL).await;

        let remote_agent = harness
            .new_remote_agent(
                Some(Url::from_str("ws://test:80/3").unwrap()),
                Some(DhtArc::FULL),
                Some(DhtArc::FULL),
            )
            .await
            .1;

        let url = select_next_target(
            harness.peer_store.clone(),
            &harness.local_agent_store.get_all().await.unwrap(),
            harness.peer_meta_store.clone(),
            &all_eligible(),
        )
        .await
        .unwrap();

        // Agent gets picked successfully while not marked as unresponsive
        assert_eq!(remote_agent.url.clone().unwrap(), url.clone().unwrap());

        harness
            .peer_meta_store
            .set_unresponsive(
                url.unwrap(),
                remote_agent.expires_at,
                Timestamp::now(),
            )
            .await
            .unwrap();

        let url = select_next_target(
            harness.peer_store.clone(),
            &harness.local_agent_store.get_all().await.unwrap(),
            harness.peer_meta_store.clone(),
            &all_eligible(),
        )
        .await
        .unwrap();

        // Must return no agent since the only non-local agent in the
        // peer store is marked unresponsive
        assert_eq!(None, url);
    }
}
