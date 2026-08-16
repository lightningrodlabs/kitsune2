use kitsune2_api::{
    AccessDecision, BlockTarget, DynBlocks, DynKnownPeers, K2Result,
    PeerAccess, PeerAccessState, Timestamp, Url,
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// Core implementation of the [`PeerAccessState`] trait.
///
/// Decisions come from two places, and only these two:
///
/// - **Blocked** is decided here, by a peer store listener that resolves a
///   URL to its agents and asks [`Blocks`](kitsune2_api::Blocks) about them.
/// - **Granted** is decided by the access module, which records it through
///   [`PeerAccessState::set_access_decision`] once a peer has proven
///   knowledge of the space secret.
///
/// A URL with neither decision is unknown, which is not the same as blocked:
/// an unknown peer can still reach the access module, and one exchange later
/// it is either granted or still unknown.
pub struct CorePeerAccessState {
    decisions: Arc<RwLock<HashMap<Url, PeerAccess>>>,
    abort_handle: tokio::task::AbortHandle,
}

impl Drop for CorePeerAccessState {
    fn drop(&mut self) {
        tracing::info!(
            "CorePeerAccessState is being dropped, aborting background task"
        );
        self.abort_handle.abort();
    }
}

impl CorePeerAccessState {
    /// Create a new instance of the [`CorePeerAccessState`].
    ///
    /// `known_peers` is used to resolve peer URLs to agent IDs regardless of
    /// block status.  `peer_store` is used only to register a listener so
    /// that block decisions are updated whenever the peer store is updated.
    pub fn new(
        known_peers: DynKnownPeers,
        blocks: DynBlocks,
        peer_store: &kitsune2_api::DynPeerStore,
    ) -> K2Result<Self> {
        let decisions = Arc::new(RwLock::new(HashMap::new()));
        peer_store.register_peer_update_listener(Arc::new({
            let known_peers = Arc::downgrade(&known_peers);
            let blocks = Arc::downgrade(&blocks);
            let decisions = decisions.clone();

            move |agent_info| {
                let known_peers = known_peers.clone();
                let blocks = blocks.clone();
                let decisions = decisions.clone();

                Box::pin(async move {
                    let Some(known_peers) = known_peers.upgrade() else {
                        tracing::info!("KnownPeers dropped, cannot make access decision");
                        return;
                    };
                    let Some(blocks) = blocks.upgrade() else {
                        tracing::info!("Blocks dropped, cannot make access decision");
                        return;
                    };

                    let peer_url = match agent_info.url.clone() {
                        Some(url) => url,
                        None => {
                            if !agent_info.is_tombstone {
                                tracing::warn!("AgentInfo has no URL: {:?}", agent_info);
                            }
                            return;
                        }
                    };

                    tracing::debug!("Making access decision for peer URL: {:?}", peer_url);

                    // Use known_peers (not peer_store) so we find blocked agents too.
                    let agents_by_url: Vec<_> = match known_peers
                        .get_by_url(peer_url.clone())
                        .await {
                        Ok(agents) => agents.into_iter()
                        .map(BlockTarget::Agent)
                        .collect(),
                        Err(e) => {
                            tracing::error!(
                                "Failed to get agents by url {:?}: {:?}",
                                peer_url,
                                e
                            );
                            return;
                        }
                    };

                    if agents_by_url.is_empty() {
                        tracing::debug!("No agents found for url, clearing any decision: {:?}", peer_url);

                        // Any existing decision can be removed
                        decisions
                            .write()
                            .expect("poisoned")
                            .remove(&peer_url);
                    } else {
                        let any_blocked = match blocks.is_any_blocked(agents_by_url).await {
                            Ok(any_blocked) => any_blocked,
                            Err(e) => {
                                tracing::error!(
                                    "Failed to check block status for url {:?}: {:?}",
                                    peer_url,
                                    e
                                );
                                return;
                            }
                        };

                        // This listener decides blocks, and only blocks. A
                        // grant is not something a peer store insert can
                        // establish: an agent info is self-issued, so anyone
                        // who knows a space id can mint one, and treating it
                        // as a credential is what made the old "unknown =
                        // blocked" default look like access control without
                        // being it. Grants come from the access module, which
                        // asks a peer to prove knowledge of the space secret.
                        let mut decisions = decisions.write().expect("poisoned");
                        if any_blocked {
                            tracing::debug!("Access decision for peer URL {peer_url:?}: Blocked");
                            decisions.insert(
                                peer_url,
                                PeerAccess {
                                    decision: AccessDecision::Blocked,
                                    decided_at: Timestamp::now(),
                                },
                            );
                        } else if let Some(existing) = decisions.get(&peer_url)
                            && existing.decision == AccessDecision::Blocked
                        {
                            // The block no longer applies. Removing it puts
                            // the peer back to unknown rather than granted,
                            // so it has to prove itself again.
                            tracing::debug!("Clearing the block on peer URL {peer_url:?}, no agent at it is blocked any more");
                            decisions.remove(&peer_url);
                        }
                    }
                })
            }
        }))?;

        let abort_handle = tokio::task::spawn({
            let decisions = decisions.clone();
            async move {
                loop {
                    // Agent information is expected to be updated regularly. If updates aren't
                    // received then the access decisions will become stale and can be pruned.

                    tokio::time::sleep(Duration::from_secs(60 * 60)).await;

                    let result = Timestamp::now() - Duration::from_secs(60 * 60);
                    let Ok(old) = result else {
                        tracing::warn!("Failed to compute old timestamp for pruning access decisions");
                        continue;
                    };

                    decisions.write().expect("poisoned").retain(|_, v| {
                        v.decided_at > old
                    });
                }
            }
        }).abort_handle();

        Ok(Self {
            decisions,
            abort_handle,
        })
    }
}

impl std::fmt::Debug for CorePeerAccessState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CorePeerAccessState").finish()
    }
}

impl PeerAccessState for CorePeerAccessState {
    fn get_access_decision(
        &self,
        peer_url: Url,
    ) -> K2Result<Option<PeerAccess>> {
        let decision = self
            .decisions
            .read()
            .expect("poisoned")
            .get(&peer_url)
            .cloned();
        Ok(decision)
    }

    fn set_access_decision(
        &self,
        peer_url: Url,
        access: PeerAccess,
    ) -> K2Result<()> {
        let mut decisions = self.decisions.write().expect("poison");

        // Blocks always win. An explicit `Blocked` entry must not be
        // overwritten by a later `Granted`, which is what the access module
        // records after a successful proof-of-knowledge exchange.
        if access.decision == AccessDecision::Granted
            && let Some(existing) = decisions.get(&peer_url)
            && existing.decision == AccessDecision::Blocked
        {
            tracing::debug!(
                ?peer_url,
                "Ignoring Granted access decision, peer is explicitly blocked"
            );
            return Ok(());
        }

        decisions.insert(peer_url, access);
        Ok(())
    }

    fn remove_access_decision(&self, peer_url: Url) -> K2Result<()> {
        self.decisions.write().expect("poison").remove(&peer_url);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::factories::{
        CoreKnownPeers, MemBlocks, MemPeerStore, MemPeerStoreConfig,
    };
    use kitsune2_api::{AccessDecision, AgentId, BlockTarget, Blocks, Id};
    use kitsune2_test_utils::agent::{AgentBuilder, TestLocalAgent};
    use std::sync::Arc;

    const AGENT_1: AgentId = AgentId(Id(bytes::Bytes::from_static(b"agent1")));
    const AGENT_2: AgentId = AgentId(Id(bytes::Bytes::from_static(b"agent2")));

    fn make_url(s: &str) -> Url {
        Url::from_str(format!("ws://a.b:80/{s}")).unwrap()
    }

    fn make_peer_store(
        blocks: Arc<MemBlocks>,
        known_peers: Arc<CoreKnownPeers>,
    ) -> kitsune2_api::DynPeerStore {
        Arc::new(MemPeerStore::new(
            MemPeerStoreConfig {
                prune_interval_s: 10,
            },
            blocks,
            known_peers,
        ))
    }

    /// Blocking one agent at a URL must block the URL even when a
    /// non-blocked agent also resides at that URL.
    #[tokio::test]
    async fn shared_url_blocked_agent_blocks_url() {
        let url = make_url("shared");
        let blocks = Arc::new(MemBlocks::default());
        let known_peers = Arc::new(CoreKnownPeers::default());
        let peer_store = make_peer_store(blocks.clone(), known_peers.clone());

        let access_state = CorePeerAccessState::new(
            known_peers.clone(),
            blocks.clone(),
            &peer_store,
        )
        .unwrap();

        // Insert both agents at the same URL; this records them in KnownPeers
        // and triggers the listener which makes block decisions.
        let info1 = AgentBuilder {
            agent: Some(AGENT_1),
            url: Some(Some(url.clone())),
            ..Default::default()
        }
        .build(TestLocalAgent::default());
        let info2 = AgentBuilder {
            agent: Some(AGENT_2),
            url: Some(Some(url.clone())),
            ..Default::default()
        }
        .build(TestLocalAgent::default());
        peer_store.insert(vec![info1, info2]).await.unwrap();

        // Allow async listener tasks to complete.
        tokio::task::yield_now().await;

        // Both agents present, neither blocked. That is not a grant: an agent
        // info is self-issued, so it says nothing about whether the peer is
        // allowed in. The peer stays unknown until it proves itself.
        assert_eq!(
            access_state.get_access_decision(url.clone()).unwrap(),
            None,
            "storing an agent info must not grant access"
        );

        // Say the peer has since proven knowledge of the space secret.
        access_state
            .set_access_decision(url.clone(), granted())
            .unwrap();

        // Block agent_1 and remove it from the peer store (as the Host must).
        blocks.block(BlockTarget::Agent(AGENT_1)).await.unwrap();
        peer_store.remove(AGENT_1).await.unwrap();

        // Allow async listener tasks to complete.
        tokio::task::yield_now().await;

        // agent_1 is now blocked; even though agent_2 (non-blocked) is still
        // at the same URL, the URL must be Blocked, and the grant it had is
        // gone.
        let decision = access_state.get_access_decision(url.clone()).unwrap();
        assert_eq!(
            decision.map(|d| d.decision),
            Some(AccessDecision::Blocked),
            "expected Blocked after agent_1 at the shared URL was blocked"
        );
    }

    /// A URL with only non-blocked agents is not blocked — and not granted
    /// either. Grants are the access module's to make.
    #[tokio::test]
    async fn url_with_no_blocked_agents_is_not_blocked_or_granted() {
        let url = make_url("clean");
        let blocks = Arc::new(MemBlocks::default());
        let known_peers = Arc::new(CoreKnownPeers::default());
        let peer_store = make_peer_store(blocks.clone(), known_peers.clone());

        let access_state = CorePeerAccessState::new(
            known_peers.clone(),
            blocks.clone(),
            &peer_store,
        )
        .unwrap();

        let info = AgentBuilder {
            agent: Some(AGENT_1),
            url: Some(Some(url.clone())),
            ..Default::default()
        }
        .build(TestLocalAgent::default());
        peer_store.insert(vec![info]).await.unwrap();
        tokio::task::yield_now().await;

        assert_eq!(
            access_state.get_access_decision(url.clone()).unwrap(),
            None
        );
    }

    /// A grant made by the access module survives further peer store
    /// activity for that URL. Nothing about storing an agent info revisits a
    /// decision the access module already made.
    #[tokio::test]
    async fn a_grant_survives_a_peer_store_update() {
        let url = make_url("granted");
        let blocks = Arc::new(MemBlocks::default());
        let known_peers = Arc::new(CoreKnownPeers::default());
        let peer_store = make_peer_store(blocks.clone(), known_peers.clone());

        let access_state = CorePeerAccessState::new(
            known_peers.clone(),
            blocks.clone(),
            &peer_store,
        )
        .unwrap();

        access_state
            .set_access_decision(url.clone(), granted())
            .unwrap();

        let info = AgentBuilder {
            agent: Some(AGENT_1),
            url: Some(Some(url.clone())),
            ..Default::default()
        }
        .build(TestLocalAgent::default());
        peer_store.insert(vec![info]).await.unwrap();
        tokio::task::yield_now().await;

        let decision = access_state.get_access_decision(url.clone()).unwrap();
        assert_eq!(decision.map(|d| d.decision), Some(AccessDecision::Granted));
    }

    /// A host that lifts a block returns the URL to unknown rather than to
    /// granted, so the peer has to prove itself again.
    #[tokio::test]
    async fn lifting_a_block_returns_a_url_to_unknown() {
        use std::sync::atomic::{AtomicBool, Ordering};

        /// A host blocks implementation whose blocks can be lifted again,
        /// which [`MemBlocks`] has no way to express.
        #[derive(Debug, Default)]
        struct ToggleBlocks(Arc<AtomicBool>);

        impl kitsune2_api::Blocks for ToggleBlocks {
            fn block(
                &self,
                _target: BlockTarget,
            ) -> kitsune2_api::BoxFut<'static, K2Result<()>> {
                self.0.store(true, Ordering::SeqCst);
                Box::pin(async { Ok(()) })
            }

            fn is_blocked(
                &self,
                _target: BlockTarget,
            ) -> kitsune2_api::BoxFut<'static, K2Result<bool>> {
                let blocked = self.0.load(Ordering::SeqCst);
                Box::pin(async move { Ok(blocked) })
            }

            fn is_any_blocked(
                &self,
                _targets: Vec<BlockTarget>,
            ) -> kitsune2_api::BoxFut<'static, K2Result<bool>> {
                let blocked = self.0.load(Ordering::SeqCst);
                Box::pin(async move { Ok(blocked) })
            }
        }

        let url = make_url("unblock");
        let flag = Arc::new(AtomicBool::new(false));
        let blocks: DynBlocks = Arc::new(ToggleBlocks(flag.clone()));
        let known_peers = Arc::new(CoreKnownPeers::default());
        let peer_store: kitsune2_api::DynPeerStore =
            Arc::new(MemPeerStore::new(
                MemPeerStoreConfig {
                    prune_interval_s: 10,
                },
                blocks.clone(),
                known_peers.clone(),
            ));

        let access_state =
            CorePeerAccessState::new(known_peers, blocks.clone(), &peer_store)
                .unwrap();

        let info = AgentBuilder {
            agent: Some(AGENT_1),
            url: Some(Some(url.clone())),
            ..Default::default()
        }
        .build(TestLocalAgent::default());
        peer_store.insert(vec![info.clone()]).await.unwrap();
        tokio::task::yield_now().await;

        // The peer proves itself, and is later blocked anyway.
        access_state
            .set_access_decision(url.clone(), granted())
            .unwrap();
        blocks.block(BlockTarget::Agent(AGENT_1)).await.unwrap();
        peer_store.remove(AGENT_1).await.unwrap();
        tokio::task::yield_now().await;
        assert_eq!(
            access_state
                .get_access_decision(url.clone())
                .unwrap()
                .map(|d| d.decision),
            Some(AccessDecision::Blocked)
        );

        // The host lifts the block and the agent is stored again.
        flag.store(false, Ordering::SeqCst);
        peer_store.insert(vec![info]).await.unwrap();
        tokio::task::yield_now().await;

        assert_eq!(
            access_state.get_access_decision(url.clone()).unwrap(),
            None,
            "an unblocked peer is unknown again, not granted"
        );
    }

    /// A URL with no recorded decision is "unknown", and `set_access_decision`
    /// records a `Granted` decision for it.
    #[tokio::test]
    async fn set_access_decision_records_grant() {
        let url = make_url("grant");
        let access_state = empty_access_state();

        assert_eq!(
            access_state.get_access_decision(url.clone()).unwrap(),
            None
        );

        access_state
            .set_access_decision(url.clone(), granted())
            .unwrap();

        let decision = access_state.get_access_decision(url.clone()).unwrap();
        assert_eq!(decision.map(|d| d.decision), Some(AccessDecision::Granted));
    }

    /// Blocks always win: an explicit `Blocked` entry must not be overwritten
    /// by a later `Granted` recorded by the access module.
    #[tokio::test]
    async fn set_access_decision_does_not_overwrite_blocked() {
        let url = make_url("blocked");
        let access_state = empty_access_state();

        access_state
            .set_access_decision(url.clone(), blocked())
            .unwrap();
        access_state
            .set_access_decision(url.clone(), granted())
            .unwrap();

        let decision = access_state.get_access_decision(url.clone()).unwrap();
        assert_eq!(
            decision.map(|d| d.decision),
            Some(AccessDecision::Blocked),
            "a Granted decision must not overwrite an explicit Blocked entry"
        );
    }

    /// A `Blocked` decision may replace an existing `Granted` decision.
    #[tokio::test]
    async fn set_access_decision_block_overwrites_granted() {
        let url = make_url("regrade");
        let access_state = empty_access_state();

        access_state
            .set_access_decision(url.clone(), granted())
            .unwrap();
        access_state
            .set_access_decision(url.clone(), blocked())
            .unwrap();

        let decision = access_state.get_access_decision(url.clone()).unwrap();
        assert_eq!(decision.map(|d| d.decision), Some(AccessDecision::Blocked));
    }

    /// Removing a decision returns the peer to the "unknown" state, and
    /// removing an absent decision is not an error.
    #[tokio::test]
    async fn remove_access_decision() {
        let url = make_url("remove");
        let access_state = empty_access_state();

        // Removing a decision that was never made is fine.
        access_state.remove_access_decision(url.clone()).unwrap();

        access_state
            .set_access_decision(url.clone(), granted())
            .unwrap();
        assert!(
            access_state
                .get_access_decision(url.clone())
                .unwrap()
                .is_some()
        );

        access_state.remove_access_decision(url.clone()).unwrap();
        assert_eq!(
            access_state.get_access_decision(url.clone()).unwrap(),
            None
        );

        // After removal a fresh grant can be recorded again.
        access_state
            .set_access_decision(url.clone(), granted())
            .unwrap();
        let decision = access_state.get_access_decision(url).unwrap();
        assert_eq!(decision.map(|d| d.decision), Some(AccessDecision::Granted));
    }

    fn empty_access_state() -> CorePeerAccessState {
        let blocks = Arc::new(MemBlocks::default());
        let known_peers = Arc::new(CoreKnownPeers::default());
        let peer_store = make_peer_store(blocks.clone(), known_peers.clone());
        CorePeerAccessState::new(known_peers, blocks, &peer_store).unwrap()
    }

    fn granted() -> PeerAccess {
        PeerAccess {
            decision: AccessDecision::Granted,
            decided_at: Timestamp::now(),
        }
    }

    fn blocked() -> PeerAccess {
        PeerAccess {
            decision: AccessDecision::Blocked,
            decided_at: Timestamp::now(),
        }
    }
}
