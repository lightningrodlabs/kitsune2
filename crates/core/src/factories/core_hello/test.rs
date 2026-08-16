//! Unit tests for the hello exchange.
//!
//! These drive two module instances against a stub transport that records
//! outgoing messages instead of sending them, so a test can hand a message to
//! the other side itself. That keeps the exchange deterministic: there is no
//! polling and no wall-clock waiting except where a test is about timeouts.

use super::*;
use crate::factories::{
    CorePeerAccessState, CoreSpaceSecret, CoreSpaceSecretConfig, MemBlocks,
    MemPeerStore, MemPeerStoreConfig, encode_space_secret,
};
use crate::{Ed25519LocalAgent, Ed25519Verifier};
use bytes::Bytes;
use kitsune2_test_utils::agent::AgentBuilder;
use kitsune2_test_utils::space::TEST_SPACE_ID;
use std::sync::Mutex as StdMutex;

/// A transport that records what a module asked it to send.
#[derive(Debug, Default)]
struct StubTransport {
    outbox: StdMutex<Vec<(Url, Bytes)>>,
    connected: StdMutex<Vec<Url>>,
    fail_sends: StdMutex<bool>,
}

impl StubTransport {
    fn take(&self) -> Vec<(Url, Bytes)> {
        std::mem::take(&mut self.outbox.lock().expect("poison"))
    }

    /// Take the single message the module sent, which is what every step of
    /// an exchange produces.
    fn take_one(&self) -> (Url, HelloMsg) {
        let mut sent = self.take();
        assert_eq!(sent.len(), 1, "expected exactly one sent message");
        let (peer, data) = sent.remove(0);
        (peer, K2HelloMessage::decode_msg(data).unwrap())
    }
}

impl Transport for StubTransport {
    fn register_space_handler(
        &self,
        _space_id: SpaceId,
        _handler: DynTxSpaceHandler,
    ) -> Option<Url> {
        None
    }

    fn register_module_handler(
        &self,
        _space_id: SpaceId,
        _module: String,
        _handler: DynTxModuleHandler,
    ) {
    }

    fn disconnect(
        &self,
        _peer: Url,
        _reason: Option<String>,
    ) -> BoxFut<'_, ()> {
        Box::pin(async {})
    }

    fn send_space_notify(
        &self,
        _peer: Url,
        _space_id: SpaceId,
        _data: Bytes,
    ) -> BoxFut<'_, K2Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn send_module(
        &self,
        peer: Url,
        _space_id: SpaceId,
        _module: String,
        data: Bytes,
    ) -> BoxFut<'_, K2Result<()>> {
        Box::pin(async move {
            if *self.fail_sends.lock().expect("poison") {
                return Err(K2Error::other("stub transport send failure"));
            }
            self.outbox.lock().expect("poison").push((peer, data));
            Ok(())
        })
    }

    fn get_connected_peers(&self) -> BoxFut<'_, K2Result<Vec<Url>>> {
        Box::pin(async { Ok(self.connected.lock().expect("poison").clone()) })
    }

    fn unregister_space(&self, _space_id: SpaceId) -> BoxFut<'_, ()> {
        Box::pin(async {})
    }

    fn dump_network_stats(&self) -> BoxFut<'_, K2Result<ApiTransportStats>> {
        Box::pin(async {
            Err(K2Error::other("not implemented for the stub transport"))
        })
    }
}

/// One node's worth of hello module plus the pieces it talks to.
struct Node {
    hello: Arc<CoreHello>,
    transport: Arc<StubTransport>,
    peer_store: DynPeerStore,
    access_state: DynPeerAccessState,
    local_agent_store: DynLocalAgentStore,
    url: Url,
}

impl Node {
    async fn new(url: &str) -> Self {
        Self::new_with(
            url,
            CoreSpaceSecretConfig::default(),
            CoreHelloConfig::default(),
        )
        .await
    }

    async fn new_with(
        url: &str,
        secret: CoreSpaceSecretConfig,
        config: CoreHelloConfig,
    ) -> Self {
        let url = Url::from_str(url).unwrap();
        let blocks = Arc::new(MemBlocks::default());
        let peer_store: DynPeerStore = Arc::new(MemPeerStore::new(
            MemPeerStoreConfig::default(),
            blocks.clone(),
        ));
        let access_state: DynPeerAccessState = Arc::new(
            CorePeerAccessState::new(peer_store.clone(), blocks).unwrap(),
        );
        let local_agent_store = local_agent_store().await;
        let transport = Arc::new(StubTransport::default());
        let space_secret: DynSpaceSecret =
            Arc::new(CoreSpaceSecret::new(secret, TEST_SPACE_ID).unwrap());

        let hello = CoreHello::create(
            config,
            TEST_SPACE_ID,
            Arc::new(Ed25519Verifier),
            space_secret,
            peer_store.clone(),
            local_agent_store.clone(),
            access_state.clone(),
            transport.clone(),
            {
                let url = url.clone();
                Arc::new(move || Some(url.clone()))
            },
        )
        .await
        .unwrap();

        Self {
            hello,
            transport,
            peer_store,
            access_state,
            local_agent_store,
            url,
        }
    }

    /// Join a local agent, so the node has an agent info to disclose.
    async fn join_agent(&self) -> AgentId {
        let local_agent: DynLocalAgent = Arc::new(Ed25519LocalAgent::default());
        let agent_id = local_agent.agent().clone();
        let info = AgentBuilder {
            url: Some(Some(self.url.clone())),
            space_id: Some(TEST_SPACE_ID),
            ..Default::default()
        }
        .build(local_agent.clone());
        self.local_agent_store.add(local_agent).await.unwrap();
        self.peer_store.insert(vec![info]).await.unwrap();
        agent_id
    }

    fn is_granted(&self, peer_url: &Url) -> bool {
        matches!(
            self.access_state
                .get_access_decision(peer_url.clone())
                .unwrap()
                .map(|a| a.decision),
            Some(AccessDecision::Granted)
        )
    }

    async fn deliver(&self, from: &Url, msg: HelloMsg) {
        self.hello.inner.handle(from.clone(), msg).await;
    }
}

/// Run an exchange to completion, pumping messages between two nodes.
async fn pump(a: &Node, b: &Node) {
    for _ in 0..8 {
        let from_a = a.transport.take();
        let from_b = b.transport.take();
        if from_a.is_empty() && from_b.is_empty() {
            return;
        }
        for (_, data) in from_a {
            b.deliver(&a.url, K2HelloMessage::decode_msg(data).unwrap())
                .await;
        }
        for (_, data) in from_b {
            a.deliver(&b.url, K2HelloMessage::decode_msg(data).unwrap())
                .await;
        }
    }
    panic!("the exchange did not settle");
}

/// A local agent store, which only the builder can construct.
async fn local_agent_store() -> DynLocalAgentStore {
    let builder =
        Arc::new(crate::default_test_builder().with_default_config().unwrap());
    builder
        .local_agent_store
        .create(builder.clone())
        .await
        .unwrap()
}

const URL_A: &str = "ws://stub.tx:80/aaa";
const URL_B: &str = "ws://stub.tx:80/bbb";
const URL_C: &str = "ws://stub.tx:80/ccc";

fn wrong_secret() -> CoreSpaceSecretConfig {
    CoreSpaceSecretConfig {
        secret: Some(encode_space_secret(b"a-secret-nobody-else-knows")),
    }
}

/// The happy path: two nodes with the same secret grant each other and
/// exchange agent infos in one exchange.
#[tokio::test(flavor = "multi_thread")]
async fn happy_path_grants_both_sides() {
    let a = Node::new(URL_A).await;
    let b = Node::new(URL_B).await;
    let agent_a = a.join_agent().await;
    let agent_b = b.join_agent().await;

    a.hello.inner.initiate(b.url.clone(), false).await;
    pump(&a, &b).await;

    assert!(a.is_granted(&b.url), "the initiator granted the responder");
    assert!(b.is_granted(&a.url), "the responder granted the initiator");

    // And each side learned the other's agent info, which is the
    // introduction that used to have to wait for a bootstrap poll.
    assert!(a.peer_store.get(agent_b).await.unwrap().is_some());
    assert!(b.peer_store.get(agent_a).await.unwrap().is_some());

    // No state is left behind for a completed exchange.
    assert!(a.hello.inner.state.lock().unwrap().is_empty());
    assert!(b.hello.inner.state.lock().unwrap().is_empty());
}

/// The default secret is the space id, so nodes that were given no secret at
/// all still grant each other — today's "open to anyone who knows the space
/// id" semantics.
#[tokio::test(flavor = "multi_thread")]
async fn default_secret_peers_grant_each_other() {
    let a = Node::new_with(
        URL_A,
        CoreSpaceSecretConfig::default(),
        CoreHelloConfig::default(),
    )
    .await;
    let b = Node::new_with(
        URL_B,
        CoreSpaceSecretConfig::default(),
        CoreHelloConfig::default(),
    )
    .await;

    a.hello.inner.initiate(b.url.clone(), false).await;
    pump(&a, &b).await;

    assert!(a.is_granted(&b.url));
    assert!(b.is_granted(&a.url));
}

/// A node with a different secret cannot prove anything, and neither side
/// records a decision or discloses an agent info.
#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_secret_is_rejected_in_both_directions() {
    let a = Node::new(URL_A).await;
    let b =
        Node::new_with(URL_B, wrong_secret(), CoreHelloConfig::default()).await;
    let agent_a = a.join_agent().await;
    let agent_b = b.join_agent().await;

    // The wrong-secret node initiates. It gets a `Respond` it cannot verify,
    // so it never sends a `Confirm`, and the honest node never discloses.
    b.hello.inner.initiate(a.url.clone(), false).await;
    pump(&a, &b).await;

    assert!(!a.is_granted(&b.url));
    assert!(!b.is_granted(&a.url));
    assert!(a.peer_store.get(agent_b.clone()).await.unwrap().is_none());
    assert!(b.peer_store.get(agent_a.clone()).await.unwrap().is_none());

    // And the other way around: the honest node initiates, the wrong-secret
    // node answers with a proof that does not verify.
    a.hello.inner.initiate(b.url.clone(), false).await;
    pump(&a, &b).await;

    assert!(!a.is_granted(&b.url));
    assert!(!b.is_granted(&a.url));
    assert!(a.peer_store.get(agent_b).await.unwrap().is_none());
    assert!(b.peer_store.get(agent_a).await.unwrap().is_none());
}

/// Reflecting a proof back at its author does not verify, because each side's
/// transcript puts its own nonce and peer id first.
#[tokio::test(flavor = "multi_thread")]
async fn a_reflected_proof_is_rejected() {
    let a = Node::new(URL_A).await;
    let b = Node::new(URL_B).await;

    // A initiates; B responds with a proof over B's transcript.
    a.hello.inner.initiate(b.url.clone(), false).await;
    let (_, initiate) = a.transport.take_one();
    b.deliver(&a.url, initiate).await;
    let (_, respond) = b.transport.take_one();
    let HelloMsg::Respond(respond) = respond else {
        panic!("expected a respond");
    };

    // An attacker echoes B's proof back to B as if it were its own answer to
    // a challenge from B. Give B a challenge in flight so it is willing to
    // look at a `Respond` at all.
    b.hello.inner.initiate(a.url.clone(), false).await;
    b.transport.take();
    b.deliver(
        &a.url,
        HelloMsg::Respond(Respond {
            proto_ver: HELLO_PROTO_VER,
            nonce_r: respond.nonce_r.clone(),
            proof_r: respond.proof_r.clone(),
        }),
    )
    .await;

    assert!(!b.is_granted(&a.url), "a reflected proof must not verify");
    assert!(b.transport.take().is_empty(), "nothing was disclosed");
}

/// A proof obtained from an honest member is useless over a connection
/// authenticated as somebody else, because the proof binds both peer ids and
/// the verifier takes the peer id from the connection.
#[tokio::test(flavor = "multi_thread")]
async fn a_relayed_proof_is_rejected() {
    let victim = Node::new(URL_A).await;
    let honest = Node::new(URL_C).await;
    let relay_url = Url::from_str(URL_B).unwrap();

    // The victim challenges the relay attacker.
    victim.hello.inner.initiate(relay_url.clone(), false).await;
    let (_, initiate) = victim.transport.take_one();
    let HelloMsg::Initiate(initiate) = initiate else {
        panic!("expected an initiate");
    };

    // The attacker forwards the victim's nonce to an honest member, which
    // answers with a proof bound to the honest member's own peer id.
    honest
        .deliver(&victim.url, HelloMsg::Initiate(initiate))
        .await;
    let (_, respond) = honest.transport.take_one();
    let HelloMsg::Respond(respond) = respond else {
        panic!("expected a respond");
    };

    // The attacker presents that proof as its own, over the connection the
    // transport authenticated as the attacker.
    victim.deliver(&relay_url, HelloMsg::Respond(respond)).await;

    assert!(
        !victim.is_granted(&relay_url),
        "a proof bound to another peer id must not verify"
    );
    assert!(
        victim.transport.take().is_empty(),
        "nothing was disclosed to the relay"
    );
}

/// The lower peer id keeps the initiator role when both sides initiate at
/// once, whichever order the initiates are seen in.
#[tokio::test(flavor = "multi_thread")]
async fn simultaneous_initiate_is_tie_broken_by_peer_id() {
    for (lower, higher) in [(URL_A, URL_B), (URL_B, URL_A)] {
        let low = Node::new(std::cmp::min(lower, higher)).await;
        let high = Node::new(std::cmp::max(lower, higher)).await;

        // Both sides open an exchange before either has heard from the other.
        low.hello.inner.initiate(high.url.clone(), false).await;
        high.hello.inner.initiate(low.url.clone(), false).await;
        let (_, low_initiate) = low.transport.take_one();
        let (_, high_initiate) = high.transport.take_one();

        // The order the crossing initiates arrive in must not matter, so run
        // it both ways round.
        if lower == URL_A {
            low.deliver(&high.url, high_initiate).await;
            high.deliver(&low.url, low_initiate).await;
        } else {
            high.deliver(&low.url, low_initiate).await;
            low.deliver(&high.url, high_initiate).await;
        }

        // The lower peer id kept its initiator role and ignored the crossing
        // initiate; the higher one abandoned its own exchange and answered.
        assert!(
            matches!(
                low.hello
                    .inner
                    .state
                    .lock()
                    .unwrap()
                    .get(&high.url)
                    .and_then(|e| e.exchange.as_ref()),
                Some((Exchange::Challenging { .. }, _))
            ),
            "the lower peer id keeps the initiator role"
        );
        assert!(
            matches!(
                high.hello
                    .inner
                    .state
                    .lock()
                    .unwrap()
                    .get(&low.url)
                    .and_then(|e| e.exchange.as_ref()),
                Some((Exchange::Responding { .. }, _))
            ),
            "the higher peer id becomes the responder"
        );

        // Exactly one exchange survived, and it completes.
        pump(&low, &high).await;
        assert!(low.is_granted(&high.url));
        assert!(high.is_granted(&low.url));
    }
}

/// An exchange nobody answers is abandoned when it times out, and the peer is
/// not retried until the backoff has elapsed.
#[tokio::test(flavor = "multi_thread")]
async fn an_unanswered_exchange_times_out_and_backs_off() {
    let config = CoreHelloConfig {
        exchange_timeout_ms: 40,
        retry_backoff_min_ms: 10_000,
        retry_backoff_max_ms: 20_000,
        ..Default::default()
    };
    let a =
        Node::new_with(URL_A, CoreSpaceSecretConfig::default(), config).await;
    let peer_url = Url::from_str(URL_B).unwrap();

    a.hello.inner.initiate(peer_url.clone(), false).await;
    a.transport.take();

    // The expiry task drops the exchange once it has run out of time.
    kitsune2_test_utils::iter_check!(5000, 10, {
        if a.hello
            .inner
            .state
            .lock()
            .unwrap()
            .get(&peer_url)
            .map(|e| e.exchange.is_none())
            .unwrap_or(false)
        {
            break;
        }
    });

    // The peer is now gated behind the retry backoff, so a further trigger
    // does not produce another initiate.
    a.hello.inner.initiate(peer_url.clone(), false).await;
    assert!(
        a.transport.take().is_empty(),
        "a peer in backoff must not be challenged again"
    );

    // And the backoff doubles, up to the configured ceiling.
    let backoff = a
        .hello
        .inner
        .state
        .lock()
        .unwrap()
        .get(&peer_url)
        .map(|e| e.backoff)
        .unwrap();
    assert_eq!(backoff, Duration::from_millis(20_000));
}

/// A fresh agent info for a peer we could not reach clears the backoff, which
/// is how an unresponsive peer is retried as soon as it shows signs of life.
#[tokio::test(flavor = "multi_thread")]
async fn a_fresh_agent_info_clears_the_backoff() {
    let a = Node::new(URL_A).await;
    let peer_url = Url::from_str(URL_B).unwrap();

    // Fail the first attempt so the peer ends up in backoff.
    *a.transport.fail_sends.lock().unwrap() = true;
    a.hello.inner.initiate(peer_url.clone(), false).await;
    *a.transport.fail_sends.lock().unwrap() = false;
    a.transport.take();

    a.hello.inner.initiate(peer_url.clone(), false).await;
    assert!(a.transport.take().is_empty(), "the peer is in backoff");

    a.hello.inner.initiate(peer_url.clone(), true).await;
    assert_eq!(
        a.transport.take().len(),
        1,
        "clearing the backoff allows an immediate retry"
    );
}

/// The number of exchanges in flight is capped, so an attacker cannot make us
/// hold unbounded state.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_exchanges_are_capped() {
    let config = CoreHelloConfig {
        max_concurrent_exchanges: 2,
        ..Default::default()
    };
    let a =
        Node::new_with(URL_A, CoreSpaceSecretConfig::default(), config).await;

    for i in 0..4 {
        let peer_url =
            Url::from_str(format!("ws://stub.tx:80/peer-{i}")).unwrap();
        a.hello.inner.initiate(peer_url, false).await;
    }

    assert_eq!(a.transport.take().len(), 2);
    assert_eq!(a.hello.inner.state.lock().unwrap().len(), 2);
}

/// An explicitly blocked peer is never challenged and never answered, even
/// though it could prove knowledge of the secret. The denylist always wins.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocked_peer_is_neither_challenged_nor_answered() {
    let a = Node::new(URL_A).await;
    let b = Node::new(URL_B).await;

    a.access_state
        .set_access_decision(
            b.url.clone(),
            PeerAccess {
                decision: AccessDecision::Blocked,
                decided_at: Timestamp::now(),
            },
        )
        .unwrap();

    a.hello.inner.initiate(b.url.clone(), false).await;
    assert!(
        a.transport.take().is_empty(),
        "a blocked peer must not be challenged"
    );

    b.hello.inner.initiate(a.url.clone(), false).await;
    let (_, initiate) = b.transport.take_one();
    a.deliver(&b.url, initiate).await;
    assert!(
        a.transport.take().is_empty(),
        "a blocked peer must not be answered"
    );
    assert!(!a.is_granted(&b.url));
}

/// A message that does not belong to any exchange we are running is dropped.
#[tokio::test(flavor = "multi_thread")]
async fn unsolicited_messages_are_dropped() {
    let a = Node::new(URL_A).await;
    let peer_url = Url::from_str(URL_B).unwrap();

    for msg in [
        HelloMsg::Respond(Respond {
            proto_ver: HELLO_PROTO_VER,
            nonce_r: Bytes::from_static(&[1_u8; HELLO_NONCE_LEN]),
            proof_r: Bytes::from_static(&[2_u8; 32]),
        }),
        HelloMsg::Confirm(Confirm {
            proof_i: Bytes::from_static(&[3_u8; 32]),
            agent_infos_i: vec![],
        }),
        HelloMsg::Ack(Ack {
            agent_infos_r: vec![],
        }),
    ] {
        a.deliver(&peer_url, msg).await;
    }

    assert!(a.transport.take().is_empty());
    assert!(!a.is_granted(&peer_url));
    assert!(a.hello.inner.state.lock().unwrap().is_empty());
}

/// A malformed nonce or a protocol version we do not speak is dropped rather
/// than answered.
#[tokio::test(flavor = "multi_thread")]
async fn malformed_initiates_are_dropped() {
    let a = Node::new(URL_A).await;
    let peer_url = Url::from_str(URL_B).unwrap();

    a.deliver(
        &peer_url,
        HelloMsg::Initiate(Initiate {
            proto_ver: HELLO_PROTO_VER,
            nonce_i: Bytes::from_static(b"too short"),
        }),
    )
    .await;
    a.deliver(
        &peer_url,
        HelloMsg::Initiate(Initiate {
            proto_ver: HELLO_PROTO_VER + 1,
            nonce_i: Bytes::from_static(&[1_u8; HELLO_NONCE_LEN]),
        }),
    )
    .await;

    assert!(a.transport.take().is_empty());
    assert!(a.hello.inner.state.lock().unwrap().is_empty());
}

/// A peer with no peer id in its URL cannot be bound to a proof, so the
/// exchange is abandoned rather than run unbound.
#[tokio::test(flavor = "multi_thread")]
async fn a_url_without_a_peer_id_is_abandoned() {
    let a = Node::new(URL_A).await;
    let peer_url = Url::from_str("ws://stub.tx:80").unwrap();
    assert!(peer_url.peer_id().is_none());

    a.deliver(
        &peer_url,
        HelloMsg::Initiate(Initiate {
            proto_ver: HELLO_PROTO_VER,
            nonce_i: Bytes::from_static(&[1_u8; HELLO_NONCE_LEN]),
        }),
    )
    .await;

    assert!(a.transport.take().is_empty());
    assert!(a.hello.inner.state.lock().unwrap().is_empty());
}

/// The join trigger challenges everyone in the peer store and everyone we are
/// connected to, which is the case a new URL never arrives for.
#[tokio::test(flavor = "multi_thread")]
async fn the_join_trigger_challenges_stored_and_connected_peers() {
    let a = Node::new(URL_A).await;
    let stored_url = Url::from_str(URL_B).unwrap();
    let connected_url = Url::from_str(URL_C).unwrap();

    a.peer_store
        .insert(vec![
            AgentBuilder {
                url: Some(Some(stored_url.clone())),
                space_id: Some(TEST_SPACE_ID),
                ..Default::default()
            }
            .build(Ed25519LocalAgent::default()),
        ])
        .await
        .unwrap();
    a.transport
        .connected
        .lock()
        .unwrap()
        .push(connected_url.clone());
    // The peer store insert grants the stored peer through the access state's
    // own listener, so clear that to see the challenge the sweep produces.
    a.transport.take();
    a.access_state
        .remove_access_decision(stored_url.clone())
        .unwrap();

    a.hello.inner.sweep().await;

    let challenged: Vec<_> =
        a.transport.take().into_iter().map(|(url, _)| url).collect();
    assert!(challenged.contains(&stored_url));
    assert!(challenged.contains(&connected_url));
}

/// A transport that cannot list its connections is tolerated: the sweep still
/// challenges the peers it found in the peer store.
#[tokio::test(flavor = "multi_thread")]
async fn the_join_trigger_tolerates_a_transport_without_connection_listing() {
    #[derive(Debug)]
    struct NoListing(Arc<StubTransport>);

    impl Transport for NoListing {
        fn register_space_handler(
            &self,
            space_id: SpaceId,
            handler: DynTxSpaceHandler,
        ) -> Option<Url> {
            self.0.register_space_handler(space_id, handler)
        }
        fn register_module_handler(
            &self,
            space_id: SpaceId,
            module: String,
            handler: DynTxModuleHandler,
        ) {
            self.0.register_module_handler(space_id, module, handler)
        }
        fn disconnect(
            &self,
            peer: Url,
            reason: Option<String>,
        ) -> BoxFut<'_, ()> {
            self.0.disconnect(peer, reason)
        }
        fn send_space_notify(
            &self,
            peer: Url,
            space_id: SpaceId,
            data: Bytes,
        ) -> BoxFut<'_, K2Result<()>> {
            self.0.send_space_notify(peer, space_id, data)
        }
        fn send_module(
            &self,
            peer: Url,
            space_id: SpaceId,
            module: String,
            data: Bytes,
        ) -> BoxFut<'_, K2Result<()>> {
            self.0.send_module(peer, space_id, module, data)
        }
        fn get_connected_peers(&self) -> BoxFut<'_, K2Result<Vec<Url>>> {
            Box::pin(async { Err(K2Error::other("not implemented")) })
        }
        fn unregister_space(&self, space_id: SpaceId) -> BoxFut<'_, ()> {
            self.0.unregister_space(space_id)
        }
        fn dump_network_stats(
            &self,
        ) -> BoxFut<'_, K2Result<ApiTransportStats>> {
            self.0.dump_network_stats()
        }
    }

    let stub = Arc::new(StubTransport::default());
    let transport: DynTransport = Arc::new(NoListing(stub.clone()));
    let url = Url::from_str(URL_A).unwrap();
    let blocks = Arc::new(MemBlocks::default());
    let peer_store: DynPeerStore = Arc::new(MemPeerStore::new(
        MemPeerStoreConfig::default(),
        blocks.clone(),
    ));
    let access_state: DynPeerAccessState = Arc::new(
        CorePeerAccessState::new(peer_store.clone(), blocks).unwrap(),
    );

    let hello = CoreHello::create(
        CoreHelloConfig::default(),
        TEST_SPACE_ID,
        Arc::new(Ed25519Verifier),
        Arc::new(
            CoreSpaceSecret::new(
                CoreSpaceSecretConfig::default(),
                TEST_SPACE_ID,
            )
            .unwrap(),
        ),
        peer_store.clone(),
        local_agent_store().await,
        access_state.clone(),
        transport.clone(),
        {
            let url = url.clone();
            Arc::new(move || Some(url.clone()))
        },
    )
    .await
    .unwrap();

    let stored_url = Url::from_str(URL_B).unwrap();
    peer_store
        .insert(vec![
            AgentBuilder {
                url: Some(Some(stored_url.clone())),
                space_id: Some(TEST_SPACE_ID),
                ..Default::default()
            }
            .build(Ed25519LocalAgent::default()),
        ])
        .await
        .unwrap();
    stub.take();
    access_state
        .remove_access_decision(stored_url.clone())
        .unwrap();

    hello.inner.sweep().await;

    let challenged: Vec<_> =
        stub.take().into_iter().map(|(url, _)| url).collect();
    assert_eq!(challenged, vec![stored_url]);
}

/// An initiate that never arrived does not deadlock the pair.
///
/// This is the Moss case: a node that joins a space first challenges a peer
/// that has not joined that space yet, so its initiate is discarded. When the
/// peer joins and challenges back, the tie-break may hand the initiator role
/// to the side whose initiate was lost, so that side repeats it rather than
/// waiting out a timeout.
#[tokio::test(flavor = "multi_thread")]
async fn a_lost_initiate_is_repeated_rather_than_waited_out() {
    let low = Node::new(URL_A).await;
    let high = Node::new(URL_B).await;
    assert!(low.url.peer_id().unwrap() < high.url.peer_id().unwrap());

    // The lower peer id challenges first, and the challenge is discarded.
    low.hello.inner.initiate(high.url.clone(), false).await;
    low.transport.take();

    // The peer later challenges back.
    high.hello.inner.initiate(low.url.clone(), false).await;
    let (_, initiate) = high.transport.take_one();
    low.deliver(&high.url, initiate).await;

    // The side holding the initiator role repeats its initiate instead of
    // ignoring the crossing one.
    let (_, repeated) = low.transport.take_one();
    assert!(matches!(repeated, HelloMsg::Initiate(_)));

    high.deliver(&low.url, repeated).await;
    pump(&low, &high).await;

    assert!(low.is_granted(&high.url));
    assert!(high.is_granted(&low.url));
}
