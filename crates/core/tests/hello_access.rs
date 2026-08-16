//! Functional tests for the hello access module.
//!
//! These wire real spaces together over the in-memory transport, with
//! bootstrap disabled: every node is given its own mem bootstrap id, so no
//! node can ever learn about another through bootstrap. Anything a node
//! learns about a peer here, it learned from a hello exchange or was handed
//! by the test itself.

use bytes::Bytes;
use kitsune2_api::*;
use kitsune2_core::factories::{
    CoreSpaceSecretConfig, CoreSpaceSecretModConfig, MemBootstrapConfig,
    MemBootstrapModConfig, encode_space_secret,
};
use kitsune2_core::{Ed25519LocalAgent, default_test_builder};
use kitsune2_test_utils::{enable_tracing, iter_check};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const SPACE_A: SpaceId = SpaceId(Id(Bytes::from_static(b"hello-space-a")));
const SPACE_B: SpaceId = SpaceId(Id(Bytes::from_static(b"hello-space-b")));

type Received = Arc<Mutex<Vec<(Url, SpaceId, Bytes)>>>;

#[derive(Debug)]
struct TestSpaceHandler(Received);

impl SpaceHandler for TestSpaceHandler {
    fn recv_notify(
        &self,
        from_peer: Url,
        space_id: SpaceId,
        data: Bytes,
    ) -> K2Result<()> {
        self.0.lock().unwrap().push((from_peer, space_id, data));
        Ok(())
    }
}

#[derive(Debug)]
struct TestKitsuneHandler(Received);

impl KitsuneHandler for TestKitsuneHandler {
    fn create_space(
        &self,
        _space_id: SpaceId,
        _config_override: Option<&Config>,
    ) -> BoxFut<'_, K2Result<DynSpaceHandler>> {
        let received = self.0.clone();
        Box::pin(async move {
            let out: DynSpaceHandler = Arc::new(TestSpaceHandler(received));
            Ok(out)
        })
    }
}

struct Node {
    kitsune: DynKitsune,
    received: Received,
}

impl Node {
    /// A node whose bootstrap is shared with nobody, so it can only learn
    /// about peers from a hello exchange or from this test.
    async fn new() -> Self {
        static ID: AtomicU64 = AtomicU64::new(0);

        let builder = default_test_builder().with_default_config().unwrap();
        builder
            .config
            .set_module_config(&MemBootstrapModConfig {
                mem_bootstrap: MemBootstrapConfig {
                    test_id: format!(
                        "hello-access-isolated-{}",
                        ID.fetch_add(1, Ordering::Relaxed)
                    ),
                    poll_freq_ms: 5_000,
                },
            })
            .unwrap();

        let kitsune = builder.build().await.unwrap();
        let received: Received = Arc::new(Mutex::new(Vec::new()));
        kitsune
            .register_handler(Arc::new(TestKitsuneHandler(received.clone())))
            .await
            .unwrap();

        Self { kitsune, received }
    }

    /// Join a space and put a local agent in it, returning the space and the
    /// agent's id once its agent info has been signed and stored.
    async fn join(
        &self,
        space_id: SpaceId,
        config_override: Option<Config>,
    ) -> (DynSpace, AgentId) {
        let space =
            self.kitsune.space(space_id, config_override).await.unwrap();
        let local_agent: DynLocalAgent = Arc::new(Ed25519LocalAgent::default());
        let agent_id = local_agent.agent().clone();
        space.local_agent_join(local_agent).await.unwrap();

        iter_check!(5000, 10, {
            if space
                .peer_store()
                .get(agent_id.clone())
                .await
                .unwrap()
                .is_some()
            {
                break;
            }
        });

        (space, agent_id)
    }

    fn received_in(&self, space_id: &SpaceId, msg: &[u8]) -> bool {
        self.received
            .lock()
            .unwrap()
            .iter()
            .any(|(_, s, d)| s == space_id && &d[..] == msg)
    }
}

/// Hand one space the other's agent info, which is what bootstrap would
/// otherwise do.
///
/// That tells a node where a peer is and nothing about whether it is allowed
/// in — an agent info is self-issued — so the peer is still unknown
/// afterwards. It does give the access module a URL to challenge, which is
/// how the two sides come to grant each other.
async fn introduce(from: &DynSpace, agent: &AgentId, to: &DynSpace) {
    let info = from.peer_store().get(agent.clone()).await.unwrap().unwrap();
    to.peer_store().insert(vec![info]).await.unwrap();
}

/// Introduce two spaces to each other and wait until each has granted the
/// other, which the hello exchange the introduction triggers is what does.
///
/// This is how the tests set up a space that is not the space under test.
async fn introduce_and_await_grants(
    s1: &DynSpace,
    agent_1: &AgentId,
    s2: &DynSpace,
    agent_2: &AgentId,
) {
    let url1 = url_of(s1).await;
    let url2 = url_of(s2).await;

    introduce(s2, agent_2, s1).await;
    introduce(s1, agent_1, s2).await;

    iter_check!(15_000, 10, {
        if is_granted(s1, &url2) && is_granted(s2, &url1) {
            break;
        }
    });
}

/// A per-space config override that gives the space a secret of its own.
fn with_secret(secret: &[u8]) -> Config {
    let config = Config::default();
    config
        .set_module_config(&CoreSpaceSecretModConfig {
            core_space_secret: CoreSpaceSecretConfig {
                secret: Some(encode_space_secret(secret)),
            },
        })
        .unwrap();
    config
}

fn is_granted(space: &DynSpace, peer_url: &Url) -> bool {
    matches!(
        space
            .peer_access_state()
            .get_access_decision(peer_url.clone())
            .unwrap()
            .map(|a| a.decision),
        Some(AccessDecision::Granted)
    )
}

async fn url_of(space: &DynSpace) -> Url {
    let mut url = None;
    iter_check!(5000, 10, {
        url = space.current_url();
        if url.is_some() {
            break;
        }
    });
    url.unwrap()
}

/// Wait for a notify to arrive, then report whether it did.
async fn notify_arrives(
    from: &DynSpace,
    to_url: &Url,
    to: &Node,
    space_id: &SpaceId,
    msg: &'static [u8],
) -> bool {
    from.send_notify(to_url.clone(), Bytes::from_static(msg))
        .await
        .unwrap();

    for _ in 0..100 {
        if to.received_in(space_id, msg) {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    false
}

/// The Moss multi-cell case, and the reason this module exists.
///
/// Two nodes are already connected because they share a space. One joins a
/// second space, then the other joins it too, over the connection that
/// already exists. No preflight runs again and no bootstrap can help, so
/// without the hello exchange the second node would be invisible to the first
/// until a bootstrap poll that here would never come.
#[tokio::test(flavor = "multi_thread")]
async fn a_peer_joining_a_second_space_is_introduced_over_the_existing_connection()
 {
    enable_tracing();

    let n1 = Node::new().await;
    let n2 = Node::new().await;

    // Both nodes are members of space A, and know each other there.
    let (a1, agent_a1) = n1.join(SPACE_A, None).await;
    let (a2, agent_a2) = n2.join(SPACE_A, None).await;
    let url1 = url_of(&a1).await;
    let url2 = url_of(&a2).await;
    introduce_and_await_grants(&a1, &agent_a1, &a2, &agent_a2).await;

    // And they are connected, which is what makes this the multi-cell case:
    // the connection outlives any single space's membership.
    assert!(notify_arrives(&a1, &url2, &n2, &SPACE_A, b"space-a").await);

    // Node 1 joins space B first. Node 2 is connected but has not joined
    // space B, so the challenge node 1 sends it goes nowhere.
    let (b1, agent_b1) = n1.join(SPACE_B, None).await;

    // Node 2 joins space B afterwards, over the existing connection.
    let (b2, agent_b2) = n2.join(SPACE_B, None).await;

    // The hello exchange introduces them to each other in space B.
    iter_check!(15_000, 10, {
        if is_granted(&b1, &url2) && is_granted(&b2, &url1) {
            break;
        }
    });

    // Each side learned the other's space B agent info from the exchange
    // itself, not from bootstrap, which is isolated per node here.
    iter_check!(5000, 10, {
        if b1
            .peer_store()
            .get(agent_b2.clone())
            .await
            .unwrap()
            .is_some()
            && b2
                .peer_store()
                .get(agent_b1.clone())
                .await
                .unwrap()
                .is_some()
        {
            break;
        }
    });

    // And traffic flows in space B, in both directions.
    assert!(notify_arrives(&b1, &url2, &n2, &SPACE_B, b"space-b-1").await);
    assert!(notify_arrives(&b2, &url1, &n1, &SPACE_B, b"space-b-2").await);
}

/// A peer that cannot prove knowledge of the space secret is never granted,
/// is never told who the members are, and cannot exchange traffic. This is
/// the read protection the access model is for.
#[tokio::test(flavor = "multi_thread")]
async fn a_peer_with_the_wrong_secret_is_never_granted_and_learns_nothing() {
    enable_tracing();

    let n1 = Node::new().await;
    let n3 = Node::new().await;

    // The two nodes share an open space, so they are connected and know each
    // other there. Knowing a peer's url in one space is exactly the position
    // an outsider is in.
    let (a1, agent_a1) = n1.join(SPACE_A, None).await;
    let (a3, agent_a3) = n3.join(SPACE_A, None).await;
    let url1 = url_of(&a1).await;
    let url3 = url_of(&a3).await;
    introduce_and_await_grants(&a1, &agent_a1, &a3, &agent_a3).await;
    assert!(notify_arrives(&a1, &url3, &n3, &SPACE_A, b"space-a").await);

    // Space B has a secret, and node 3 does not have it.
    let (b1, agent_b1) = n1
        .join(SPACE_B, Some(with_secret(b"the-real-space-secret")))
        .await;
    let (b3, agent_b3) = n3
        .join(SPACE_B, Some(with_secret(b"a-guess-at-the-secret")))
        .await;

    // Give the exchange every chance to run and fail, in both directions.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    assert!(
        !is_granted(&b1, &url3),
        "a peer that cannot prove knowledge of the secret must not be granted"
    );
    assert!(!is_granted(&b3, &url1));

    // Critically, no space B agent info was disclosed in either direction.
    assert!(
        b3.peer_store()
            .get(agent_b1.clone())
            .await
            .unwrap()
            .is_none(),
        "an ungranted peer must never be told who the members are"
    );
    assert!(
        b1.peer_store()
            .get(agent_b3.clone())
            .await
            .unwrap()
            .is_none()
    );

    // And no traffic flows, in either direction.
    assert!(!notify_arrives(&b3, &url1, &n1, &SPACE_B, b"from-outsider").await);
    assert!(!notify_arrives(&b1, &url3, &n3, &SPACE_B, b"to-outsider").await);

    // The open space they do share is unaffected.
    assert!(notify_arrives(&a1, &url3, &n3, &SPACE_A, b"space-a-again").await);
}

/// Blocking beats proving: a peer that completed a hello exchange stops being
/// talked to the moment one of its agents is blocked.
#[tokio::test(flavor = "multi_thread")]
async fn blocking_a_granted_peer_stops_its_traffic() {
    enable_tracing();

    let n1 = Node::new().await;
    let n2 = Node::new().await;

    let (a1, agent_a1) = n1.join(SPACE_A, None).await;
    let (a2, agent_a2) = n2.join(SPACE_A, None).await;
    let url1 = url_of(&a1).await;
    let url2 = url_of(&a2).await;
    introduce_and_await_grants(&a1, &agent_a1, &a2, &agent_a2).await;
    assert!(notify_arrives(&a1, &url2, &n2, &SPACE_A, b"space-a").await);

    // Both join a second space and grant each other through a hello exchange.
    let (b1, _agent_b1) = n1.join(SPACE_B, None).await;
    let (b2, agent_b2) = n2.join(SPACE_B, None).await;
    iter_check!(15_000, 10, {
        if is_granted(&b1, &url2) && is_granted(&b2, &url1) {
            break;
        }
    });
    assert!(notify_arrives(&b1, &url2, &n2, &SPACE_B, b"before-block").await);

    // Node 1 blocks node 2's agent in space B. The grant lands a moment
    // before the agent info it is about, which arrives with the last message
    // of the exchange.
    iter_check!(5000, 10, {
        if b1
            .peer_store()
            .get(agent_b2.clone())
            .await
            .unwrap()
            .is_some()
        {
            break;
        }
    });
    b1.blocks()
        .block(BlockTarget::Agent(agent_b2.clone()))
        .await
        .unwrap();
    b1.peer_store().remove(agent_b2).await.unwrap();
    iter_check!(5000, 10, {
        if !is_granted(&b1, &url2) {
            break;
        }
    });

    // Traffic stops in both directions, and the block is not something node 2
    // can prove its way out of.
    assert!(!notify_arrives(&b1, &url2, &n2, &SPACE_B, b"after-block").await);
    assert!(
        !notify_arrives(&b2, &url1, &n1, &SPACE_B, b"after-block-back").await
    );
    assert!(!is_granted(&b1, &url2));
}

/// With no secret configured the secret is the space id, so a space is open
/// to anyone who knows it — today's semantics. An unknown peer's non-hello
/// messages are still dropped until an exchange completes, which under the
/// default it always does.
#[tokio::test(flavor = "multi_thread")]
async fn a_default_secret_space_grants_an_unknown_peer_in_one_exchange() {
    enable_tracing();

    let n1 = Node::new().await;
    let n2 = Node::new().await;

    let (a1, agent_a1) = n1.join(SPACE_A, None).await;
    let (a2, agent_a2) = n2.join(SPACE_A, None).await;
    let url1 = url_of(&a1).await;
    let url2 = url_of(&a2).await;

    // Node 1 has heard of nobody, so node 2 is unknown, and an unknown peer's
    // non-hello traffic is dropped even in a space with no secret at all.
    assert!(!is_granted(&a1, &url2));
    assert!(!notify_arrives(&a1, &url2, &n2, &SPACE_A, b"unknown").await);

    // Node 1 now learns of node 2's url the way bootstrap would tell it.
    // A url with no decision about it is challenged, and with the default
    // secret — the space id — both sides can prove knowledge immediately.
    introduce(&a2, &agent_a2, &a1).await;
    introduce(&a1, &agent_a1, &a2).await;

    iter_check!(15_000, 10, {
        if is_granted(&a1, &url2) && is_granted(&a2, &url1) {
            break;
        }
    });

    assert!(notify_arrives(&a1, &url2, &n2, &SPACE_A, b"known").await);
    assert!(notify_arrives(&a2, &url1, &n1, &SPACE_A, b"known-back").await);
}

/// Grant state is in-memory, hourly pruned and restart lossy — always
/// asymmetrically. A peer that still thinks it is granted heals the pair on
/// its first dropped message, rather than staying silently deaf.
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_message_from_a_forgotten_peer_heals_the_pair() {
    enable_tracing();

    let n1 = Node::new().await;
    let n2 = Node::new().await;

    let (a1, agent_a1) = n1.join(SPACE_A, None).await;
    let (a2, agent_a2) = n2.join(SPACE_A, None).await;
    let url1 = url_of(&a1).await;
    let url2 = url_of(&a2).await;
    introduce_and_await_grants(&a1, &agent_a1, &a2, &agent_a2).await;
    assert!(notify_arrives(&a1, &url2, &n2, &SPACE_A, b"space-a").await);

    let (b1, _agent_b1) = n1.join(SPACE_B, None).await;
    let (b2, _agent_b2) = n2.join(SPACE_B, None).await;
    iter_check!(15_000, 10, {
        if is_granted(&b1, &url2) && is_granted(&b2, &url1) {
            break;
        }
    });
    assert!(notify_arrives(&b2, &url1, &n1, &SPACE_B, b"before-loss").await);

    // Node 1 forgets node 2, as pruning or a restart would make it. Node 2
    // still believes it is granted, so it keeps sending.
    b1.peer_access_state()
        .remove_access_decision(url2.clone())
        .unwrap();
    assert!(!is_granted(&b1, &url2));

    // The first message node 2 sends is dropped — but the drop is what
    // triggers the re-challenge, with no join and no peer store insert
    // involved, and bootstrap isolated.
    assert!(!notify_arrives(&b2, &url1, &n1, &SPACE_B, b"during-loss").await);

    iter_check!(15_000, 10, {
        if is_granted(&b1, &url2) {
            break;
        }
    });
    assert!(notify_arrives(&b2, &url1, &n1, &SPACE_B, b"after-heal").await);

    // The negative: a blocked peer's messages must not trigger a challenge,
    // so blocking is not something a peer can talk its way out of by
    // continuing to send. (That no challenge is even attempted is asserted
    // directly in the mem transport and hello module unit tests.)
    let agent_b2_now = b2.peer_store().get_all().await.unwrap();
    let agent_b2_now = agent_b2_now
        .iter()
        .find(|i| i.url == Some(url2.clone()) && !i.is_tombstone)
        .unwrap()
        .agent
        .clone();
    b1.blocks()
        .block(BlockTarget::Agent(agent_b2_now.clone()))
        .await
        .unwrap();
    b1.peer_store().remove(agent_b2_now).await.unwrap();
    iter_check!(5000, 10, {
        if !is_granted(&b1, &url2) {
            break;
        }
    });

    assert!(!notify_arrives(&b2, &url1, &n1, &SPACE_B, b"while-blocked").await);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(
        !is_granted(&b1, &url2),
        "a blocked peer must not be re-granted by sending us messages"
    );
}
