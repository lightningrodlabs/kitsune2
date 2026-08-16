//! Integration test for the hello access module over the real transport.
//!
//! Everything here runs on the production wiring: the iroh transport, a real
//! bootstrap server, and the default builder's access module and space
//! secret. The point is the one thing the in-memory functional tests cannot
//! show — that a proof bound to the peer id the transport authenticates holds
//! up when that peer id is a real iroh endpoint id, and that knowing a space
//! id is not enough to get into a space whose secret you do not have.

use bytes::Bytes;
use kitsune2::default_builder;
use kitsune2_api::{
    AccessDecision, BoxFut, Builder, Config, DhtArc, DynKitsune, DynSpace,
    DynSpaceHandler, K2Result, KitsuneHandler, LocalAgent, SpaceHandler,
    SpaceId, Url,
};
use kitsune2_core::Ed25519LocalAgent;
use kitsune2_core::factories::config::{
    CoreBootstrapConfig, CoreBootstrapModConfig,
};
use kitsune2_core::factories::{
    CoreSpaceSecretConfig, CoreSpaceSecretModConfig, encode_space_secret,
};
use kitsune2_test_utils::{
    bootstrap::TestBootstrapSrv, enable_tracing, iter_check,
    space::TEST_SPACE_ID,
};
#[cfg(feature = "transport-iroh")]
use kitsune2_transport_iroh::{
    IrohTransportFactory,
    config::{IrohTransportConfig, IrohTransportModConfig},
};
use std::sync::{Arc, Mutex};

type Received = Arc<Mutex<Vec<Bytes>>>;

#[derive(Debug)]
struct TestSpaceHandler(Received);

impl SpaceHandler for TestSpaceHandler {
    fn recv_notify(
        &self,
        _from_peer: Url,
        _space_id: SpaceId,
        data: Bytes,
    ) -> K2Result<()> {
        self.0.lock().unwrap().push(data);
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
    space: DynSpace,
    url: Url,
    received: Received,
    _kitsune: DynKitsune,
}

/// A production-wired node that has joined the test space with the given
/// secret, and published itself to bootstrap.
async fn make_node(
    bootstrap_url: &str,
    relay_url: &str,
    secret: &[u8],
) -> Node {
    let builder = Builder {
        #[cfg(feature = "transport-iroh")]
        transport: IrohTransportFactory::create(),
        ..default_builder()
    }
    .with_default_config()
    .unwrap();

    builder
        .config
        .set_module_config(&CoreBootstrapModConfig {
            core_bootstrap: CoreBootstrapConfig {
                server_url: Some(bootstrap_url.to_owned()),
                backoff_min_ms: 1000,
                backoff_max_ms: 1000,
                ..Default::default()
            },
        })
        .unwrap();

    #[cfg(feature = "transport-iroh")]
    builder
        .config
        .set_module_config(&IrohTransportModConfig {
            iroh_transport: IrohTransportConfig {
                relay_url: Some(relay_url.to_string()),
                relay_allow_plain_text: true,
                ..Default::default()
            },
        })
        .unwrap();
    let _ = relay_url;

    let received: Received = Arc::new(Mutex::new(Vec::new()));
    let kitsune = builder.build().await.unwrap();
    kitsune
        .register_handler(Arc::new(TestKitsuneHandler(received.clone())))
        .await
        .unwrap();

    // The space secret is per space, so it is supplied as a config override
    // for this space rather than on the builder.
    let space_config = Config::default();
    space_config
        .set_module_config(&CoreSpaceSecretModConfig {
            core_space_secret: CoreSpaceSecretConfig {
                secret: Some(encode_space_secret(secret)),
            },
        })
        .unwrap();

    let space = kitsune
        .space(TEST_SPACE_ID, Some(space_config))
        .await
        .unwrap();

    let local_agent = Arc::new(Ed25519LocalAgent::default());
    local_agent.set_tgt_storage_arc_hint(DhtArc::FULL);
    space.local_agent_join(local_agent.clone()).await.unwrap();

    // Wait for the agent info to be signed, stored and given a url.
    let mut url = None;
    iter_check!(30_000, 100, {
        if space
            .peer_store()
            .get(local_agent.agent().clone())
            .await
            .unwrap()
            .is_some()
        {
            url = space.current_url();
            if url.is_some() {
                break;
            }
        }
    });

    Node {
        space,
        url: url.unwrap(),
        received,
        _kitsune: kitsune,
    }
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

/// Send a notify and report whether it arrived within a few seconds.
async fn notify_arrives(
    from: &DynSpace,
    to: &Node,
    msg: &'static [u8],
) -> bool {
    from.send_notify(to.url.clone(), Bytes::from_static(msg))
        .await
        .unwrap();

    for _ in 0..100 {
        if to.received.lock().unwrap().iter().any(|d| &d[..] == msg) {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    false
}

/// Over the real transport, a space admits the peers that can prove knowledge
/// of its secret and refuses the one that cannot — even though all three
/// nodes know the space id and find each other through bootstrap.
#[tokio::test(flavor = "multi_thread")]
async fn a_space_admits_only_peers_that_prove_knowledge_of_its_secret() {
    enable_tracing();

    let bootstrap_server = TestBootstrapSrv::new(false).await;
    let bootstrap_url = bootstrap_server.addr().to_string();
    let relay_url = format!("{bootstrap_url}/relay");

    const SECRET: &[u8] = b"the-space-secret-only-members-have";
    const GUESS: &[u8] = b"a-wrong-guess-at-the-space-secret";

    let member_1 = make_node(&bootstrap_url, &relay_url, SECRET).await;
    let member_2 = make_node(&bootstrap_url, &relay_url, SECRET).await;
    // The outsider knows the space id — it is public, and bootstrap will hand
    // it the members' urls — but not the secret.
    let outsider = make_node(&bootstrap_url, &relay_url, GUESS).await;

    // The two members find each other through bootstrap and admit each other
    // by exchanging proofs.
    iter_check!(60_000, 200, {
        if is_granted(&member_1.space, &member_2.url)
            && is_granted(&member_2.space, &member_1.url)
        {
            break;
        }
    });

    // Traffic flows between them, in both directions.
    assert!(
        notify_arrives(&member_1.space, &member_2, b"member-to-member").await
    );
    assert!(notify_arrives(&member_2.space, &member_1, b"member-back").await);

    // The outsider has had at least as long to try, and cannot be admitted by
    // either member, nor admit them.
    assert!(
        !is_granted(&member_1.space, &outsider.url),
        "a peer that cannot prove knowledge of the secret must not be admitted"
    );
    assert!(!is_granted(&member_2.space, &outsider.url));
    assert!(!is_granted(&outsider.space, &member_1.url));

    // And no traffic flows to or from it.
    assert!(
        !notify_arrives(&outsider.space, &member_1, b"from-outsider").await
    );
    assert!(!notify_arrives(&member_1.space, &outsider, b"to-outsider").await);

    // The members are unaffected by the outsider's attempts.
    assert!(notify_arrives(&member_1.space, &member_2, b"still-fine").await);
}
