use kitsune2_api::*;
use kitsune2_test_utils::{
    agent::*, enable_tracing, iter_check, space::TEST_SPACE_ID,
};
use std::sync::{Arc, Mutex};

#[tokio::test(flavor = "multi_thread")]
async fn space_local_agent_join_leave() {
    #[derive(Debug)]
    struct S;

    impl SpaceHandler for S {}

    #[derive(Debug)]
    struct K;

    impl KitsuneHandler for K {
        fn create_space(
            &self,
            _space_id: SpaceId,
            _config_override: Option<&Config>,
        ) -> BoxFut<'_, K2Result<DynSpaceHandler>> {
            Box::pin(async move {
                let s: DynSpaceHandler = Arc::new(S);
                Ok(s)
            })
        }
    }

    let h: DynKitsuneHandler = Arc::new(K);
    let k1 = Builder {
        verifier: Arc::new(TestVerifier),
        ..crate::default_test_builder()
    }
    .with_default_config()
    .unwrap()
    .build()
    .await
    .unwrap();
    k1.register_handler(h).await.unwrap();

    let bob = Arc::new(TestLocalAgent::default()) as DynLocalAgent;
    let ned = Arc::new(TestLocalAgent::default()) as DynLocalAgent;

    assert!(k1.space_if_exists(TEST_SPACE_ID).await.is_none());
    assert_eq!(0, k1.list_spaces().len());

    let s1 = k1.space(TEST_SPACE_ID, None).await.unwrap();

    assert!(k1.space_if_exists(TEST_SPACE_ID).await.is_some());
    assert!(
        k1.space_if_exists(bytes::Bytes::from_static(b"nope").into())
            .await
            .is_none()
    );
    assert_eq!(1, k1.list_spaces().len());

    s1.local_agent_join(bob.clone()).await.unwrap();
    s1.local_agent_join(ned.clone()).await.unwrap();

    let mut active_peer_count = 0;

    iter_check!(1000, {
        active_peer_count = 0;
        for peer in s1.peer_store().get_all().await.unwrap() {
            if !peer.is_tombstone {
                active_peer_count += 1;
            }
        }
        if active_peer_count == 2 {
            break;
        }
    });

    if active_peer_count != 2 {
        panic!("expected 2 active agents, got {active_peer_count}");
    }

    s1.local_agent_leave(bob.agent().clone()).await;

    iter_check!(1000, {
        active_peer_count = 0;
        for peer in s1.peer_store().get_all().await.unwrap() {
            if !peer.is_tombstone {
                active_peer_count += 1;
            }
        }
        if active_peer_count == 1 {
            break;
        }
    });

    if active_peer_count != 1 {
        panic!("expected 1 active agents, got {active_peer_count}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn space_notify_send_recv() {
    enable_tracing();

    type Item = (Url, SpaceId, bytes::Bytes);
    type Recv = Arc<Mutex<Vec<Item>>>;
    let recv = Arc::new(Mutex::new(Vec::new()));

    #[derive(Debug)]
    struct S(Recv);

    impl SpaceHandler for S {
        fn recv_notify(
            &self,
            from_peer: Url,
            space_id: SpaceId,
            data: bytes::Bytes,
        ) -> K2Result<()> {
            self.0.lock().unwrap().push((from_peer, space_id, data));
            Ok(())
        }
    }

    let (u_s, mut u_r) = tokio::sync::mpsc::unbounded_channel();

    #[derive(Debug)]
    struct K(Recv, tokio::sync::mpsc::UnboundedSender<Url>);

    impl KitsuneHandler for K {
        fn new_listening_address(&self, this_url: Url) -> BoxFut<'static, ()> {
            let _ = self.1.send(this_url);
            Box::pin(async move {})
        }

        fn create_space(
            &self,
            _space_id: SpaceId,
            _config_override: Option<&Config>,
        ) -> BoxFut<'_, K2Result<DynSpaceHandler>> {
            Box::pin(async move {
                let s: DynSpaceHandler = Arc::new(S(self.0.clone()));
                Ok(s)
            })
        }
    }

    let h: DynKitsuneHandler = Arc::new(K(recv.clone(), u_s.clone()));
    let k1 = Builder {
        verifier: Arc::new(TestVerifier),
        ..crate::default_test_builder()
    }
    .with_default_config()
    .unwrap()
    .build()
    .await
    .unwrap();
    k1.register_handler(h).await.unwrap();
    let s1 = k1.space(TEST_SPACE_ID.clone(), None).await.unwrap();
    let u1 = u_r.recv().await.unwrap();

    let h: DynKitsuneHandler = Arc::new(K(recv.clone(), u_s.clone()));
    let k2 = Builder {
        verifier: Arc::new(TestVerifier),
        ..crate::default_test_builder()
    }
    .with_default_config()
    .unwrap()
    .build()
    .await
    .unwrap();
    k2.register_handler(h).await.unwrap();
    let s2 = k2.space(TEST_SPACE_ID.clone(), None).await.unwrap();
    let u2 = u_r.recv().await.unwrap();

    println!("url: {u1}, {u2}");

    let bob = Arc::new(TestLocalAgent::default()) as DynLocalAgent;
    let bob_info = AgentBuilder {
        url: Some(Some(u2.clone())),
        ..Default::default()
    }
    .build(bob.clone());
    let ned = Arc::new(TestLocalAgent::default()) as DynLocalAgent;
    let ned_info = AgentBuilder {
        url: Some(Some(u1.clone())),
        ..Default::default()
    }
    .build(ned.clone());

    s1.peer_store().insert(vec![bob_info]).await.unwrap();
    s2.peer_store().insert(vec![ned_info]).await.unwrap();

    // Join local agents to spaces before sending messages
    s1.local_agent_join(ned.clone()).await.unwrap();
    s2.local_agent_join(bob.clone()).await.unwrap();

    // Knowing where a peer is does not admit it. Each side challenges the
    // other's url, and only once both have proven knowledge of the space
    // secret does anything but the access module's own traffic flow.
    let granted = |space: &DynSpace, url: &Url| {
        matches!(
            space
                .peer_access_state()
                .get_access_decision(url.clone())
                .unwrap()
                .map(|a| a.decision),
            Some(AccessDecision::Granted)
        )
    };
    iter_check!(15_000, 10, {
        if granted(&s1, &u2) && granted(&s2, &u1) {
            break;
        }
    });

    s1.send_notify(u2.clone(), bytes::Bytes::from_static(b"hello"))
        .await
        .unwrap();

    iter_check!(5000, 10, {
        if !recv.lock().unwrap().is_empty() {
            break;
        }
    });
    let (f, s, d) = recv.lock().unwrap().remove(0);
    assert_eq!(u1.clone(), f);
    assert_eq!(TEST_SPACE_ID, s);
    assert_eq!("hello", String::from_utf8_lossy(&d));

    s2.send_notify(u1.clone(), bytes::Bytes::from_static(b"world"))
        .await
        .unwrap();

    iter_check!(5000, 10, {
        if !recv.lock().unwrap().is_empty() {
            break;
        }
    });
    let (f, s, d) = recv.lock().unwrap().remove(0);
    assert_eq!(u2, f);
    assert_eq!(TEST_SPACE_ID, s);
    assert_eq!("world", String::from_utf8_lossy(&d));
}

// this is a bit of an integration test...
// but the space module is a bit of an integration module...
#[tokio::test(flavor = "multi_thread")]
async fn space_local_agent_periodic_re_sign_and_bootstrap() {
    #[derive(Debug)]
    struct B(pub Mutex<Vec<Arc<AgentInfoSigned>>>);

    impl Bootstrap for B {
        fn put(&self, info: Arc<AgentInfoSigned>) {
            self.0.lock().unwrap().push(info);
        }
    }

    #[derive(Debug)]
    struct BF(pub Arc<B>);

    impl BootstrapFactory for BF {
        fn default_config(&self, _config: &mut Config) -> K2Result<()> {
            Ok(())
        }

        fn validate_config(&self, _config: &Config) -> K2Result<()> {
            Ok(())
        }

        fn create(
            &self,
            _builder: Arc<Builder>,
            _peer_store: DynPeerStore,
            _space_id: SpaceId,
            _tx: DynTransport,
        ) -> BoxFut<'static, K2Result<DynBootstrap>> {
            let out: DynBootstrap = self.0.clone();
            Box::pin(async move { Ok(out) })
        }
    }

    #[derive(Debug)]
    struct S;

    impl SpaceHandler for S {}

    #[derive(Debug)]
    struct K;

    impl KitsuneHandler for K {
        fn create_space(
            &self,
            _space_id: SpaceId,
            _config_override: Option<&Config>,
        ) -> BoxFut<'_, K2Result<DynSpaceHandler>> {
            Box::pin(async move {
                let s: DynSpaceHandler = Arc::new(S);
                Ok(s)
            })
        }
    }

    let b = Arc::new(B(Mutex::new(Vec::new())));

    let builder = Builder {
        verifier: Arc::new(TestVerifier),
        bootstrap: Arc::new(BF(b.clone())),
        ..crate::default_test_builder()
    }
    .with_default_config()
    .unwrap();

    builder
        .config
        .set_module_config(&super::CoreSpaceModConfig {
            core_space: super::CoreSpaceConfig {
                // check every 5 millis if we need to re-sign
                re_sign_freq_ms: 5,
                // setting this to a big number like 60 minutes makes
                // it so we *always* re-sign agent infos, because the
                // 20min+now expiry times are always within this time range
                re_sign_expire_time_ms: 1000 * 60 * 60,
            },
        })
        .unwrap();

    let h: DynKitsuneHandler = Arc::new(K);
    let k1 = builder.build().await.unwrap();
    k1.register_handler(h).await.unwrap();

    let bob = Arc::new(TestLocalAgent::default()) as DynLocalAgent;

    let s1 = k1.space(TEST_SPACE_ID.clone(), None).await.unwrap();

    s1.local_agent_join(bob.clone()).await.unwrap();

    iter_check!(1000, {
        // see if bootstrap has received at least 5 new updated agent infos
        if b.0.lock().unwrap().len() >= 5 {
            break;
        }
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn broadcast_new_agent_info_on_resign() {
    #[derive(Debug)]
    struct S;

    impl SpaceHandler for S {}

    #[derive(Debug)]
    struct K;

    impl KitsuneHandler for K {
        fn create_space(
            &self,
            _space_id: SpaceId,
            _config_override: Option<&Config>,
        ) -> BoxFut<'_, K2Result<DynSpaceHandler>> {
            Box::pin(async move {
                let s: DynSpaceHandler = Arc::new(S);
                Ok(s)
            })
        }
    }

    #[derive(Debug)]
    struct PublishStub(Mutex<Vec<(Arc<AgentInfoSigned>, Url)>>);

    impl Publish for PublishStub {
        fn publish_ops(
            &self,
            _ops: Vec<PublishOp>,
            _target: Url,
        ) -> BoxFut<'_, K2Result<()>> {
            unimplemented!()
        }

        fn publish_agent(
            &self,
            agent_info: Arc<AgentInfoSigned>,
            target: Url,
        ) -> BoxFut<'_, K2Result<()>> {
            self.0.lock().unwrap().push((agent_info, target));
            Box::pin(async { Ok(()) })
        }
    }

    #[derive(Debug)]
    struct PF(DynPublish);

    impl PublishFactory for PF {
        fn default_config(&self, _config: &mut Config) -> K2Result<()> {
            Ok(())
        }

        fn validate_config(&self, _config: &Config) -> K2Result<()> {
            Ok(())
        }

        fn create(
            &self,
            _builder: Arc<Builder>,
            _space_id: SpaceId,
            _fetch: DynFetch,
            _peer_store: DynPeerStore,
            _peer_meta_store: DynPeerMetaStore,
            _transport: DynTransport,
        ) -> BoxFut<'static, K2Result<DynPublish>> {
            let out: DynPublish = self.0.clone();
            Box::pin(async move { Ok(out) })
        }
    }

    let p = Arc::new(PublishStub(Mutex::new(Vec::new())));

    let builder = Builder {
        verifier: Arc::new(TestVerifier),
        publish: Arc::new(PF(p.clone())),
        ..crate::default_test_builder()
    }
    .with_default_config()
    .unwrap();

    builder
        .config
        .set_module_config(&super::CoreSpaceModConfig {
            core_space: super::CoreSpaceConfig {
                // check every 5 millis if we need to re-sign
                re_sign_freq_ms: 5,
                // setting this to a big number like 60 minutes makes
                // it so we *always* re-sign agent infos, because the
                // 20min+now expiry times are always within this time range
                re_sign_expire_time_ms: 1000 * 60 * 60,
            },
        })
        .unwrap();

    let h: DynKitsuneHandler = Arc::new(K);
    let k1 = builder.build().await.unwrap();
    k1.register_handler(h).await.unwrap();

    let s1 = k1.space(TEST_SPACE_ID.clone(), None).await.unwrap();

    // Join alice to the space and then remove her from the local agent store.
    // This is a quick way to create a new agent info into the peer store for alice.
    let alice = Arc::new(TestLocalAgent::default()) as DynLocalAgent;
    s1.local_agent_join(alice.clone()).await.unwrap();
    s1.local_agent_store()
        .remove(alice.agent().clone())
        .await
        .unwrap();

    let bob = Arc::new(TestLocalAgent::default()) as DynLocalAgent;
    s1.local_agent_join(bob.clone()).await.unwrap();

    iter_check!(1000, {
        // see if we have done at least 5 broadcasts
        if p.0.lock().unwrap().len() >= 5 {
            break;
        }
    });

    let broadcast = p.0.lock().unwrap().last().cloned().unwrap();
    assert_eq!(bob.agent(), &broadcast.0.agent);
}

/// A transport that behaves like the in-memory transport but keeps the
/// space handler it is handed, so a test can drive the space's transport
/// callbacks directly.
#[derive(Debug)]
struct HandlerCapturingTransport {
    inner: DynTransport,
    space_handler: Arc<Mutex<Option<DynTxSpaceHandler>>>,
}

impl Transport for HandlerCapturingTransport {
    fn register_space_handler(
        &self,
        space_id: SpaceId,
        handler: DynTxSpaceHandler,
    ) -> Option<Url> {
        *self.space_handler.lock().unwrap() = Some(handler.clone());
        self.inner.register_space_handler(space_id, handler)
    }

    fn register_module_handler(
        &self,
        space_id: SpaceId,
        module: String,
        handler: DynTxModuleHandler,
    ) {
        self.inner
            .register_module_handler(space_id, module, handler)
    }

    fn disconnect(&self, peer: Url, reason: Option<String>) -> BoxFut<'_, ()> {
        self.inner.disconnect(peer, reason)
    }

    fn send_space_notify(
        &self,
        peer: Url,
        space_id: SpaceId,
        data: bytes::Bytes,
    ) -> BoxFut<'_, K2Result<()>> {
        self.inner.send_space_notify(peer, space_id, data)
    }

    fn send_module(
        &self,
        peer: Url,
        space_id: SpaceId,
        module: String,
        data: bytes::Bytes,
    ) -> BoxFut<'_, K2Result<()>> {
        self.inner.send_module(peer, space_id, module, data)
    }

    fn dial(&self, space_id: SpaceId, peer: Url) -> BoxFut<'_, K2Result<()>> {
        self.inner.dial(space_id, peer)
    }

    fn get_connected_peers(&self) -> BoxFut<'_, K2Result<Vec<Url>>> {
        self.inner.get_connected_peers()
    }

    fn unregister_space(&self, space_id: SpaceId) -> BoxFut<'_, ()> {
        self.inner.unregister_space(space_id)
    }

    fn dump_network_stats(&self) -> BoxFut<'_, K2Result<ApiTransportStats>> {
        self.inner.dump_network_stats()
    }
}

#[derive(Debug)]
struct HandlerCapturingTransportFactory {
    space_handler: Arc<Mutex<Option<DynTxSpaceHandler>>>,
}

impl TransportFactory for HandlerCapturingTransportFactory {
    fn default_config(&self, _config: &mut Config) -> K2Result<()> {
        Ok(())
    }

    fn validate_config(&self, _config: &Config) -> K2Result<()> {
        Ok(())
    }

    fn create(
        &self,
        builder: Arc<Builder>,
        handler: DynTxHandler,
    ) -> BoxFut<'static, K2Result<DynTransport>> {
        let space_handler = self.space_handler.clone();
        Box::pin(async move {
            let inner = crate::factories::MemTransportFactory::create()
                .create(builder, handler)
                .await?;
            let out: DynTransport = Arc::new(HandlerCapturingTransport {
                inner,
                space_handler,
            });
            Ok(out)
        })
    }
}

/// A peer that completes a connection with us is responsive, whatever an
/// earlier failed dial recorded about it.
#[tokio::test(flavor = "multi_thread")]
async fn peer_connect_clears_the_unresponsive_mark() {
    #[derive(Debug)]
    struct S;
    impl SpaceHandler for S {}

    #[derive(Debug)]
    struct K;
    impl KitsuneHandler for K {
        fn create_space(
            &self,
            _space_id: SpaceId,
            _config_override: Option<&Config>,
        ) -> BoxFut<'_, K2Result<DynSpaceHandler>> {
            Box::pin(async move {
                let s: DynSpaceHandler = Arc::new(S);
                Ok(s)
            })
        }
    }

    let space_handler = Arc::new(Mutex::new(None));
    let builder = Builder {
        verifier: Arc::new(TestVerifier),
        transport: Arc::new(HandlerCapturingTransportFactory {
            space_handler: space_handler.clone(),
        }),
        ..crate::default_test_builder()
    }
    .with_default_config()
    .unwrap();

    let kitsune = builder.build().await.unwrap();
    kitsune.register_handler(Arc::new(K)).await.unwrap();
    let space = kitsune.space(TEST_SPACE_ID.clone(), None).await.unwrap();
    let translator = space_handler
        .lock()
        .unwrap()
        .clone()
        .expect("the space registers its handler with the transport");

    let peer = Url::from_str("ws://peer.test:80/peer").unwrap();
    let meta = space.peer_meta_store().clone();
    meta.set_unresponsive(
        peer.clone(),
        Timestamp::now() + std::time::Duration::from_secs(600),
        Timestamp::now(),
    )
    .await
    .unwrap();
    assert!(meta.get_unresponsive(peer.clone()).await.unwrap().is_some());

    translator.peer_connect(peer.clone()).unwrap();

    iter_check!(1000, {
        if meta.get_unresponsive(peer.clone()).await.unwrap().is_none() {
            break;
        }
    });
}
