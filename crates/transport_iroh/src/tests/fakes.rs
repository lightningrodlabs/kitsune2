//! Shared test doubles for driving [`IrohTransport`] without a network.
//!
//! [`FakeEndpoint`] stands in for iroh: `connect` runs whatever the test
//! configured and records every dial target it was handed, and the relay
//! state it reports is fixed per instance. [`DialableConnection`] is a
//! connection whose send side accepts frames into a mock stream and whose
//! receive side never yields, so a dialled context lives until the test ends.

use crate::close_code::CloseCode;
use crate::connection::{Connection, DynConnection};
use crate::endpoint::{DynIrohEndpoint, Endpoint, EndpointAddrWatcher};
use crate::stream::mock::MockSendStream;
use crate::stream::{DynIrohRecvStream, DynIrohSendStream};
use crate::test_utils::MockTxHandler;
use crate::tests::support::StubTxImp;
use crate::{IrohTransport, IrohTransportConfig};
use bytes::Bytes;
use iroh::{EndpointAddr, EndpointId, RelayConfig, RelayUrl, TransportAddr};
use kitsune2_api::{
    BoxFut, DefaultTransport, K2Error, K2Result, Timestamp, TxImpHnd, Url,
};
use kitsune2_test_utils::space::TEST_SPACE_ID;
use n0_watcher::Disconnected;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

/// What [`FakeEndpoint::connect`] does with a dial target.
pub(super) type ConnectFn = Arc<
    dyn Fn(EndpointAddr) -> BoxFut<'static, K2Result<DynConnection>>
        + Send
        + Sync,
>;

/// A connect that fails immediately with the given message.
pub(super) fn connect_fails(msg: &'static str) -> ConnectFn {
    Arc::new(move |_| Box::pin(async move { Err(K2Error::other(msg)) }))
}

/// A connect that never completes, so the transport's own timeout fires.
pub(super) fn connect_hangs() -> ConnectFn {
    Arc::new(|_| Box::pin(std::future::pending()))
}

/// A connect that hands out the same connection every time.
pub(super) fn connect_yields(conn: DynConnection) -> ConnectFn {
    Arc::new(move |_| {
        let conn = conn.clone();
        Box::pin(async move { Ok(conn) })
    })
}

pub(super) struct FakeEndpoint {
    pub connect: ConnectFn,
    /// Every `EndpointAddr` handed to `connect`, in order.
    pub connect_targets: Arc<Mutex<Vec<EndpointAddr>>>,
    /// What `is_home_relay_known_down` reports.
    pub relay_known_down: bool,
    /// What `is_home_relay_connected` reports.
    pub relay_connected: bool,
    /// What `discover_direct_addrs` reports for any peer.
    pub direct_addrs: Vec<TransportAddr>,
}

impl Default for FakeEndpoint {
    fn default() -> Self {
        Self {
            connect: connect_fails("connect not configured for this test"),
            connect_targets: Arc::new(Mutex::new(Vec::new())),
            relay_known_down: false,
            relay_connected: true,
            direct_addrs: Vec::new(),
        }
    }
}

impl std::fmt::Debug for FakeEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeEndpoint").finish()
    }
}

impl Endpoint for FakeEndpoint {
    fn watch_addr(&self) -> Box<dyn EndpointAddrWatcher> {
        Box::new(PendingWatcher)
    }

    fn accept(&self) -> BoxFut<'_, Option<K2Result<DynConnection>>> {
        Box::pin(std::future::pending())
    }

    fn connect(
        &self,
        endpoint_addr: EndpointAddr,
        _alpn: &[u8],
    ) -> BoxFut<'_, K2Result<DynConnection>> {
        self.connect_targets
            .lock()
            .unwrap()
            .push(endpoint_addr.clone());
        (self.connect)(endpoint_addr)
    }

    fn close(&self) -> BoxFut<'_, ()> {
        Box::pin(async {})
    }

    fn insert_relay(
        &self,
        _url: RelayUrl,
        _config: Arc<RelayConfig>,
    ) -> BoxFut<'_, ()> {
        Box::pin(async {})
    }

    fn remove_relay(
        &self,
        _url: &RelayUrl,
    ) -> BoxFut<'_, Option<Arc<RelayConfig>>> {
        Box::pin(async { None })
    }

    fn id_bytes(&self) -> [u8; 32] {
        [0u8; 32]
    }

    fn is_home_relay_known_down(&self) -> bool {
        self.relay_known_down
    }

    fn is_home_relay_connected(&self) -> bool {
        self.relay_connected
    }

    fn discover_direct_addrs(
        &self,
        _endpoint_id: EndpointId,
        _timeout: Duration,
    ) -> BoxFut<'_, Vec<TransportAddr>> {
        let addrs = self.direct_addrs.clone();
        Box::pin(async move { addrs })
    }
}

/// An `EndpointAddrWatcher` whose `updated()` future never resolves.
struct PendingWatcher;

impl EndpointAddrWatcher for PendingWatcher {
    fn updated(&mut self) -> BoxFut<'_, Result<EndpointAddr, Disconnected>> {
        Box::pin(std::future::pending())
    }
}

/// A connection that accepts outbound frames and never delivers inbound ones.
pub(super) struct DialableConnection {
    remote_id: EndpointId,
    /// Every frame written on the connection's send stream.
    pub send_stream: MockSendStream,
}

impl DialableConnection {
    pub fn new(remote_id: EndpointId) -> Arc<Self> {
        Arc::new(Self {
            remote_id,
            send_stream: MockSendStream::new(),
        })
    }
}

impl Connection for DialableConnection {
    fn open_uni(&self) -> BoxFut<'_, K2Result<DynIrohSendStream>> {
        let stream: DynIrohSendStream = Arc::new(self.send_stream.clone());
        Box::pin(async move { Ok(stream) })
    }

    fn accept_uni(&self) -> BoxFut<'_, K2Result<DynIrohRecvStream>> {
        Box::pin(std::future::pending())
    }

    fn remote_id(&self) -> EndpointId {
        self.remote_id
    }

    fn close(&self, _code: CloseCode, _reason: &[u8]) {}

    fn is_direct(&self) -> bool {
        false
    }

    fn remote_close_reason(&self) -> Option<(CloseCode, Bytes)> {
        None
    }
}

/// Recorded `set_unresponsive` calls as `(peer, when)` pairs.
pub(super) type UnresponsiveCalls = Arc<Mutex<Vec<(Url, Timestamp)>>>;

/// Build a `TxImpHnd` with a space handler registered, so that
/// `set_unresponsive` reaches the recording mock.
pub(super) fn build_handler_with_space(
    set_unresponsive_calls: UnresponsiveCalls,
) -> Arc<TxImpHnd> {
    let calls = set_unresponsive_calls.clone();
    let mock = Arc::new(MockTxHandler {
        set_unresponsive: Arc::new(move |peer, ts| {
            calls.lock().unwrap().push((peer, ts));
            Ok(())
        }),
        ..Default::default()
    });
    let handler = TxImpHnd::new(mock.clone());
    let transport = DefaultTransport::create(&handler, Arc::new(StubTxImp));
    transport.register_space_handler(TEST_SPACE_ID, mock);
    handler
}

pub(super) fn fake_remote_url() -> Url {
    // 64 hex characters → valid iroh EndpointId encoding.
    Url::from_str(
        "https://relay.example.com:443/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    .unwrap()
}

/// A URL of our own that is distinct from [`fake_remote_url`].
pub(super) fn fake_local_url() -> Url {
    Url::from_str(
        "https://relay.example.com:443/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    )
    .unwrap()
}

/// Build an `IrohTransport` directly from its component pieces, bypassing the
/// async `create()` constructor so unit tests can inject a fake `Endpoint`
/// and observe `connections` / `local_url` after the test runs. The
/// background tasks held by the struct are stubbed out with no-op spawns.
pub(super) fn build_transport(
    endpoint: DynIrohEndpoint,
    handler: Arc<TxImpHnd>,
    connections: crate::Connections,
    local_url: Arc<RwLock<Option<Url>>>,
    config: IrohTransportConfig,
) -> IrohTransport {
    let noop_handle = || tokio::spawn(async {}).abort_handle();
    IrohTransport {
        endpoint,
        handler,
        local_url,
        connections,
        connection_locks: Arc::new(Mutex::new(HashMap::new())),
        watch_addr_task: noop_handle(),
        accept_task: noop_handle(),
        relay_keepalive_task: None,
        space_relay_keepalives: Arc::new(Mutex::new(HashMap::new())),
        config,
        space_relays: Arc::new(RwLock::new(HashMap::new())),
    }
}
