//! Doubles for exercising this crate without multicast or a transport: a
//! [`Daemon`] the test feeds browse events into, transports that record
//! their dials, and the small helpers every test module wants.

use crate::discovery::{self, Daemon};
use crate::fingerprint::SpaceFingerprint;
use kitsune2_api::{
    DialOutcome, DynTransport, K2Result, MockTransport, SpaceId, Url,
};
use mdns_sd::{ServiceEvent, ServiceInfo};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub const SERVICE_TYPE: &str = "_k2test._udp.local.";

pub fn url(s: &str) -> Url {
    Url::from_str(s).unwrap()
}

pub fn space_id(bytes: &[u8]) -> SpaceId {
    SpaceId::from(bytes::Bytes::copy_from_slice(bytes))
}

/// Long enough for spawned dial tasks and browse-loop routing to run.
pub async fn settle() {
    tokio::time::sleep(Duration::from_millis(50)).await;
}

/// Every URL a recording transport was asked to dial, in order.
pub type Dials = Arc<Mutex<Vec<Url>>>;

/// Poll until `cond` holds, failing the test if it does not within two
/// seconds.
pub async fn wait_until(cond: impl Fn() -> bool) {
    kitsune2_test_utils::iter_check!(2000, 5, {
        if cond() {
            break;
        }
    });
}

/// Poll until at least `n` dials were recorded, then return them all.
pub async fn wait_for_dials(dials: &Dials, n: usize) -> Vec<Url> {
    wait_until(|| dials.lock().unwrap().len() >= n).await;
    dials.lock().unwrap().clone()
}

/// A transport reporting `connected` as its open connections and
/// recording every dial before answering it with `dial_fn`.
pub fn transport_with<F, Fut>(
    connected: Vec<Url>,
    dial_fn: F,
) -> (DynTransport, Dials)
where
    F: Fn(Url) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = K2Result<DialOutcome>> + Send + 'static,
{
    let dials: Dials = Arc::new(Mutex::new(Vec::new()));
    let record = dials.clone();
    let mut mock = MockTransport::new();
    mock.expect_dial().returning(move |_space, url| {
        record.lock().unwrap().push(url.clone());
        Box::pin(dial_fn(url))
    });
    mock.expect_get_connected_peers().returning(move || {
        let connected = connected.clone();
        Box::pin(async move { Ok(connected) })
    });
    (Arc::new(mock), dials)
}

/// A transport recording its dials, each answered as connected, and
/// reporting `connected` as its open connections.
pub fn recording_transport(connected: Vec<Url>) -> (DynTransport, Dials) {
    transport_with(connected, |_| async { Ok(DialOutcome::Connected) })
}

/// A transport recording its dials, each of which blocks until `release`
/// is notified; `connected` is what it reports as open connections.
pub fn blocking_transport(
    release: Arc<tokio::sync::Notify>,
    connected: Vec<Url>,
) -> (DynTransport, Dials) {
    let dials: Dials = Arc::new(Mutex::new(Vec::new()));
    let record = dials.clone();
    let mut mock = MockTransport::new();
    mock.expect_dial().returning(move |_space, url| {
        record.lock().unwrap().push(url);
        let release = release.clone();
        Box::pin(async move {
            release.notified().await;
            Ok(DialOutcome::Connected)
        })
    });
    mock.expect_get_connected_peers().returning(move || {
        let connected = connected.clone();
        Box::pin(async move { Ok(connected) })
    });
    (Arc::new(mock), dials)
}

/// A resolved-service event as `mdns-sd` would deliver it for an
/// announcement with the given instance name and TXT fields.
pub fn resolved(instance: &str, txt: &[(&str, &str)]) -> ServiceEvent {
    let info = ServiceInfo::new(
        SERVICE_TYPE,
        instance,
        &format!("{instance}.local."),
        "192.0.2.10",
        0,
        txt,
    )
    .unwrap();
    ServiceEvent::ServiceResolved(Box::new(info.as_resolved_service()))
}

/// A resolved-service event for a record of space `fp` naming `url`.
pub fn resolved_peer(
    instance: &str,
    fp: &SpaceFingerprint,
    url: &str,
) -> ServiceEvent {
    let fp = fp.encode();
    resolved(instance, &[("spacefp", &fp), ("url", url)])
}

/// A fingerprint for tests that have no space secret to derive from.
pub fn test_fp(seed: &[u8]) -> SpaceFingerprint {
    SpaceFingerprint::from(bytes::Bytes::copy_from_slice(seed))
}

/// The event `mdns-sd` delivers when a record goes away.
pub fn removed(instance: &str) -> ServiceEvent {
    ServiceEvent::ServiceRemoved(
        SERVICE_TYPE.to_string(),
        discovery::fullname(SERVICE_TYPE, instance),
    )
}

/// A `register` call as `(instance, txt)`.
pub type Registered = (String, Vec<(String, String)>);

/// A [`Daemon`] that needs no multicast: the test feeds it the browse
/// events it wants seen and reads back what was announced.
#[derive(Debug)]
pub struct FakeDaemon {
    events: flume::Sender<ServiceEvent>,
    browse_rx: flume::Receiver<ServiceEvent>,
    /// Every `register` call.
    pub registered: Mutex<Vec<Registered>>,
    /// Every `unregister` call.
    pub unregistered: Mutex<Vec<String>>,
}

impl FakeDaemon {
    pub fn new() -> Arc<Self> {
        let (events, browse_rx) = flume::unbounded();
        Arc::new(Self {
            events,
            browse_rx,
            registered: Mutex::new(Vec::new()),
            unregistered: Mutex::new(Vec::new()),
        })
    }

    /// Deliver a browse event as if the LAN had produced it.
    pub fn deliver(&self, event: ServiceEvent) {
        self.events.send(event).unwrap();
    }
}

impl Daemon for FakeDaemon {
    fn service_type(&self) -> &str {
        SERVICE_TYPE
    }

    fn browse(&self) -> K2Result<flume::Receiver<ServiceEvent>> {
        Ok(self.browse_rx.clone())
    }

    fn register(&self, instance: &str, txt: &[(&str, &str)]) -> K2Result<()> {
        let txt = txt
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        self.registered
            .lock()
            .unwrap()
            .push((instance.to_string(), txt));
        Ok(())
    }

    fn unregister(&self, instance: &str) -> K2Result<()> {
        self.unregistered.lock().unwrap().push(instance.to_string());
        Ok(())
    }
}
