//! In-memory fake backend for unit-testing task management without a
//! real Reticulum network.
//!
//! The fake implements the [`crate::destination::Endpoint`],
//! [`crate::destination::Destination`], and [`crate::destination::Link`]
//! traits with dumb channels so tests can:
//!
//! - Push synthetic `AnnounceInfo` events into
//!   `recv_announces()` via [`FakeEndpoint::inject_announce`].
//! - Push synthetic inbound links into `recv_links()` via
//!   [`FakeEndpoint::inject_link`].
//! - Inspect what was sent: [`FakeLink::sent`], [`FakeDestination::announces_sent`].
//! - Observe every `add_destination` call:
//!   [`FakeEndpoint::destinations_added`].

use crate::destination::{
    AnnounceInfo, Destination, DynDestination, DynLink, Endpoint, Link,
    LinkId, LinkStatus,
};
use bytes::Bytes;
use kitsune2_api::{BoxFut, K2Result};
use rns_transport::destination::DestinationName;
use rns_transport::hash::AddressHash;
use rns_transport::identity::Identity;
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, mpsc};

type AddedDests = Arc<Mutex<Vec<(DestinationName, Arc<FakeDestination>)>>>;

/// Fake endpoint -- push inputs, read outputs.
pub(crate) struct FakeEndpoint {
    announce_tx: broadcast::Sender<AnnounceInfo>,
    links_tx: Mutex<Option<mpsc::Sender<DynLink>>>,
    resource_tx: Mutex<Option<mpsc::Sender<(LinkId, Bytes)>>>,
    /// Every destination added (for inspection).
    pub(crate) destinations_added: AddedDests,
}

impl std::fmt::Debug for FakeEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeEndpoint").finish()
    }
}

impl FakeEndpoint {
    /// Create a new fake endpoint.
    pub fn new() -> Arc<Self> {
        let (announce_tx, _) = broadcast::channel(64);
        Arc::new(Self {
            announce_tx,
            links_tx: Mutex::new(None),
            resource_tx: Mutex::new(None),
            destinations_added: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Inject an announce event for listeners to observe.
    pub fn inject_announce(&self, info: AnnounceInfo) {
        let _ = self.announce_tx.send(info);
    }

    /// Inject an inbound link event. `recv_links()` must have been called
    /// at least once beforehand so the sender is initialised.
    pub async fn inject_link(&self, link: DynLink) {
        let tx = { self.links_tx.lock().unwrap().clone() };
        if let Some(tx) = tx {
            let _ = tx.send(link).await;
        }
    }

    /// Inject a resource-data event (simulates inbound data on a link).
    pub async fn inject_data(&self, link_id: LinkId, data: Bytes) {
        let tx = { self.resource_tx.lock().unwrap().clone() };
        if let Some(tx) = tx {
            let _ = tx.send((link_id, data)).await;
        }
    }
}

impl Default for FakeEndpoint {
    fn default() -> Self {
        Arc::try_unwrap(Self::new())
            .unwrap_or_else(|_| unreachable!("single Arc just created"))
    }
}

impl Endpoint for FakeEndpoint {
    fn add_destination(
        &self,
        name: DestinationName,
    ) -> BoxFut<'_, K2Result<DynDestination>> {
        Box::pin(async move {
            let dest = FakeDestination::new(name);
            self.destinations_added
                .lock()
                .unwrap()
                .push((name, dest.clone()));
            Ok(dest as DynDestination)
        })
    }

    fn link_to(
        &self,
        _identity: Identity,
        _app_name: String,
        _aspect: String,
    ) -> BoxFut<'_, K2Result<DynLink>> {
        Box::pin(async move {
            Err(kitsune2_api::K2Error::other(
                "FakeEndpoint::link_to not implemented for this test",
            ))
        })
    }

    fn send_packet(&self, _packet: &[u8]) -> BoxFut<'_, K2Result<()>> {
        Box::pin(async move { Ok(()) })
    }

    fn send_resource(
        &self,
        _link_id: &LinkId,
        _data: &[u8],
    ) -> BoxFut<'_, K2Result<()>> {
        Box::pin(async move { Ok(()) })
    }

    fn packet_mdu(&self) -> usize {
        464
    }

    fn recv_announces(
        &self,
    ) -> BoxFut<'_, K2Result<broadcast::Receiver<AnnounceInfo>>> {
        Box::pin(async move { Ok(self.announce_tx.subscribe()) })
    }

    fn recv_resource_data(
        &self,
    ) -> BoxFut<'_, K2Result<mpsc::Receiver<(LinkId, Bytes)>>> {
        Box::pin(async move {
            let (tx, rx) = mpsc::channel(64);
            *self.resource_tx.lock().unwrap() = Some(tx);
            Ok(rx)
        })
    }

    fn recv_links(&self) -> BoxFut<'_, K2Result<mpsc::Receiver<DynLink>>> {
        Box::pin(async move {
            let (tx, rx) = mpsc::channel(64);
            *self.links_tx.lock().unwrap() = Some(tx);
            Ok(rx)
        })
    }
}

/// Fake destination — records announce calls.
pub struct FakeDestination {
    name: DestinationName,
    address_hash: AddressHash,
    /// Every call to `announce()` (the app_data that was passed in).
    pub announces_sent: Arc<Mutex<Vec<Option<Vec<u8>>>>>,
}

impl std::fmt::Debug for FakeDestination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeDestination")
            .field("address_hash", &self.address_hash)
            .finish()
    }
}

impl FakeDestination {
    /// Create a fake destination with a deterministic address hash
    /// derived from the name.
    pub fn new(name: DestinationName) -> Arc<Self> {
        let mut seed = [0u8; 16];
        let slice = name.as_name_hash_slice();
        let n = slice.len().min(16);
        seed[..n].copy_from_slice(&slice[..n]);
        Arc::new(Self {
            name,
            address_hash: AddressHash::new(seed),
            announces_sent: Arc::new(Mutex::new(Vec::new())),
        })
    }
}

impl Destination for FakeDestination {
    fn address_hash(&self) -> AddressHash {
        self.address_hash
    }

    fn name(&self) -> DestinationName {
        self.name
    }

    fn announce<'a>(
        &'a self,
        app_data: Option<&'a [u8]>,
    ) -> BoxFut<'a, K2Result<Vec<u8>>> {
        Box::pin(async move {
            self.announces_sent
                .lock()
                .unwrap()
                .push(app_data.map(|s| s.to_vec()));
            Ok(Vec::new())
        })
    }
}

/// Fake link — records sends, exposes a controllable status.
pub(crate) struct FakeLink {
    id: LinkId,
    peer_hash: AddressHash,
    local_dest_hash: AddressHash,
    status: Mutex<LinkStatus>,
    /// Every `data_packet(data)` call's bytes.
    pub(crate) sent: Arc<Mutex<Vec<Vec<u8>>>>,
    /// True if `teardown()` was called.
    pub(crate) torn_down: Arc<Mutex<bool>>,
}

impl std::fmt::Debug for FakeLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeLink")
            .field("id", &self.id)
            .field("peer_hash", &self.peer_hash)
            .finish()
    }
}

impl FakeLink {
    /// Create a fake link with given id / peer / local destination seed bytes.
    pub(crate) fn new(id: u8, peer: u8, local_dest: u8) -> Arc<Self> {
        Arc::new(Self {
            id: AddressHash::new([id; 16]),
            peer_hash: AddressHash::new([peer; 16]),
            local_dest_hash: AddressHash::new([local_dest; 16]),
            status: Mutex::new(LinkStatus::Active),
            sent: Arc::new(Mutex::new(Vec::new())),
            torn_down: Arc::new(Mutex::new(false)),
        })
    }

    /// Change the link status (e.g. to simulate a disconnect).
    #[allow(dead_code)]
    pub(crate) fn set_status(&self, status: LinkStatus) {
        *self.status.lock().unwrap() = status;
    }
}

impl Link for FakeLink {
    fn id(&self) -> LinkId {
        self.id
    }

    fn peer_identity_hash(&self) -> AddressHash {
        self.peer_hash
    }

    fn local_destination_hash(&self) -> AddressHash {
        self.local_dest_hash
    }

    fn status(&self) -> LinkStatus {
        *self.status.lock().unwrap()
    }

    fn data_packet(&self, data: &[u8]) -> K2Result<Vec<u8>> {
        self.sent.lock().unwrap().push(data.to_vec());
        Ok(data.to_vec())
    }

    fn teardown(&self) -> Option<Vec<u8>> {
        *self.torn_down.lock().unwrap() = true;
        None
    }
}

/// Helper: fabricate a test `AnnounceInfo` for a given name and identity seed.
pub fn fake_announce(
    name: DestinationName,
    identity: Identity,
) -> AnnounceInfo {
    let name_hash_slice = name.as_name_hash_slice();
    let mut name_hash = [0u8; 10];
    let n = name_hash_slice.len().min(10);
    name_hash[..n].copy_from_slice(&name_hash_slice[..n]);
    AnnounceInfo {
        identity,
        app_data: Bytes::new(),
        name_hash,
        hops: 0,
    }
}

/// Fabricate a test `Identity`. Uses `OsRng` so each call yields a
/// distinct identity; tests that need cross-call equality should save
/// and reuse the returned value.
pub fn fake_identity() -> Identity {
    use rand_core::OsRng;
    let pi = rns_transport::identity::PrivateIdentity::new_from_rand(OsRng);
    *pi.as_identity()
}
