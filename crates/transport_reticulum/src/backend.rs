//! Real `rns_transport` backend implementation of the `destination` traits.
//!
//! Thin wrappers over `rns_transport::transport::Transport`,
//! `SingleInputDestination`, and `Link`, used by
//! [`crate::node::ReticulumNode::from_config`].
//!
//! # Event bridges
//!
//! `rns_transport` exposes its runtime state through several broadcast
//! channels: `recv_announces`, `in_link_events`, `out_link_events`,
//! `received_data_events`, `resource_events`. The Reticulum transport's
//! routers want a uniform shape — one announce stream, one inbound-link
//! stream, one `(link_id, bytes)` data stream. [`RealEndpoint`] spawns
//! bridge tasks on construction that perform that fan-in.

use crate::destination::{
    AnnounceInfo, Destination, DynDestination, DynLink, Endpoint, Link,
    LinkId, LinkStatus,
};
use bytes::Bytes;
use kitsune2_api::{BoxFut, K2Error, K2Result};
use rand_core::OsRng;
use rns_transport::destination::{new_out, DestinationName};
use rns_transport::hash::AddressHash;
use rns_transport::identity::{Identity, PrivateIdentity};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, Mutex as TokioMutex};
use tracing::{debug, trace, warn};

/// Shared handle to a live `rns_transport::Transport`.
pub(crate) type SharedTransport =
    Arc<TokioMutex<rns_transport::transport::Transport>>;

/// Buffer size for the bridge channels. Large enough to absorb short
/// bursts without dropping; an MPSC pressure dropping is worse than
/// lagging here because kitsune2's gossip can retransmit.
const BRIDGE_CHANNEL_SIZE: usize = 256;

/// Real `Endpoint` implementation backed by `rns_transport::Transport`.
pub(crate) struct RealEndpoint {
    transport: SharedTransport,
    identity: PrivateIdentity,
    /// Bridged announce stream — one publisher, many subscribers.
    announce_bridge_tx: broadcast::Sender<AnnounceInfo>,
    /// Bridged inbound-link stream. Held on the endpoint to keep the
    /// bridge task's mpsc sender alive for the endpoint's lifetime,
    /// even after `recv_links()` hands off the receiver.
    _links_tx: mpsc::Sender<DynLink>,
    /// Holder for the links receiver until `recv_links()` is called.
    links_rx: TokioMutex<Option<mpsc::Receiver<DynLink>>>,
    /// Bridged link-data stream. Same retention rationale as `_links_tx`.
    _data_tx: mpsc::Sender<(LinkId, Bytes)>,
    /// Holder for the data receiver until `recv_resource_data()` is called.
    data_rx: TokioMutex<Option<mpsc::Receiver<(LinkId, Bytes)>>>,
}

impl std::fmt::Debug for RealEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RealEndpoint")
            .field("identity_hash", &self.identity.as_identity().address_hash)
            .finish()
    }
}

impl RealEndpoint {
    /// Create a new `RealEndpoint`, spawning the three event-bridge tasks.
    pub(crate) async fn new(
        transport: SharedTransport,
        identity: PrivateIdentity,
    ) -> Self {
        let (announce_bridge_tx, _) =
            broadcast::channel::<AnnounceInfo>(BRIDGE_CHANNEL_SIZE);
        let (links_tx, links_rx) =
            mpsc::channel::<DynLink>(BRIDGE_CHANNEL_SIZE);
        let (data_tx, data_rx) =
            mpsc::channel::<(LinkId, Bytes)>(BRIDGE_CHANNEL_SIZE);

        // Subscribe to the underlying streams once, then let each bridge
        // task own its receiver.
        let (announce_rx, inbound_link_rx, received_data_rx, resource_rx) = {
            let t = transport.lock().await;
            (
                t.recv_announces().await,
                t.in_link_events(),
                t.received_data_events(),
                t.resource_events(),
            )
        };

        spawn_announce_bridge(announce_rx, announce_bridge_tx.clone());
        spawn_inbound_link_bridge(
            inbound_link_rx,
            transport.clone(),
            identity.as_identity().address_hash,
            links_tx.clone(),
        );
        spawn_received_data_bridge(received_data_rx, data_tx.clone());
        spawn_resource_bridge(resource_rx, data_tx.clone());

        Self {
            transport,
            identity,
            announce_bridge_tx,
            _links_tx: links_tx,
            links_rx: TokioMutex::new(Some(links_rx)),
            _data_tx: data_tx,
            data_rx: TokioMutex::new(Some(data_rx)),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn identity(&self) -> &PrivateIdentity {
        &self.identity
    }

    #[allow(dead_code)]
    pub(crate) fn transport(&self) -> &SharedTransport {
        &self.transport
    }
}

fn spawn_announce_bridge(
    mut announce_rx: broadcast::Receiver<
        rns_transport::transport::AnnounceEvent,
    >,
    tx: broadcast::Sender<AnnounceInfo>,
) {
    tokio::spawn(async move {
        loop {
            match announce_rx.recv().await {
                Ok(ev) => {
                    let identity = {
                        let dest = ev.destination.lock().await;
                        dest.identity
                    };
                    let app_data =
                        Bytes::copy_from_slice(ev.app_data.as_slice());
                    let info = AnnounceInfo {
                        identity,
                        app_data,
                        name_hash: ev.name_hash,
                        hops: ev.hops,
                    };
                    let _ = tx.send(info);
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(n, "announce bridge lagged");
                }
                Err(broadcast::error::RecvError::Closed) => {
                    debug!("announce bridge closed");
                    break;
                }
            }
        }
    });
}

/// Bridge `Transport::in_link_events` → our `recv_links` mpsc.
///
/// We yield a `DynLink` only on `LinkEvent::Activated`, when the
/// handshake has completed and the link is usable. `find_in_link`
/// looks up the live `Arc<Mutex<Link>>` in the transport's handler.
fn spawn_inbound_link_bridge(
    mut link_rx: broadcast::Receiver<
        rns_transport::destination::link::LinkEventData,
    >,
    transport: SharedTransport,
    _local_identity_hash: AddressHash,
    tx: mpsc::Sender<DynLink>,
) {
    tokio::spawn(async move {
        use rns_transport::destination::link::LinkEvent;
        loop {
            match link_rx.recv().await {
                Ok(event) => {
                    match event.event {
                        LinkEvent::Activated => {
                            // Find the underlying Link so we can wrap it.
                            let link_arc = {
                                let t = transport.lock().await;
                                t.find_in_link(&event.id).await
                            };
                            let Some(link_arc) = link_arc else {
                                warn!(
                                    link_id = ?event.id,
                                    "Activated link not found in transport"
                                );
                                continue;
                            };
                            match RealLink::from_inner(link_arc).await {
                                Ok(real) => {
                                    if tx
                                        .send(Arc::new(real) as DynLink)
                                        .await
                                        .is_err()
                                    {
                                        debug!(
                                            "links bridge: receiver closed, exiting"
                                        );
                                        return;
                                    }
                                }
                                Err(e) => {
                                    warn!(
                                        ?e,
                                        "Failed to wrap activated link"
                                    );
                                }
                            }
                        }
                        LinkEvent::Closed => {
                            // The router's `remove_link` path is currently
                            // triggered from `disconnect()` / tests rather
                            // than this event. Wiring closures through
                            // would need a parallel mpsc channel; TODO
                            // when link closure handling lands.
                            trace!(
                                link_id = ?event.id,
                                "inbound link closed (unhandled)"
                            );
                        }
                        LinkEvent::Data(_) => {
                            // Small-packet data is mirrored into
                            // `received_data_events` by the transport's
                            // internal forwarder; let that bridge handle
                            // it to avoid double-deliver.
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(n, "inbound link bridge lagged");
                }
                Err(broadcast::error::RecvError::Closed) => {
                    debug!("inbound link bridge closed");
                    break;
                }
            }
        }
    });
}

/// Bridge `Transport::received_data_events` → our data mpsc.
///
/// rns_transport's internal forwarder sets `ReceivedData.destination =
/// link_id` for data received via a Link::Data event (`data_packet`
/// traffic), so we can use that field directly as the LinkId.
fn spawn_received_data_bridge(
    mut rx: broadcast::Receiver<rns_transport::transport::ReceivedData>,
    tx: mpsc::Sender<(LinkId, Bytes)>,
) {
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(ev) => {
                    let link_id = ev.destination;
                    let data = Bytes::copy_from_slice(ev.data.as_slice());
                    if tx.send((link_id, data)).await.is_err() {
                        debug!("received-data bridge: receiver closed");
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(n, "received-data bridge lagged");
                }
                Err(broadcast::error::RecvError::Closed) => {
                    debug!("received-data bridge closed");
                    break;
                }
            }
        }
    });
}

/// Bridge `Transport::resource_events` → our data mpsc. Only
/// `ResourceEventKind::Complete` surfaces reassembled inbound payloads;
/// `Progress` / `OutboundComplete` are ignored (for now).
fn spawn_resource_bridge(
    mut rx: broadcast::Receiver<rns_transport::resource::ResourceEvent>,
    tx: mpsc::Sender<(LinkId, Bytes)>,
) {
    tokio::spawn(async move {
        use rns_transport::resource::ResourceEventKind;
        loop {
            match rx.recv().await {
                Ok(ev) => {
                    if let ResourceEventKind::Complete(complete) = ev.kind {
                        let link_id = ev.link_id;
                        let data = Bytes::from(complete.data);
                        if tx.send((link_id, data)).await.is_err() {
                            debug!("resource bridge: receiver closed");
                            return;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(n, "resource bridge lagged");
                }
                Err(broadcast::error::RecvError::Closed) => {
                    debug!("resource bridge closed");
                    break;
                }
            }
        }
    });
}

impl Endpoint for RealEndpoint {
    fn add_destination(
        &self,
        name: DestinationName,
    ) -> BoxFut<'_, K2Result<DynDestination>> {
        Box::pin(async move {
            let identity = self.identity.clone();
            let dest_arc = {
                let mut t = self.transport.lock().await;
                t.add_destination(identity, name).await
            };
            let dest = RealDestination::new(dest_arc, name).await;
            Ok(Arc::new(dest) as DynDestination)
        })
    }

    fn link_to(
        &self,
        identity: Identity,
        app_name: String,
        aspect: String,
    ) -> BoxFut<'_, K2Result<DynLink>> {
        Box::pin(async move {
            // Derive the peer's per-space destination descriptor from
            // their public identity + the aspect string.
            let out_dest = new_out(identity, &app_name, &aspect);
            let desc = out_dest.desc;

            let link_arc = {
                let t = self.transport.lock().await;
                t.link(desc).await
            };
            let real = RealLink::from_inner(link_arc).await?;
            Ok(Arc::new(real) as DynLink)
        })
    }

    fn send_packet(&self, packet: &[u8]) -> BoxFut<'_, K2Result<()>> {
        // `data_packet` on our `RealLink` returns the encoded Packet
        // bytes. We need an actual `rns_transport::Packet` to hand to
        // `Transport::send_packet`. Decoding it from a byte slice is
        // work we haven't wired up -- in practice the code path used
        // is `RealLink::send_over_link -> send_resource` for large
        // frames. Short packets can be revisited when step 15's
        // functional tests exercise the ≤ MDU path.
        let _ = packet;
        Box::pin(async move {
            Err(K2Error::other(
                "RealEndpoint::send_packet: ≤ MDU send path not yet wired",
            ))
        })
    }

    fn send_resource(
        &self,
        link_id: &LinkId,
        data: &[u8],
    ) -> BoxFut<'_, K2Result<()>> {
        let link_id = *link_id;
        let data = data.to_vec();
        Box::pin(async move {
            let t = self.transport.lock().await;
            t.send_resource(&link_id, data, None).await.map_err(|e| {
                K2Error::other(format!("rns send_resource failed: {e:?}"))
            })?;
            Ok(())
        })
    }

    fn packet_mdu(&self) -> usize {
        rns_transport::packet::PACKET_MDU
    }

    fn recv_announces(
        &self,
    ) -> BoxFut<'_, K2Result<broadcast::Receiver<AnnounceInfo>>> {
        Box::pin(async move { Ok(self.announce_bridge_tx.subscribe()) })
    }

    fn recv_resource_data(
        &self,
    ) -> BoxFut<'_, K2Result<mpsc::Receiver<(LinkId, Bytes)>>> {
        Box::pin(async move {
            let mut slot = self.data_rx.lock().await;
            slot.take().ok_or_else(|| {
                K2Error::other(
                    "recv_resource_data can only be called once per RealEndpoint",
                )
            })
        })
    }

    fn recv_links(
        &self,
    ) -> BoxFut<'_, K2Result<mpsc::Receiver<DynLink>>> {
        Box::pin(async move {
            let mut slot = self.links_rx.lock().await;
            slot.take().ok_or_else(|| {
                K2Error::other(
                    "recv_links can only be called once per RealEndpoint",
                )
            })
        })
    }
}

/// Real `Destination` implementation.
struct RealDestination {
    inner: Arc<
        TokioMutex<rns_transport::destination::SingleInputDestination>,
    >,
    name: DestinationName,
    address_hash: AddressHash,
}

impl std::fmt::Debug for RealDestination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RealDestination")
            .field("address_hash", &self.address_hash)
            .finish()
    }
}

impl RealDestination {
    async fn new(
        inner: Arc<
            TokioMutex<rns_transport::destination::SingleInputDestination>,
        >,
        name: DestinationName,
    ) -> Self {
        let address_hash = {
            let d = inner.lock().await;
            d.desc.address_hash
        };
        Self {
            inner,
            name,
            address_hash,
        }
    }
}

impl Destination for RealDestination {
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
            let mut guard = self.inner.lock().await;
            let packet =
                guard.announce(OsRng, app_data).map_err(|e| {
                    K2Error::other(format!("rns announce failed: {e:?}"))
                })?;
            Ok(packet.data.as_slice().to_vec())
        })
    }
}

/// Real `Link` implementation. Caches immutable fields so trait
/// methods can answer without acquiring the link's mutex.
pub(crate) struct RealLink {
    _inner: Arc<TokioMutex<rns_transport::destination::link::Link>>,
    id: LinkId,
    peer_hash: AddressHash,
    local_dest_hash: AddressHash,
    /// Cached status snapshot. Updated opportunistically.
    cached_status: std::sync::Mutex<LinkStatus>,
}

impl std::fmt::Debug for RealLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RealLink")
            .field("id", &self.id)
            .field("peer_hash", &self.peer_hash)
            .finish()
    }
}

impl RealLink {
    /// Wrap an existing `Arc<Mutex<Link>>` by reading its immutable
    /// fields once under the lock.
    async fn from_inner(
        inner: Arc<TokioMutex<rns_transport::destination::link::Link>>,
    ) -> K2Result<Self> {
        let (id, peer_hash, local_dest_hash, status) = {
            let link = inner.lock().await;
            let id = *link.id();
            let peer_hash = link.peer_identity().address_hash;
            // `destination` on a Link is the *remote* DestinationDesc
            // for outbound links, and our own for inbound links. We
            // use it as-is -- the links router only inspects
            // `local_destination_hash` on inbound links, where this
            // matches our registered per-space destination.
            let local_dest_hash = link.destination().address_hash;
            let status = map_status(link.status());
            (id, peer_hash, local_dest_hash, status)
        };
        Ok(Self {
            _inner: inner,
            id,
            peer_hash,
            local_dest_hash,
            cached_status: std::sync::Mutex::new(status),
        })
    }
}

fn map_status(s: rns_transport::destination::link::LinkStatus) -> LinkStatus {
    use rns_transport::destination::link::LinkStatus as R;
    match s {
        R::Pending => LinkStatus::Pending,
        R::Handshake => LinkStatus::Handshake,
        R::Active => LinkStatus::Active,
        R::Stale => LinkStatus::Stale,
        R::Closed => LinkStatus::Closed,
    }
}

impl Link for RealLink {
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
        *self.cached_status.lock().expect("poisoned")
    }

    fn data_packet(&self, data: &[u8]) -> K2Result<Vec<u8>> {
        // Encode the data as a Reticulum Packet ready for
        // `Transport::send_packet`. Taking a sync lock here is ok
        // because `data_packet` on the underlying `Link` is a
        // synchronous encrypt+frame operation -- but rns wraps it
        // behind a tokio::Mutex, which we can't sync-lock safely from
        // async context. See `send_packet` above for the full story;
        // the ≤ MDU path is wired up once step 15 demands it.
        let _ = data;
        Err(K2Error::other(
            "RealLink::data_packet: ≤ MDU send path not yet wired (use send_resource via send_over_link)",
        ))
    }

    fn teardown(&self) -> Option<Vec<u8>> {
        None
    }
}
