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

use crate::config::{ReticulumInterfaceConfig, ReticulumTransportConfig};
use crate::destination::{
    AnnounceInfo, Destination, DynDestination, DynEndpoint, DynLink, Endpoint,
    Link, LinkId, LinkStatus,
};
use crate::types::{AddressHash, DestinationName, Identity, PrivateIdentity};
use bytes::Bytes;
use kitsune2_api::{BoxFut, K2Error, K2Result};
use rand_core::OsRng;
use rns_transport::destination::new_out;
use std::sync::Arc;
use tokio::sync::{Mutex as TokioMutex, broadcast, mpsc};
use tracing::{debug, info, warn};

/// Shared handle to a live `rns_transport::Transport`.
pub(crate) type SharedTransport =
    Arc<TokioMutex<rns_transport::transport::Transport>>;

/// Build a `DynEndpoint` for the LXMF-rs backend from a
/// `ReticulumTransportConfig` and a caller-supplied identity.
///
/// Constructs the underlying `rns_transport::Transport`, applies the
/// subset of its `TransportConfig` that kitsune2 cares about, brings up
/// the configured interfaces, and wraps the result in a `RealEndpoint`.
/// Called from [`crate::node::ReticulumNode::from_config`] so the
/// backend choice is confined to this crate.
pub(crate) async fn create_endpoint_from_config(
    config: &ReticulumTransportConfig,
    identity: PrivateIdentity,
) -> K2Result<DynEndpoint> {
    let identity_hash = identity.as_identity().address_hash;

    // `broadcast: true` is load-bearing. rns's internal `path_table`
    // only populates routes for Link IDs once an announce has been
    // observed for that destination; link establishment alone does not
    // add a route. With `broadcast: false`,
    // `Transport::send_packet_with_outcome` hits `DroppedNoRoute`
    // (surfaced to callers as `RnsError::ConnectionError`) whenever
    // it's asked to send a Data packet to a Link ID that hasn't been
    // advertised by announce yet — which is the normal case for
    // resource-manager traffic like our preflight frames on a
    // freshly-Active link. Setting `broadcast: true` makes the
    // fallback branch send the packet on all interfaces, which for a
    // point-to-point TCP interface just means "deliver to the one peer
    // on the other end." See `tests/two_node_tcp_preflight.rs` for the
    // regression target.
    let mut transport_config = rns_transport::transport::TransportConfig::new(
        format!("kitsune2-{}", identity_hash.to_hex_string()),
        &identity,
        true,
    );
    transport_config
        .set_link_idle_timeout_secs(config.link_idle_timeout_s as u64);
    transport_config
        .set_link_proof_timeout_secs(config.connect_timeout_s as u64);

    let transport = rns_transport::transport::Transport::new(transport_config);
    let transport = Arc::new(TokioMutex::new(transport));

    start_interfaces(&transport, &config.interfaces).await?;

    Ok(Arc::new(RealEndpoint::new(transport, identity).await))
}

/// Serialize a `PrivateIdentity` to bytes for on-disk persistence.
///
/// Uses `rns_transport`'s raw-key format — 64 bytes: 32-byte private
/// key followed by 32-byte signing key. Back-compatible with the
/// `identity_path` files written before the backend split.
pub(crate) fn save_identity_bytes(identity: &PrivateIdentity) -> Vec<u8> {
    identity.to_private_key_bytes().to_vec()
}

/// Deserialize a `PrivateIdentity` from bytes written by
/// [`save_identity_bytes`].
pub(crate) fn load_identity_bytes(bytes: &[u8]) -> K2Result<PrivateIdentity> {
    PrivateIdentity::from_private_key_bytes(bytes).map_err(|e| {
        K2Error::other(format!("invalid LXMF-rs identity bytes: {e:?}"))
    })
}

/// Spawn each configured interface on the Transport's `InterfaceManager`.
async fn start_interfaces(
    transport: &SharedTransport,
    interfaces: &[ReticulumInterfaceConfig],
) -> K2Result<()> {
    let iface_manager = {
        let t = transport.lock().await;
        t.iface_manager()
    };
    let mut mgr = iface_manager.lock().await;
    for iface in interfaces {
        match iface {
            ReticulumInterfaceConfig::TcpClient { target } => {
                let client = rns_transport::iface::tcp_client::TcpClient::new(
                    target.clone(),
                );
                mgr.spawn(
                    client,
                    rns_transport::iface::tcp_client::TcpClient::spawn,
                );
                info!(%target, "Started Reticulum TCP client interface");
            }
            ReticulumInterfaceConfig::TcpServer { bind } => {
                let server = rns_transport::iface::tcp_server::TcpServer::new(
                    bind.clone(),
                    iface_manager.clone(),
                );
                mgr.spawn(
                    server,
                    rns_transport::iface::tcp_server::TcpServer::spawn,
                );
                info!(%bind, "Started Reticulum TCP server interface");
            }
            ReticulumInterfaceConfig::Udp { bind, group } => {
                let (effective_bind, effective_forward) =
                    crate::config::resolve_udp_addrs(bind, group.as_deref());
                let udp = rns_transport::iface::udp::UdpInterface::new(
                    effective_bind.clone(),
                    effective_forward.clone(),
                );
                mgr.spawn(udp, rns_transport::iface::udp::UdpInterface::spawn);
                info!(
                    bind = %effective_bind,
                    forward = ?effective_forward,
                    multicast = crate::config::is_multicast_addr(&effective_bind),
                    "Started Reticulum UDP interface",
                );
            }
        }
    }
    Ok(())
}

/// Buffer size for the bridge channels. Large enough to absorb short
/// bursts without dropping; an MPSC pressure dropping is worse than
/// lagging here because kitsune2's gossip can retransmit.
const BRIDGE_CHANNEL_SIZE: usize = 256;

/// Per-link cached status, shared with `RealLink::status()`.
type LinkStatusCache =
    Arc<std::sync::RwLock<std::collections::HashMap<LinkId, LinkStatus>>>;

/// Real `Endpoint` implementation backed by `rns_transport::Transport`.
pub(crate) struct RealEndpoint {
    transport: SharedTransport,
    identity: PrivateIdentity,
    /// Bridged announce stream — one publisher, many subscribers.
    announce_bridge_tx: broadcast::Sender<AnnounceInfo>,
    /// Per-link status mirror, kept in sync with rns link events.
    link_status: LinkStatusCache,
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
    /// Bridged link-close stream. Same retention rationale as `_links_tx`.
    _close_tx: mpsc::Sender<LinkId>,
    /// Holder for the close receiver until `recv_link_closures()` is called.
    close_rx: TokioMutex<Option<mpsc::Receiver<LinkId>>>,
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
        let (close_tx, close_rx) = mpsc::channel::<LinkId>(BRIDGE_CHANNEL_SIZE);

        // Subscribe to the underlying streams once, then let each bridge
        // task own its receiver. We subscribe to `out_link_events` too
        // so link-close notifications fire for outbound-initiated links.
        let (
            announce_rx,
            inbound_link_rx,
            outbound_link_rx,
            received_data_rx,
            resource_rx,
        ) = {
            let t = transport.lock().await;
            (
                t.recv_announces().await,
                t.in_link_events(),
                t.out_link_events(),
                t.received_data_events(),
                t.resource_events(),
            )
        };

        let link_status: LinkStatusCache =
            Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));

        spawn_announce_bridge(announce_rx, announce_bridge_tx.clone());
        spawn_inbound_link_bridge(
            inbound_link_rx,
            transport.clone(),
            identity.as_identity().address_hash,
            links_tx.clone(),
            close_tx.clone(),
            link_status.clone(),
        );
        spawn_outbound_link_status_bridge(
            outbound_link_rx,
            close_tx.clone(),
            link_status.clone(),
        );
        spawn_received_data_bridge(received_data_rx, data_tx.clone());
        spawn_resource_bridge(resource_rx, data_tx.clone());

        Self {
            transport,
            identity,
            announce_bridge_tx,
            link_status,
            _links_tx: links_tx,
            links_rx: TokioMutex::new(Some(links_rx)),
            _data_tx: data_tx,
            data_rx: TokioMutex::new(Some(data_rx)),
            _close_tx: close_tx,
            close_rx: TokioMutex::new(Some(close_rx)),
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
    close_tx: mpsc::Sender<LinkId>,
    status_cache: LinkStatusCache,
) {
    tokio::spawn(async move {
        use rns_transport::destination::link::LinkEvent;
        loop {
            match link_rx.recv().await {
                Ok(event) => {
                    match event.event {
                        LinkEvent::Activated => {
                            // Mirror the active state.
                            status_cache
                                .write()
                                .expect("poisoned")
                                .insert(event.id, LinkStatus::Active);
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
                            match RealLink::from_inner(
                                link_arc,
                                status_cache.clone(),
                                transport.clone(),
                            )
                            .await
                            {
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
                                    warn!(?e, "Failed to wrap activated link");
                                }
                            }
                        }
                        LinkEvent::Closed => {
                            status_cache
                                .write()
                                .expect("poisoned")
                                .insert(event.id, LinkStatus::Closed);
                            // Forward to the close router so the peer
                            // refcount is decremented and
                            // `peer_disconnect` fires on the last close.
                            if close_tx.send(event.id).await.is_err() {
                                debug!(
                                    "link-close bridge: receiver closed, exiting"
                                );
                                return;
                            }
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

/// Bridge `Transport::out_link_events` → status cache + close mpsc.
///
/// Outbound links don't surface as new `DynLink`s (we already returned
/// one from `link_to`), but we still need their state transitions to:
/// - update the per-link status cache so `RealLink::status()` can
///   reflect the live Active state without locking,
/// - forward `Closed` to the link-close router for the same teardown
///   semantics inbound links get.
fn spawn_outbound_link_status_bridge(
    mut link_rx: broadcast::Receiver<
        rns_transport::destination::link::LinkEventData,
    >,
    close_tx: mpsc::Sender<LinkId>,
    status_cache: LinkStatusCache,
) {
    tokio::spawn(async move {
        use rns_transport::destination::link::LinkEvent;
        loop {
            match link_rx.recv().await {
                Ok(event) => match event.event {
                    LinkEvent::Activated => {
                        status_cache
                            .write()
                            .expect("poisoned")
                            .insert(event.id, LinkStatus::Active);
                    }
                    LinkEvent::Closed => {
                        status_cache
                            .write()
                            .expect("poisoned")
                            .insert(event.id, LinkStatus::Closed);
                        if close_tx.send(event.id).await.is_err() {
                            debug!(
                                "outbound link-close bridge: receiver closed, exiting"
                            );
                            return;
                        }
                    }
                    LinkEvent::Data(_) => {}
                },
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(n, "outbound link bridge lagged");
                }
                Err(broadcast::error::RecvError::Closed) => {
                    debug!("outbound link bridge closed");
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
///
/// **Important:** `received_data_events` also sees individual resource
/// fragments (`PacketContext::Resource` and friends) — those must be
/// filtered out, otherwise the data router would see fragments
/// in addition to the assembled `ResourceEvent::Complete` payloads
/// the resource bridge forwards. We let through only generic-data
/// (`PacketContext::None`) packets — the framing the kitsune2 data
/// router expects.
fn spawn_received_data_bridge(
    mut rx: broadcast::Receiver<rns_transport::transport::ReceivedData>,
    tx: mpsc::Sender<(LinkId, Bytes)>,
) {
    use rns_transport::packet::PacketContext;
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(ev) => {
                    // Skip resource fragments — the resource bridge
                    // delivers the reassembled whole.
                    if !matches!(ev.context, Some(PacketContext::None) | None) {
                        continue;
                    }
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
            // The destination needs a handle to the Transport so its
            // `announce()` can actually emit the packet via
            // `send_packet`.
            let dest =
                RealDestination::new(dest_arc, name, self.transport.clone())
                    .await;
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
            let real = RealLink::from_inner(
                link_arc,
                self.link_status.clone(),
                self.transport.clone(),
            )
            .await?;
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
        _link_id: &LinkId,
        _data: &[u8],
    ) -> BoxFut<'_, K2Result<()>> {
        // Retired: the transport-level chunking layer in
        // `crate::chunking` fragments oversized payloads over
        // `Link::send_small`, replacing upstream `Resource`. See
        // `PLAN-beechat-chunking.md` §7 for why — chief among the
        // reasons is that `Resource` silently drops the first
        // transfer on a freshly-Active link, which is the race
        // `tests/two_node_tcp_preflight.rs` was written to catch.
        Box::pin(async move {
            Err(K2Error::other(
                "LXMF-rs backend: send_resource is retired — the chunking layer in crate::chunking fragments over Link::send_small",
            ))
        })
    }

    fn packet_mdu(&self) -> usize {
        // `Link::data_packet` wraps the plaintext in fernet, which adds
        // `FERNET_OVERHEAD_SIZE` (IV + HMAC = 48 bytes) plus up to
        // `FERNET_MAX_PADDING_SIZE` (AES block = 16 bytes). The
        // resulting ciphertext has to fit in `PACKET_MDU` (464). So the
        // real plaintext ceiling for data_packet is `LXMF_MAX_PAYLOAD`
        // (400). Anything larger must go via `send_resource`.
        rns_transport::packet::LXMF_MAX_PAYLOAD
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

    fn recv_links(&self) -> BoxFut<'_, K2Result<mpsc::Receiver<DynLink>>> {
        Box::pin(async move {
            let mut slot = self.links_rx.lock().await;
            slot.take().ok_or_else(|| {
                K2Error::other(
                    "recv_links can only be called once per RealEndpoint",
                )
            })
        })
    }

    fn recv_link_closures(
        &self,
    ) -> BoxFut<'_, K2Result<mpsc::Receiver<LinkId>>> {
        Box::pin(async move {
            let mut slot = self.close_rx.lock().await;
            slot.take().ok_or_else(|| {
                K2Error::other(
                    "recv_link_closures can only be called once per RealEndpoint",
                )
            })
        })
    }
}

/// Real `Destination` implementation.
struct RealDestination {
    inner: Arc<TokioMutex<rns_transport::destination::SingleInputDestination>>,
    name: DestinationName,
    address_hash: AddressHash,
    /// Handle back to the owning Transport so `announce()` can call
    /// `send_packet` on the announce it produces.
    transport: SharedTransport,
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
        transport: SharedTransport,
    ) -> Self {
        let address_hash = {
            let d = inner.lock().await;
            d.desc.address_hash
        };
        Self {
            inner,
            name,
            address_hash,
            transport,
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
            // Generate the announce packet and a serialized copy for
            // any caller that wants to inspect the bytes.
            let (packet, bytes) = {
                let mut guard = self.inner.lock().await;
                let p = guard.announce(OsRng, app_data).map_err(|e| {
                    K2Error::other(format!("rns announce failed: {e:?}"))
                })?;
                let b = p.data.as_slice().to_vec();
                (p, b)
            };
            // Actually emit it on the network.
            let tp = self.transport.lock().await;
            tp.send_packet(packet).await;
            Ok(bytes)
        })
    }
}

/// Real `Link` implementation. Caches immutable fields so trait
/// methods can answer without acquiring the link's mutex.
pub(crate) struct RealLink {
    inner: Arc<TokioMutex<rns_transport::destination::link::Link>>,
    id: LinkId,
    peer_hash: AddressHash,
    local_dest_hash: AddressHash,
    /// Handle back to the owning Transport so `send_small` can call
    /// `send_packet` after `data_packet` produces the rns Packet.
    transport: SharedTransport,
    /// Shared status mirror updated by the in/out link event bridges.
    status_cache: LinkStatusCache,
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
    /// fields once under the lock and seeding the status cache.
    async fn from_inner(
        inner: Arc<TokioMutex<rns_transport::destination::link::Link>>,
        status_cache: LinkStatusCache,
        transport: SharedTransport,
    ) -> K2Result<Self> {
        let (id, peer_hash, local_dest_hash, status) = {
            let link = inner.lock().await;
            let id = *link.id();
            let peer_hash = link.peer_identity().address_hash;
            let local_dest_hash = link.destination().address_hash;
            let status = map_status(link.status());
            (id, peer_hash, local_dest_hash, status)
        };
        status_cache
            .write()
            .expect("poisoned")
            .entry(id)
            .or_insert(status);
        Ok(Self {
            inner,
            id,
            peer_hash,
            local_dest_hash,
            transport,
            status_cache,
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
        // Read from the shared status mirror updated by the in/out
        // link event bridges. No lock contention with rns internals.
        self.status_cache
            .read()
            .expect("poisoned")
            .get(&self.id)
            .copied()
            .unwrap_or(LinkStatus::Pending)
    }

    fn send_small<'a>(&'a self, data: &'a [u8]) -> BoxFut<'a, K2Result<()>> {
        Box::pin(async move {
            // Build the rns Packet under the link's lock, then drop
            // the lock before calling Transport::send_packet (which
            // takes its own internal locks).
            let packet = {
                let link = self.inner.lock().await;
                link.data_packet(data).map_err(|e| {
                    K2Error::other(format!(
                        "rns Link::data_packet failed: {e:?}"
                    ))
                })?
            };
            let tp = self.transport.lock().await;
            tp.send_packet(packet).await;
            Ok(())
        })
    }

    fn teardown(&self) -> Option<Vec<u8>> {
        None
    }
}
