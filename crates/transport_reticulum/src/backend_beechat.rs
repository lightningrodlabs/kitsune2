//! Beechat Reticulum backend — `destination` trait implementations
//! against [`reticulum`](https://github.com/BeechatNetworkSystemsLtd/Reticulum-rs).
//!
//! Parallels [`crate::backend_lxmf`] but over Beechat's API. See
//! `PLAN-beechat-backend.md` §5 for the full design. The most important
//! differences from LXMF-rs:
//!
//! - Beechat's `AnnounceEvent` has no `name_hash`; we derive it by
//!   locking the `SingleOutputDestination` and reading its
//!   `DestinationName`. `hops` is surfaced by our fork
//!   (`../Reticulum-rs-lrl`) — upstream doesn't carry it on the
//!   event — so we forward the fork's `ev.hops` directly.
//! - There is no Resource abstraction. Data flows only through
//!   `LinkEvent::Data(LinkPayload)` on the in/out link event streams.
//!   The `PACKET_MDU` is 2048 bytes, so payloads above that bound error
//!   out for now — a chunking layer is tracked as Phase 4 of the plan.
//! - Peer identity on a `Link` is read via `Link::peer_identity()` —
//!   a getter exposed by our Beechat fork
//!   (`../Reticulum-rs-lrl`). Upstream keeps the field private,
//!   which forced an unreliable `destination().identity` reading
//!   that was wrong for inbound links (§Risks-1 of
//!   `PLAN-beechat-backend.md`). The fork adds one small getter and
//!   this backend uses it uniformly for inbound and outbound links.
//! - `TransportConfig` in Beechat has no link-idle / link-proof timeout
//!   setters; those values are compile-time constants in the crate.
//!   Beechat-specific knobs (`set_retransmit`, `set_announce_forever`,
//!   etc.) are exposed through `ReticulumTransportConfig` additions
//!   gated by `#[cfg(feature = "backend-beechat")]`.

use crate::config::{ReticulumInterfaceConfig, ReticulumTransportConfig};
use crate::destination::{
    AnnounceInfo, Destination, DynDestination, DynEndpoint, DynLink, Endpoint,
    Link, LinkId, LinkStatus,
};
use crate::types::{AddressHash, DestinationName, Identity, PrivateIdentity};
use bytes::Bytes;
use kitsune2_api::{BoxFut, K2Error, K2Result};
use reticulum::destination::SingleOutputDestination;
use std::sync::Arc;
use tokio::sync::{Mutex as TokioMutex, broadcast, mpsc};
use tracing::{debug, info, warn};

/// Shared handle to a live `reticulum::transport::Transport`.
pub(crate) type SharedTransport =
    Arc<TokioMutex<reticulum::transport::Transport>>;

/// Bridge channel buffer. Same rationale as the LXMF-rs backend:
/// absorb bursts, lag instead of dropping on contention.
const BRIDGE_CHANNEL_SIZE: usize = 256;

/// Per-link cached status, shared with `RealLink::status()`. Mirrors
/// the LXMF-rs backend — the Beechat `Link::status` is behind a
/// `tokio::Mutex`, so reading it from `RealLink::status()` (sync) is
/// impossible; the in/out link bridges update this cache on
/// Activated/Closed instead.
type LinkStatusCache =
    Arc<std::sync::RwLock<std::collections::HashMap<LinkId, LinkStatus>>>;

/// Build a `DynEndpoint` for the Beechat backend from a
/// `ReticulumTransportConfig`.
///
/// Mirrors `backend_lxmf::create_endpoint_from_config` but against
/// `reticulum::transport::Transport`. Beechat's `TransportConfig`
/// doesn't expose the link-idle / link-proof timeout setters our LXMF
/// backend uses; those values are compile-time constants in Beechat.
/// We read `ReticulumTransportConfig::beechat` for the Beechat-only
/// flags (`retransmit`, `announce_forever`, etc.).
pub(crate) async fn create_endpoint_from_config(
    config: &ReticulumTransportConfig,
    identity: PrivateIdentity,
) -> K2Result<DynEndpoint> {
    let identity_hash = identity.as_identity().address_hash;

    let mut transport_config = reticulum::transport::TransportConfig::new(
        format!("kitsune2-{}", identity_hash.to_hex_string()),
        &identity,
        false,
    );

    // Optional Beechat-only flags. Each is surfaced via
    // `ReticulumTransportConfig::beechat`; all are `Option<bool>` with
    // `None` meaning "leave the Beechat default".
    let b = &config.beechat;
    if let Some(v) = b.retransmit {
        transport_config.set_retransmit(v);
    }
    if let Some(v) = b.broadcast {
        transport_config.set_broadcast(v);
    }
    if let Some(v) = b.reroute_eager {
        transport_config.set_reroute_eager(v);
    }
    if let Some(v) = b.restart_outlinks {
        transport_config.set_restart_outlinks(v);
    }
    if let Some(v) = b.announce_forever {
        transport_config.set_announce_forever(v);
    }

    let transport = reticulum::transport::Transport::new(transport_config);
    let transport = Arc::new(TokioMutex::new(transport));

    start_interfaces(&transport, &config.interfaces).await?;

    Ok(Arc::new(RealEndpoint::new(transport, identity).await))
}

/// Spawn each configured interface on the Transport's
/// `InterfaceManager`. Beechat's interface API is near-identical to
/// LXMF-rs's, with one wrinkle: the TCP server takes the
/// `iface_manager` by value (an `Arc<tokio::sync::Mutex>`), while the
/// TCP client takes only an address.
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
                let client = reticulum::iface::tcp_client::TcpClient::new(
                    target.clone(),
                );
                mgr.spawn(
                    client,
                    reticulum::iface::tcp_client::TcpClient::spawn,
                );
                info!(%target, "Started Beechat TCP client interface");
            }
            ReticulumInterfaceConfig::TcpServer { bind } => {
                let server = reticulum::iface::tcp_server::TcpServer::new(
                    bind.clone(),
                    iface_manager.clone(),
                );
                mgr.spawn(
                    server,
                    reticulum::iface::tcp_server::TcpServer::spawn,
                );
                info!(%bind, "Started Beechat TCP server interface");
            }
            ReticulumInterfaceConfig::Udp { bind, group } => {
                let udp = reticulum::iface::udp::UdpInterface::new(
                    bind.clone(),
                    group.clone(),
                );
                mgr.spawn(udp, reticulum::iface::udp::UdpInterface::spawn);
                info!(%bind, ?group, "Started Beechat UDP interface");
            }
        }
    }
    Ok(())
}

/// Serialize a `PrivateIdentity` to bytes for on-disk persistence.
///
/// Beechat's `PrivateIdentity` doesn't expose `to_private_key_bytes`
/// like LXMF-rs does; it has `to_hex_string` / `new_from_hex_string`
/// covering the same 64-byte key material. We store the hex string as
/// UTF-8 bytes, matching the existing `identity_path` file contract.
pub(crate) fn save_identity_bytes(identity: &PrivateIdentity) -> Vec<u8> {
    identity.to_hex_string().into_bytes()
}

/// Deserialize a `PrivateIdentity` from bytes written by
/// [`save_identity_bytes`].
pub(crate) fn load_identity_bytes(bytes: &[u8]) -> K2Result<PrivateIdentity> {
    let hex = std::str::from_utf8(bytes).map_err(|e| {
        K2Error::other_src("identity file is not valid UTF-8 hex", e)
    })?;
    PrivateIdentity::new_from_hex_string(hex.trim())
        .map_err(|e| K2Error::other(format!("invalid identity hex: {e:?}")))
}

/// Real `Endpoint` implementation backed by
/// `reticulum::transport::Transport`.
pub(crate) struct RealEndpoint {
    transport: SharedTransport,
    identity: PrivateIdentity,
    /// Bridged announce stream — one publisher, many subscribers.
    announce_bridge_tx: broadcast::Sender<AnnounceInfo>,
    /// Per-link status mirror, kept in sync by the in/out link bridges.
    link_status: LinkStatusCache,
    _links_tx: mpsc::Sender<DynLink>,
    links_rx: TokioMutex<Option<mpsc::Receiver<DynLink>>>,
    _data_tx: mpsc::Sender<(LinkId, Bytes)>,
    data_rx: TokioMutex<Option<mpsc::Receiver<(LinkId, Bytes)>>>,
    _close_tx: mpsc::Sender<LinkId>,
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

        // Subscribe to the underlying streams once, then let each
        // bridge task own its receiver. Beechat has no
        // `resource_events` — both inbound and outbound `Data` payloads
        // flow through the link event streams.
        let (announce_rx, inbound_link_rx, outbound_link_rx) = {
            let t = transport.lock().await;
            (
                t.recv_announces().await,
                t.in_link_events(),
                t.out_link_events(),
            )
        };

        let link_status: LinkStatusCache =
            Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));

        spawn_announce_bridge(announce_rx, announce_bridge_tx.clone());
        spawn_inbound_link_bridge(
            inbound_link_rx,
            transport.clone(),
            links_tx.clone(),
            data_tx.clone(),
            close_tx.clone(),
            link_status.clone(),
        );
        spawn_outbound_link_bridge(
            outbound_link_rx,
            data_tx.clone(),
            close_tx.clone(),
            link_status.clone(),
        );

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

/// Bridge `Transport::recv_announces` → [`AnnounceInfo`]. Locks the
/// announce's `SingleOutputDestination` once to pull the identity and
/// name hash out; Beechat doesn't surface these on the event itself.
fn spawn_announce_bridge(
    mut announce_rx: broadcast::Receiver<reticulum::transport::AnnounceEvent>,
    tx: broadcast::Sender<AnnounceInfo>,
) {
    tokio::spawn(async move {
        loop {
            match announce_rx.recv().await {
                Ok(ev) => {
                    let (identity, name_hash) = {
                        let dest = ev.destination.lock().await;
                        let identity = dest.desc.identity;
                        let mut nh = [0u8; 10];
                        let slice = dest.desc.name.as_name_hash_slice();
                        let n = slice.len().min(10);
                        nh[..n].copy_from_slice(&slice[..n]);
                        (identity, nh)
                    };
                    let app_data =
                        Bytes::copy_from_slice(ev.app_data.as_slice());
                    let info = AnnounceInfo {
                        identity,
                        app_data,
                        name_hash,
                        // `hops` is surfaced by our fork; upstream
                        // Beechat doesn't expose it on `AnnounceEvent`.
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

/// Bridge `Transport::in_link_events` → our `recv_links` mpsc (on
/// `Activated`), our data mpsc (on `Data`), and our close mpsc
/// (on `Closed`).
fn spawn_inbound_link_bridge(
    mut link_rx: broadcast::Receiver<
        reticulum::destination::link::LinkEventData,
    >,
    transport: SharedTransport,
    links_tx: mpsc::Sender<DynLink>,
    data_tx: mpsc::Sender<(LinkId, Bytes)>,
    close_tx: mpsc::Sender<LinkId>,
    status_cache: LinkStatusCache,
) {
    tokio::spawn(async move {
        use reticulum::destination::link::LinkEvent;
        loop {
            match link_rx.recv().await {
                Ok(event) => match event.event {
                    LinkEvent::Activated => {
                        status_cache
                            .write()
                            .expect("poisoned")
                            .insert(event.id, LinkStatus::Active);
                        let link_arc = {
                            let t = transport.lock().await;
                            t.find_in_link(&event.id).await
                        };
                        let Some(link_arc) = link_arc else {
                            warn!(
                                link_id = ?event.id,
                                "Activated inbound link not found in transport"
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
                                if links_tx
                                    .send(Arc::new(real) as DynLink)
                                    .await
                                    .is_err()
                                {
                                    debug!(
                                        "inbound links bridge: receiver closed"
                                    );
                                    return;
                                }
                            }
                            Err(e) => {
                                warn!(
                                    ?e,
                                    "Failed to wrap activated inbound link"
                                );
                            }
                        }
                    }
                    LinkEvent::Data(payload) => {
                        let data = Bytes::copy_from_slice(payload.as_slice());
                        if data_tx.send((event.id, data)).await.is_err() {
                            debug!("inbound data bridge: receiver closed");
                            return;
                        }
                    }
                    LinkEvent::Closed => {
                        status_cache
                            .write()
                            .expect("poisoned")
                            .insert(event.id, LinkStatus::Closed);
                        if close_tx.send(event.id).await.is_err() {
                            debug!("inbound close bridge: receiver closed");
                            return;
                        }
                    }
                    LinkEvent::Proof(_) => {}
                },
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

/// Bridge `Transport::out_link_events` → status cache, data mpsc, and
/// close mpsc. Outbound `Activated` doesn't surface a new `DynLink`
/// (we already returned one from `link_to`), but we do still want to
/// mirror it in the status cache so `RealLink::status()` can reflect
/// the live Active state without locking — and so `wait_for_link_active`
/// can unblock.
fn spawn_outbound_link_bridge(
    mut link_rx: broadcast::Receiver<
        reticulum::destination::link::LinkEventData,
    >,
    data_tx: mpsc::Sender<(LinkId, Bytes)>,
    close_tx: mpsc::Sender<LinkId>,
    status_cache: LinkStatusCache,
) {
    tokio::spawn(async move {
        use reticulum::destination::link::LinkEvent;
        loop {
            match link_rx.recv().await {
                Ok(event) => match event.event {
                    LinkEvent::Activated => {
                        status_cache
                            .write()
                            .expect("poisoned")
                            .insert(event.id, LinkStatus::Active);
                    }
                    LinkEvent::Data(payload) => {
                        let data = Bytes::copy_from_slice(payload.as_slice());
                        if data_tx.send((event.id, data)).await.is_err() {
                            debug!("outbound data bridge: receiver closed");
                            return;
                        }
                    }
                    LinkEvent::Closed => {
                        status_cache
                            .write()
                            .expect("poisoned")
                            .insert(event.id, LinkStatus::Closed);
                        if close_tx.send(event.id).await.is_err() {
                            debug!("outbound close bridge: receiver closed");
                            return;
                        }
                    }
                    LinkEvent::Proof(_) => {}
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
            // `send_packet`. Matches the LXMF-rs backend after the
            // announce-emits-packet fix.
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
            // Derive the remote `DestinationDesc` from the peer's
            // public identity + aspect. Beechat has no `new_out`
            // helper like LXMF-rs; we construct a
            // `SingleOutputDestination` directly and take its `desc`.
            let name = DestinationName::new(&app_name, &aspect);
            let out_dest = SingleOutputDestination::new(identity, name);
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

    fn send_packet(&self, _packet: &[u8]) -> BoxFut<'_, K2Result<()>> {
        // NOTE: This is the `Endpoint::send_packet(&[u8])` trait
        // method and is NOT the send path for announces or link data
        // — don't confuse it with Beechat's
        // `reticulum::transport::Transport::send_packet(Packet)`,
        // which this backend DOES call from
        // `RealDestination::announce` and `RealLink::send_small` to
        // actually emit packets on the wire.
        //
        // This trait method takes opaque bytes (produced by a legacy
        // `Link::data_packet` signature that no longer exists). Every
        // real send path now goes through `send_small` (≤ MDU) or
        // `send_resource` (> MDU). Leaving this stubbed is fine; it's
        // not called from the router layer.
        Box::pin(async move {
            Err(K2Error::other(
                "Beechat backend: Endpoint::send_packet(&[u8]) is unused — send paths go through Link::send_small or Endpoint::send_resource",
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
            if data.len() > reticulum::packet::PACKET_MDU {
                return Err(K2Error::other(format!(
                    "Beechat backend: payload {} bytes exceeds PACKET_MDU {} \
                     (chunking layer is Phase 4, see PLAN-beechat-backend.md)",
                    data.len(),
                    reticulum::packet::PACKET_MDU,
                )));
            }

            let t = self.transport.lock().await;

            // Look up the Link by id: for `send_resource` called from
            // `TxImp::send`, the link is outbound; the data router
            // responds on inbound links for the preflight path. Try
            // out_links first, fall back to in_links.
            let link = match t.find_out_link(&link_id).await {
                Some(l) => l,
                None => match t.find_in_link(&link_id).await {
                    Some(l) => l,
                    None => {
                        return Err(K2Error::other(format!(
                            "send_resource: link {link_id:?} not found"
                        )));
                    }
                },
            };

            let packet = {
                let link = link.lock().await;
                link.data_packet(&data).map_err(|e| {
                    K2Error::other(format!("Beechat data_packet failed: {e:?}"))
                })?
            };

            t.send_packet(packet).await;
            Ok(())
        })
    }

    fn packet_mdu(&self) -> usize {
        reticulum::packet::PACKET_MDU
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

/// Real `Destination` implementation backed by
/// `reticulum::destination::SingleInputDestination`.
struct RealDestination {
    inner: Arc<TokioMutex<reticulum::destination::SingleInputDestination>>,
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
        inner: Arc<TokioMutex<reticulum::destination::SingleInputDestination>>,
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
            // callers that want to inspect the bytes.
            let (packet, bytes) = {
                // Beechat's `announce` takes `&self`, not `&mut self`,
                // so a shared lock is enough.
                let guard = self.inner.lock().await;
                let p = guard.announce(rand_core::OsRng, app_data).map_err(
                    |e| {
                        K2Error::other(format!(
                            "Beechat announce failed: {e:?}"
                        ))
                    },
                )?;
                let b = p.data.as_slice().to_vec();
                (p, b)
            };
            // Actually emit it on the network — same fix as the
            // LXMF-rs backend. Without this, `announce()` generates
            // bytes but no packet ever reaches the wire.
            let tp = self.transport.lock().await;
            tp.send_packet(packet).await;
            Ok(bytes)
        })
    }
}

/// Real `Link` implementation. Caches immutable fields so trait
/// methods can answer without acquiring the link's mutex.
///
/// `peer_hash` is read via `Link::peer_identity()` — a getter added
/// by our Beechat fork (`reticulum = { path = "../Reticulum-rs-lrl" }`
/// in the workspace root). Upstream keeps that field private, which
/// would force a fragile `destination().identity` reading that's
/// wrong for inbound links.
pub(crate) struct RealLink {
    inner: Arc<TokioMutex<reticulum::destination::link::Link>>,
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
    async fn from_inner(
        inner: Arc<TokioMutex<reticulum::destination::link::Link>>,
        status_cache: LinkStatusCache,
        transport: SharedTransport,
    ) -> K2Result<Self> {
        let (id, peer_hash, local_dest_hash, status) = {
            let link = inner.lock().await;
            let id = *link.id();
            // Read the peer identity via the fork-added getter. This
            // is correct for both inbound links (peer identity
            // extracted from the link request) and outbound links
            // (populated during handshake when the proof arrives).
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

fn map_status(s: reticulum::destination::link::LinkStatus) -> LinkStatus {
    use reticulum::destination::link::LinkStatus as R;
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
        // Read from the shared status mirror populated by the in/out
        // link event bridges. No contention with Beechat internals.
        self.status_cache
            .read()
            .expect("poisoned")
            .get(&self.id)
            .copied()
            .unwrap_or(LinkStatus::Pending)
    }

    fn send_small<'a>(&'a self, data: &'a [u8]) -> BoxFut<'a, K2Result<()>> {
        Box::pin(async move {
            // Build the Beechat Packet under the link lock, then drop
            // the lock before calling Transport::send_packet (which
            // takes its own internal locks).
            let packet = {
                let link = self.inner.lock().await;
                link.data_packet(data).map_err(|e| {
                    K2Error::other(format!(
                        "Beechat Link::data_packet failed: {e:?}"
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
