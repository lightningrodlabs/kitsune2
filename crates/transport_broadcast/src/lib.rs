#![deny(missing_docs)]
//! Kitsune2 transport over shared broadcast media.
//!
//! This crate carries kitsune2 traffic over media where transmission is
//! inherently one-to-all — an in-process test "air", UDP multicast on a
//! LAN, and in the future BLE advertising, ultrasonic sound or optical
//! channels. No connections exist at the physical layer; "reachability"
//! degrades to "was heard recently", which eliminates connection
//! management (NAT traversal, relays, reconnection policy) entirely.
//!
//! See `docs/design/broadcast-transport.md` in the repository root for
//! the full design, including the phase-2 native-broadcast protocol
//! (Trickle beacons, chain-head repair) that this phase-1 crate lays
//! the groundwork for.
//!
//! # Phase 1: unicast emulation
//!
//! This implementation satisfies the standard connection-shaped
//! [`TxImp`] contract so every existing kitsune2 module (gossip, fetch,
//! publish, bootstrap) runs unmodified:
//!
//! - Frames carry ephemeral source/destination node ids; nodes drop
//!   frames not addressed to them ([`frame`]).
//! - A "connection" is an entry in a peer table, opened on first
//!   contact in either direction (with the usual preflight exchange)
//!   and closed by idle timeout or handler error.
//! - Payloads larger than the medium MTU are fragmented and
//!   reassembled ([`chunking`]).
//!
//! Peer urls look like `ws://<medium>.bcast:1/<node-id-hex>`. The `ws`
//! scheme is nominal (there is no websocket); it follows the precedent
//! of core's `MemTransport` urls and keeps this crate compatible with
//! the published `kitsune2_api`.
//!
//! # Known phase-1 limitations
//!
//! By design (see the design doc): no per-frame FEC or retransmission,
//! so lossy media will drop whole logical payloads (upper layers
//! already tolerate this, but throughput suffers); and frame payloads
//! are cleartext `K2Proto` messages, so space ids are visible on the
//! air. Suitable for in-process testing and trusted LANs, not for
//! hostile or slow media — those arrive with phase 2.

use kitsune2_api::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

mod chunking;
mod frame;
pub mod medium;
pub mod mediums;

use frame::{FrameTag, NodeId};
use medium::DynBroadcastMedium;

pub use medium::BroadcastMedium;
pub use mediums::mem::{MemAir, MemAirConfig, mem_medium};
pub use mediums::udp_multicast::{UdpMulticastConfig, UdpMulticastMedium};

/// BroadcastTransport configuration types.
pub mod config {
    /// Configuration for the
    /// [`BroadcastTransportFactory`](super::BroadcastTransportFactory).
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    #[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
    #[serde(rename_all = "camelCase")]
    pub struct BroadcastTransportConfig {
        /// Which medium backend to use: `"mem"` (in-process, testing)
        /// or `"udpMulticast"`.
        ///
        /// Default: `"udpMulticast"`.
        pub medium: String,

        /// Close a virtual connection after this many milliseconds
        /// without hearing from the peer.
        ///
        /// Default: 30000.
        pub idle_timeout_ms: u32,

        /// Settings for the udp multicast medium; unused for `"mem"`.
        pub udp_multicast: super::UdpMulticastConfig,
    }

    impl Default for BroadcastTransportConfig {
        fn default() -> Self {
            Self {
                medium: "udpMulticast".into(),
                idle_timeout_ms: 30_000,
                udp_multicast: Default::default(),
            }
        }
    }

    /// Module-level config wrapper.
    #[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
    #[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
    #[serde(rename_all = "camelCase")]
    pub struct BroadcastTransportModConfig {
        /// The actual config for the transport.
        pub broadcast_transport: BroadcastTransportConfig,
    }
}

pub use config::*;

/// Kitsune2 transport factory backed by a broadcast medium.
#[derive(Debug)]
pub struct BroadcastTransportFactory {
    medium_override: Option<DynBroadcastMedium>,
}

impl BroadcastTransportFactory {
    /// Create a factory whose medium is selected by module config
    /// (`broadcastTransport.medium`).
    pub fn create() -> DynTransportFactory {
        Arc::new(Self {
            medium_override: None,
        })
    }

    /// Create a factory bound to the given medium instance, ignoring
    /// the `medium` config key. This is how tests inject an isolated
    /// [`MemAir`], and how embedders provide custom media.
    pub fn create_with_medium(
        medium: DynBroadcastMedium,
    ) -> DynTransportFactory {
        Arc::new(Self {
            medium_override: Some(medium),
        })
    }
}

impl TransportFactory for BroadcastTransportFactory {
    fn default_config(&self, config: &mut Config) -> K2Result<()> {
        config.set_module_config(&BroadcastTransportModConfig::default())
    }

    fn validate_config(&self, config: &Config) -> K2Result<()> {
        let config: BroadcastTransportModConfig = config.get_module_config()?;
        let config = config.broadcast_transport;
        if self.medium_override.is_none()
            && !matches!(config.medium.as_str(), "mem" | "udpMulticast")
        {
            return Err(K2Error::other(format!(
                "unknown broadcast medium: {} (expected \"mem\" or \
                 \"udpMulticast\")",
                config.medium
            )));
        }
        if config.idle_timeout_ms == 0 {
            return Err(K2Error::other(
                "broadcastTransport.idleTimeoutMs must be non-zero",
            ));
        }
        Ok(())
    }

    fn create(
        &self,
        builder: Arc<Builder>,
        handler: DynTxHandler,
    ) -> BoxFut<'static, K2Result<DynTransport>> {
        let medium_override = self.medium_override.clone();
        Box::pin(async move {
            let config: BroadcastTransportModConfig =
                builder.config.get_module_config()?;
            let config = config.broadcast_transport;

            let medium = match medium_override {
                Some(medium) => medium,
                None => match config.medium.as_str() {
                    "mem" => MemAir::global(),
                    "udpMulticast" => {
                        UdpMulticastMedium::create(&config.udp_multicast)
                            .await?
                    }
                    other => {
                        return Err(K2Error::other(format!(
                            "unknown broadcast medium: {other}"
                        )));
                    }
                },
            };

            let handler = TxImpHnd::new(handler);
            let imp =
                BroadcastTransport::create(config, medium, handler.clone())
                    .await;
            Ok(DefaultTransport::create(&handler, imp))
        })
    }
}

/// State for one open virtual connection.
#[derive(Debug)]
struct PeerEntry {
    url: Url,
    last_heard: Instant,
    opened_at_s: u64,
    send_message_count: u64,
    send_bytes: u64,
    recv_message_count: u64,
    recv_bytes: u64,
}

type Peers = Arc<Mutex<HashMap<NodeId, PeerEntry>>>;

/// Broadcast-medium transport implementation. See the crate docs.
struct BroadcastTransport {
    node_id: NodeId,
    this_url: Url,
    medium: DynBroadcastMedium,
    handler: Arc<TxImpHnd>,
    peers: Peers,
    reassembler: Mutex<chunking::Reassembler>,
    next_sequence_id: AtomicU32,
    idle_timeout: Duration,
    tasks: Mutex<tokio::task::JoinSet<()>>,
}

impl std::fmt::Debug for BroadcastTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BroadcastTransport")
            .field("node_id", &self.node_id)
            .field("url", &self.this_url)
            .field("medium", &self.medium)
            .finish()
    }
}

impl Drop for BroadcastTransport {
    fn drop(&mut self) {
        self.tasks.lock().unwrap().abort_all();
    }
}

impl BroadcastTransport {
    async fn create(
        config: BroadcastTransportConfig,
        medium: DynBroadcastMedium,
        handler: Arc<TxImpHnd>,
    ) -> DynTxImp {
        let node_id = NodeId::random();
        let this_url = node_url(&medium, node_id);
        handler.new_listening_address(this_url.clone(), None).await;

        let out = Arc::new(Self {
            node_id,
            this_url,
            medium,
            handler,
            peers: Arc::new(Mutex::new(HashMap::new())),
            reassembler: Mutex::new(chunking::Reassembler::default()),
            next_sequence_id: AtomicU32::new(0),
            idle_timeout: Duration::from_millis(config.idle_timeout_ms as u64),
            tasks: Mutex::new(tokio::task::JoinSet::new()),
        });

        // Listen task: everything heard on the air runs through
        // handle_frame.
        {
            let this = out.clone();
            let mut frames = this.medium.frames();
            out.tasks.lock().unwrap().spawn(async move {
                use futures::StreamExt;
                while let Some(frame) = frames.next().await {
                    this.handle_frame(&frame).await;
                }
                tracing::info!("broadcast medium frame stream ended");
            });
        }

        // Reaper task: idle virtual connections get closed, and
        // stalled chunk reassemblies evicted.
        {
            let this = out.clone();
            let mut reassembler_tick = tokio::time::interval(
                this.idle_timeout
                    .div_f32(4.0)
                    .max(Duration::from_millis(250)),
            );
            reassembler_tick.set_missed_tick_behavior(
                tokio::time::MissedTickBehavior::Delay,
            );
            out.tasks.lock().unwrap().spawn(async move {
                loop {
                    reassembler_tick.tick().await;
                    this.reap_idle();
                }
            });
        }

        out
    }

    /// Ensure a virtual connection to `peer` exists, running the
    /// connect + preflight dance if it does not.
    ///
    /// Returns the peer's url.
    async fn ensure_peer(&self, peer: NodeId) -> K2Result<Url> {
        {
            let mut peers = self.peers.lock().unwrap();
            if let Some(entry) = peers.get_mut(&peer) {
                entry.last_heard = Instant::now();
                return Ok(entry.url.clone());
            }
        }

        let url = node_url(&self.medium, peer);

        // peer_connect gives modules/spaces a veto and returns the
        // preflight payload we owe the peer.
        let preflight = self.handler.peer_connect(url.clone()).await?;

        // Insert before transmitting: the preflight response can race
        // back before transmit returns.
        {
            let mut peers = self.peers.lock().unwrap();
            peers.entry(peer).or_insert_with(|| PeerEntry {
                url: url.clone(),
                last_heard: Instant::now(),
                opened_at_s: std::time::SystemTime::UNIX_EPOCH
                    .elapsed()
                    .unwrap_or_default()
                    .as_secs(),
                send_message_count: 0,
                send_bytes: 0,
                recv_message_count: 0,
                recv_bytes: 0,
            });
        }

        if let Err(err) = self
            .transmit_payload(peer, FrameTag::Preflight, &preflight)
            .await
        {
            self.close_peer(
                &peer,
                Some(format!("preflight send failed: {err}")),
            );
            return Err(err);
        }

        Ok(url)
    }

    /// Process one frame heard on the air.
    async fn handle_frame(&self, raw: &[u8]) {
        let frame = match frame::decode_frame(raw) {
            Ok(frame) => frame,
            Err(err) => {
                // Not one of ours; broadcast media are noisy.
                tracing::trace!(?err, "ignoring undecodable frame");
                return;
            }
        };
        if frame.src == self.node_id {
            // Our own transmission looped back.
            return;
        }
        if frame.dst != self.node_id {
            // Phase 1 is strictly unicast emulation.
            return;
        }

        let url = match self.ensure_peer(frame.src).await {
            Ok(url) => url,
            Err(err) => {
                tracing::debug!(
                    ?err,
                    src = ?frame.src,
                    "rejecting virtual connection"
                );
                return;
            }
        };

        let payload = match frame.tag {
            FrameTag::Preflight | FrameTag::Data => Some(frame.payload.clone()),
            FrameTag::Chunk => {
                let mut peers = self.peers.lock().unwrap();
                match peers.get_mut(&frame.src) {
                    Some(_) => {}
                    None => return,
                }
                drop(peers);
                let mut reassembler = self.reassembler.lock().unwrap();
                match reassembler.accept(
                    frame.src,
                    &frame.payload,
                    Instant::now(),
                ) {
                    Ok(done) => done,
                    Err(err) => {
                        tracing::debug!(?err, "dropping malformed chunk");
                        None
                    }
                }
            }
        };

        let Some(payload) = payload else {
            return;
        };

        {
            let mut peers = self.peers.lock().unwrap();
            if let Some(entry) = peers.get_mut(&frame.src) {
                entry.recv_message_count += 1;
                entry.recv_bytes += payload.len() as u64;
            }
        }

        if let Err(err) = self.handler.recv_data(url, payload).await {
            tracing::debug!(
                ?err,
                src = ?frame.src,
                "handler rejected data, closing virtual connection"
            );
            self.close_peer(&frame.src, Some(format!("{err:?}")));
        }
    }

    /// Encode and transmit one logical payload, chunking as needed.
    async fn transmit_payload(
        &self,
        dst: NodeId,
        tag: FrameTag,
        payload: &[u8],
    ) -> K2Result<()> {
        let max_plain = self
            .medium
            .mtu()
            .checked_sub(frame::HEADER_LEN)
            .ok_or_else(|| {
                K2Error::other("broadcast medium mtu smaller than frame header")
            })?;

        if payload.len() <= max_plain {
            return self
                .medium
                .transmit(frame::encode_frame(self.node_id, dst, tag, payload))
                .await;
        }

        // Preflight payloads must fit one frame: they are the first
        // thing on a virtual connection and reassembly state for an
        // unknown peer is not worth the complexity at this phase.
        if tag != FrameTag::Data {
            return Err(K2Error::other(format!(
                "broadcast {tag:?} payload of {} bytes exceeds the \
                 single-frame limit of {max_plain}",
                payload.len()
            )));
        }

        let max_fragment = max_plain.saturating_sub(chunking::CHUNK_HEADER_LEN);
        let sequence_id = self.next_sequence_id.fetch_add(1, Ordering::Relaxed);
        for chunk in
            chunking::split_into_chunks(sequence_id, payload, max_fragment)?
        {
            self.medium
                .transmit(frame::encode_frame(
                    self.node_id,
                    dst,
                    FrameTag::Chunk,
                    &chunk,
                ))
                .await?;
        }
        Ok(())
    }

    /// Close a virtual connection and notify the handler.
    fn close_peer(&self, peer: &NodeId, reason: Option<String>) {
        let removed = self.peers.lock().unwrap().remove(peer);
        self.reassembler.lock().unwrap().forget(peer);
        if let Some(entry) = removed {
            self.handler.peer_disconnect(entry.url, reason);
        }
    }

    /// Close every virtual connection that has been idle too long, and
    /// evict stalled reassemblies.
    fn reap_idle(&self) {
        let now = Instant::now();
        let stale: Vec<NodeId> = self
            .peers
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, entry)| {
                now.duration_since(entry.last_heard) > self.idle_timeout
            })
            .map(|(id, _)| *id)
            .collect();
        for peer in stale {
            tracing::debug!(?peer, "closing idle virtual connection");
            self.close_peer(&peer, Some("idle timeout".into()));
        }
        self.reassembler
            .lock()
            .unwrap()
            .prune(now, self.idle_timeout);
    }
}

impl TxImp for BroadcastTransport {
    fn url(&self) -> Option<Url> {
        Some(self.this_url.clone())
    }

    fn disconnect(
        &self,
        peer: Url,
        payload: Option<(String, bytes::Bytes)>,
    ) -> BoxFut<'_, ()> {
        Box::pin(async move {
            let Some(node_id) = url_node_id(&peer) else {
                return;
            };
            let known = self.peers.lock().unwrap().contains_key(&node_id);
            if !known {
                return;
            }
            let reason = if let Some((reason, payload)) = payload {
                // A graceful disconnect is a K2Proto Disconnect message
                // traveling as ordinary data; the peer's handler closes
                // its side when it processes it.
                let _ = self
                    .transmit_payload(node_id, FrameTag::Data, &payload)
                    .await;
                Some(reason)
            } else {
                None
            };
            self.close_peer(&node_id, reason);
        })
    }

    fn send(&self, peer: Url, data: bytes::Bytes) -> BoxFut<'_, K2Result<()>> {
        Box::pin(async move {
            let node_id = url_node_id(&peer).ok_or_else(|| {
                K2Error::other(format!("invalid broadcast peer url: {peer}"))
            })?;
            self.ensure_peer(node_id).await?;
            let result =
                self.transmit_payload(node_id, FrameTag::Data, &data).await;
            match &result {
                Ok(()) => {
                    let mut peers = self.peers.lock().unwrap();
                    if let Some(entry) = peers.get_mut(&node_id) {
                        entry.send_message_count += 1;
                        entry.send_bytes += data.len() as u64;
                    }
                }
                Err(err) => {
                    self.close_peer(
                        &node_id,
                        Some(format!("send failed: {err}")),
                    );
                }
            }
            result
        })
    }

    fn get_connected_peers(&self) -> BoxFut<'_, K2Result<Vec<Url>>> {
        Box::pin(async move {
            Ok(self
                .peers
                .lock()
                .unwrap()
                .values()
                .map(|entry| entry.url.clone())
                .collect())
        })
    }

    fn dump_network_stats(&self) -> BoxFut<'_, K2Result<TransportStats>> {
        Box::pin(async move {
            let connections = self
                .peers
                .lock()
                .unwrap()
                .values()
                .map(|entry| TransportConnectionStats {
                    // The peer id (node-id hex), matching the iroh transport's
                    // pub_key semantics so consumers (e.g. holochain's per-app
                    // stats filter) can match entries against peer-store URLs.
                    pub_key: entry
                        .url
                        .peer_id()
                        .unwrap_or_default()
                        .to_string(),
                    send_message_count: entry.send_message_count,
                    send_bytes: entry.send_bytes,
                    recv_message_count: entry.recv_message_count,
                    recv_bytes: entry.recv_bytes,
                    opened_at_s: entry.opened_at_s,
                    is_direct: true,
                })
                .collect();
            Ok(TransportStats {
                backend: format!("broadcast-{}", self.medium.kind()),
                peer_urls: vec![self.this_url.clone()],
                connections,
            })
        })
    }
}

/// Build the url for a node id on a medium.
fn node_url(medium: &DynBroadcastMedium, node_id: NodeId) -> Url {
    Url::from_str(format!(
        "ws://{}.bcast:1/{}",
        medium.kind(),
        node_id.to_hex()
    ))
    .expect("statically valid url shape")
}

/// Extract the node id from a broadcast peer url.
fn url_node_id(url: &Url) -> Option<NodeId> {
    NodeId::from_hex(url.peer_id()?).ok()
}

#[cfg(test)]
mod test;
