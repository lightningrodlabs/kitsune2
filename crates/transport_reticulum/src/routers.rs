//! Global router tasks that glue the per-link Reticulum event streams
//! to the kitsune2 `TxImpHnd` handler callbacks.
//!
//! Two tasks run for the lifetime of the transport:
//!
//! - [`spawn_links_router`] consumes the endpoint's inbound-link stream.
//!   For each new inbound link, it looks up which local per-space
//!   destination the peer linked to (via
//!   `Link::local_destination_hash`), maps that to a `SpaceId`, inserts
//!   the link into the right `PeerState` entry (creating one if
//!   necessary), and on first-link-to-peer kicks off the outbound
//!   preflight frame via [`start_preflight`]. Last-link-close triggers
//!   `TxImpHnd::peer_disconnect`.
//! - [`spawn_data_router`] consumes the endpoint's resource-data stream.
//!   Each incoming `(LinkId, Bytes)` is decoded as a `ReticulumFrame`
//!   and dispatched: `Preflight` frames are validated via
//!   `TxImpHnd::peer_connect` and flip the per-peer preflight state to
//!   `Ready`; `Data` frames are handed to `TxImpHnd::recv_data` — but
//!   only once preflight is ready.
//!
//! All of this is driven through the `Endpoint` trait, so unit tests
//! can exercise the full flow against the in-memory fake.

use crate::destination::{DynEndpoint, DynLink, LinkId, LinkStatus};
use crate::frame::{decode_frame, encode_frame, ReticulumFrame};
use crate::peer_state::{PeerState, PreflightState};
use crate::url::identity_hash_to_url;
use bytes::Bytes;
use kitsune2_api::{SpaceId, TxImpHnd, Url};
use rns_transport::hash::AddressHash;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::task::AbortHandle;
use tracing::{debug, trace, warn};

/// Maps an inbound `LinkId` → `(peer_url, space_id)` so the data
/// router can find the right `PeerState` without iterating every peer.
pub(crate) type LinkRegistry =
    Arc<RwLock<HashMap<LinkId, (Url, SpaceId)>>>;

/// Shared state accessed by both router tasks plus `TxImp::send`.
#[derive(Clone, Debug)]
pub(crate) struct RouterState {
    /// Map of `our-destination-hash` → `space_id`, populated on
    /// `register_space`.
    pub dest_hash_to_space: Arc<RwLock<HashMap<AddressHash, SpaceId>>>,
    /// Peer-state map keyed by peer URL.
    pub peer_states: Arc<RwLock<HashMap<Url, Arc<PeerState>>>>,
    /// Link ID → (peer, space) index.
    pub link_registry: LinkRegistry,
    /// Max frame bytes for encoded sends.
    pub max_frame_bytes: usize,
}

impl RouterState {
    pub(crate) fn new(max_frame_bytes: usize) -> Self {
        Self {
            dest_hash_to_space: Arc::new(RwLock::new(HashMap::new())),
            peer_states: Arc::new(RwLock::new(HashMap::new())),
            link_registry: Arc::new(RwLock::new(HashMap::new())),
            max_frame_bytes,
        }
    }

    pub(crate) fn register_dest(
        &self,
        dest_hash: AddressHash,
        space_id: SpaceId,
    ) {
        self.dest_hash_to_space
            .write()
            .expect("poisoned")
            .insert(dest_hash, space_id);
    }

    pub(crate) fn unregister_space(&self, space_id: &SpaceId) {
        self.dest_hash_to_space
            .write()
            .expect("poisoned")
            .retain(|_, v| v != space_id);
        // Also drop any links for that space.
        self.link_registry
            .write()
            .expect("poisoned")
            .retain(|_, (_, s)| s != space_id);
    }
}

/// Spawn the inbound-link router. Returns an abort handle the caller
/// must retain for the life of the transport.
pub(crate) fn spawn_links_router(
    mut rx: tokio::sync::mpsc::Receiver<DynLink>,
    state: RouterState,
    handler: Arc<TxImpHnd>,
    endpoint: DynEndpoint,
) -> AbortHandle {
    tokio::spawn(async move {
        while let Some(link) = rx.recv().await {
            route_new_link(&link, &state, &handler, &endpoint).await;
        }
        debug!("links router: channel closed");
    })
    .abort_handle()
}

async fn route_new_link(
    link: &DynLink,
    state: &RouterState,
    handler: &Arc<TxImpHnd>,
    endpoint: &DynEndpoint,
) {
    // Map the local destination hash back to a SpaceId.
    let local_dest = link.local_destination_hash();
    let space_id = {
        let map = state.dest_hash_to_space.read().expect("poisoned");
        match map.get(&local_dest).cloned() {
            Some(s) => s,
            None => {
                warn!(
                    ?local_dest,
                    "Inbound link for unknown local destination -- dropping"
                );
                return;
            }
        }
    };

    let peer_hash = link.peer_identity_hash();
    let peer_url = match identity_hash_to_url(&peer_hash) {
        Ok(u) => u,
        Err(e) => {
            warn!(?e, "Failed to derive peer URL from identity hash");
            return;
        }
    };

    // Insert into PeerState.
    let peer_state = {
        let mut states = state.peer_states.write().expect("poisoned");
        states
            .entry(peer_url.clone())
            .or_insert_with(PeerState::new)
            .clone()
    };
    let first_link =
        peer_state.insert_link(space_id.clone(), link.clone());

    // Index the link so the data router can find (peer, space) fast.
    state
        .link_registry
        .write()
        .expect("poisoned")
        .insert(link.id(), (peer_url.clone(), space_id.clone()));

    debug!(
        ?peer_url,
        ?space_id,
        link_id = ?link.id(),
        first_link,
        "Inbound link registered"
    );

    if first_link {
        // First link to this peer: kick off preflight.
        if let Err(e) = start_preflight(
            &peer_url,
            link,
            &peer_state,
            handler,
            endpoint,
            state.max_frame_bytes,
        )
        .await
        {
            warn!(?e, ?peer_url, "Failed to start preflight");
        }
    }
}

/// Kick off the outbound preflight exchange for a peer on the given link.
///
/// Fetches preflight bytes from `handler.peer_connect(url)`, encodes a
/// `ReticulumFrame::Preflight`, sends it over the link, and flips the
/// peer's preflight state to `Sent`.
pub(crate) async fn start_preflight(
    peer_url: &Url,
    link: &DynLink,
    peer_state: &Arc<PeerState>,
    handler: &Arc<TxImpHnd>,
    endpoint: &DynEndpoint,
    max_frame_bytes: usize,
) -> kitsune2_api::K2Result<()> {
    // Only move from `None` -> `Sent`. Concurrent callers would see
    // a Sent/Ready state and bail.
    {
        let mut pf = peer_state.preflight_state.lock().expect("poisoned");
        if *pf != PreflightState::None {
            let state = *pf;
            trace!(?peer_url, ?state, "preflight already in flight");
            return Ok(());
        }
        *pf = PreflightState::Sent;
    }

    let preflight_bytes = handler.peer_connect(peer_url.clone()).await?;
    let frame = ReticulumFrame::Preflight(preflight_bytes);
    let encoded = encode_frame(&frame, max_frame_bytes)?;
    send_over_link(link, &encoded, endpoint, max_frame_bytes).await?;

    debug!(?peer_url, "preflight sent");
    Ok(())
}

/// Send one encoded frame over a link, choosing `data_packet` + raw
/// send for small payloads and `send_resource` for larger ones.
///
/// The MDU boundary is exposed by `Endpoint::packet_mdu()`. kitsune2
/// gossip traffic is almost always above the ~464-byte Reticulum MDU,
/// so in practice the Resource path dominates.
pub(crate) async fn send_over_link(
    link: &DynLink,
    encoded: &[u8],
    endpoint: &DynEndpoint,
    _max_frame_bytes: usize,
) -> kitsune2_api::K2Result<()> {
    if encoded.len() <= endpoint.packet_mdu() {
        let packet = link.data_packet(encoded)?;
        endpoint.send_packet(&packet).await
    } else {
        let link_id = link.id();
        endpoint.send_resource(&link_id, encoded).await
    }
}

/// Spawn the resource-data router. Consumes `(LinkId, Bytes)` events,
/// decodes `ReticulumFrame` and dispatches to `TxImpHnd`. Requires an
/// already-subscribed receiver.
pub(crate) fn spawn_data_router(
    mut rx: tokio::sync::mpsc::Receiver<(LinkId, Bytes)>,
    state: RouterState,
    handler: Arc<TxImpHnd>,
) -> AbortHandle {
    tokio::spawn(async move {
        while let Some((link_id, data)) = rx.recv().await {
            if let Err(e) = route_data(&link_id, data, &state, &handler).await
            {
                warn!(?e, ?link_id, "data router: dispatch failed");
            }
        }
        debug!("data router: channel closed");
    })
    .abort_handle()
}

async fn route_data(
    link_id: &LinkId,
    data: Bytes,
    state: &RouterState,
    handler: &Arc<TxImpHnd>,
) -> kitsune2_api::K2Result<()> {
    let (peer_url, _space_id) = {
        let reg = state.link_registry.read().expect("poisoned");
        match reg.get(link_id) {
            Some(entry) => entry.clone(),
            None => {
                trace!(?link_id, "data for unknown link -- dropping");
                return Ok(());
            }
        }
    };

    let peer_state = {
        let states = state.peer_states.read().expect("poisoned");
        match states.get(&peer_url) {
            Some(s) => s.clone(),
            None => {
                trace!(?peer_url, "data for unknown peer -- dropping");
                return Ok(());
            }
        }
    };

    // The outer ReticulumFrame tag is our own transport-level preflight
    // state machine hint. The inner bytes are always an encoded K2Proto;
    // `TxImpHnd::recv_data` decodes it and routes Preflight / Notify /
    // Module / Disconnect internally.
    let frame = decode_frame(&data)?;
    match frame {
        ReticulumFrame::Preflight(bytes) => {
            // Feed the preflight through; kitsune2 will route it to
            // preflight_validate_incoming. On success, flip our state
            // to Ready.
            handler.recv_data(peer_url.clone(), bytes).await?;
            {
                let mut pf =
                    peer_state.preflight_state.lock().expect("poisoned");
                *pf = PreflightState::Ready;
            }
            debug!(?peer_url, "preflight received, peer ready");
        }
        ReticulumFrame::Data(bytes) => {
            // Gate on preflight readiness. Frames received before
            // preflight completes are dropped (the remote shouldn't
            // have sent them).
            let ready = matches!(
                *peer_state
                    .preflight_state
                    .lock()
                    .expect("poisoned"),
                PreflightState::Ready
            );
            if !ready {
                warn!(?peer_url, "data frame before preflight ready -- dropping");
                return Ok(());
            }
            handler.recv_data(peer_url, bytes).await?;
        }
    }
    Ok(())
}

/// Remove a link that has closed, decrementing the peer's refcount
/// and firing `peer_disconnect` on the last close.
///
/// Currently unused at runtime — link-close detection requires a
/// `LinkEvent::Closed` bridge from `rns_transport::Transport::in_link_events`
/// which is not yet implemented. The function exists so the data router
/// and tests can trigger the teardown path uniformly.
pub(crate) async fn remove_link(
    link_id: &LinkId,
    reason: Option<String>,
    state: &RouterState,
    handler: &Arc<TxImpHnd>,
) {
    let entry = state.link_registry.write().expect("poisoned").remove(link_id);
    let (peer_url, space_id) = match entry {
        Some(e) => e,
        None => return,
    };

    let (last_link, drop_peer) = {
        let states = state.peer_states.read().expect("poisoned");
        match states.get(&peer_url) {
            Some(ps) => (ps.remove_link(&space_id), ps.link_count() == 0),
            None => return,
        }
    };

    if last_link && drop_peer {
        state
            .peer_states
            .write()
            .expect("poisoned")
            .remove(&peer_url);
        handler.peer_disconnect(peer_url, reason);
    }
}

/// Best-effort read of a link's status for diagnostics.
#[allow(dead_code)]
pub(crate) fn link_is_active(link: &DynLink) -> bool {
    matches!(link.status(), LinkStatus::Active | LinkStatus::Handshake)
}
