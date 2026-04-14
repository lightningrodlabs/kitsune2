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
use crate::frame::{ReticulumFrame, decode_frame, encode_frame};
use crate::peer_state::PeerState;
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
pub(crate) type LinkRegistry = Arc<RwLock<HashMap<LinkId, (Url, SpaceId)>>>;

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
    local_main_identity: AddressHash,
) -> AbortHandle {
    tokio::spawn(async move {
        while let Some(link) = rx.recv().await {
            route_new_link(
                &link,
                &state,
                &handler,
                &endpoint,
                local_main_identity,
            )
            .await;
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
    local_main_identity: AddressHash,
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
    let first_link = peer_state.insert_link(space_id.clone(), link.clone());

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
            local_main_identity,
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
/// `ReticulumFrame::Preflight` (carrying our main identity so the
/// receiver can re-key its `PeerState` under the URL kitsune2
/// advertises in `AgentInfoSigned.url`), sends it over the link, and
/// flips `local_sent`. If flipping `local_sent` now makes the peer
/// ready (remote preflight already arrived), drains any buffered
/// Data frames up to the handler.
pub(crate) async fn start_preflight(
    peer_url: &Url,
    local_main_identity: AddressHash,
    link: &DynLink,
    peer_state: &Arc<PeerState>,
    handler: &Arc<TxImpHnd>,
    endpoint: &DynEndpoint,
    max_frame_bytes: usize,
) -> kitsune2_api::K2Result<()> {
    // Guard against concurrent callers: only the first caller to
    // flip `local_sent` from false→true actually sends the preflight.
    // Note: we do NOT check `remote_received` here — the remote may
    // have already sent its preflight before we reached this point
    // (races against `wait_for_link_active`), but we still need to
    // send ours so the remote can mark its own state Ready.
    let newly_ready = {
        let mut pf = peer_state.preflight_state.lock().expect("poisoned");
        if pf.local_sent {
            trace!(?peer_url, "preflight already sent");
            return Ok(());
        }
        let was_ready = pf.is_ready();
        pf.local_sent = true;
        !was_ready && pf.is_ready()
    };

    let preflight_bytes = handler.peer_connect(peer_url.clone()).await?;
    let frame = ReticulumFrame::Preflight {
        sender_main_identity: local_main_identity,
        payload: preflight_bytes,
    };
    let encoded = encode_frame(&frame, max_frame_bytes)?;
    send_over_link(link, &encoded, endpoint, max_frame_bytes).await?;

    debug!(?peer_url, "preflight sent");

    if newly_ready {
        drain_pending_data(peer_url, peer_state, handler).await;
    }
    Ok(())
}

/// Drain any Data frames buffered while preflight was incomplete and
/// dispatch them to the handler in FIFO order. Called from whichever
/// task flipped the final bit of `PreflightState`.
///
/// Dispatched under `peer_url`, which at the Preflight-arm call site
/// is the *main* URL after re-keying; at the `start_preflight` call
/// site, re-keying has not happened yet (the remote preflight is what
/// triggers it), so `peer_url` is still the ephemeral URL — but in
/// that path nothing is queued yet anyway (the queue is only written
/// in the data-router's Data arm, which runs on a `peer_state` that
/// has already been re-keyed if a Preflight was seen).
async fn drain_pending_data(
    peer_url: &Url,
    peer_state: &Arc<PeerState>,
    handler: &Arc<TxImpHnd>,
) {
    let queued = peer_state.drain_pending();
    if queued.is_empty() {
        return;
    }
    debug!(
        ?peer_url,
        count = queued.len(),
        "draining buffered data frames now that preflight is ready"
    );
    for bytes in queued {
        if let Err(e) = handler.recv_data(peer_url.clone(), bytes).await {
            warn!(
                ?e,
                ?peer_url,
                "recv_data failed while draining buffered frame"
            );
        }
    }
}

/// Send one encoded frame over a link, picking the right path for
/// the payload size.
///
/// rns has two delivery primitives on a Link:
/// - `data_packet` — single rns Packet, ≤ `PACKET_MDU` (~464 bytes).
///   No advertise/request/fragments/proof round-trip; the payload
///   ships in one packet and arrives at the receiver as a `Link::Data`
///   event mirrored into `received_data_events`.
/// - `send_resource` — for larger payloads. Multi-packet transfer
///   with its own handshake.
///
/// Using Resource for *every* size, which we did initially, was
/// unreliable for tiny preflight frames: rns's resource manager
/// appears to silently drop the very first Resource transfer on a
/// freshly-Active link. Keeping small frames on `data_packet` avoids
/// that path entirely.
pub(crate) async fn send_over_link(
    link: &DynLink,
    encoded: &[u8],
    endpoint: &DynEndpoint,
    _max_frame_bytes: usize,
) -> kitsune2_api::K2Result<()> {
    if encoded.len() <= endpoint.packet_mdu() {
        link.send_small(encoded).await
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
            if let Err(e) = route_data(&link_id, data, &state, &handler).await {
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
    // Resource transfers can complete before the links router has
    // observed the matching `LinkEvent::Activated` and registered the
    // link in `RouterState`. Both events come from the same rns
    // Transport but flow through separate broadcast channels with
    // independent receivers, so a tiny preflight Resource (a few
    // bytes — single fragment + proof) can land in the data router
    // before the links router has caught up. Retry a few times
    // rather than dropping.
    let mut peer_entry = None;
    for attempt in 0..20 {
        {
            let reg = state.link_registry.read().expect("poisoned");
            if let Some(entry) = reg.get(link_id) {
                peer_entry = Some(entry.clone());
                break;
            }
        }
        if attempt == 0 {
            trace!(
                ?link_id,
                "data router: link not yet registered, awaiting links router"
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let (peer_url, _space_id) = match peer_entry {
        Some(entry) => entry,
        None => {
            let reg = state.link_registry.read().expect("poisoned");
            let known: Vec<LinkId> = reg.keys().copied().collect();
            warn!(
                ?link_id,
                ?known,
                "data router: link still unknown after retries -- dropping"
            );
            return Ok(());
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
        ReticulumFrame::Preflight {
            sender_main_identity,
            payload,
        } => {
            // rns exposes the remote's *ephemeral* per-link identity,
            // not their main identity. When the links router inserts
            // the link into the registry + PeerState, it has to key
            // by the ephemeral URL (it's all rns gives us). That URL
            // doesn't match the `AgentInfoSigned.url` kitsune2 knows
            // for the peer — so any message keyed by it would be
            // dropped by the block check.
            //
            // The preflight frame carries the sender's main identity
            // hash for exactly this reason: re-key PeerState +
            // link_registry under the main URL before dispatching the
            // preflight up to TxImpHnd.
            let main_url = match crate::url::identity_hash_to_url(
                &sender_main_identity,
            ) {
                Ok(u) => u,
                Err(e) => {
                    warn!(
                        ?e,
                        ?peer_url,
                        "data router: invalid main-identity hash in preflight"
                    );
                    return Ok(());
                }
            };
            let main_peer_url = if main_url == peer_url {
                peer_url
            } else {
                debug!(
                    ephemeral = %peer_url,
                    main = %main_url,
                    "data router: re-keying PeerState to main identity URL"
                );
                // Move the PeerState entry under the main URL.
                {
                    let mut states =
                        state.peer_states.write().expect("poisoned");
                    if let Some(existing) = states.remove(&peer_url) {
                        states.insert(main_url.clone(), existing);
                    }
                }
                // Update every link_registry entry that referenced the
                // ephemeral URL so future data frames arrive under main.
                {
                    let mut reg =
                        state.link_registry.write().expect("poisoned");
                    for entry in reg.values_mut() {
                        if entry.0 == peer_url {
                            entry.0 = main_url.clone();
                        }
                    }
                }
                main_url
            };

            // Re-fetch the PeerState from its (possibly new) key.
            let peer_state_after = state
                .peer_states
                .read()
                .expect("poisoned")
                .get(&main_peer_url)
                .cloned();
            let ps = match peer_state_after {
                Some(ps) => ps,
                None => peer_state,
            };

            handler.recv_data(main_peer_url.clone(), payload).await?;
            let newly_ready = {
                let mut pf = ps.preflight_state.lock().expect("poisoned");
                let was_ready = pf.is_ready();
                pf.remote_received = true;
                !was_ready && pf.is_ready()
            };
            debug!(?main_peer_url, "preflight received");
            if newly_ready {
                drain_pending_data(&main_peer_url, &ps, handler).await;
            }
        }
        ReticulumFrame::Data(bytes) => {
            // Gate on preflight readiness. The two router tasks run on
            // independent tokio schedulers, so a Data frame genuinely
            // can arrive before this side's links router has had a
            // chance to flip `local_sent` in `start_preflight`. Buffer
            // rather than drop — the drain runs when whichever task
            // finally completes the handshake.
            let ready = peer_state
                .preflight_state
                .lock()
                .expect("poisoned")
                .is_ready();
            if !ready {
                if peer_state.push_pending(bytes) {
                    debug!(
                        ?peer_url,
                        "data frame before preflight ready -- buffering"
                    );
                } else {
                    warn!(
                        ?peer_url,
                        cap = crate::peer_state::MAX_PENDING_DATA_FRAMES,
                        "pending-data cap hit -- dropping frame"
                    );
                }
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
    let entry = state
        .link_registry
        .write()
        .expect("poisoned")
        .remove(link_id);
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

/// Block until the peer's preflight state is `Ready` (or timeout).
///
/// The data router on the remote side drops `Data` frames that arrive
/// before its preflight state is Ready. rns sends our preflight and
/// data as two independent Resource transfers with no ordering
/// guarantee, so a fast data frame can lap the preflight on the wire.
/// Waiting locally for our own state to flip — which happens when we
/// receive the remote's preflight in response — is a serviceable
/// proxy for "the link's preflight handshake is done in both
/// directions."
pub(crate) async fn wait_for_preflight_ready(
    peer_state: &Arc<PeerState>,
    timeout: std::time::Duration,
) -> kitsune2_api::K2Result<()> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if peer_state
            .preflight_state
            .lock()
            .expect("poisoned")
            .is_ready()
        {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(kitsune2_api::K2Error::other(format!(
                "timed out waiting for preflight Ready (timeout {timeout:?})"
            )));
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// Block until the link reaches `Active` (or `Closed` / timeout).
///
/// rns's `Transport::link()` returns immediately with a Link in
/// `Pending` state; the proof round-trip happens in the background.
/// Sending resources on a non-Active link races the remote's link
/// mirror — locally the send reports success while the remote drops
/// fragments because reassembly requires the link to be live on its
/// side first. This helper polls `Link::status()` so the caller can
/// hold off until the handshake settles.
pub(crate) async fn wait_for_link_active(
    link: &DynLink,
    timeout: std::time::Duration,
) -> kitsune2_api::K2Result<()> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match link.status() {
            LinkStatus::Active => return Ok(()),
            LinkStatus::Closed => {
                return Err(kitsune2_api::K2Error::other(
                    "link closed during wait_for_link_active",
                ));
            }
            _ => {}
        }
        if std::time::Instant::now() >= deadline {
            return Err(kitsune2_api::K2Error::other(format!(
                "timed out waiting for link to become Active (timeout {timeout:?})"
            )));
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// Spawn the link-close router. Consumes `LinkId`s from
/// `Endpoint::recv_link_closures()` and hands them to [`remove_link`],
/// which decrements the per-peer refcount and fires
/// `TxImpHnd::peer_disconnect` on the last close.
pub(crate) fn spawn_close_router(
    mut rx: tokio::sync::mpsc::Receiver<LinkId>,
    state: RouterState,
    handler: Arc<TxImpHnd>,
) -> AbortHandle {
    tokio::spawn(async move {
        while let Some(link_id) = rx.recv().await {
            remove_link(&link_id, None, &state, &handler).await;
        }
        debug!("close router: channel closed");
    })
    .abort_handle()
}
