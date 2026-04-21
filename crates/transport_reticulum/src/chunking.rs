//! Transport-level fragmentation + reassembly for Reticulum Link payloads.
//!
//! This layer sits above the backend `Endpoint` / `Link` trait and
//! below the kitsune2 `TxImp::send` call site, transparently splitting
//! any outbound Data payload that doesn't fit in the backend's
//! plaintext MDU into `TAG_CHUNKED` fragments and reassembling them
//! on the receive side.
//!
//! The sender algorithm is stateless per call apart from a per-link
//! monotonically increasing `sequence_id` (u32, see [`LinkChunkState`]).
//! The receiver keeps one in-flight reassembly slot per link (see
//! [`LinkRecvState`]) — a second sequence arriving while the first
//! is still incomplete evicts it with a `warn!`. See §5 / §6 of
//! `PLAN-beechat-chunking.md` for the full design rationale.
//!
//! Every function in this module is **pure** (no async, no I/O, no
//! Reticulum types) so the state-machine invariants can be covered
//! by fast, deterministic unit tests.

use crate::destination::LinkId;
use crate::frame::{CHUNKED_HEADER_SIZE, TAG_DATA, encode_chunked_fragment};
use bytes::Bytes;
use kitsune2_api::{K2Error, K2Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tracing::{trace, warn};

/// Minimum plaintext MDU the chunker will accept. Anything smaller
/// would mean the per-fragment payload cap is < 55 bytes, at which
/// point the header dominates and we're almost certainly looking at
/// a misconfigured backend. A hard floor makes that bug observable
/// at fragment time instead of producing a silent flood of 1-byte
/// fragments.
const MIN_PLAINTEXT_MDU: usize = 64;

/// Per-link send-side state: a monotonic `sequence_id` counter.
///
/// Allocated on first fragmentation against a link and dropped when
/// the link closes. Wraps around at `u32::MAX` — at one large send
/// per second that's ~136 years, so the wrap is purely theoretical.
#[derive(Debug, Default)]
pub(crate) struct LinkChunkState {
    next_sequence_id: AtomicU32,
}

impl LinkChunkState {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Allocate a fresh `sequence_id`. `fetch_add` wraps on overflow,
    /// which is fine — the receiver evicts stale sequences by timeout
    /// long before any meaningful collision could occur.
    fn next_sequence_id(&self) -> u32 {
        self.next_sequence_id.fetch_add(1, Ordering::Relaxed)
    }
}

/// Send-side chunk-state map, one `LinkChunkState` per live link.
pub(crate) type SendChunkStates =
    Arc<RwLock<HashMap<LinkId, Arc<LinkChunkState>>>>;

/// Get-or-create the per-link send state.
pub(crate) fn get_or_init_send_state(
    states: &SendChunkStates,
    link_id: LinkId,
) -> Arc<LinkChunkState> {
    {
        let map = states.read().expect("poisoned");
        if let Some(s) = map.get(&link_id) {
            return s.clone();
        }
    }
    let mut map = states.write().expect("poisoned");
    map.entry(link_id)
        .or_insert_with(|| Arc::new(LinkChunkState::new()))
        .clone()
}

/// One encoded packet's worth of bytes — either a single `TAG_DATA`
/// frame (small-frame fast path) or one of N `TAG_CHUNKED` fragments.
pub(crate) type EncodedPacket = Bytes;

/// Fragment a Data payload for one outbound link.
///
/// - If `1 + payload.len() <= plaintext_mdu` (single-packet fast path),
///   returns one `TAG_DATA` frame unchanged.
/// - Otherwise, returns N `TAG_CHUNKED` fragments, each carrying
///   `CHUNKED_HEADER_SIZE` (9) bytes of header plus up to
///   `plaintext_mdu - 9` bytes of payload.
///
/// `max_frame_bytes` caps the original payload; a fragment_count
/// exceeding `u16::MAX` would also error — this is effectively
/// unreachable because `max_frame_bytes` (default 1 MiB) is much
/// smaller than `u16::MAX × (plaintext_mdu - 9)`.
pub(crate) fn fragment_data(
    payload: &[u8],
    plaintext_mdu: usize,
    max_frame_bytes: usize,
    state: &LinkChunkState,
) -> K2Result<Vec<EncodedPacket>> {
    if plaintext_mdu < MIN_PLAINTEXT_MDU {
        return Err(K2Error::other(format!(
            "chunking: plaintext_mdu {plaintext_mdu} below minimum {MIN_PLAINTEXT_MDU} \
             (backend misconfiguration — packet_mdu() must return the plaintext ceiling)"
        )));
    }
    if payload.len() > max_frame_bytes {
        return Err(K2Error::other(format!(
            "chunking: payload {} bytes exceeds max_frame_bytes {}",
            payload.len(),
            max_frame_bytes,
        )));
    }

    // Fast path: fits in one packet as a plain `TAG_DATA` frame.
    if payload.len() < plaintext_mdu {
        let mut buf = Vec::with_capacity(1 + payload.len());
        buf.push(TAG_DATA);
        buf.extend_from_slice(payload);
        return Ok(vec![Bytes::from(buf)]);
    }

    let body_cap = plaintext_mdu - CHUNKED_HEADER_SIZE;
    // `div_ceil` on usize is stable as of Rust 1.73; the workspace is
    // on edition 2024, so this is fine.
    let fragment_count = payload.len().div_ceil(body_cap);
    if fragment_count > u16::MAX as usize {
        return Err(K2Error::other(format!(
            "chunking: {fragment_count} fragments exceeds u16::MAX (raise plaintext_mdu or lower max_frame_bytes)"
        )));
    }
    let fragment_count = fragment_count as u16;

    let sequence_id = state.next_sequence_id();
    let mut out = Vec::with_capacity(fragment_count as usize);
    for i in 0..fragment_count as usize {
        let start = i * body_cap;
        let end = ((i + 1) * body_cap).min(payload.len());
        out.push(encode_chunked_fragment(
            sequence_id,
            i as u16,
            fragment_count,
            &payload[start..end],
        ));
    }
    Ok(out)
}

/// Per-link receive-side state. Holds at most one in-flight sequence;
/// a new sequence arriving while one is still buffered evicts it.
#[derive(Debug, Default)]
pub(crate) struct LinkRecvState {
    pub(crate) inflight: Option<InflightSequence>,
}

/// One actively-reassembling sequence.
#[derive(Debug)]
pub(crate) struct InflightSequence {
    pub(crate) sequence_id: u32,
    pub(crate) fragment_count: u16,
    /// Sparse vector: `None` = not yet received, `Some(Bytes)` = received.
    pub(crate) fragments: Vec<Option<Bytes>>,
    pub(crate) received_count: u16,
    pub(crate) total_buffered_bytes: usize,
    pub(crate) started_at: Instant,
}

impl InflightSequence {
    fn new(sequence_id: u32, fragment_count: u16, now: Instant) -> Self {
        let mut fragments = Vec::with_capacity(fragment_count as usize);
        fragments.resize(fragment_count as usize, None);
        Self {
            sequence_id,
            fragment_count,
            fragments,
            received_count: 0,
            total_buffered_bytes: 0,
            started_at: now,
        }
    }

    fn is_complete(&self) -> bool {
        self.received_count == self.fragment_count
    }

    /// Assemble the reassembled payload by concatenating fragments in
    /// strict index order. Caller must have confirmed `is_complete()`.
    fn finalize(self) -> Bytes {
        let mut buf = Vec::with_capacity(self.total_buffered_bytes);
        for slot in self.fragments.into_iter() {
            let frag = slot.expect("is_complete implies every slot is Some");
            buf.extend_from_slice(&frag);
        }
        Bytes::from(buf)
    }
}

/// Receive-side chunk-state map, one `LinkRecvState` per live link.
pub(crate) type RecvChunkStates = Arc<RwLock<HashMap<LinkId, LinkRecvState>>>;

/// Outcome of handing one fragment to the chunker.
#[derive(Debug)]
pub(crate) enum IngestResult {
    /// Fragment accepted; sequence is still incomplete.
    Buffered,
    /// Fragment completed the sequence; here's the reassembled payload.
    Completed(Bytes),
    /// Fragment was rejected — logs have already been emitted at
    /// `warn!` or `trace!` level as appropriate. The caller should
    /// simply drop the frame.
    Rejected,
}

/// One `TAG_CHUNKED` fragment as passed to [`ingest_fragment`].
/// Collecting the four per-fragment fields into a struct keeps the
/// ingest signature readable and mirrors the `ReticulumFrame::Chunked`
/// variant that produces them.
#[derive(Debug)]
pub(crate) struct Fragment {
    pub(crate) sequence_id: u32,
    pub(crate) fragment_index: u16,
    pub(crate) fragment_count: u16,
    pub(crate) payload: Bytes,
}

/// Accept one `TAG_CHUNKED` fragment into the per-link reassembly
/// buffer. Returns whether the sequence is now complete, and if so,
/// the reassembled bytes.
///
/// Invariants enforced:
/// - A new `sequence_id` on a link with a different incomplete
///   in-flight sequence evicts the old one (with a `warn!`).
/// - A fragment whose `fragment_count` disagrees with the in-flight
///   sequence's `fragment_count` is rejected (`warn!`).
/// - Duplicate fragments (same index) are silently dropped at
///   `trace!` level.
/// - `max_frame_bytes` is enforced cumulatively on the receive side
///   as well: if buffered bytes would exceed it, the whole sequence
///   is dropped.
pub(crate) fn ingest_fragment(
    states: &RecvChunkStates,
    link_id: &LinkId,
    fragment: Fragment,
    max_frame_bytes: usize,
    now: Instant,
) -> IngestResult {
    let Fragment {
        sequence_id,
        fragment_index,
        fragment_count,
        payload,
    } = fragment;
    // `decode_frame` already rejects `fragment_count < 2` and
    // `fragment_index >= fragment_count`, but re-assert here because
    // `ingest_fragment` is also a callable unit-test entry point.
    if fragment_count < 2 {
        warn!(
            ?link_id,
            sequence_id,
            fragment_count,
            "chunking: rejected — fragment_count < 2"
        );
        return IngestResult::Rejected;
    }
    if fragment_index >= fragment_count {
        warn!(
            ?link_id,
            sequence_id,
            fragment_index,
            fragment_count,
            "chunking: rejected — fragment_index out of range"
        );
        return IngestResult::Rejected;
    }

    let mut map = states.write().expect("poisoned");
    let link_state = map.entry(*link_id).or_default();

    // Decide whether to start a new in-flight sequence, or continue
    // the existing one, or reject this fragment outright.
    let should_reset = match link_state.inflight.as_ref() {
        None => true,
        Some(inf) => {
            if inf.sequence_id == sequence_id {
                if inf.fragment_count != fragment_count {
                    warn!(
                        ?link_id,
                        sequence_id,
                        inflight_count = inf.fragment_count,
                        got_count = fragment_count,
                        "chunking: fragment_count mismatch — rejecting fragment"
                    );
                    return IngestResult::Rejected;
                }
                false
            } else {
                // New sequence on a link that already has an incomplete
                // one — evict the old one.
                warn!(
                    ?link_id,
                    dropped_seq = inf.sequence_id,
                    received = inf.received_count,
                    expected = inf.fragment_count,
                    new_seq = sequence_id,
                    "chunking: dropping incomplete sequence for newer one"
                );
                true
            }
        }
    };

    if should_reset {
        link_state.inflight =
            Some(InflightSequence::new(sequence_id, fragment_count, now));
    }

    let inflight = link_state.inflight.as_mut().expect("just populated above");

    if inflight.fragments[fragment_index as usize].is_some() {
        // Duplicate — silently drop.
        trace!(
            ?link_id,
            sequence_id, fragment_index, "chunking: duplicate fragment"
        );
        return IngestResult::Buffered;
    }

    let projected_total = inflight.total_buffered_bytes + payload.len();
    if projected_total > max_frame_bytes {
        warn!(
            ?link_id,
            sequence_id,
            projected_total,
            max_frame_bytes,
            "chunking: reassembled payload would exceed max_frame_bytes — dropping sequence"
        );
        link_state.inflight = None;
        return IngestResult::Rejected;
    }

    inflight.total_buffered_bytes = projected_total;
    inflight.received_count += 1;
    inflight.fragments[fragment_index as usize] = Some(payload);

    if inflight.is_complete() {
        let inflight = link_state.inflight.take().expect("checked above");
        IngestResult::Completed(inflight.finalize())
    } else {
        IngestResult::Buffered
    }
}

/// Evict every in-flight sequence older than `timeout`. Called on a
/// periodic tick from the reassembly sweeper task.
///
/// Returns the number of evicted sequences (useful for tests /
/// metrics).
pub(crate) fn sweep_expired(
    states: &RecvChunkStates,
    now: Instant,
    timeout: std::time::Duration,
) -> usize {
    let mut map = states.write().expect("poisoned");
    let mut evicted = 0;
    for (link_id, link_state) in map.iter_mut() {
        if let Some(inf) = link_state.inflight.as_ref()
            && now.duration_since(inf.started_at) >= timeout
        {
            warn!(
                ?link_id,
                sequence_id = inf.sequence_id,
                received = inf.received_count,
                expected = inf.fragment_count,
                age_s = now.duration_since(inf.started_at).as_secs(),
                "chunking: reassembly timeout — dropping incomplete sequence"
            );
            link_state.inflight = None;
            evicted += 1;
        }
    }
    evicted
}

/// Drop every piece of chunk state (send + recv) associated with a
/// closing link. Called from `routers::remove_link`.
pub(crate) fn drop_link(
    send: &SendChunkStates,
    recv: &RecvChunkStates,
    link_id: &LinkId,
) {
    send.write().expect("poisoned").remove(link_id);
    recv.write().expect("poisoned").remove(link_id);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{CHUNKED_HEADER_SIZE, ReticulumFrame, decode_frame};

    fn link_id(seed: u8) -> LinkId {
        LinkId::new([seed; 16])
    }

    fn send_states() -> SendChunkStates {
        Arc::new(RwLock::new(HashMap::new()))
    }

    fn recv_states() -> RecvChunkStates {
        Arc::new(RwLock::new(HashMap::new()))
    }

    // -----------------------------------------------------------------
    // Send-side: `fragment_data`.
    // -----------------------------------------------------------------

    #[test]
    fn fragment_single_packet_returns_tag_data() {
        let state = LinkChunkState::new();
        let payload = b"tiny".as_slice();
        let packets = fragment_data(payload, 1984, 1 << 20, &state).unwrap();
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0][0], 0x01, "single-packet must use TAG_DATA");
        assert_eq!(&packets[0][1..], payload);
        // Fast path does not consume a sequence_id.
        assert_eq!(state.next_sequence_id.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn fragment_at_exact_mdu_boundary_single() {
        // 1 + payload.len() == plaintext_mdu is still the fast path.
        let mdu = 100;
        let payload = vec![0x5au8; mdu - 1];
        let state = LinkChunkState::new();
        let packets = fragment_data(&payload, mdu, 1 << 20, &state).unwrap();
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0][0], 0x01);
    }

    #[test]
    fn fragment_one_byte_over_mdu_splits_to_two() {
        // 1 + payload.len() > plaintext_mdu → chunked path.
        let mdu = 100;
        let payload = vec![0xc3u8; mdu]; // 1 + 100 > 100
        let state = LinkChunkState::new();
        let packets = fragment_data(&payload, mdu, 1 << 20, &state).unwrap();
        // body_cap = 100 - 9 = 91. 100 / 91 → 2 fragments.
        assert_eq!(packets.len(), 2);
        assert_eq!(packets[0][0], 0x02);
        assert_eq!(packets[1][0], 0x02);
    }

    #[test]
    fn fragment_count_math_matches_div_ceil() {
        // At plaintext_mdu = 1984 → body_cap = 1975. 50 KiB → ⌈51200/1975⌉ = 26 fragments.
        let payload = vec![0xaau8; 50 * 1024];
        let state = LinkChunkState::new();
        let packets = fragment_data(&payload, 1984, 1 << 20, &state).unwrap();
        assert_eq!(packets.len(), 26);

        // Each fragment has the same fragment_count, indexed 0..26.
        for (i, pkt) in packets.iter().enumerate() {
            match decode_frame(pkt).unwrap() {
                ReticulumFrame::Chunked {
                    fragment_index,
                    fragment_count,
                    ..
                } => {
                    assert_eq!(fragment_index as usize, i);
                    assert_eq!(fragment_count, 26);
                }
                _ => panic!("expected Chunked"),
            }
        }

        // All fragments share one sequence_id (the first allocation).
        let seqs: std::collections::HashSet<u32> = packets
            .iter()
            .map(|pkt| match decode_frame(pkt).unwrap() {
                ReticulumFrame::Chunked { sequence_id, .. } => sequence_id,
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(seqs.len(), 1);
        assert_eq!(seqs.into_iter().next().unwrap(), 0);
    }

    #[test]
    fn fragment_rejects_payload_over_max_frame_bytes() {
        let state = LinkChunkState::new();
        let payload = vec![0u8; 1000];
        let err = fragment_data(&payload, 1984, 500, &state).unwrap_err();
        assert!(err.to_string().contains("max_frame_bytes"));
    }

    #[test]
    fn fragment_rejects_mdu_below_minimum() {
        let state = LinkChunkState::new();
        let err = fragment_data(b"x", 16, 1 << 20, &state).unwrap_err();
        assert!(err.to_string().contains("plaintext_mdu"));
    }

    #[test]
    fn fragment_allocates_distinct_sequence_ids() {
        // Two consecutive large sends on the same link → different seqs.
        let state = LinkChunkState::new();
        let payload = vec![0u8; 5_000];
        let p1 = fragment_data(&payload, 1984, 1 << 20, &state).unwrap();
        let p2 = fragment_data(&payload, 1984, 1 << 20, &state).unwrap();
        let seq1 = match decode_frame(&p1[0]).unwrap() {
            ReticulumFrame::Chunked { sequence_id, .. } => sequence_id,
            _ => unreachable!(),
        };
        let seq2 = match decode_frame(&p2[0]).unwrap() {
            ReticulumFrame::Chunked { sequence_id, .. } => sequence_id,
            _ => unreachable!(),
        };
        assert_eq!(seq1, 0);
        assert_eq!(seq2, 1);
    }

    #[test]
    fn fragment_each_body_respects_mdu() {
        let mdu = 200;
        let body_cap = mdu - CHUNKED_HEADER_SIZE; // 191
        let state = LinkChunkState::new();
        let payload = vec![0u8; 1000];
        let packets = fragment_data(&payload, mdu, 1 << 20, &state).unwrap();
        // All except the last fragment are exactly body_cap-sized.
        for pkt in packets.iter().take(packets.len() - 1) {
            assert_eq!(pkt.len(), CHUNKED_HEADER_SIZE + body_cap);
        }
        let last_len = payload.len() - body_cap * (packets.len() - 1);
        assert_eq!(
            packets.last().unwrap().len(),
            CHUNKED_HEADER_SIZE + last_len
        );
    }

    #[test]
    fn get_or_init_send_state_is_stable_per_link() {
        let states = send_states();
        let s1 = get_or_init_send_state(&states, link_id(1));
        let s2 = get_or_init_send_state(&states, link_id(1));
        let s3 = get_or_init_send_state(&states, link_id(2));
        assert!(Arc::ptr_eq(&s1, &s2));
        assert!(!Arc::ptr_eq(&s1, &s3));
    }

    // -----------------------------------------------------------------
    // Receive-side: `ingest_fragment`.
    // -----------------------------------------------------------------

    /// Helper: fragment a payload then feed those fragments back into
    /// `ingest_fragment` in the supplied order. Returns the final
    /// `IngestResult` from the last fragment (which must be the
    /// completion), plus the intermediate results.
    fn roundtrip(
        payload: &[u8],
        mdu: usize,
        feed_order: &[usize],
    ) -> (Vec<IngestResult>, Bytes) {
        let send_state = LinkChunkState::new();
        let packets =
            fragment_data(payload, mdu, 1 << 20, &send_state).unwrap();
        // Must actually be chunked for this helper to be meaningful.
        assert!(packets.len() > 1);
        assert_eq!(feed_order.len(), packets.len());

        let recv = recv_states();
        let link = link_id(7);
        let mut results = Vec::new();
        let mut final_bytes: Option<Bytes> = None;
        for &i in feed_order {
            let frame = decode_frame(&packets[i]).unwrap();
            let (seq, idx, cnt, p) = match frame {
                ReticulumFrame::Chunked {
                    sequence_id,
                    fragment_index,
                    fragment_count,
                    payload,
                } => (sequence_id, fragment_index, fragment_count, payload),
                _ => unreachable!(),
            };
            let r = ingest_fragment(
                &recv,
                &link,
                Fragment {
                    sequence_id: seq,
                    fragment_index: idx,
                    fragment_count: cnt,
                    payload: p,
                },
                1 << 20,
                Instant::now(),
            );
            if let IngestResult::Completed(ref b) = r {
                final_bytes = Some(b.clone());
            }
            results.push(r);
        }
        (results, final_bytes.expect("expected Completed"))
    }

    #[test]
    fn ingest_in_order_round_trip() {
        let payload: Vec<u8> = (0u8..=255).cycle().take(10_000).collect();
        let order: Vec<usize> = (0..6).collect(); // 10000/(1984-9)=6
        let (_results, out) = roundtrip(&payload, 1984, &order);
        assert_eq!(out.as_ref(), payload.as_slice());
    }

    #[test]
    fn ingest_reverse_order_round_trip() {
        let payload: Vec<u8> = (0u8..=255).cycle().take(10_000).collect();
        let order: Vec<usize> = (0..6).rev().collect();
        let (_r, out) = roundtrip(&payload, 1984, &order);
        assert_eq!(out.as_ref(), payload.as_slice());
    }

    #[test]
    fn ingest_adversarial_order_round_trip() {
        let payload: Vec<u8> = (0u8..=255).cycle().take(10_000).collect();
        // Interleaved odd/even.
        let order = vec![1, 3, 5, 0, 2, 4];
        let (_r, out) = roundtrip(&payload, 1984, &order);
        assert_eq!(out.as_ref(), payload.as_slice());
    }

    #[test]
    fn ingest_duplicate_fragment_is_silently_ignored() {
        let payload = vec![0xaau8; 5_000];
        let send_state = LinkChunkState::new();
        let packets =
            fragment_data(&payload, 1984, 1 << 20, &send_state).unwrap();
        assert!(packets.len() > 1);

        let recv = recv_states();
        let link = link_id(2);
        // Feed fragment 0 twice, then the rest in order.
        for pkt in std::iter::once(&packets[0])
            .chain(std::iter::once(&packets[0]))
            .chain(packets.iter().skip(1))
        {
            let (seq, idx, cnt, p) = match decode_frame(pkt).unwrap() {
                ReticulumFrame::Chunked {
                    sequence_id,
                    fragment_index,
                    fragment_count,
                    payload,
                } => (sequence_id, fragment_index, fragment_count, payload),
                _ => unreachable!(),
            };
            let r = ingest_fragment(
                &recv,
                &link,
                Fragment {
                    sequence_id: seq,
                    fragment_index: idx,
                    fragment_count: cnt,
                    payload: p,
                },
                1 << 20,
                Instant::now(),
            );
            if let IngestResult::Completed(out) = r {
                assert_eq!(out.as_ref(), payload.as_slice());
                return;
            }
        }
        panic!("sequence never completed");
    }

    fn frag(
        sequence_id: u32,
        fragment_index: u16,
        fragment_count: u16,
        payload: Bytes,
    ) -> Fragment {
        Fragment {
            sequence_id,
            fragment_index,
            fragment_count,
            payload,
        }
    }

    #[test]
    fn ingest_fragment_count_mismatch_is_rejected() {
        let recv = recv_states();
        let link = link_id(3);
        // First fragment declares count=4.
        let r = ingest_fragment(
            &recv,
            &link,
            frag(42, 0, 4, Bytes::from_static(b"hello")),
            1 << 20,
            Instant::now(),
        );
        assert!(matches!(r, IngestResult::Buffered));
        // Second fragment, same sequence but declares count=7.
        let r = ingest_fragment(
            &recv,
            &link,
            frag(42, 1, 7, Bytes::from_static(b"world")),
            1 << 20,
            Instant::now(),
        );
        assert!(matches!(r, IngestResult::Rejected));
        // The in-flight sequence should still be there with its
        // original count (4) and the first fragment buffered.
        let map = recv.read().unwrap();
        let inf = map.get(&link).unwrap().inflight.as_ref().unwrap();
        assert_eq!(inf.fragment_count, 4);
        assert_eq!(inf.received_count, 1);
    }

    #[test]
    fn ingest_new_sequence_evicts_old_incomplete() {
        let recv = recv_states();
        let link = link_id(4);
        // Start sequence 1, buffer one fragment.
        let r = ingest_fragment(
            &recv,
            &link,
            frag(1, 0, 3, Bytes::from_static(b"aaa")),
            1 << 20,
            Instant::now(),
        );
        assert!(matches!(r, IngestResult::Buffered));
        // A new sequence arrives — should evict sequence 1.
        let r = ingest_fragment(
            &recv,
            &link,
            frag(2, 0, 2, Bytes::from_static(b"bb")),
            1 << 20,
            Instant::now(),
        );
        assert!(matches!(r, IngestResult::Buffered));
        let map = recv.read().unwrap();
        let inf = map.get(&link).unwrap().inflight.as_ref().unwrap();
        assert_eq!(inf.sequence_id, 2);
        assert_eq!(inf.fragment_count, 2);
        assert_eq!(inf.received_count, 1);
    }

    #[test]
    fn ingest_count_zero_or_one_rejected() {
        let recv = recv_states();
        let link = link_id(5);
        let r = ingest_fragment(
            &recv,
            &link,
            frag(1, 0, 0, Bytes::new()),
            1 << 20,
            Instant::now(),
        );
        assert!(matches!(r, IngestResult::Rejected));
        let r = ingest_fragment(
            &recv,
            &link,
            frag(1, 0, 1, Bytes::new()),
            1 << 20,
            Instant::now(),
        );
        assert!(matches!(r, IngestResult::Rejected));
    }

    #[test]
    fn ingest_index_out_of_range_rejected() {
        let recv = recv_states();
        let link = link_id(6);
        let r = ingest_fragment(
            &recv,
            &link,
            frag(1, 5, 5, Bytes::new()),
            1 << 20,
            Instant::now(),
        );
        assert!(matches!(r, IngestResult::Rejected));
    }

    #[test]
    fn ingest_exceeds_max_frame_bytes_drops_sequence() {
        let recv = recv_states();
        let link = link_id(8);
        // First fragment: 600 bytes. Cap: 1000. Fine.
        let r = ingest_fragment(
            &recv,
            &link,
            frag(1, 0, 2, Bytes::from(vec![0u8; 600])),
            1000,
            Instant::now(),
        );
        assert!(matches!(r, IngestResult::Buffered));
        // Second fragment: 500 bytes. 600+500 > 1000 → rejected.
        let r = ingest_fragment(
            &recv,
            &link,
            frag(1, 1, 2, Bytes::from(vec![0u8; 500])),
            1000,
            Instant::now(),
        );
        assert!(matches!(r, IngestResult::Rejected));
        // Sequence should be gone.
        assert!(recv.read().unwrap().get(&link).unwrap().inflight.is_none());
    }

    // -----------------------------------------------------------------
    // Sweeper.
    // -----------------------------------------------------------------

    #[test]
    fn sweep_evicts_only_stale_sequences() {
        let recv = recv_states();
        let link_a = link_id(0xaa);
        let link_b = link_id(0xbb);
        // Sequence on link_a, old.
        ingest_fragment(
            &recv,
            &link_a,
            frag(1, 0, 2, Bytes::from_static(b"x")),
            1 << 20,
            Instant::now() - std::time::Duration::from_secs(60),
        );
        // Sequence on link_b, recent.
        ingest_fragment(
            &recv,
            &link_b,
            frag(1, 0, 2, Bytes::from_static(b"y")),
            1 << 20,
            Instant::now(),
        );
        let evicted = sweep_expired(
            &recv,
            Instant::now(),
            std::time::Duration::from_secs(30),
        );
        assert_eq!(evicted, 1);
        assert!(
            recv.read()
                .unwrap()
                .get(&link_a)
                .unwrap()
                .inflight
                .is_none()
        );
        assert!(
            recv.read()
                .unwrap()
                .get(&link_b)
                .unwrap()
                .inflight
                .is_some()
        );
    }

    #[test]
    fn drop_link_clears_both_sides() {
        let send = send_states();
        let recv = recv_states();
        let link = link_id(9);
        let _ = get_or_init_send_state(&send, link);
        ingest_fragment(
            &recv,
            &link,
            frag(1, 0, 2, Bytes::from_static(b"z")),
            1 << 20,
            Instant::now(),
        );
        assert!(send.read().unwrap().contains_key(&link));
        assert!(recv.read().unwrap().contains_key(&link));
        drop_link(&send, &recv, &link);
        assert!(!send.read().unwrap().contains_key(&link));
        assert!(!recv.read().unwrap().contains_key(&link));
    }
}
