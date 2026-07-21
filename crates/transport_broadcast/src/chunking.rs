//! Fragmentation and reassembly for payloads larger than the medium MTU.
//!
//! A logical `DATA` payload that does not fit in one frame is split
//! into `CHUNK` frames. Each chunk payload carries its own 8-byte
//! header:
//!
//! ```text
//! +--------------+------------+------------+----------+
//! | sequence_id  | frag_index | frag_count | fragment |
//! | 4 B (u32 BE) | 2 B (u16)  | 2 B (u16)  | var      |
//! +--------------+------------+------------+----------+
//! ```
//!
//! The sender allocates `sequence_id`s from one per-transport monotonic
//! counter; the receiver keys reassembly by `(sender NodeId,
//! sequence_id)` and keeps **one** in-flight slot per sender — a new
//! sequence arriving while another is incomplete evicts the old one
//! with a warning. Slots that stop making progress are evicted by
//! timeout. This mirrors the reticulum transport's chunking layer, with
//! link ids replaced by sender ids because broadcast media have no
//! links.
//!
//! Everything here is pure (no async, no I/O) so the state-machine
//! invariants are covered by fast deterministic unit tests.

use crate::frame::NodeId;
use bytes::Bytes;
use kitsune2_api::{K2Error, K2Result};
use std::collections::HashMap;
use std::time::Instant;

/// Size of the per-chunk header described in the module docs.
pub(crate) const CHUNK_HEADER_LEN: usize = 4 + 2 + 2;

/// Split `data` into chunk payloads (header included), each with at
/// most `max_fragment` bytes of fragment body.
///
/// Errors if `max_fragment` is zero or the data would need more than
/// `u16::MAX` fragments. Callers only invoke this for payloads that
/// exceed the plain-DATA limit, so the result always has >= 2 chunks.
pub(crate) fn split_into_chunks(
    sequence_id: u32,
    data: &[u8],
    max_fragment: usize,
) -> K2Result<Vec<Bytes>> {
    if max_fragment == 0 {
        return Err(K2Error::other(
            "broadcast medium MTU too small to carry chunk fragments",
        ));
    }
    let count = data.len().div_ceil(max_fragment);
    if count > u16::MAX as usize {
        return Err(K2Error::other(format!(
            "payload of {} bytes needs {count} fragments, exceeding the \
             chunking limit of {}",
            data.len(),
            u16::MAX
        )));
    }
    let count = count as u16;
    Ok(data
        .chunks(max_fragment)
        .enumerate()
        .map(|(index, fragment)| {
            let mut buf = Vec::with_capacity(CHUNK_HEADER_LEN + fragment.len());
            buf.extend_from_slice(&sequence_id.to_be_bytes());
            buf.extend_from_slice(&(index as u16).to_be_bytes());
            buf.extend_from_slice(&count.to_be_bytes());
            buf.extend_from_slice(fragment);
            Bytes::from(buf)
        })
        .collect())
}

/// One in-flight reassembly.
#[derive(Debug)]
struct Slot {
    sequence_id: u32,
    fragments: Vec<Option<Bytes>>,
    received: usize,
    last_progress: Instant,
}

/// Reassembles chunk payloads back into logical payloads.
///
/// One slot per sender; see module docs for the eviction rules.
#[derive(Debug, Default)]
pub(crate) struct Reassembler {
    slots: HashMap<NodeId, Slot>,
}

impl Reassembler {
    /// Accept one chunk payload from `src`.
    ///
    /// Returns `Ok(Some(payload))` when this chunk completes a logical
    /// payload, `Ok(None)` while more chunks are needed, and `Err` for
    /// malformed chunks (which callers should treat as air noise).
    pub fn accept(
        &mut self,
        src: NodeId,
        chunk: &[u8],
        now: Instant,
    ) -> K2Result<Option<Bytes>> {
        if chunk.len() < CHUNK_HEADER_LEN {
            return Err(K2Error::other("broadcast chunk too short"));
        }
        let sequence_id = u32::from_be_bytes(chunk[0..4].try_into().unwrap());
        let index = u16::from_be_bytes(chunk[4..6].try_into().unwrap());
        let count = u16::from_be_bytes(chunk[6..8].try_into().unwrap());
        if count < 2 {
            // Single-fragment payloads must travel as plain DATA frames.
            return Err(K2Error::other(format!(
                "broadcast chunk with fragment count {count} (must be >= 2)"
            )));
        }
        if index >= count {
            return Err(K2Error::other(format!(
                "broadcast chunk index {index} out of range (count {count})"
            )));
        }
        let fragment = Bytes::copy_from_slice(&chunk[CHUNK_HEADER_LEN..]);

        let slot = match self.slots.get_mut(&src) {
            Some(slot) if slot.sequence_id == sequence_id => slot,
            Some(slot) => {
                tracing::warn!(
                    ?src,
                    old_sequence = slot.sequence_id,
                    new_sequence = sequence_id,
                    "evicting incomplete broadcast reassembly for new sequence"
                );
                *slot = Slot {
                    sequence_id,
                    fragments: vec![None; count as usize],
                    received: 0,
                    last_progress: now,
                };
                self.slots.get_mut(&src).unwrap()
            }
            None => self.slots.entry(src).or_insert(Slot {
                sequence_id,
                fragments: vec![None; count as usize],
                received: 0,
                last_progress: now,
            }),
        };

        if slot.fragments.len() != count as usize {
            // Same sequence id but a different fragment count: the
            // sender is misbehaving or we caught a collision. Drop the
            // slot so a clean retransmit can succeed.
            self.slots.remove(&src);
            return Err(K2Error::other(
                "broadcast chunk fragment count changed mid-sequence",
            ));
        }

        if slot.fragments[index as usize].is_none() {
            slot.fragments[index as usize] = Some(fragment);
            slot.received += 1;
            slot.last_progress = now;
        }

        if slot.received == slot.fragments.len() {
            let slot = self.slots.remove(&src).unwrap();
            let mut out = Vec::new();
            for fragment in slot.fragments {
                out.extend_from_slice(&fragment.unwrap());
            }
            return Ok(Some(Bytes::from(out)));
        }
        Ok(None)
    }

    /// Evict slots that have not made progress within `timeout`.
    pub fn prune(&mut self, now: Instant, timeout: std::time::Duration) {
        self.slots.retain(|src, slot| {
            let keep = now.duration_since(slot.last_progress) < timeout;
            if !keep {
                tracing::warn!(
                    ?src,
                    sequence = slot.sequence_id,
                    received = slot.received,
                    of = slot.fragments.len(),
                    "evicting stalled broadcast reassembly"
                );
            }
            keep
        });
    }

    /// Drop any in-flight reassembly for a departed sender.
    pub fn forget(&mut self, src: &NodeId) {
        self.slots.remove(src);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;

    fn node(seed: u8) -> NodeId {
        NodeId([seed; 8])
    }

    fn reassemble_all(
        r: &mut Reassembler,
        src: NodeId,
        chunks: &[Bytes],
    ) -> Option<Bytes> {
        let now = Instant::now();
        let mut out = None;
        for c in chunks {
            if let Some(done) = r.accept(src, c, now).unwrap() {
                out = Some(done);
            }
        }
        out
    }

    #[test]
    fn split_and_reassemble_round_trip() {
        let data: Vec<u8> = (0..=255).cycle().take(10_000).collect();
        let chunks = split_into_chunks(7, &data, 999).unwrap();
        assert_eq!(chunks.len(), 11);
        let mut r = Reassembler::default();
        let out = reassemble_all(&mut r, node(1), &chunks).unwrap();
        assert_eq!(&out[..], &data[..]);
        assert!(r.slots.is_empty());
    }

    #[test]
    fn out_of_order_and_duplicated_chunks() {
        let data = vec![7_u8; 5000];
        let mut chunks = split_into_chunks(1, &data, 1000).unwrap();
        chunks.reverse();
        // Duplicate one mid-stream chunk.
        chunks.insert(2, chunks[2].clone());
        let mut r = Reassembler::default();
        let out = reassemble_all(&mut r, node(1), &chunks).unwrap();
        assert_eq!(&out[..], &data[..]);
    }

    #[test]
    fn interleaved_senders_do_not_collide() {
        let data_a = vec![1_u8; 3000];
        let data_b = vec![2_u8; 3000];
        // Same sequence id from two different senders.
        let chunks_a = split_into_chunks(42, &data_a, 1000).unwrap();
        let chunks_b = split_into_chunks(42, &data_b, 1000).unwrap();
        let mut r = Reassembler::default();
        let now = Instant::now();
        let mut done_a = None;
        let mut done_b = None;
        for (a, b) in chunks_a.iter().zip(chunks_b.iter()) {
            done_a = r.accept(node(1), a, now).unwrap().or(done_a);
            done_b = r.accept(node(2), b, now).unwrap().or(done_b);
        }
        assert_eq!(&done_a.unwrap()[..], &data_a[..]);
        assert_eq!(&done_b.unwrap()[..], &data_b[..]);
    }

    #[test]
    fn new_sequence_evicts_incomplete_old_one() {
        let old = split_into_chunks(1, &vec![1_u8; 3000], 1000).unwrap();
        let new = split_into_chunks(2, &vec![2_u8; 2000], 1000).unwrap();
        let mut r = Reassembler::default();
        let now = Instant::now();
        // Deliver only part of the old sequence.
        assert!(r.accept(node(1), &old[0], now).unwrap().is_none());
        // Then the full new one.
        assert!(r.accept(node(1), &new[0], now).unwrap().is_none());
        let done = r.accept(node(1), &new[1], now).unwrap().unwrap();
        assert_eq!(&done[..], &vec![2_u8; 2000][..]);
        // The old sequence can no longer complete.
        assert!(r.accept(node(1), &old[1], now).unwrap().is_none());
        assert!(r.accept(node(1), &old[2], now).unwrap().is_none());
    }

    #[test]
    fn stalled_slot_pruned_by_timeout() {
        let chunks = split_into_chunks(1, &vec![0_u8; 3000], 1000).unwrap();
        let mut r = Reassembler::default();
        let start = Instant::now();
        assert!(r.accept(node(1), &chunks[0], start).unwrap().is_none());
        r.prune(start + Duration::from_secs(60), Duration::from_secs(30));
        assert!(r.slots.is_empty());
    }

    #[test]
    fn malformed_chunks_rejected() {
        let mut r = Reassembler::default();
        let now = Instant::now();
        // Too short.
        assert!(r.accept(node(1), b"short", now).is_err());
        // count < 2.
        let mut c = Vec::new();
        c.extend_from_slice(&1_u32.to_be_bytes());
        c.extend_from_slice(&0_u16.to_be_bytes());
        c.extend_from_slice(&1_u16.to_be_bytes());
        c.push(0);
        assert!(r.accept(node(1), &c, now).is_err());
        // index >= count.
        let mut c = Vec::new();
        c.extend_from_slice(&1_u32.to_be_bytes());
        c.extend_from_slice(&5_u16.to_be_bytes());
        c.extend_from_slice(&2_u16.to_be_bytes());
        c.push(0);
        assert!(r.accept(node(1), &c, now).is_err());
    }

    #[test]
    fn split_rejects_impossible_inputs() {
        assert!(split_into_chunks(1, b"data", 0).is_err());
        // Would need more than u16::MAX fragments.
        let big = vec![0_u8; (u16::MAX as usize + 1) * 2];
        assert!(split_into_chunks(1, &big, 1).is_err());
    }
}
