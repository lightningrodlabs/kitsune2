//! Minimal tag-byte framing for Reticulum link payloads.
//!
//! Unlike the Iroh transport, Reticulum already provides message boundaries
//! (each `data_packet()` or Resource transfer is a discrete payload), so we
//! do **not** need length-prefixed framing. We only need a single tag byte
//! to distinguish preflight from data payloads.
//!
//! ```text
//! Preflight Frame:
//! +------+----------------------+------------+
//! | 0x00 | main identity hash   | Preflight  |
//! | 1 B  | 16 B                 | K2Proto    |
//! +------+----------------------+------------+
//!
//! Data Frame:
//! +------+------+
//! | 0x01 | Data |
//! | 1 B  | Var  |
//! +------+------+
//!
//! Chunked Data Fragment (one of N; see `chunking.rs`):
//! +------+---------------+-------------+-------------+-------------------+
//! | 0x02 | sequence_id   | frag_index  | frag_count  | fragment_payload  |
//! | 1 B  | 4 B  (u32 BE) | 2 B  (u16)  | 2 B  (u16)  | Var               |
//! +------+---------------+-------------+-------------+-------------------+
//! ```
//!
//! The preflight frame carries the sender's **main** identity address
//! hash (the one derived from their `PrivateIdentity`). This is
//! distinct from the per-link ephemeral identity rns sees in the link
//! request, which `Link::peer_identity()` exposes. The transport needs
//! the main identity to key its `PeerState` and `link_registry` under
//! the same URL that kitsune2's `AgentInfoSigned.url` advertises —
//! otherwise gossip messages arrive under one URL while the peer
//! store has the agent under another, and kitsune2's block check
//! filters them all out.
//!
//! `TAG_CHUNKED` frames fragment one logical Data payload across
//! multiple Link packets when it exceeds the backend's plaintext MDU;
//! reassembly is owned by [`crate::chunking`]. A single-fragment
//! Data payload always uses `TAG_DATA` directly — `TAG_CHUNKED` with
//! `fragment_count = 1` is malformed and rejected on decode.

use crate::types::AddressHash;
use bytes::Bytes;
use kitsune2_api::{K2Error, K2Result};

/// Tag byte for preflight frames.
const TAG_PREFLIGHT: u8 = 0x00;
/// Tag byte for data frames.
pub(crate) const TAG_DATA: u8 = 0x01;
/// Tag byte for one fragment of a chunked Data payload.
pub(crate) const TAG_CHUNKED: u8 = 0x02;

/// Fixed header size of a `TAG_CHUNKED` frame: tag (1) + sequence_id
/// (4) + fragment_index (2) + fragment_count (2).
pub(crate) const CHUNKED_HEADER_SIZE: usize = 1 + 4 + 2 + 2;

/// A decoded Reticulum frame.
#[derive(Debug, PartialEq)]
pub(crate) enum ReticulumFrame {
    /// Preflight exchange payload (per-peer, carried on first link).
    /// Carries the sender's *main* identity address hash so the
    /// receiver can key `PeerState` by the same URL that shows up in
    /// the sender's `AgentInfoSigned.url`.
    Preflight {
        sender_main_identity: AddressHash,
        payload: Bytes,
    },
    /// Application data, sent whole in one packet.
    Data(Bytes),
    /// One fragment of a chunked Data payload — reassembly lives in
    /// [`crate::chunking`]. All fragments of a single logical frame
    /// share the same `sequence_id` and `fragment_count`; individual
    /// fragments are identified by `fragment_index` in `[0, count)`.
    Chunked {
        sequence_id: u32,
        fragment_index: u16,
        fragment_count: u16,
        payload: Bytes,
    },
}

/// Encode a frame for transmission over a Reticulum link.
pub(crate) fn encode_frame(
    frame: &ReticulumFrame,
    max_frame_bytes: usize,
) -> K2Result<Bytes> {
    match frame {
        ReticulumFrame::Preflight {
            sender_main_identity,
            payload,
        } => {
            let total = 1 + 16 + payload.len();
            if total > max_frame_bytes {
                return Err(K2Error::other(format!(
                    "Reticulum preflight frame too large: {total} > {max_frame_bytes}"
                )));
            }
            let mut buf = Vec::with_capacity(total);
            buf.push(TAG_PREFLIGHT);
            buf.extend_from_slice(sender_main_identity.as_slice());
            buf.extend_from_slice(payload);
            Ok(Bytes::from(buf))
        }
        ReticulumFrame::Data(data) => {
            let total = 1 + data.len();
            if total > max_frame_bytes {
                return Err(K2Error::other(format!(
                    "Reticulum data frame too large: {total} > {max_frame_bytes}"
                )));
            }
            let mut buf = Vec::with_capacity(total);
            buf.push(TAG_DATA);
            buf.extend_from_slice(data);
            Ok(Bytes::from(buf))
        }
        ReticulumFrame::Chunked {
            sequence_id,
            fragment_index,
            fragment_count,
            payload,
        } => {
            let total = CHUNKED_HEADER_SIZE + payload.len();
            if total > max_frame_bytes {
                return Err(K2Error::other(format!(
                    "Reticulum chunked frame too large: {total} > {max_frame_bytes}"
                )));
            }
            Ok(encode_chunked_fragment(
                *sequence_id,
                *fragment_index,
                *fragment_count,
                payload,
            ))
        }
    }
}

/// Encode one `TAG_CHUNKED` fragment. Used by the chunking layer on
/// the send side; does **not** enforce `max_frame_bytes` because the
/// chunker's per-fragment payload cap is the plaintext MDU, not the
/// logical-frame limit.
pub(crate) fn encode_chunked_fragment(
    sequence_id: u32,
    fragment_index: u16,
    fragment_count: u16,
    payload: &[u8],
) -> Bytes {
    let mut buf = Vec::with_capacity(CHUNKED_HEADER_SIZE + payload.len());
    buf.push(TAG_CHUNKED);
    buf.extend_from_slice(&sequence_id.to_be_bytes());
    buf.extend_from_slice(&fragment_index.to_be_bytes());
    buf.extend_from_slice(&fragment_count.to_be_bytes());
    buf.extend_from_slice(payload);
    Bytes::from(buf)
}

/// Decode a frame received from a Reticulum link.
pub(crate) fn decode_frame(data: &[u8]) -> K2Result<ReticulumFrame> {
    if data.is_empty() {
        return Err(K2Error::other("Empty Reticulum frame"));
    }
    let tag = data[0];
    match tag {
        TAG_PREFLIGHT => {
            if data.len() < 1 + 16 {
                return Err(K2Error::other(
                    "Reticulum preflight frame too short (missing identity)",
                ));
            }
            let mut id_bytes = [0u8; 16];
            id_bytes.copy_from_slice(&data[1..17]);
            Ok(ReticulumFrame::Preflight {
                sender_main_identity: AddressHash::new(id_bytes),
                payload: Bytes::copy_from_slice(&data[17..]),
            })
        }
        TAG_DATA => {
            Ok(ReticulumFrame::Data(Bytes::copy_from_slice(&data[1..])))
        }
        TAG_CHUNKED => {
            if data.len() < CHUNKED_HEADER_SIZE {
                return Err(K2Error::other(
                    "Reticulum chunked frame too short (missing header)",
                ));
            }
            let sequence_id =
                u32::from_be_bytes(data[1..5].try_into().unwrap());
            let fragment_index =
                u16::from_be_bytes(data[5..7].try_into().unwrap());
            let fragment_count =
                u16::from_be_bytes(data[7..9].try_into().unwrap());
            if fragment_count < 2 {
                // `TAG_CHUNKED` with count 0 or 1 is malformed:
                // single-fragment payloads must go through `TAG_DATA`.
                return Err(K2Error::other(format!(
                    "Reticulum chunked frame with fragment_count={fragment_count} (must be >= 2)"
                )));
            }
            if fragment_index >= fragment_count {
                return Err(K2Error::other(format!(
                    "Reticulum chunked frame fragment_index {fragment_index} >= fragment_count {fragment_count}"
                )));
            }
            Ok(ReticulumFrame::Chunked {
                sequence_id,
                fragment_index,
                fragment_count,
                payload: Bytes::copy_from_slice(&data[CHUNKED_HEADER_SIZE..]),
            })
        }
        _ => Err(K2Error::other(format!(
            "Unknown Reticulum frame tag: 0x{tag:02x}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(seed: u8) -> AddressHash {
        AddressHash::new([seed; 16])
    }

    #[test]
    fn round_trip_preflight() {
        let original = ReticulumFrame::Preflight {
            sender_main_identity: hash(0xAB),
            payload: Bytes::from_static(b"hello preflight"),
        };
        let encoded = encode_frame(&original, 1024).unwrap();
        let decoded = decode_frame(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn round_trip_data() {
        let original = ReticulumFrame::Data(Bytes::from_static(b"some data"));
        let encoded = encode_frame(&original, 1024).unwrap();
        let decoded = decode_frame(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn frame_too_large() {
        let big = ReticulumFrame::Data(Bytes::from(vec![0u8; 100]));
        assert!(encode_frame(&big, 50).is_err());
    }

    #[test]
    fn empty_frame() {
        assert!(decode_frame(&[]).is_err());
    }

    #[test]
    fn unknown_tag() {
        assert!(decode_frame(&[0xff, 0x01, 0x02]).is_err());
    }

    #[test]
    fn preflight_empty_payload() {
        let frame = ReticulumFrame::Preflight {
            sender_main_identity: hash(0xCD),
            payload: Bytes::new(),
        };
        let encoded = encode_frame(&frame, 1024).unwrap();
        // tag (1) + identity (16) + empty payload.
        assert_eq!(encoded.len(), 17);
        let decoded = decode_frame(&encoded).unwrap();
        assert_eq!(frame, decoded);
    }

    #[test]
    fn preflight_short_frame_missing_identity() {
        // Tag byte only — no room for the 16-byte identity.
        assert!(decode_frame(&[TAG_PREFLIGHT, 0x01]).is_err());
    }

    #[test]
    fn round_trip_chunked() {
        let payload = Bytes::from_static(b"fragment body");
        let encoded = encode_chunked_fragment(0x0102_0304, 3, 7, &payload);
        // Header layout: tag | seq (BE u32) | index (BE u16) | count (BE u16).
        assert_eq!(encoded[0], TAG_CHUNKED);
        assert_eq!(&encoded[1..5], &[0x01, 0x02, 0x03, 0x04]);
        assert_eq!(&encoded[5..7], &[0x00, 0x03]);
        assert_eq!(&encoded[7..9], &[0x00, 0x07]);
        let decoded = decode_frame(&encoded).unwrap();
        assert_eq!(
            decoded,
            ReticulumFrame::Chunked {
                sequence_id: 0x0102_0304,
                fragment_index: 3,
                fragment_count: 7,
                payload,
            }
        );
    }

    #[test]
    fn chunked_empty_payload_round_trips() {
        // A zero-byte fragment is structurally valid (the chunking
        // layer doesn't produce them in practice, but the decoder
        // should still round-trip it rather than returning a spurious
        // short-header error).
        let encoded = encode_chunked_fragment(42, 0, 2, &[]);
        assert_eq!(encoded.len(), CHUNKED_HEADER_SIZE);
        let decoded = decode_frame(&encoded).unwrap();
        match decoded {
            ReticulumFrame::Chunked {
                sequence_id,
                fragment_index,
                fragment_count,
                payload,
            } => {
                assert_eq!(sequence_id, 42);
                assert_eq!(fragment_index, 0);
                assert_eq!(fragment_count, 2);
                assert!(payload.is_empty());
            }
            _ => panic!("expected Chunked"),
        }
    }

    #[test]
    fn chunked_short_header_rejected() {
        // Tag byte + 7 bytes of header, needs 9.
        let mut buf = vec![TAG_CHUNKED];
        buf.extend_from_slice(&[0u8; 7]);
        assert!(decode_frame(&buf).is_err());
    }

    #[test]
    fn chunked_count_one_rejected() {
        let encoded = encode_chunked_fragment(1, 0, 1, b"x");
        assert!(decode_frame(&encoded).is_err());
    }

    #[test]
    fn chunked_count_zero_rejected() {
        let encoded = encode_chunked_fragment(1, 0, 0, b"x");
        assert!(decode_frame(&encoded).is_err());
    }

    #[test]
    fn chunked_index_out_of_range_rejected() {
        // index == count is invalid.
        let encoded = encode_chunked_fragment(1, 5, 5, b"x");
        assert!(decode_frame(&encoded).is_err());
        // index > count is also invalid.
        let encoded = encode_chunked_fragment(1, 9, 5, b"x");
        assert!(decode_frame(&encoded).is_err());
    }

    #[test]
    fn chunked_frame_too_large_on_encode() {
        // `encode_frame` (through `ReticulumFrame::Chunked`) honours
        // `max_frame_bytes`; `encode_chunked_fragment` (used internally)
        // intentionally doesn't.
        let frame = ReticulumFrame::Chunked {
            sequence_id: 1,
            fragment_index: 0,
            fragment_count: 2,
            payload: Bytes::from(vec![0u8; 100]),
        };
        assert!(encode_frame(&frame, 50).is_err());
    }
}
