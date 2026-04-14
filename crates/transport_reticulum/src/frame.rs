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

use bytes::Bytes;
use kitsune2_api::{K2Error, K2Result};
use rns_transport::hash::AddressHash;

/// Tag byte for preflight frames.
const TAG_PREFLIGHT: u8 = 0x00;
/// Tag byte for data frames.
const TAG_DATA: u8 = 0x01;

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
    /// Application data.
    Data(Bytes),
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
    }
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
}
