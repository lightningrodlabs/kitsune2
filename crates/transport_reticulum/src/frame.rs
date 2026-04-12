//! Minimal tag-byte framing for Reticulum link payloads.
//!
//! Unlike the Iroh transport, Reticulum already provides message boundaries
//! (each `data_packet()` or Resource transfer is a discrete payload), so we
//! do **not** need length-prefixed framing. We only need a single tag byte
//! to distinguish preflight from data payloads.
//!
//! ```text
//! Preflight Frame:
//! +------+-----------+
//! | 0x00 | Preflight |
//! | 1 B  |   Data    |
//! +------+-----------+
//!
//! Data Frame:
//! +------+------+
//! | 0x01 | Data |
//! | 1 B  | Var  |
//! +------+------+
//! ```
//!
//! The preflight frame does **not** carry a URL field (unlike Iroh's
//! preflight). The remote's URL is deterministic from its Identity hash,
//! and `Link::peer_identity()` exposes that Identity directly.

use bytes::Bytes;
use kitsune2_api::{K2Error, K2Result};

/// Tag byte for preflight frames.
const TAG_PREFLIGHT: u8 = 0x00;
/// Tag byte for data frames.
const TAG_DATA: u8 = 0x01;

/// A decoded Reticulum frame.
#[derive(Debug, PartialEq)]
pub(crate) enum ReticulumFrame {
    /// Preflight exchange payload (per-peer, carried on first link).
    Preflight(Bytes),
    /// Application data.
    Data(Bytes),
}

/// Encode a frame for transmission over a Reticulum link.
pub(crate) fn encode_frame(
    frame: &ReticulumFrame,
    max_frame_bytes: usize,
) -> K2Result<Bytes> {
    let (tag, payload) = match frame {
        ReticulumFrame::Preflight(data) => (TAG_PREFLIGHT, data),
        ReticulumFrame::Data(data) => (TAG_DATA, data),
    };

    let total = 1 + payload.len();
    if total > max_frame_bytes {
        return Err(K2Error::other(format!(
            "Reticulum frame too large: {total} > {max_frame_bytes}"
        )));
    }

    let mut buf = Vec::with_capacity(total);
    buf.push(tag);
    buf.extend_from_slice(payload);
    Ok(Bytes::from(buf))
}

/// Decode a frame received from a Reticulum link.
pub(crate) fn decode_frame(data: &[u8]) -> K2Result<ReticulumFrame> {
    if data.is_empty() {
        return Err(K2Error::other("Empty Reticulum frame"));
    }
    let tag = data[0];
    let payload = Bytes::copy_from_slice(&data[1..]);
    match tag {
        TAG_PREFLIGHT => Ok(ReticulumFrame::Preflight(payload)),
        TAG_DATA => Ok(ReticulumFrame::Data(payload)),
        _ => Err(K2Error::other(format!(
            "Unknown Reticulum frame tag: 0x{tag:02x}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_preflight() {
        let original =
            ReticulumFrame::Preflight(Bytes::from_static(b"hello preflight"));
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
        let frame = ReticulumFrame::Preflight(Bytes::new());
        let encoded = encode_frame(&frame, 1024).unwrap();
        assert_eq!(encoded.len(), 1);
        let decoded = decode_frame(&encoded).unwrap();
        assert_eq!(frame, decoded);
    }
}
