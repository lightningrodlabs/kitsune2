//! Wire framing for broadcast media.
//!
//! Every frame on the air carries a fixed 20-byte header:
//!
//! ```text
//! +-------+---------+------------+------------+-----+---------+
//! | magic | version | src NodeId | dst NodeId | tag | payload |
//! | 2 B   | 1 B     | 8 B        | 8 B        | 1 B | var     |
//! +-------+---------+------------+------------+-----+---------+
//! ```
//!
//! `src`/`dst` are ephemeral per-transport-instance ids. The all-zero
//! dst is reserved as the broadcast address for phase-2 native mode;
//! phase-1 unicast emulation always addresses a specific node and
//! non-addressees drop the frame at the header check.
//!
//! Payload tags:
//!
//! - `PREFLIGHT` / `DATA` payloads are whole `K2Proto`-encoded messages
//!   handed directly to [`kitsune2_api::TxImpHnd::recv_data`].
//! - `CHUNK` payloads carry one fragment of a logical `DATA` payload
//!   that exceeded the medium MTU; layout and reassembly live in
//!   [`crate::chunking`].
//!
//! There is no explicit disconnect tag: kitsune2 already models a
//! graceful disconnect as a `K2Proto` Disconnect message, which travels
//! as a normal `DATA` frame and causes the receiving handler to close
//! the virtual connection.

use bytes::Bytes;
use kitsune2_api::{K2Error, K2Result};

/// Frame magic: "k" + 0xB0 ("broadcast").
pub(crate) const MAGIC: [u8; 2] = [0x6b, 0xb0];

/// Current wire version.
pub(crate) const VERSION: u8 = 1;

/// Fixed header size: magic (2) + version (1) + src (8) + dst (8) + tag (1).
pub(crate) const HEADER_LEN: usize = 2 + 1 + 8 + 8 + 1;

/// An ephemeral node id identifying one transport instance on the air.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct NodeId(pub [u8; 8]);

impl std::fmt::Debug for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NodeId({})", self.to_hex())
    }
}

impl NodeId {
    /// The reserved broadcast destination. Unaddressed until the
    /// phase-2 native-broadcast mode lands; kept so the wire format
    /// does not change when it does.
    #[allow(dead_code)]
    pub const BROADCAST: NodeId = NodeId([0; 8]);

    /// Generate a fresh random id.
    pub fn random() -> Self {
        let mut bytes = [0_u8; 8];
        rand::Rng::fill(&mut rand::thread_rng(), &mut bytes[..]);
        if bytes == [0; 8] {
            // Vanishingly unlikely, but the all-zero id is reserved.
            bytes[0] = 1;
        }
        Self(bytes)
    }

    /// Lowercase hex representation, used as the url peer id segment.
    pub fn to_hex(self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Parse from the hex representation produced by [`Self::to_hex`].
    pub fn from_hex(s: &str) -> K2Result<Self> {
        if s.len() != 16 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(K2Error::other(format!(
                "invalid broadcast node id: {s}"
            )));
        }
        let mut bytes = [0_u8; 8];
        for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
            let hi = (chunk[0] as char).to_digit(16).unwrap() as u8;
            let lo = (chunk[1] as char).to_digit(16).unwrap() as u8;
            bytes[i] = (hi << 4) | lo;
        }
        Ok(Self(bytes))
    }
}

/// Payload tag byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FrameTag {
    /// Connection-validation payload (`K2Proto` Preflight message).
    Preflight,
    /// A whole `K2Proto` message.
    Data,
    /// One fragment of a chunked `DATA` payload.
    Chunk,
}

impl FrameTag {
    fn to_byte(self) -> u8 {
        match self {
            FrameTag::Preflight => 0,
            FrameTag::Data => 1,
            FrameTag::Chunk => 2,
        }
    }

    fn from_byte(b: u8) -> K2Result<Self> {
        match b {
            0 => Ok(FrameTag::Preflight),
            1 => Ok(FrameTag::Data),
            2 => Ok(FrameTag::Chunk),
            _ => {
                Err(K2Error::other(format!("unknown broadcast frame tag: {b}")))
            }
        }
    }
}

/// A decoded frame.
#[derive(Debug, PartialEq)]
pub(crate) struct Frame {
    pub src: NodeId,
    pub dst: NodeId,
    pub tag: FrameTag,
    pub payload: Bytes,
}

/// Encode a frame for transmission.
pub(crate) fn encode_frame(
    src: NodeId,
    dst: NodeId,
    tag: FrameTag,
    payload: &[u8],
) -> Bytes {
    let mut buf = Vec::with_capacity(HEADER_LEN + payload.len());
    buf.extend_from_slice(&MAGIC);
    buf.push(VERSION);
    buf.extend_from_slice(&src.0);
    buf.extend_from_slice(&dst.0);
    buf.push(tag.to_byte());
    buf.extend_from_slice(payload);
    Bytes::from(buf)
}

/// Decode a frame heard on the air.
///
/// Broadcast media are noisy: callers should treat an `Err` here as
/// "not one of ours" and drop the frame quietly.
pub(crate) fn decode_frame(data: &[u8]) -> K2Result<Frame> {
    if data.len() < HEADER_LEN {
        return Err(K2Error::other("broadcast frame too short"));
    }
    if data[0..2] != MAGIC {
        return Err(K2Error::other("bad broadcast frame magic"));
    }
    if data[2] != VERSION {
        return Err(K2Error::other(format!(
            "unsupported broadcast frame version: {}",
            data[2]
        )));
    }
    let mut src = [0_u8; 8];
    src.copy_from_slice(&data[3..11]);
    let mut dst = [0_u8; 8];
    dst.copy_from_slice(&data[11..19]);
    let tag = FrameTag::from_byte(data[19])?;
    Ok(Frame {
        src: NodeId(src),
        dst: NodeId(dst),
        tag,
        payload: Bytes::copy_from_slice(&data[HEADER_LEN..]),
    })
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn node_id_hex_round_trip() {
        let id = NodeId::random();
        assert_eq!(id, NodeId::from_hex(&id.to_hex()).unwrap());
    }

    #[test]
    fn node_id_bad_hex_rejected() {
        assert!(NodeId::from_hex("nope").is_err());
        assert!(NodeId::from_hex("0123456789abcdef0").is_err());
        assert!(NodeId::from_hex("0123456789abcdeg").is_err());
    }

    #[test]
    fn frame_round_trip() {
        let src = NodeId::random();
        let dst = NodeId::random();
        for tag in [FrameTag::Preflight, FrameTag::Data, FrameTag::Chunk] {
            let enc = encode_frame(src, dst, tag, b"hello air");
            let dec = decode_frame(&enc).unwrap();
            assert_eq!(dec.src, src);
            assert_eq!(dec.dst, dst);
            assert_eq!(dec.tag, tag);
            assert_eq!(&dec.payload[..], b"hello air");
        }
    }

    #[test]
    fn empty_payload_round_trips() {
        let enc = encode_frame(
            NodeId::random(),
            NodeId::BROADCAST,
            FrameTag::Data,
            b"",
        );
        let dec = decode_frame(&enc).unwrap();
        assert!(dec.payload.is_empty());
        assert_eq!(dec.dst, NodeId::BROADCAST);
    }

    #[test]
    fn noise_rejected() {
        assert!(decode_frame(b"").is_err());
        assert!(decode_frame(b"short").is_err());
        // Right length, wrong magic.
        assert!(decode_frame(&[0xff; HEADER_LEN]).is_err());
        // Good magic, bad version.
        let mut frame = encode_frame(
            NodeId::random(),
            NodeId::random(),
            FrameTag::Data,
            b"x",
        )
        .to_vec();
        frame[2] = 99;
        assert!(decode_frame(&frame).is_err());
        // Good header, unknown tag.
        let mut frame = encode_frame(
            NodeId::random(),
            NodeId::random(),
            FrameTag::Data,
            b"x",
        )
        .to_vec();
        frame[19] = 42;
        assert!(decode_frame(&frame).is_err());
    }
}
