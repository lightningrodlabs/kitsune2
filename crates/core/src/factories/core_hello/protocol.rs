//! The hello wire protocol messages.
//!
//! These are the protobuf types generated from `crates/core/proto/hello.proto`
//! plus encode/decode helpers for the [`K2HelloMessage`] envelope.

use bytes::Bytes;
use kitsune2_api::*;
use prost::Message;

pub(crate) mod proto {
    include!("../../../proto/gen/kitsune2.hello.rs");
}

pub use proto::k2_hello_message::Msg as HelloMsg;
pub use proto::{Ack, Confirm, Initiate, K2HelloMessage, Respond};

/// The hello protocol version this implementation speaks.
pub const HELLO_PROTO_VER: u32 = 1;

impl K2HelloMessage {
    /// Wrap a hello message in an envelope.
    pub fn new(msg: HelloMsg) -> Self {
        Self { msg: Some(msg) }
    }

    /// Encode this message as a [`Bytes`] buffer.
    pub fn encode_msg(&self) -> K2Result<Bytes> {
        let mut out = bytes::BytesMut::new();
        Message::encode(self, &mut out).map_err(|err| {
            K2Error::other_src("Failed to encode K2HelloMessage", err)
        })?;
        Ok(out.freeze())
    }

    /// Decode a hello message from a byte buffer.
    ///
    /// Returns an error for an envelope with no message in it, since that
    /// carries no meaning and indicates a peer that is not following the
    /// protocol.
    pub fn decode_msg(bytes: Bytes) -> K2Result<HelloMsg> {
        let envelope = <Self as Message>::decode(bytes).map_err(|err| {
            K2Error::other_src("Failed to decode K2HelloMessage", err)
        })?;
        envelope
            .msg
            .ok_or_else(|| K2Error::other("Empty K2HelloMessage envelope"))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn envelope_round_trip() {
        for msg in [
            HelloMsg::Initiate(Initiate {
                proto_ver: HELLO_PROTO_VER,
                nonce_i: Bytes::from_static(&[1u8; 32]),
            }),
            HelloMsg::Respond(Respond {
                proto_ver: HELLO_PROTO_VER,
                nonce_r: Bytes::from_static(&[2u8; 32]),
                proof_r: Bytes::from_static(&[3u8; 32]),
            }),
            HelloMsg::Confirm(Confirm {
                proof_i: Bytes::from_static(&[4u8; 32]),
                agent_infos_i: vec!["info-a".to_string(), "info-b".to_string()],
            }),
            HelloMsg::Ack(Ack {
                agent_infos_r: vec!["info-c".to_string()],
            }),
        ] {
            let enc = K2HelloMessage::new(msg.clone()).encode_msg().unwrap();
            let dec = K2HelloMessage::decode_msg(enc).unwrap();
            assert_eq!(msg, dec);
        }
    }

    #[test]
    fn empty_envelope_is_an_error() {
        let enc = K2HelloMessage { msg: None }.encode_msg().unwrap();
        assert!(K2HelloMessage::decode_msg(enc).is_err());
    }

    #[test]
    fn garbage_does_not_decode_to_a_message() {
        // Field 1 as a varint, which is the wrong wire type for the
        // `initiate` message field.
        let garbage = Bytes::from_static(&[0x08, 0x01]);
        assert!(K2HelloMessage::decode_msg(garbage).is_err());
    }
}
