//! Compact wire format for `AgentInfoSigned` inside an rns announce
//! packet's `app_data`.
//!
//! # Why not `AgentInfoSigned::encode()` directly?
//!
//! `AgentInfoSigned::encode()` returns canonical JSON of the form
//! `{"agentInfo":"<escaped inner JSON>","signature":"<base64>"}`. Two
//! things in that wrapper are expensive on the wire:
//!
//! - The inner agent_info JSON is embedded as a **JSON string**, so
//!   every `"` in the canonical form is escaped to `\"`. That roughly
//!   doubles the quote count before DEFLATE can compress it back down.
//! - The 64-byte Ed25519 signature is **base64-encoded** (88 bytes
//!   with padding). Signature bytes are high-entropy — DEFLATE cannot
//!   recover the base64 overhead.
//!
//! An rns announce packet's `app_data` budget is `PACKET_MDU` (464)
//! minus the fixed announce overhead: two 32-byte keys, 10-byte name
//! hash, 10-byte rand hash, 64-byte signature — 148 bytes, leaving
//! **316 bytes** of app_data. If the destination ever enables
//! ratchets, subtract a further 32 bytes (→ 284). Kitsune2 does not
//! currently enable ratchets on its destinations, so we size against
//! the no-ratchet budget. Real Holochain `AgentInfoSigned`s after
//! JSON wrapping + DEFLATE were exceeding that, producing
//! `RnsError::OutOfMemory` inside `Destination::announce` when the
//! `PacketDataBuffer` (capacity `PACKET_MDU`) overflowed.
//!
//! # Wire format
//!
//! ```text
//! | u16 BE sig_len | signature bytes | agent_info JSON bytes (rest) |
//! ```
//!
//! The entire envelope is then DEFLATE-compressed. The inner bytes
//! are the verbatim output of [`AgentInfoSigned::get_encoded`] — the
//! exact material the signature was computed over — so verification
//! succeeds after reconstruction.

use flate2::{Compression, read::DeflateDecoder, write::DeflateEncoder};
use kitsune2_api::{AgentInfoSigned, DynVerifier, K2Error, K2Result};
use std::io::{Read, Write};
use std::sync::Arc;
use tracing::debug;

/// Real ceiling for announce `app_data`: `PACKET_MDU` (464) minus the
/// fixed announce overhead of 148 bytes (pub_key + verifying_key +
/// name_hash + rand_hash + signature). If ratchets are ever enabled
/// on the destination this drops by another 32 bytes to 284. We
/// return a clean `K2Error` before handing an oversize payload to
/// rns, which would otherwise surface as `RnsError::OutOfMemory`
/// from `PacketDataBuffer::write`.
const MAX_ENCODED_BYTES: usize = 316;

/// Encode an `AgentInfoSigned` for rns announce `app_data`.
///
/// Takes the raw signed JSON bytes + raw signature bytes (no outer
/// JSON wrapping, no base64), DEFLATEs the pair. The returned bytes
/// are intended to be stored via `ReticulumNode::set_my_agent_info`
/// and pulled by the announce publisher.
pub fn encode_announce_wire(info: &AgentInfoSigned) -> K2Result<bytes::Bytes> {
    let agent_info_json = info.get_encoded().as_bytes();
    let signature = info.get_signature();
    let sig_len = signature.len();
    if sig_len > u16::MAX as usize {
        return Err(K2Error::other(format!(
            "announce wire: signature too long ({sig_len} bytes)"
        )));
    }

    let mut raw = Vec::with_capacity(2 + sig_len + agent_info_json.len());
    raw.extend_from_slice(&(sig_len as u16).to_be_bytes());
    raw.extend_from_slice(signature);
    raw.extend_from_slice(agent_info_json);

    let mut enc = DeflateEncoder::new(Vec::new(), Compression::best());
    enc.write_all(&raw)
        .map_err(|e| K2Error::other_src("announce wire: deflate write", e))?;
    let compressed = enc
        .finish()
        .map_err(|e| K2Error::other_src("announce wire: deflate finish", e))?;

    debug!(
        encoded_bytes = compressed.len(),
        raw_bytes = raw.len(),
        agent_info_json_bytes = agent_info_json.len(),
        budget = MAX_ENCODED_BYTES,
        "announce wire: encoded AgentInfoSigned"
    );
    if compressed.len() > MAX_ENCODED_BYTES {
        return Err(K2Error::other(format!(
            "announce wire: encoded {} bytes exceeds MAX_ENCODED_BYTES {}",
            compressed.len(),
            MAX_ENCODED_BYTES,
        )));
    }
    Ok(bytes::Bytes::from(compressed))
}

/// Decode an announce `app_data` blob back into an `AgentInfoSigned`.
///
/// Inflates, splits off the length-prefixed signature, then passes
/// the raw JSON + signature to `AgentInfoSigned::decode_parts` so
/// the verifier sees the exact signed bytes.
pub(crate) fn decode_announce_wire(
    verifier: &DynVerifier,
    input: &[u8],
) -> K2Result<Arc<AgentInfoSigned>> {
    let mut dec = DeflateDecoder::new(input);
    let mut raw = Vec::new();
    dec.read_to_end(&mut raw)
        .map_err(|e| K2Error::other_src("announce wire: inflate", e))?;

    if raw.len() < 2 {
        return Err(K2Error::other("announce wire: truncated (no sig len)"));
    }
    let sig_len = u16::from_be_bytes([raw[0], raw[1]]) as usize;
    let rest = &raw[2..];
    if rest.len() < sig_len {
        return Err(K2Error::other(format!(
            "announce wire: truncated (need {sig_len}-byte sig, have {})",
            rest.len(),
        )));
    }
    let signature = bytes::Bytes::copy_from_slice(&rest[..sig_len]);
    let agent_info_json = std::str::from_utf8(&rest[sig_len..])
        .map_err(|e| K2Error::other_src("announce wire: non-UTF8 JSON", e))?
        .to_string();

    AgentInfoSigned::decode_parts(&**verifier, agent_info_json, signature)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use kitsune2_api::{
        AgentId, AgentInfo, BoxFut, DhtArc, SpaceId, Timestamp, Url, Verifier,
    };
    use std::time::Duration;

    #[derive(Debug)]
    struct YesVerifier;
    impl Verifier for YesVerifier {
        fn verify(
            &self,
            _agent_info: &AgentInfo,
            _message: &[u8],
            _signature: &[u8],
        ) -> bool {
            true
        }
    }

    #[derive(Debug)]
    struct FakeSigner;
    impl kitsune2_api::Signer for FakeSigner {
        fn sign<'a, 'b: 'a, 'c: 'a>(
            &'a self,
            _agent_info: &AgentInfo,
            _message: &'c [u8],
        ) -> BoxFut<'a, K2Result<Bytes>> {
            // Realistic 64-byte Ed25519-shaped signature.
            Box::pin(async move { Ok(Bytes::from(vec![0xabu8; 64])) })
        }
    }

    async fn mk(url: &str) -> Arc<AgentInfoSigned> {
        let info = AgentInfo {
            agent: AgentId::from(Bytes::from(vec![0x11; 32])),
            space: SpaceId::from(Bytes::from_static(b"alpha-space")),
            created_at: Timestamp::now(),
            expires_at: Timestamp::now() + Duration::from_secs(3600),
            is_tombstone: false,
            url: Some(Url::from_str(url).unwrap()),
            storage_arc: DhtArc::FULL,
        };
        AgentInfoSigned::sign(&FakeSigner, info).await.unwrap()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn round_trip_preserves_fields_and_signature() {
        let signed =
            mk("ret://reticulum:1/00112233445566778899aabbccddeeff").await;
        let verifier: DynVerifier = Arc::new(YesVerifier);
        let wire = encode_announce_wire(&signed).unwrap();
        let decoded = decode_announce_wire(&verifier, &wire).unwrap();

        assert_eq!(decoded.agent, signed.agent);
        assert_eq!(decoded.space, signed.space);
        assert_eq!(decoded.url, signed.url);
        assert_eq!(decoded.get_signature(), signed.get_signature());
        assert_eq!(decoded.get_encoded(), signed.get_encoded());
    }

    /// Holochain-shaped AgentInfoSigned — the real production input —
    /// must fit within the rns announce budget. This is the regression
    /// test for the original `OutOfMemory` failure.
    ///
    /// Budget = `PACKET_MDU` (464) − fixed announce overhead (148:
    /// pub_key + verifying_key + name_hash + rand_hash + signature).
    /// If ratchets are enabled this drops to 284; kitsune2 currently
    /// does not enable them.
    #[tokio::test(flavor = "current_thread")]
    async fn holochain_shaped_payload_fits_budget() {
        const _: () = assert!(
            MAX_ENCODED_BYTES <= 316,
            "MAX_ENCODED_BYTES exceeds no-ratchet announce budget (316)"
        );
        let signed =
            mk("ret://reticulum:1/00112233445566778899aabbccddeeff").await;
        let wire = encode_announce_wire(&signed).unwrap();
        assert!(
            wire.len() <= MAX_ENCODED_BYTES,
            "encoded announce app_data is {} bytes, over budget {}",
            wire.len(),
            MAX_ENCODED_BYTES,
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn decode_truncated_fails_cleanly() {
        let verifier: DynVerifier = Arc::new(YesVerifier);
        let signed =
            mk("ret://reticulum:1/00112233445566778899aabbccddeeff").await;
        let mut wire = encode_announce_wire(&signed).unwrap().to_vec();
        wire.truncate(wire.len() / 2);
        let err = decode_announce_wire(&verifier, &wire);
        assert!(err.is_err(), "truncated input should fail to decode");
    }
}
