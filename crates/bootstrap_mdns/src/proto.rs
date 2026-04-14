//! Wire protocol for the mDNS bootstrap.
//!
//! Three concerns are handled here and nowhere else, so the cryptographic
//! primitives and byte layouts are isolated from the mDNS announce/browse
//! glue and from the kitsune2 module plumbing:
//!
//! 1. [`space_fingerprint`] — the commitment to a [`SpaceId`](kitsune2_api::SpaceId)
//!    that gets broadcast over mDNS.
//! 2. [`Hello`] / [`HelloAck`] — the mutual challenge/response messages
//!    exchanged over the peer-to-peer channel before any signed agent
//!    info is sent.
//! 3. [`AgentInfoBatch`] — the post-handshake payload carrying
//!    `AgentInfoSigned` records for a space.
//!
//! All messages are JSON-encoded for simplicity and debuggability, with an
//! explicit `proto_ver` byte so future iterations can evolve the schema
//! without guessing.

use bytes::Bytes;
use hmac::{Hmac, Mac};
use kitsune2_api::{AgentInfoSigned, K2Error, K2Result, SpaceId};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

/// Wire protocol version. Bump if the format of any message below changes
/// incompatibly. Peers running a newer version of the protocol MUST reject
/// older or unknown versions at the [`Hello`] step.
pub const PROTO_VER: u8 = 1;

/// Domain tag used when hashing the space id for mDNS announcement.
pub const FP_DOMAIN_TAG: &[u8] = b"k2-mdns-v1";

/// Domain tag used in the HMAC proof-of-knowledge step.
pub const PROOF_DOMAIN_TAG: &[u8] = b"k2-mdns-proof-v1";

/// Length of a nonce in bytes.
pub const NONCE_LEN: usize = 32;

/// Length of a space fingerprint in bytes.
pub const FP_LEN: usize = 32;

/// A random per-session challenge. Never reused across sessions.
pub type Nonce = [u8; NONCE_LEN];

/// Compute the public fingerprint of a [`SpaceId`].
///
/// This is `SHA-256(space_id || FP_DOMAIN_TAG)`. It is broadcast on mDNS so
/// members can find one another, while a non-member who does not already
/// know the space id cannot derive it from the fingerprint.
pub fn space_fingerprint(space_id: &SpaceId) -> [u8; FP_LEN] {
    use sha2::Digest;
    let mut h = Sha256::new();
    h.update(space_id.as_ref());
    h.update(FP_DOMAIN_TAG);
    h.finalize().into()
}

/// Compute the HMAC proof that binds knowledge of `space_id` to this session.
///
/// `n_self` is the local nonce, `n_peer` is the remote nonce. Ordering
/// matters — the two sides produce different proof bytes, so neither side
/// can replay the other's proof back at them.
pub fn proof(
    space_id: &SpaceId,
    n_self: &Nonce,
    n_peer: &Nonce,
) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(space_id.as_ref())
        .expect("HMAC accepts any key length");
    mac.update(PROOF_DOMAIN_TAG);
    mac.update(n_self);
    mac.update(n_peer);
    mac.finalize().into_bytes().into()
}

/// Verify a proof produced by the peer. Returns true on match.
///
/// `n_us` is **our** nonce (which the peer treated as `n_peer` when
/// producing the proof); `n_them` is **their** nonce (which the peer used
/// as `n_self`). Expressed another way: we check `HMAC(space, TAG, n_them, n_us)`.
pub fn verify_proof(
    space_id: &SpaceId,
    n_us: &Nonce,
    n_them: &Nonce,
    peer_proof: &[u8; 32],
) -> bool {
    let mut mac = Hmac::<Sha256>::new_from_slice(space_id.as_ref())
        .expect("HMAC accepts any key length");
    mac.update(PROOF_DOMAIN_TAG);
    mac.update(n_them);
    mac.update(n_us);
    mac.verify_slice(peer_proof).is_ok()
}

/// Generate a fresh random nonce.
pub fn fresh_nonce() -> Nonce {
    use rand::RngCore;
    let mut n = [0u8; NONCE_LEN];
    rand::thread_rng().fill_bytes(&mut n);
    n
}

/// First handshake message. Each side sends one unprompted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    /// Protocol version. Peer MUST reject unknown values.
    pub proto_ver: u8,
    /// Fingerprint the sender expects the other side to match against.
    /// Lets the receiver early-abort if this session is cross-space.
    #[serde(with = "serde_bytes")]
    pub space_fp: Vec<u8>,
    /// The sender's fresh nonce.
    #[serde(with = "serde_bytes")]
    pub nonce: Vec<u8>,
}

/// Second handshake message. Sent after receiving the peer's [`Hello`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloAck {
    /// HMAC proof that the sender knows the real space id.
    #[serde(with = "serde_bytes")]
    pub proof: Vec<u8>,
}

/// Post-handshake payload: the sender's set of known, non-expired
/// [`AgentInfoSigned`] records for the space.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentInfoBatch {
    /// JSON-encoded AgentInfoSigned records, one per entry. Using the same
    /// encoding as the WAN bootstrap server keeps verification logic
    /// uniform.
    pub infos: Vec<String>,
}

impl AgentInfoBatch {
    /// Construct a batch from in-memory records.
    pub fn from_infos(infos: &[std::sync::Arc<AgentInfoSigned>]) -> Self {
        let encoded = infos
            .iter()
            .map(|i| i.encode().expect("encode never fails on AgentInfoSigned"))
            .collect();
        Self { infos: encoded }
    }

    /// Decode the batch back into verified [`AgentInfoSigned`] records.
    /// The `verifier` is the one from the kitsune2 builder.
    pub fn decode(
        &self,
        verifier: &kitsune2_api::DynVerifier,
    ) -> K2Result<Vec<std::sync::Arc<AgentInfoSigned>>> {
        self.infos
            .iter()
            .map(|s| AgentInfoSigned::decode(verifier, s.as_bytes()))
            .collect()
    }
}

/// Serialize a message to bytes for sending over a framed channel.
pub fn encode<T: Serialize>(msg: &T) -> K2Result<Bytes> {
    serde_json::to_vec(msg)
        .map(Bytes::from)
        .map_err(|e| K2Error::other_src("encode mdns proto msg", e))
}

/// Deserialize a message from bytes.
pub fn decode<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> K2Result<T> {
    serde_json::from_slice(bytes)
        .map_err(|e| K2Error::other_src("decode mdns proto msg", e))
}

mod serde_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        bytes: &[u8],
        s: S,
    ) -> Result<S::Ok, S::Error> {
        s.serialize_str(&hex::encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(d)?;
        hex::decode(s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn space(bytes: &[u8]) -> SpaceId {
        SpaceId::from(bytes::Bytes::copy_from_slice(bytes))
    }

    #[test]
    fn fingerprint_is_stable_and_unique() {
        let a = space_fingerprint(&space(b"alpha"));
        let b = space_fingerprint(&space(b"beta"));
        let a2 = space_fingerprint(&space(b"alpha"));
        assert_eq!(a, a2);
        assert_ne!(a, b);
        assert_eq!(a.len(), FP_LEN);
    }

    #[test]
    fn proof_is_asymmetric_and_verifies() {
        let s = space(b"secret");
        let n_a = fresh_nonce();
        let n_b = fresh_nonce();

        // A produces proof treating its own nonce as n_self.
        let proof_from_a = proof(&s, &n_a, &n_b);
        let proof_from_b = proof(&s, &n_b, &n_a);

        // Proofs must differ — otherwise an attacker could replay one
        // side's proof to impersonate the other.
        assert_ne!(proof_from_a, proof_from_b);

        // B verifies A's proof using its own nonce as n_us.
        assert!(verify_proof(&s, &n_b, &n_a, &proof_from_a));
        // And vice versa.
        assert!(verify_proof(&s, &n_a, &n_b, &proof_from_b));

        // Swapping nonces must fail.
        assert!(!verify_proof(&s, &n_a, &n_b, &proof_from_a));

        // Different space must fail.
        let wrong = space(b"other");
        assert!(!verify_proof(&wrong, &n_b, &n_a, &proof_from_a));
    }

    #[test]
    fn hello_roundtrip() {
        let fp = space_fingerprint(&space(b"x"));
        let n = fresh_nonce();
        let hello = Hello {
            proto_ver: PROTO_VER,
            space_fp: fp.to_vec(),
            nonce: n.to_vec(),
        };
        let bytes = encode(&hello).unwrap();
        let back: Hello = decode(&bytes).unwrap();
        assert_eq!(back.proto_ver, PROTO_VER);
        assert_eq!(back.space_fp, fp.to_vec());
        assert_eq!(back.nonce, n.to_vec());
    }
}
