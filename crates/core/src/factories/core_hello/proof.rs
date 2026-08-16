//! Proof computation and verification for the hello exchange.
//!
//! A hello proof is `HMAC-SHA256(k_hello, T)`, where `k_hello` is the space's
//! `"k2-hello-v1"` derived key and `T` is the transcript built by
//! [`transcript`](super::transcript). Keeping this separate from the exchange
//! state machine keeps it pure and unit testable.

use bytes::Bytes;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Compute a hello proof over a transcript.
pub fn hello_proof(key: &[u8], transcript: &[u8]) -> Bytes {
    let mut mac = HmacSha256::new_from_slice(key)
        .expect("HMAC accepts keys of any length");
    mac.update(transcript);
    Bytes::copy_from_slice(&mac.finalize().into_bytes())
}

/// Verify a hello proof over a transcript.
///
/// The comparison is constant time, and a proof of the wrong length is simply
/// invalid rather than an error.
pub fn verify_hello_proof(key: &[u8], transcript: &[u8], proof: &[u8]) -> bool {
    let mut mac = HmacSha256::new_from_slice(key)
        .expect("HMAC accepts keys of any length");
    mac.update(transcript);
    mac.verify_slice(proof).is_ok()
}

#[cfg(test)]
mod test {
    use super::*;

    const KEY: &[u8] = b"a-space-hello-key";

    #[test]
    fn proof_round_trip() {
        let proof = hello_proof(KEY, b"transcript");
        assert_eq!(proof.len(), 32);
        assert!(verify_hello_proof(KEY, b"transcript", &proof));
    }

    #[test]
    fn a_different_key_does_not_verify() {
        let proof = hello_proof(KEY, b"transcript");
        assert!(!verify_hello_proof(b"another-key", b"transcript", &proof));
    }

    #[test]
    fn a_different_transcript_does_not_verify() {
        let proof = hello_proof(KEY, b"transcript");
        assert!(!verify_hello_proof(KEY, b"other-transcript", &proof));
    }

    #[test]
    fn a_malformed_proof_does_not_verify() {
        assert!(!verify_hello_proof(KEY, b"transcript", b""));
        assert!(!verify_hello_proof(KEY, b"transcript", b"short"));
        assert!(!verify_hello_proof(KEY, b"transcript", &[0_u8; 64]));
    }
}
