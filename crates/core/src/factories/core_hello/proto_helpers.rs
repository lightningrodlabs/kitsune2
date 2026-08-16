//! Transcript construction for the hello proof-of-knowledge exchange.
//!
//! A hello proof is an HMAC over a *transcript*: a byte string that binds the
//! session (both fresh nonces) and the channel (both authenticated peer ids).
//! Building it is pure, so it lives here on its own and is unit tested
//! independently of the exchange state machine.

use bytes::{BufMut, Bytes, BytesMut};
use kitsune2_api::*;

/// The domain separation tag for hello proof transcripts.
pub const HELLO_PROOF_TAG: &str = "k2-hello-proof-v1";

/// The length in bytes of a hello nonce.
pub const HELLO_NONCE_LEN: usize = 32;

/// A hello nonce: exactly [`HELLO_NONCE_LEN`] bytes.
pub type HelloNonce = [u8; HELLO_NONCE_LEN];

/// Build the transcript a hello proof is computed over.
///
/// The transcript is:
///
/// ```text
/// len(tag) || tag || nonce_self || nonce_peer
///          || len(peer_id_self) || peer_id_self
///          || len(peer_id_peer) || peer_id_peer
/// ```
///
/// where each `len` is a big-endian `u32` byte count.
///
/// # Framing
///
/// The nonces are fixed length ([`HELLO_NONCE_LEN`], enforced by the parameter
/// types) so they are written bare. Every variable-length field — the tag and
/// both peer ids — is length-prefixed. Peer ids are variable-length strings
/// whose shape differs across transports, so bare-concatenating them would be
/// ambiguous: `("ab", "c")` and `("a", "bc")` would produce identical
/// transcripts, and a peer could pick an id that makes its transcript collide
/// with another pair's.
///
/// # Roles
///
/// "Self" is the side computing the proof and "peer" is the other side, so
/// the two sides of one exchange produce different transcripts and therefore
/// different proofs. That is what stops a proof from being reflected back at
/// its author.
///
/// The peer ids must be the [`Url::peer_id`] segments of each side's kitsune2
/// URL, never full URLs; see [`transcript_for_urls`].
pub fn transcript(
    tag: &str,
    nonce_self: &HelloNonce,
    nonce_peer: &HelloNonce,
    peer_id_self: &str,
    peer_id_peer: &str,
) -> Bytes {
    let mut out = BytesMut::with_capacity(
        4 + tag.len()
            + 2 * HELLO_NONCE_LEN
            + 4
            + peer_id_self.len()
            + 4
            + peer_id_peer.len(),
    );

    put_len_prefixed(&mut out, tag.as_bytes());
    out.put_slice(nonce_self);
    out.put_slice(nonce_peer);
    put_len_prefixed(&mut out, peer_id_self.as_bytes());
    put_len_prefixed(&mut out, peer_id_peer.as_bytes());

    out.freeze()
}

/// Build a hello transcript from the two sides' kitsune2 URLs.
///
/// This extracts the [`Url::peer_id`] segment of each URL and hands those to
/// [`transcript`]. Full URLs must never enter a transcript: a node
/// legitimately holds several URLs at once (a global relay URL plus per-space
/// relay URLs), the relay half changes on failover, and the two sides will not
/// reliably agree on one. The peer id is the component the transport
/// cryptographically authenticates.
///
/// The verifying side must pass the `peer` [`Url`] the transport handed to the
/// module handler, which is connection-derived, and never a URL taken from
/// message contents.
///
/// # Errors
///
/// Returns an error if either URL has no peer id segment. A URL without a peer
/// id should never reach a module handler, but the exchange must be abandoned
/// rather than proceed with an unbound transcript if one does.
pub fn transcript_for_urls(
    tag: &str,
    nonce_self: &HelloNonce,
    nonce_peer: &HelloNonce,
    url_self: &Url,
    url_peer: &Url,
) -> K2Result<Bytes> {
    let peer_id_self = url_self.peer_id().ok_or_else(|| {
        K2Error::other(format!(
            "Cannot build a hello transcript, our own url has no peer id: {url_self}"
        ))
    })?;
    let peer_id_peer = url_peer.peer_id().ok_or_else(|| {
        K2Error::other(format!(
            "Cannot build a hello transcript, peer url has no peer id: {url_peer}"
        ))
    })?;

    Ok(transcript(
        tag,
        nonce_self,
        nonce_peer,
        peer_id_self,
        peer_id_peer,
    ))
}

fn put_len_prefixed(out: &mut BytesMut, value: &[u8]) {
    out.put_u32(value.len() as u32);
    out.put_slice(value);
}

#[cfg(test)]
mod test {
    use super::*;

    const NONCE_A: HelloNonce = [0xaa; HELLO_NONCE_LEN];
    const NONCE_B: HelloNonce = [0xbb; HELLO_NONCE_LEN];

    #[test]
    fn transcript_is_deterministic() {
        let a = transcript(HELLO_PROOF_TAG, &NONCE_A, &NONCE_B, "self", "peer");
        let b = transcript(HELLO_PROOF_TAG, &NONCE_A, &NONCE_B, "self", "peer");
        assert_eq!(a, b);
    }

    #[test]
    fn transcript_differs_when_roles_are_swapped() {
        // The two sides of one exchange: each puts its own nonce and its own
        // peer id first. The resulting transcripts must differ, otherwise a
        // proof could be reflected back at its author.
        let mine =
            transcript(HELLO_PROOF_TAG, &NONCE_A, &NONCE_B, "alice", "bob");
        let theirs =
            transcript(HELLO_PROOF_TAG, &NONCE_B, &NONCE_A, "bob", "alice");
        assert_ne!(mine, theirs);
    }

    #[test]
    fn transcript_differs_on_each_input() {
        let base =
            transcript(HELLO_PROOF_TAG, &NONCE_A, &NONCE_B, "alice", "bob");

        assert_ne!(
            base,
            transcript("k2-hello-proof-v2", &NONCE_A, &NONCE_B, "alice", "bob"),
            "tag must be bound"
        );
        assert_ne!(
            base,
            transcript(
                HELLO_PROOF_TAG,
                &[0xcc; HELLO_NONCE_LEN],
                &NONCE_B,
                "alice",
                "bob"
            ),
            "self nonce must be bound"
        );
        assert_ne!(
            base,
            transcript(
                HELLO_PROOF_TAG,
                &NONCE_A,
                &[0xcc; HELLO_NONCE_LEN],
                "alice",
                "bob"
            ),
            "peer nonce must be bound"
        );
        assert_ne!(
            base,
            transcript(HELLO_PROOF_TAG, &NONCE_A, &NONCE_B, "carol", "bob"),
            "self peer id must be bound"
        );
        assert_ne!(
            base,
            transcript(HELLO_PROOF_TAG, &NONCE_A, &NONCE_B, "alice", "carol"),
            "peer peer id must be bound"
        );
    }

    #[test]
    fn peer_ids_are_not_bare_concatenated() {
        // Bare concatenation would make these two pairs produce identical
        // transcripts, letting a peer choose an id that collides with a
        // different pair.
        let a = transcript(HELLO_PROOF_TAG, &NONCE_A, &NONCE_B, "ab", "c");
        let b = transcript(HELLO_PROOF_TAG, &NONCE_A, &NONCE_B, "a", "bc");
        assert_ne!(a, b);

        // Same for the boundary between the tag and the rest.
        let c = transcript("tagx", &NONCE_A, &NONCE_B, "a", "b");
        let d = transcript("tag", &NONCE_A, &NONCE_B, "xa", "b");
        assert_ne!(c, d);
    }

    #[test]
    fn transcript_layout_is_as_documented() {
        let t = transcript("t", &NONCE_A, &NONCE_B, "ab", "cde");
        let mut expected = Vec::new();
        expected.extend_from_slice(&1u32.to_be_bytes());
        expected.extend_from_slice(b"t");
        expected.extend_from_slice(&NONCE_A);
        expected.extend_from_slice(&NONCE_B);
        expected.extend_from_slice(&2u32.to_be_bytes());
        expected.extend_from_slice(b"ab");
        expected.extend_from_slice(&3u32.to_be_bytes());
        expected.extend_from_slice(b"cde");
        assert_eq!(t, Bytes::from(expected));
    }

    #[test]
    fn transcript_for_urls_uses_peer_ids_only() {
        // Two URLs for the same peer id, differing in the relay half, as
        // happens when a node holds a global relay URL and a per-space one.
        let mine = Url::from_str("ws://relay-1.test:80/alice").unwrap();
        let mine_other_relay =
            Url::from_str("ws://relay-2.test:80/alice").unwrap();
        let theirs = Url::from_str("ws://relay-1.test:80/bob").unwrap();

        let a = transcript_for_urls(
            HELLO_PROOF_TAG,
            &NONCE_A,
            &NONCE_B,
            &mine,
            &theirs,
        )
        .unwrap();
        let b = transcript_for_urls(
            HELLO_PROOF_TAG,
            &NONCE_A,
            &NONCE_B,
            &mine_other_relay,
            &theirs,
        )
        .unwrap();
        assert_eq!(a, b, "a relay change must not change the transcript");

        assert_eq!(
            a,
            transcript(HELLO_PROOF_TAG, &NONCE_A, &NONCE_B, "alice", "bob")
        );
    }

    #[test]
    fn transcript_for_urls_rejects_urls_without_peer_ids() {
        let peer_url = Url::from_str("ws://relay.test:80/bob").unwrap();
        let server_url = Url::from_str("ws://relay.test:80").unwrap();
        assert!(server_url.peer_id().is_none());

        assert!(
            transcript_for_urls(
                HELLO_PROOF_TAG,
                &NONCE_A,
                &NONCE_B,
                &server_url,
                &peer_url,
            )
            .is_err(),
            "our own url having no peer id must abort the exchange"
        );
        assert!(
            transcript_for_urls(
                HELLO_PROOF_TAG,
                &NONCE_A,
                &NONCE_B,
                &peer_url,
                &server_url,
            )
            .is_err(),
            "the peer url having no peer id must abort the exchange"
        );
    }
}
