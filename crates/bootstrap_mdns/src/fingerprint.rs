//! The space commitment that is broadcast over mDNS.
//!
//! A node must be able to tell which LAN announcements belong to a space it
//! is in without telling the LAN which spaces those are. The fingerprint is a
//! plain hash of the space id under a fixed domain tag: members of the space
//! can compute and match it, a passive observer only learns that some space
//! exists. An observer holding a list of candidate space ids can still
//! confirm a match by hashing each candidate, which is an accepted limit of
//! any discovery scheme that matches on a shared identifier.

use kitsune2_api::SpaceId;
use sha2::{Digest, Sha256};

/// Domain tag mixed into the fingerprint hash, so that a space id hashed for
/// some other purpose can never collide with an mDNS announcement.
pub const FP_DOMAIN_TAG: &[u8] = b"k2-mdns-v1";

/// Length of a space fingerprint in bytes.
pub const FP_LEN: usize = 32;

/// A space fingerprint: `SHA-256(space_id || FP_DOMAIN_TAG)`.
pub type SpaceFingerprint = [u8; FP_LEN];

/// Compute the fingerprint that identifies `space_id` on the LAN.
pub fn space_fingerprint(space_id: &SpaceId) -> SpaceFingerprint {
    let mut h = Sha256::new();
    h.update(space_id.as_ref());
    h.update(FP_DOMAIN_TAG);
    h.finalize().into()
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
}
