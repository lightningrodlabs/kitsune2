//! The space commitment that is broadcast over mDNS.
//!
//! A node must be able to tell which LAN announcements belong to a space it
//! is in without telling the LAN which spaces those are. The fingerprint is
//! key material derived from the space secret for this one purpose, the
//! same way the hello module derives its proof key: members of the space
//! can compute and match it, while a non-member cannot compute it at all,
//! and learning it reveals neither the secret nor any other derived key.
//!
//! When the host configures no space secret, kitsune2 falls back to using
//! the space id as the secret. The fingerprint is then computable by anyone
//! holding a candidate space id, which is an accepted limit of that default
//! rather than of this scheme.

use base64::prelude::*;
use bytes::Bytes;
use kitsune2_api::{Builder, K2Result, SpaceId};
use std::sync::Arc;

/// The purpose under which the fingerprint is derived from the space
/// secret. See [`SpaceSecret::derive_key`](kitsune2_api::SpaceSecret::derive_key).
pub const MDNS_KEY_PURPOSE: &str = "k2-mdns-v1";

/// The commitment a space announces and matches on.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct SpaceFingerprint(Bytes);

impl std::fmt::Debug for SpaceFingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SpaceFingerprint({})", self.encode())
    }
}

impl From<Bytes> for SpaceFingerprint {
    fn from(bytes: Bytes) -> Self {
        Self(bytes)
    }
}

impl SpaceFingerprint {
    /// Derive the fingerprint of `space_id` from the builder's space secret.
    pub async fn derive(
        builder: &Arc<Builder>,
        space_id: &SpaceId,
    ) -> K2Result<Self> {
        let secret = builder
            .space_secret
            .create(builder.clone(), space_id.clone())
            .await?;
        let key = secret
            .derive_key(space_id.clone(), MDNS_KEY_PURPOSE)
            .await?;
        Ok(Self(key))
    }

    /// The TXT encoding: url-safe base64 without padding.
    pub fn encode(&self) -> String {
        BASE64_URL_SAFE_NO_PAD.encode(&self.0)
    }

    /// Parse a TXT value produced by [`encode`](Self::encode).
    pub fn decode(txt: &str) -> Option<Self> {
        let bytes = BASE64_URL_SAFE_NO_PAD.decode(txt).ok()?;
        if bytes.is_empty() {
            return None;
        }
        Some(Self(bytes.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fp(bytes: &[u8]) -> SpaceFingerprint {
        SpaceFingerprint::from(Bytes::copy_from_slice(bytes))
    }

    #[test]
    fn encode_round_trips_through_decode() {
        let a = fp(&[7u8; 32]);
        let txt = a.encode();
        assert!(!txt.contains('='), "no padding: {txt}");
        assert_eq!(SpaceFingerprint::decode(&txt), Some(a));
    }

    #[test]
    fn decode_rejects_what_encode_never_produces() {
        assert!(SpaceFingerprint::decode("").is_none());
        assert!(SpaceFingerprint::decode("not base64!").is_none());
        assert!(SpaceFingerprint::decode("AAAA====").is_none());
    }

    #[tokio::test]
    async fn derivation_is_stable_per_space_and_distinct_across_spaces() {
        let builder = Arc::new(
            kitsune2_core::default_test_builder()
                .with_default_config()
                .unwrap(),
        );
        let alpha = SpaceId::from(Bytes::from_static(b"alpha"));
        let beta = SpaceId::from(Bytes::from_static(b"beta"));
        let a = SpaceFingerprint::derive(&builder, &alpha).await.unwrap();
        let a2 = SpaceFingerprint::derive(&builder, &alpha).await.unwrap();
        let b = SpaceFingerprint::derive(&builder, &beta).await.unwrap();
        assert_eq!(a, a2);
        assert_ne!(a, b);
        assert_ne!(a.0, alpha.0.0, "the fingerprint is not the space id");
    }
}
