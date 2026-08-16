//! Space secret implementations.
//!
//! A [`SpaceSecret`] hands kitsune2 purpose-scoped key material derived from a
//! host-held space secret. The access module proves knowledge of that material
//! to gain access to a space.
//!
//! # Status
//!
//! This module currently only provides [`NoopSpaceSecret`], a placeholder that
//! returns a fixed key for every space and purpose. It exists so that the
//! [`Builder::space_secret`] slot can be populated while the real
//! implementation is being built. It is **not** a meaningful access control
//! mechanism: every space derives the same key, so any two nodes running it
//! will always be able to prove knowledge to each other.
//!
//! `CoreSpaceSecret`, which derives keys with `HKDF-SHA256` from a
//! configured per-space secret (defaulting to the space id, i.e. today's
//! "open to anyone who knows the space id" semantics), replaces this as the
//! registered default in both the test and production builders.

use bytes::Bytes;
use kitsune2_api::*;
use std::sync::Arc;

/// A placeholder [`SpaceSecretFactory`] that produces [`NoopSpaceSecret`].
///
/// See the [module docs](self) for why this exists and why it is not an
/// access control mechanism.
#[derive(Debug)]
pub struct NoopSpaceSecretFactory {}

impl NoopSpaceSecretFactory {
    /// Construct a new `NoopSpaceSecretFactory`.
    pub fn create() -> DynSpaceSecretFactory {
        let out: DynSpaceSecretFactory = Arc::new(Self {});
        out
    }
}

impl SpaceSecretFactory for NoopSpaceSecretFactory {
    fn default_config(&self, _config: &mut Config) -> K2Result<()> {
        Ok(())
    }

    fn validate_config(&self, _config: &Config) -> K2Result<()> {
        Ok(())
    }

    fn create(
        &self,
        _builder: Arc<Builder>,
        _space_id: SpaceId,
    ) -> BoxFut<'static, K2Result<DynSpaceSecret>> {
        Box::pin(async move {
            let out: DynSpaceSecret = Arc::new(NoopSpaceSecret);
            Ok(out)
        })
    }
}

/// A placeholder [`SpaceSecret`] that returns a fixed key.
///
/// See the [module docs](self) for why this exists and why it is not an
/// access control mechanism.
#[derive(Debug)]
pub struct NoopSpaceSecret;

/// The fixed key returned by [`NoopSpaceSecret::derive_key`].
const NOOP_KEY: &[u8] = b"kitsune2-noop-space-secret-key--";

impl SpaceSecret for NoopSpaceSecret {
    fn derive_key(
        &self,
        _space_id: SpaceId,
        _purpose: &str,
    ) -> BoxFut<'static, K2Result<Bytes>> {
        Box::pin(async move { Ok(Bytes::from_static(NOOP_KEY)) })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn noop_secret_returns_fixed_key() {
        let secret = NoopSpaceSecret;
        let a = secret
            .derive_key(SpaceId::from(Bytes::from_static(b"a")), "purpose-a")
            .await
            .unwrap();
        let b = secret
            .derive_key(SpaceId::from(Bytes::from_static(b"b")), "purpose-b")
            .await
            .unwrap();
        assert_eq!(a, b);
        assert_eq!(a, Bytes::from_static(NOOP_KEY));
    }
}
