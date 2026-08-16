//! Space secret implementations.
//!
//! A [`SpaceSecret`] hands kitsune2 purpose-scoped key material derived from a
//! host-held space secret. The access module proves knowledge of that material
//! to gain access to a space.

use base64::prelude::*;
use bytes::Bytes;
use hkdf::Hkdf;
use kitsune2_api::*;
use sha2::Sha256;
use std::sync::Arc;

/// The length in bytes of the key material [`CoreSpaceSecret`] derives.
pub const CORE_SPACE_SECRET_KEY_LEN: usize = 32;

/// CoreSpaceSecret configuration types.
mod config {
    /// Configuration parameters for [CoreSpaceSecretFactory](super::CoreSpaceSecretFactory).
    #[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
    #[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
    #[serde(rename_all = "camelCase")]
    pub struct CoreSpaceSecretConfig {
        /// The space secret, base64 encoded (url-safe, no padding).
        ///
        /// This is the root secret knowledge of which admits a peer to the
        /// space. Because a builder's config applies to every space it
        /// creates, a host running more than one space must supply this
        /// through the per-space config override accepted by
        /// [`SpaceFactory::create`](kitsune2_api::SpaceFactory::create).
        ///
        /// Default: unset, which means the secret is the space id itself.
        /// That makes the space "open to anyone who knows the space id",
        /// which is kitsune2's historical behaviour and remains the default.
        #[serde(default)]
        #[cfg_attr(feature = "schema", schemars(default))]
        pub secret: Option<String>,
    }

    /// Module-level configuration for CoreSpaceSecret.
    #[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
    #[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
    #[serde(rename_all = "camelCase")]
    pub struct CoreSpaceSecretModConfig {
        /// CoreSpaceSecret configuration.
        ///
        /// Defaulted, so that a config which has never had defaults applied
        /// to it still validates as "no secret configured".
        #[serde(default)]
        #[cfg_attr(feature = "schema", schemars(default))]
        pub core_space_secret: CoreSpaceSecretConfig,
    }
}

pub use config::*;

/// A production-ready [`SpaceSecretFactory`] that produces [`CoreSpaceSecret`].
#[derive(Debug)]
pub struct CoreSpaceSecretFactory {}

impl CoreSpaceSecretFactory {
    /// Construct a new `CoreSpaceSecretFactory`.
    pub fn create() -> DynSpaceSecretFactory {
        let out: DynSpaceSecretFactory = Arc::new(Self {});
        out
    }
}

impl SpaceSecretFactory for CoreSpaceSecretFactory {
    fn default_config(&self, config: &mut Config) -> K2Result<()> {
        config.set_module_config(&CoreSpaceSecretModConfig::default())
    }

    fn validate_config(&self, config: &Config) -> K2Result<()> {
        let config: CoreSpaceSecretModConfig = config.get_module_config()?;
        if let Some(secret) = &config.core_space_secret.secret {
            decode_secret(secret)?;
        }
        Ok(())
    }

    fn create(
        &self,
        builder: Arc<Builder>,
        space_id: SpaceId,
    ) -> BoxFut<'static, K2Result<DynSpaceSecret>> {
        Box::pin(async move {
            let config: CoreSpaceSecretModConfig =
                builder.config.get_module_config()?;
            let out: DynSpaceSecret = Arc::new(CoreSpaceSecret::new(
                config.core_space_secret,
                space_id,
            )?);
            Ok(out)
        })
    }
}

/// A [`SpaceSecret`] that derives purpose-scoped key material from a
/// configured per-space secret with `HKDF-SHA256`.
///
/// The secret comes from [`CoreSpaceSecretConfig::secret`]. When that is unset
/// the space id is used as the secret, which makes the space open to anyone
/// who knows the space id — kitsune2's historical semantics, and the default.
///
/// Derivation is
/// `HKDF-SHA256(salt = space_id, ikm = secret, info = purpose)`, expanded to
/// [`CORE_SPACE_SECRET_KEY_LEN`] bytes. Every purpose therefore yields
/// independent key material: disclosing one derived key reveals neither the
/// secret nor any other derived key.
#[derive(Debug)]
pub struct CoreSpaceSecret {
    secret: Bytes,
}

impl CoreSpaceSecret {
    /// Construct a `CoreSpaceSecret` for a single space.
    pub fn new(
        config: CoreSpaceSecretConfig,
        space_id: SpaceId,
    ) -> K2Result<Self> {
        let secret = match &config.secret {
            Some(secret) => decode_secret(secret)?,
            None => {
                tracing::debug!(
                    ?space_id,
                    "No space secret configured, using the space id as the secret"
                );
                space_id.0.0.clone()
            }
        };
        Ok(Self { secret })
    }
}

impl SpaceSecret for CoreSpaceSecret {
    fn derive_key(
        &self,
        space_id: SpaceId,
        purpose: &str,
    ) -> BoxFut<'static, K2Result<Bytes>> {
        let secret = self.secret.clone();
        let purpose = purpose.to_string();
        Box::pin(async move {
            let hkdf = Hkdf::<Sha256>::new(Some(&space_id.0.0), &secret);
            let mut out = [0_u8; CORE_SPACE_SECRET_KEY_LEN];
            hkdf.expand(purpose.as_bytes(), &mut out).map_err(|err| {
                K2Error::other_src("Failed to derive space key material", err)
            })?;
            Ok(Bytes::copy_from_slice(&out))
        })
    }
}

fn decode_secret(secret: &str) -> K2Result<Bytes> {
    let decoded =
        BASE64_URL_SAFE_NO_PAD.decode(secret).map_err(|err| {
            K2Error::other_src(
                "Failed to decode the configured space secret, expected url-safe base64 without padding",
                err,
            )
        })?;
    if decoded.is_empty() {
        return Err(K2Error::other("The configured space secret is empty"));
    }
    Ok(Bytes::from(decoded))
}

/// Encode a raw space secret for [`CoreSpaceSecretConfig::secret`].
pub fn encode_space_secret(secret: &[u8]) -> String {
    BASE64_URL_SAFE_NO_PAD.encode(secret)
}

/// A placeholder [`SpaceSecretFactory`] that produces [`NoopSpaceSecret`].
///
/// See [`NoopSpaceSecret`] for why this exists and why it is not an access
/// control mechanism.
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

/// A placeholder [`SpaceSecret`] that returns a fixed key for every space and
/// every purpose.
///
/// It is **not** a meaningful access control mechanism: every space derives the
/// same key, so any two nodes running it will always be able to prove knowledge
/// to each other. Prefer [`CoreSpaceSecret`], which is the registered default
/// in the test builder and is what production builders should use.
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

    const SPACE_A: SpaceId = SpaceId(Id(Bytes::from_static(b"space-a")));
    const SPACE_B: SpaceId = SpaceId(Id(Bytes::from_static(b"space-b")));

    fn default_secret(space_id: SpaceId) -> CoreSpaceSecret {
        CoreSpaceSecret::new(CoreSpaceSecretConfig::default(), space_id)
            .unwrap()
    }

    fn configured_secret(space_id: SpaceId, secret: &[u8]) -> CoreSpaceSecret {
        CoreSpaceSecret::new(
            CoreSpaceSecretConfig {
                secret: Some(encode_space_secret(secret)),
            },
            space_id,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn derive_key_is_deterministic() {
        let a = default_secret(SPACE_A)
            .derive_key(SPACE_A, "k2-hello-v1")
            .await
            .unwrap();
        let b = default_secret(SPACE_A)
            .derive_key(SPACE_A, "k2-hello-v1")
            .await
            .unwrap();
        assert_eq!(a, b);
        assert_eq!(a.len(), CORE_SPACE_SECRET_KEY_LEN);
    }

    #[tokio::test]
    async fn purposes_derive_independent_keys() {
        let secret = default_secret(SPACE_A);
        let hello = secret.derive_key(SPACE_A, "k2-hello-v1").await.unwrap();
        let bootstrap = secret
            .derive_key(SPACE_A, "k2-bootstrap-auth-v1")
            .await
            .unwrap();
        assert_ne!(hello, bootstrap);
    }

    #[tokio::test]
    async fn spaces_derive_independent_keys() {
        // Two spaces with the default (space id) secret.
        let a = default_secret(SPACE_A)
            .derive_key(SPACE_A, "k2-hello-v1")
            .await
            .unwrap();
        let b = default_secret(SPACE_B)
            .derive_key(SPACE_B, "k2-hello-v1")
            .await
            .unwrap();
        assert_ne!(a, b);

        // And two spaces sharing one configured secret still derive
        // different key material, because the space id is the salt.
        let a = configured_secret(SPACE_A, b"shared")
            .derive_key(SPACE_A, "k2-hello-v1")
            .await
            .unwrap();
        let b = configured_secret(SPACE_B, b"shared")
            .derive_key(SPACE_B, "k2-hello-v1")
            .await
            .unwrap();
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn different_secrets_derive_different_keys() {
        let right = configured_secret(SPACE_A, b"the-right-secret")
            .derive_key(SPACE_A, "k2-hello-v1")
            .await
            .unwrap();
        let wrong = configured_secret(SPACE_A, b"the-wrong-secret")
            .derive_key(SPACE_A, "k2-hello-v1")
            .await
            .unwrap();
        assert_ne!(right, wrong);

        // The default (space id) secret is just another secret, so it must
        // not collide with a configured one either.
        let default = default_secret(SPACE_A)
            .derive_key(SPACE_A, "k2-hello-v1")
            .await
            .unwrap();
        assert_ne!(default, right);
    }

    #[test]
    fn invalid_configured_secrets_are_rejected() {
        assert!(
            CoreSpaceSecret::new(
                CoreSpaceSecretConfig {
                    secret: Some("not base64!!".to_string()),
                },
                SPACE_A,
            )
            .is_err()
        );
        assert!(
            CoreSpaceSecret::new(
                CoreSpaceSecretConfig {
                    secret: Some(String::new()),
                },
                SPACE_A,
            )
            .is_err()
        );
    }

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
