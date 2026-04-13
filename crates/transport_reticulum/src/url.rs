//! URL <-> identity-hash conversion for the `ret://` scheme.
//!
//! Canonical form: `ret://reticulum:1/<identity-hash-hex>`
//!
//! The host (`reticulum`) and port (`1`) are constants -- Reticulum routes
//! by destination hash, not by IP. The path carries the peer's stable
//! Identity address hash as a lowercase hex string.

use crate::types::AddressHash;
use kitsune2_api::{K2Error, K2Result, Url};

/// The constant authority used in `ret://` URLs.
const RET_AUTHORITY: &str = "reticulum:1";

/// Convert an identity address hash to a `ret://` URL.
pub(crate) fn identity_hash_to_url(hash: &AddressHash) -> K2Result<Url> {
    let s = format!("ret://{RET_AUTHORITY}/{}", hash.to_hex_string());
    Url::from_str(&s)
}

/// Extract the identity address hash from a `ret://` URL.
pub(crate) fn url_to_identity_hash(url: &Url) -> K2Result<AddressHash> {
    let s = url.as_str();
    let prefix = format!("ret://{RET_AUTHORITY}/");
    let hex_str = s.strip_prefix(&prefix).ok_or_else(|| {
        K2Error::other(format!("URL does not match ret:// canonical form: {s}"))
    })?;
    if hex_str.len() != 32 {
        return Err(K2Error::other(format!(
            "Identity hash hex must be 32 chars, got {}",
            hex_str.len()
        )));
    }
    AddressHash::new_from_hex_string(hex_str).map_err(|e| {
        K2Error::other(format!("Invalid identity hash hex: {e:?}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let hash = AddressHash::new([
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba,
            0x98, 0x76, 0x54, 0x32, 0x10,
        ]);
        let url = identity_hash_to_url(&hash).unwrap();
        assert_eq!(
            url.as_str(),
            "ret://reticulum:1/0123456789abcdeffedcba9876543210"
        );
        let decoded = url_to_identity_hash(&url).unwrap();
        assert_eq!(decoded, hash);
    }

    #[test]
    fn wrong_scheme() {
        let url = Url::from_str(
            "http://reticulum:1/0123456789abcdeffedcba9876543210",
        )
        .unwrap();
        assert!(url_to_identity_hash(&url).is_err());
    }

    #[test]
    fn wrong_length() {
        let url = Url::from_str("ret://reticulum:1/0123").unwrap();
        assert!(url_to_identity_hash(&url).is_err());
    }
}
