//! Backend-agnostic re-exports of the Reticulum type surface used by
//! this crate.
//!
//! Both supported Reticulum implementations — LXMF-rs (`rns_transport`)
//! and Beechat (`reticulum`) — expose structurally identical types for
//! hashes, identities, and destination names (same Reticulum protocol,
//! same crypto). Modules outside [`backend_lxmf`](crate::backend_lxmf)
//! and [`backend_beechat`](crate::backend_beechat) import from here
//! instead of reaching into either backend crate directly, so the
//! backend choice is a single flip of a `#[cfg]` arm.

#[cfg(feature = "backend-lxmf")]
pub(crate) use rns_transport::destination::DestinationName;
#[cfg(feature = "backend-lxmf")]
pub(crate) use rns_transport::hash::AddressHash;
#[cfg(feature = "backend-lxmf")]
pub(crate) use rns_transport::identity::{Identity, PrivateIdentity};

#[cfg(feature = "backend-beechat")]
pub(crate) use reticulum::destination::DestinationName;
#[cfg(feature = "backend-beechat")]
pub(crate) use reticulum::hash::AddressHash;
#[cfg(feature = "backend-beechat")]
pub(crate) use reticulum::identity::{Identity, PrivateIdentity};
