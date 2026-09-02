#![deny(missing_docs)]
//! mDNS-based LAN bootstrap for Kitsune2.
//!
//! This crate announces Kitsune2 agents on the local network and discovers
//! peers in the same space — **without** leaking the raw [`SpaceId`](kitsune2_api::SpaceId).
//! Only a commitment to the space (`H(space_id || tag)`) is broadcast over
//! mDNS. After discovery, peers complete a mutual proof-of-knowledge
//! handshake over a peer-to-peer channel before exchanging signed agent
//! info, which then flows into the peer store through the normal
//! [`BootstrapFactory`](kitsune2_api::BootstrapFactory) interface.
//!
//! ## Layering
//!
//! This crate is transport-independent — it uses `mdns-sd` directly rather
//! than piggy-backing on iroh's mDNS discovery. That keeps it usable over
//! any kitsune2 transport and isolates it from iroh API churn. The price
//! is one extra mDNS service record broadcast on the LAN when the iroh
//! transport is also using its own mDNS discovery for dialability — those
//! broadcasts are cheap and the separation of concerns is worth it.
//!
//! ## Privacy
//!
//! - The raw `SpaceId` is never sent over mDNS. Only the 32-byte
//!   fingerprint is.
//! - An adversary with a pre-existing list of candidate space ids can
//!   precompute fingerprints and confirm presence. This is an inherent
//!   limit of any discovery protocol that must match on a shared
//!   identifier. Time-bucketed fingerprints can raise the cost in a
//!   future iteration.
//! - mDNS is unauthenticated. The handshake protocol's HMAC step is what
//!   guarantees both sides actually share the space secret before any
//!   signed agent info is exchanged.

pub mod config;
pub mod discovery;
pub mod proto;
pub mod session;

mod factory;
pub use factory::*;
