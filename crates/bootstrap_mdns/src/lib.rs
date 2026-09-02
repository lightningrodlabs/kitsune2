#![deny(missing_docs)]
//! mDNS-based LAN peer discovery for Kitsune2.
//!
//! This crate lets Kitsune2 nodes on the same local network find each other
//! without a bootstrap server, and without telling the LAN which spaces they
//! are in. It is a [`BootstrapFactory`](kitsune2_api::BootstrapFactory), but
//! a deliberately narrow one: it announces this node's peer URL under a
//! commitment to the space, and dials the peers it hears announcing the
//! same commitment. That is all.
//!
//! ## What happens after a dial
//!
//! A dial makes the transport open a connection and run its preflight. From
//! there the space's access module takes over: it challenges the new peer to
//! prove knowledge of the space's secret, proves the same in return, and only
//! then are signed agent infos exchanged and stored. This crate never inserts
//! anything into the peer store and never sees an agent info from the LAN, so
//! a spoofed announcement can at most cause one rate-limited dial to a peer
//! that then fails the access exchange.
//!
//! ## Privacy
//!
//! - The raw `SpaceId` is never sent over mDNS. Only the 32-byte
//!   [`fingerprint`] is, alongside the peer URL, which is public
//!   by nature.
//! - An adversary with a pre-existing list of candidate space ids can
//!   precompute fingerprints and confirm presence. This is an inherent
//!   limit of any discovery protocol that must match on a shared
//!   identifier.
//! - mDNS is unauthenticated, which is why discovery decides nothing on
//!   its own: membership is established by the access module over the
//!   authenticated transport connection.
//!
//! ## Layering
//!
//! This crate uses `mdns-sd` directly rather than piggy-backing on the
//! transport's own LAN discovery. The transport's discovery makes a peer
//! *dialable* by its transport id; this crate is what tells a node *which*
//! peers are worth dialling for a given space. Keeping the two apart keeps
//! this crate usable over any transport and out of the way of transport
//! API churn.

pub mod browse;
pub mod config;
pub mod dial_policy;
pub mod discovery;
pub mod fingerprint;

mod factory;
pub use factory::*;
