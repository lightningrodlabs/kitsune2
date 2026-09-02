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
//! a spoofed announcement can at most cause bounded dials to a peer that
//! then fails the access exchange: once when the announcement is first
//! heard, then at most once per `redialIntervalMs` with exponential backoff
//! for as long as it stays announced and unconnected, under the
//! `maxConcurrentDials` in-flight cap.
//!
//! ## Wiring
//!
//! The factory reports honestly when its daemon cannot start. Whether a
//! space may run without LAN discovery is decided where the bootstrap stack
//! is assembled, and the intended wiring — the one Holochain uses — makes
//! the WAN bootstrap mandatory and the LAN one optional:
//!
//! ```ignore
//! CompositeBootstrapFactory::create(vec![
//!     CoreBootstrapFactory::create(),
//!     OptionalBootstrapFactory::create(MdnsBootstrapFactory::create()),
//! ])
//! ```
//!
//! One factory keeps one mDNS daemon per service type, shared by every
//! space announcing under it: one browse, one reconciliation ticker, one
//! hostname.
//!
//! ## Privacy
//!
//! - The raw `SpaceId` is never sent over mDNS. Only the
//!   [`fingerprint`] is, alongside the peer URL, which is public by
//!   nature. The fingerprint is key material derived from the space secret
//!   for this one purpose — the same property the hello proof rests on — so
//!   a non-member cannot compute it.
//! - When the host configures no space secret, kitsune2 uses the space id
//!   as the secret, and an adversary with a list of candidate space ids can
//!   then compute fingerprints and confirm presence. That is a limit of the
//!   open-space default, not of the announcement.
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

pub mod config;
pub mod fingerprint;

mod browse;
mod dials;
mod discovery;
mod factory;
mod shared;
mod space;

pub use factory::MdnsBootstrapFactory;

#[cfg(test)]
mod test_support;
