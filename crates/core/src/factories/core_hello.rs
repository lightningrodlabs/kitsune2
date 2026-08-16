//! The hello module: a space-scoped proof-of-knowledge (PoK) access module.
//!
//! The hello module answers one question about a peer URL: *does the peer at
//! this URL know this space's secret?* A peer that can prove it does is
//! granted access to the space and has its agent infos accepted; a peer that
//! cannot is never gossiped with, never served fetch requests, and never told
//! who the members are.
//!
//! Because it is what produces access decisions, its own messages are exempt
//! from the access gate, in the same way preflight messages are. Its module id
//! is [`HELLO_MOD_NAME`](kitsune2_api::HELLO_MOD_NAME).
//!
//! # Wire protocol
//!
//! Four messages, two round trips:
//!
//! ```text
//! Initiate  { proto_ver, nonce_i }                       I -> R
//! Respond   { proto_ver, nonce_r, proof_r }              R -> I
//! Confirm   { proof_i, agent_infos_i }                   I -> R
//! Ack       { agent_infos_r }                            R -> I
//! ```
//!
//! Proofs are `HMAC-SHA256(k_hello, T)`, where `k_hello` is the space's
//! `"k2-hello-v1"` derived key and `T` is a transcript that binds both nonces
//! and both authenticated peer ids:
//!
//! ```text
//! T_r = HELLO_PROOF_TAG || nonce_r || nonce_i || peer_id_r || peer_id_i
//! T_i = HELLO_PROOF_TAG || nonce_i || nonce_r || peer_id_i || peer_id_r
//! ```
//!
//! (See [`transcript`] for the exact framing; the variable-length fields are
//! length-prefixed rather than bare-concatenated.)
//!
//! Protocol rules:
//!
//! - Nonces are fresh 32-byte values per exchange, never reused.
//! - Self-nonce-first ordering makes the two proofs distinct bytes, which
//!   prevents an attacker from reflecting a proof back at its author.
//! - Proofs bind both **peer ids** — the [`Url::peer_id`](kitsune2_api::Url::peer_id)
//!   path segment, which the transport authenticates at the connection layer —
//!   and never full URLs. This is what defeats relaying a proof obtained from
//!   an honest member. Full URLs cannot be used: a node legitimately holds
//!   several at once and the two sides will not reliably agree on one.
//! - Agent infos are disclosed only *after* verifying the counterparty's
//!   proof. The responder proves first and discloses nothing; the initiator
//!   proves and discloses in `Confirm`; the responder discloses in `Ack`.
//! - The verifying side must take the peer id from the `peer` URL the
//!   transport passed to the module handler, never from message contents.

mod protocol;
pub use protocol::*;

mod proto_helpers;
pub use proto_helpers::*;
