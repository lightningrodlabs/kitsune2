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
//! `"k2-hello-v1"` derived key and `T` is a transcript that binds the protocol
//! version, both nonces and both authenticated peer ids:
//!
//! ```text
//! T_r = HELLO_PROOF_TAG || proto_ver || nonce_r || nonce_i || peer_id_r || peer_id_i
//! T_i = HELLO_PROOF_TAG || proto_ver || nonce_i || nonce_r || peer_id_i || peer_id_r
//! ```
//!
//! (See [`transcript`] for the exact framing; the variable-length fields are
//! length-prefixed rather than bare-concatenated.)
//!
//! Protocol rules:
//!
//! - Nonces are fresh 32-byte values per exchange, never reused.
//! - The protocol version bound into a proof is the one the *prover*
//!   advertised, and the version a verifier binds is the one it read out of
//!   the message it is verifying. A version rewritten in flight therefore
//!   makes the proofs disagree instead of silently downgrading the exchange.
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
//!
//! # State machine
//!
//! One exchange is in flight per peer URL, in one of the roles below. Any
//! state that does not reach `Granted` within
//! [`CoreHelloConfig::exchange_timeout_ms`] is dropped and retried on a
//! backoff.
//!
//! ```mermaid
//! stateDiagram-v2
//!     [*] --> Challenging: trigger fires, we send Initiate
//!     [*] --> Responding: Initiate received, we send Respond
//!     Challenging --> Responding: simultaneous initiate,\nour peer id is higher
//!     Challenging --> AwaitingAck: Respond verified,\nwe grant and send Confirm
//!     Responding --> [*]: Confirm verified,\nwe grant and send Ack
//!     AwaitingAck --> [*]: Ack received,\nagent infos stored
//!     Challenging --> [*]: proof invalid or timed out
//!     Responding --> [*]: proof invalid or timed out
//! ```
//!
//! # Triggers
//!
//! An exchange is initiated when a local agent joins the space (toward every
//! URL in the peer store and every connected peer), when a new URL appears in
//! the peer store with no access decision, and when an incoming message from
//! an ungranted peer is dropped by the enforcement path. The last of those is
//! what heals asymmetric access state: grant state is in-memory and
//! restart-lossy, so without it a peer that forgot us would stay silently
//! deaf until the next join. Explicitly blocked peers never trigger an
//! exchange, because the denylist always wins.
//!
//! Rate limiting is the per-URL exchange state itself: at most one exchange
//! is in flight per URL, at most
//! [`CoreHelloConfig::max_concurrent_exchanges`] in total, and a failed
//! exchange gates the next attempt behind a doubling backoff between
//! [`CoreHelloConfig::retry_backoff_min_ms`] and
//! [`CoreHelloConfig::retry_backoff_max_ms`]. A fresh agent info for a URL
//! clears that backoff, so an unresponsive peer is retried as soon as it
//! shows signs of life.

mod protocol;
pub use protocol::*;

mod proto_helpers;
pub use proto_helpers::*;

mod proof;
pub use proof::*;

#[cfg(test)]
mod test;

use kitsune2_api::*;
use rand::Rng;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

/// The purpose the hello module derives its key material for.
///
/// See [`SpaceSecret::derive_key`].
pub const HELLO_KEY_PURPOSE: &str = "k2-hello-v1";

/// A source for this node's current URL in a space, if it has one yet.
///
/// The hello module reads this every time it needs to bind a transcript, so
/// that a URL learned or changed after construction is picked up.
pub type CurrentUrlFn = Arc<dyn Fn() -> Option<Url> + 'static + Send + Sync>;

/// CoreHello configuration types.
mod config {
    /// Configuration parameters for [CoreHello](super::CoreHello).
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    #[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
    #[serde(rename_all = "camelCase")]
    pub struct CoreHelloConfig {
        /// How long in millis an exchange may stay in flight before it is
        /// abandoned and retried on a backoff.
        ///
        /// Default: 10s.
        #[cfg_attr(feature = "schema", schemars(default))]
        pub exchange_timeout_ms: u32,

        /// The backoff in millis before the first retry toward a peer whose
        /// exchange failed or went unanswered.
        ///
        /// Default: 30s.
        #[cfg_attr(feature = "schema", schemars(default))]
        pub retry_backoff_min_ms: u32,

        /// The ceiling in millis for the doubling retry backoff.
        ///
        /// Default: 5m.
        #[cfg_attr(feature = "schema", schemars(default))]
        pub retry_backoff_max_ms: u32,

        /// The most exchanges that may be in flight at once.
        ///
        /// This bounds the state an attacker can make us hold. Triggers that
        /// arrive while the limit is reached are dropped rather than queued;
        /// the peers they name are challenged again by a later trigger.
        ///
        /// Default: 32.
        #[cfg_attr(feature = "schema", schemars(default))]
        pub max_concurrent_exchanges: u32,
    }

    impl Default for CoreHelloConfig {
        fn default() -> Self {
            Self {
                exchange_timeout_ms: 10_000,
                retry_backoff_min_ms: 30_000,
                retry_backoff_max_ms: 300_000,
                max_concurrent_exchanges: 32,
            }
        }
    }

    impl CoreHelloConfig {
        /// Get exchange_timeout_ms as a [std::time::Duration].
        pub fn exchange_timeout(&self) -> std::time::Duration {
            std::time::Duration::from_millis(self.exchange_timeout_ms as u64)
        }

        /// Get retry_backoff_min_ms as a [std::time::Duration].
        pub fn retry_backoff_min(&self) -> std::time::Duration {
            std::time::Duration::from_millis(self.retry_backoff_min_ms as u64)
        }

        /// Get retry_backoff_max_ms as a [std::time::Duration].
        pub fn retry_backoff_max(&self) -> std::time::Duration {
            std::time::Duration::from_millis(self.retry_backoff_max_ms as u64)
        }
    }

    /// Module-level configuration for CoreHello.
    #[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
    #[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
    #[serde(rename_all = "camelCase")]
    pub struct CoreHelloModConfig {
        /// CoreHello configuration.
        pub core_hello: CoreHelloConfig,
    }
}

pub use config::*;

/// The role we hold in the one exchange that may be in flight toward a peer.
#[derive(Debug)]
enum Exchange {
    /// We sent an `Initiate` and are waiting for a `Respond`.
    Challenging {
        /// The nonce we sent.
        our_nonce: HelloNonce,
    },

    /// We answered an `Initiate` and are waiting for a `Confirm`.
    Responding {
        /// The nonce we sent.
        our_nonce: HelloNonce,

        /// The nonce the peer sent.
        their_nonce: HelloNonce,

        /// The protocol version the peer advertised in its `Initiate`. The
        /// peer's proof binds it, so it has to be remembered until the
        /// `Confirm` that carries that proof arrives.
        their_proto_ver: u32,
    },

    /// We verified a `Respond` and sent a `Confirm`, and are waiting for the
    /// `Ack` that carries the peer's agent infos. The peer is already granted
    /// at this point.
    AwaitingAck,
}

/// How we answer an incoming `Initiate`.
enum Answer {
    /// Repeat our own `Initiate`, because we hold the initiator role.
    Initiate(HelloNonce),

    /// Answer as responder, with the given nonce of ours.
    Respond(HelloNonce),
}

/// What we remember about one peer URL.
#[derive(Debug)]
struct PeerExchange {
    /// The exchange in flight, and when it started.
    exchange: Option<(Exchange, Instant)>,

    /// The earliest instant at which we may initiate toward this peer again.
    retry_after: Option<Instant>,

    /// The backoff to apply the next time an exchange with this peer fails.
    backoff: Duration,
}

impl PeerExchange {
    fn new(backoff: Duration) -> Self {
        Self {
            exchange: None,
            retry_after: None,
            backoff,
        }
    }

    /// True if there is nothing left worth remembering about this peer.
    fn is_idle(&self) -> bool {
        self.exchange.is_none() && self.retry_after.is_none()
    }
}

/// The hello module: the access module for a space.
///
/// See the `core_hello` module documentation for the wire protocol, the state
/// machine and the triggers.
#[derive(Debug)]
pub struct CoreHello {
    inner: Arc<HelloInner>,
    task: tokio::task::AbortHandle,
}

impl Drop for CoreHello {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl CoreHello {
    /// Construct a hello module for a space and register it as the space's
    /// access module on the transport.
    ///
    /// `current_url` is read whenever a transcript is built, so it must
    /// report the URL the transport currently reaches this node at for this
    /// space.
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        config: CoreHelloConfig,
        space_id: SpaceId,
        verifier: DynVerifier,
        space_secret: DynSpaceSecret,
        peer_store: DynPeerStore,
        local_agent_store: DynLocalAgentStore,
        access_state: DynPeerAccessState,
        transport: DynTransport,
        current_url: CurrentUrlFn,
    ) -> K2Result<Arc<Self>> {
        let hello_key = space_secret
            .derive_key(space_id.clone(), HELLO_KEY_PURPOSE)
            .await?;

        let inner = Arc::new(HelloInner {
            config: config.clone(),
            space_id: space_id.clone(),
            hello_key,
            verifier,
            peer_store: peer_store.clone(),
            local_agent_store,
            access_state,
            transport: Arc::downgrade(&transport),
            current_url,
            state: Mutex::new(HashMap::new()),
        });

        transport.register_module_handler(
            space_id,
            HELLO_MOD_NAME.to_string(),
            Arc::new(HelloMessageHandler {
                inner: inner.clone(),
            }),
        );

        // A new URL in the peer store is a peer we may not have met yet, and
        // a fresh agent info for a URL we failed to reach is a reason to try
        // it again right away.
        peer_store.register_peer_update_listener(Arc::new({
            let inner = Arc::downgrade(&inner);
            move |agent_info| {
                let inner = inner.clone();
                Box::pin(async move {
                    let Some(inner) = inner.upgrade() else {
                        return;
                    };
                    // Spawned rather than awaited: this listener runs inline
                    // in `PeerStore::insert`, and handling an exchange
                    // inserts into the peer store.
                    tokio::task::spawn(async move {
                        inner.peer_updated(agent_info).await;
                    });
                })
            }
        }))?;

        let task = tokio::task::spawn(expire_and_retry_task(
            inner.clone(),
            tick_interval(&config),
        ))
        .abort_handle();

        Ok(Arc::new(Self { inner, task }))
    }

    /// Notify the module that a local agent joined the space.
    ///
    /// This challenges every URL in the peer store and every connected peer,
    /// which is what introduces this node to peers we are already connected
    /// to but have never spoken to in this space.
    pub fn notify_local_agent_join(&self) {
        let inner = self.inner.clone();
        tokio::task::spawn(async move {
            // Wait for the joining agent's info to be signed and stored, so
            // that the exchange has something to disclose. Bounded, because
            // challenging without an info to offer is still better than never
            // challenging at all.
            inner.await_local_agent_infos().await;
            inner.sweep().await;
        });
    }

    /// Notify the module that an incoming message from an ungranted peer was
    /// dropped.
    ///
    /// This must not be called for explicitly blocked peers.
    pub fn notify_ungranted_message_dropped(&self, peer_url: Url) {
        let inner = self.inner.clone();
        tokio::task::spawn(async move {
            inner.initiate(peer_url, false).await;
        });
    }
}

/// Tick often enough that a timed-out exchange is noticed promptly, without
/// waking up pointlessly on a long timeout.
fn tick_interval(config: &CoreHelloConfig) -> Duration {
    (config.exchange_timeout() / 4).max(Duration::from_millis(25))
}

async fn expire_and_retry_task(inner: Arc<HelloInner>, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;
        inner.expire_and_retry().await;
    }
}

struct HelloInner {
    config: CoreHelloConfig,
    space_id: SpaceId,
    hello_key: bytes::Bytes,
    verifier: DynVerifier,
    peer_store: DynPeerStore,
    local_agent_store: DynLocalAgentStore,
    access_state: DynPeerAccessState,
    transport: WeakDynTransport,
    current_url: CurrentUrlFn,
    state: Mutex<HashMap<Url, PeerExchange>>,
}

impl std::fmt::Debug for HelloInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HelloInner")
            .field("space_id", &self.space_id)
            .finish()
    }
}

impl HelloInner {
    /// Challenge every URL we know of: everyone in the peer store, and
    /// everyone we hold a connection to.
    async fn sweep(&self) {
        let mut urls = std::collections::HashSet::new();

        match self.peer_store.get_all().await {
            Ok(infos) => {
                urls.extend(infos.iter().filter_map(|i| i.url.clone()))
            }
            Err(err) => {
                tracing::debug!(
                    ?err,
                    "Could not read the peer store to find peers to challenge"
                );
            }
        }

        // A transport is not obliged to be able to list its connections, and
        // a node that cannot is still perfectly usable, so this is tolerated
        // quietly.
        match self.transport.upgrade() {
            Some(transport) => match transport.get_connected_peers().await {
                Ok(peers) => urls.extend(peers),
                Err(err) => {
                    tracing::debug!(
                        ?err,
                        "Could not list connected peers to challenge"
                    );
                }
            },
            None => {
                tracing::debug!("Transport dropped, not challenging peers");
                return;
            }
        }

        for url in urls {
            self.initiate(url, false).await;
        }
    }

    /// Handle a peer store update.
    async fn peer_updated(&self, agent_info: Arc<AgentInfoSigned>) {
        let Some(peer_url) = agent_info.url.clone() else {
            return;
        };

        match self.access_state.get_access_decision(peer_url.clone()) {
            // A fresh agent info from a peer we have not managed to reach is
            // the signal to stop waiting out the backoff and try again.
            Ok(None) => self.initiate(peer_url, true).await,
            Ok(Some(_)) => (),
            Err(err) => {
                tracing::debug!(
                    ?err,
                    ?peer_url,
                    "Could not read the access decision for an updated peer"
                );
            }
        }
    }

    /// Open an exchange toward a peer, unless we should not.
    ///
    /// `clear_backoff` abandons any retry backoff for this peer first, which
    /// is what a fresh agent info for the peer means.
    async fn initiate(&self, peer_url: Url, clear_backoff: bool) {
        let Some(our_url) = (self.current_url)() else {
            tracing::debug!(
                ?peer_url,
                "Not challenging a peer, we have no url of our own yet"
            );
            return;
        };
        if peer_url == our_url {
            return;
        }

        match self.access_state.get_access_decision(peer_url.clone()) {
            Ok(Some(access)) => {
                // Both terminal decisions mean there is nothing to ask: a
                // blocked peer stays blocked no matter what it can prove, and
                // a granted peer has already proven it.
                match access.decision {
                    AccessDecision::Blocked => tracing::debug!(
                        ?peer_url,
                        "Not challenging an explicitly blocked peer"
                    ),
                    AccessDecision::Granted => (),
                }
                self.forget(&peer_url);
                return;
            }
            Ok(None) => (),
            Err(err) => {
                tracing::debug!(
                    ?err,
                    ?peer_url,
                    "Could not read the access decision for a peer to challenge"
                );
                return;
            }
        }

        let our_nonce = fresh_nonce();

        // Build the transcript we will need before recording any state, so
        // that a URL we cannot bind a proof to leaves nothing behind.
        let init = Initiate {
            proto_ver: HELLO_PROTO_VER,
            nonce_i: bytes::Bytes::copy_from_slice(&our_nonce),
        };

        {
            let mut state = self.state.lock().expect("poison");
            let now = Instant::now();
            let in_flight =
                state.values().filter(|e| e.exchange.is_some()).count();

            let entry = state.entry(peer_url.clone()).or_insert_with(|| {
                PeerExchange::new(self.config.retry_backoff_min())
            });

            if clear_backoff {
                entry.retry_after = None;
                entry.backoff = self.config.retry_backoff_min();
            }

            if entry.exchange.is_some() {
                tracing::debug!(
                    ?peer_url,
                    "Not challenging a peer, an exchange is already in flight"
                );
                return;
            }
            if let Some(retry_after) = entry.retry_after
                && retry_after > now
            {
                tracing::debug!(
                    ?peer_url,
                    "Not challenging a peer yet, waiting out the retry backoff"
                );
                return;
            }
            if in_flight >= self.config.max_concurrent_exchanges as usize {
                tracing::debug!(
                    ?peer_url,
                    "Not challenging a peer, too many exchanges are in flight"
                );
                if entry.is_idle() {
                    state.remove(&peer_url);
                }
                return;
            }

            entry.exchange = Some((Exchange::Challenging { our_nonce }, now));
        }

        tracing::debug!(?peer_url, "Opening a hello exchange");
        if let Err(err) = self.send(&peer_url, HelloMsg::Initiate(init)).await {
            tracing::debug!(?err, ?peer_url, "Could not send a hello initiate");
            self.fail(&peer_url);
        }
    }

    /// Handle an incoming hello message from a peer.
    async fn handle(&self, peer_url: Url, msg: HelloMsg) {
        let Some(our_url) = (self.current_url)() else {
            tracing::debug!(
                ?peer_url,
                "Dropping a hello message, we have no url of our own yet"
            );
            return;
        };

        // The denylist wins over anything a peer can prove.
        match self.access_state.get_access_decision(peer_url.clone()) {
            Ok(Some(access)) if access.decision == AccessDecision::Blocked => {
                tracing::debug!(
                    ?peer_url,
                    "Dropping a hello message from an explicitly blocked peer"
                );
                return;
            }
            Ok(_) => (),
            Err(err) => {
                tracing::debug!(
                    ?err,
                    ?peer_url,
                    "Could not read the access decision for a hello message"
                );
                return;
            }
        }

        match msg {
            HelloMsg::Initiate(init) => {
                self.handle_initiate(peer_url, our_url, init).await
            }
            HelloMsg::Respond(respond) => {
                self.handle_respond(peer_url, our_url, respond).await
            }
            HelloMsg::Confirm(confirm) => {
                self.handle_confirm(peer_url, our_url, confirm).await
            }
            HelloMsg::Ack(ack) => self.handle_ack(peer_url, ack).await,
        }
    }

    async fn handle_initiate(
        &self,
        peer_url: Url,
        our_url: Url,
        init: Initiate,
    ) {
        if init.proto_ver != HELLO_PROTO_VER {
            tracing::debug!(
                ?peer_url,
                proto_ver = init.proto_ver,
                "Dropping a hello initiate that speaks another protocol version"
            );
            return;
        }
        let Some(their_nonce) = to_nonce(&init.nonce_i) else {
            tracing::warn!(
                ?peer_url,
                len = init.nonce_i.len(),
                "Dropping a hello initiate with a malformed nonce"
            );
            return;
        };

        // Both peer ids must be available before anything else, because
        // every answer to an initiate is bound to them.
        let (Some(our_peer_id), Some(their_peer_id)) =
            (our_url.peer_id(), peer_url.peer_id())
        else {
            tracing::debug!(
                ?peer_url,
                "Abandoning a hello exchange that cannot be bound to a channel"
            );
            return;
        };

        let answer = {
            let mut state = self.state.lock().expect("poison");
            let now = Instant::now();
            let in_flight =
                state.values().filter(|e| e.exchange.is_some()).count();

            let entry = state.entry(peer_url.clone()).or_insert_with(|| {
                PeerExchange::new(self.config.retry_backoff_min())
            });

            match &entry.exchange {
                // Simultaneous initiate. Both sides compare the same two peer
                // ids the transcripts bind, so exactly one exchange survives.
                // The lower peer id keeps its initiator role; the higher one
                // drops its own exchange and answers as responder. Peer ids
                // are used rather than full URLs because a URL is not stable
                // across relay failover.
                Some((Exchange::Challenging { our_nonce }, _)) => {
                    if our_peer_id == their_peer_id {
                        tracing::warn!(
                            ?peer_url,
                            "Dropping a hello initiate from a peer claiming our own peer id"
                        );
                        return;
                    }
                    if our_peer_id.as_bytes() < their_peer_id.as_bytes() {
                        // We keep the initiator role. Our initiate is sent
                        // again rather than merely not answered, because the
                        // peer initiating at all is evidence it never saw the
                        // first one — which is exactly what happens when we
                        // challenged it for a space it had not joined yet.
                        // The nonce is unchanged, so a peer that did see the
                        // first one recognises this as the same exchange.
                        tracing::debug!(
                            ?peer_url,
                            "Answering a crossing hello initiate by repeating our own, we keep the initiator role"
                        );
                        Answer::Initiate(*our_nonce)
                    } else {
                        tracing::debug!(
                            ?peer_url,
                            "Abandoning our hello exchange for a crossing initiate, the peer keeps the initiator role"
                        );
                        let our_nonce = fresh_nonce();
                        entry.exchange = Some((
                            Exchange::Responding {
                                our_nonce,
                                their_nonce,
                                their_proto_ver: init.proto_ver,
                            },
                            now,
                        ));
                        Answer::Respond(our_nonce)
                    }
                }

                // The same initiate again, so our respond was lost. Repeat
                // it, with the nonce we are still waiting on a confirm for.
                Some((
                    Exchange::Responding {
                        our_nonce,
                        their_nonce: pending,
                        ..
                    },
                    _,
                )) if *pending == their_nonce => {
                    tracing::debug!(
                        ?peer_url,
                        "Repeating our hello respond for a repeated initiate"
                    );
                    Answer::Respond(*our_nonce)
                }

                // Anything else is a new exchange, which replaces whatever
                // we were doing with this peer: a peer that initiates has
                // forgotten the old exchange, so there is nothing left to
                // finish.
                _ => {
                    if entry.exchange.is_none()
                        && in_flight
                            >= self.config.max_concurrent_exchanges as usize
                    {
                        tracing::debug!(
                            ?peer_url,
                            "Dropping a hello initiate, too many exchanges are in flight"
                        );
                        if entry.is_idle() {
                            state.remove(&peer_url);
                        }
                        return;
                    }

                    let our_nonce = fresh_nonce();
                    entry.exchange = Some((
                        Exchange::Responding {
                            our_nonce,
                            their_nonce,
                            their_proto_ver: init.proto_ver,
                        },
                        now,
                    ));
                    Answer::Respond(our_nonce)
                }
            }
        };

        let msg = match answer {
            Answer::Initiate(our_nonce) => HelloMsg::Initiate(Initiate {
                proto_ver: HELLO_PROTO_VER,
                nonce_i: bytes::Bytes::copy_from_slice(&our_nonce),
            }),
            // The responder proves first and discloses nothing.
            Answer::Respond(our_nonce) => {
                // The proof binds the version this respond advertises, so a
                // version rewritten in flight breaks verification.
                let transcript = transcript(
                    HELLO_PROOF_TAG,
                    HELLO_PROTO_VER,
                    &our_nonce,
                    &their_nonce,
                    our_peer_id,
                    their_peer_id,
                );
                HelloMsg::Respond(Respond {
                    proto_ver: HELLO_PROTO_VER,
                    nonce_r: bytes::Bytes::copy_from_slice(&our_nonce),
                    proof_r: hello_proof(&self.hello_key, &transcript),
                })
            }
        };

        if let Err(err) = self.send(&peer_url, msg).await {
            tracing::debug!(
                ?err,
                ?peer_url,
                "Could not answer a hello initiate"
            );
            self.fail(&peer_url);
        }
    }

    async fn handle_respond(
        &self,
        peer_url: Url,
        our_url: Url,
        respond: Respond,
    ) {
        if respond.proto_ver != HELLO_PROTO_VER {
            tracing::debug!(
                ?peer_url,
                proto_ver = respond.proto_ver,
                "Dropping a hello respond that speaks another protocol version"
            );
            return;
        }
        let Some(their_nonce) = to_nonce(&respond.nonce_r) else {
            tracing::warn!(
                ?peer_url,
                len = respond.nonce_r.len(),
                "Dropping a hello respond with a malformed nonce"
            );
            return;
        };

        let our_nonce = {
            let state = self.state.lock().expect("poison");
            match state.get(&peer_url).and_then(|e| e.exchange.as_ref()) {
                Some((Exchange::Challenging { our_nonce }, _)) => *our_nonce,
                _ => {
                    tracing::debug!(
                        ?peer_url,
                        "Dropping a hello respond we did not ask for"
                    );
                    return;
                }
            }
        };

        // Their proof is over their transcript: their nonce and peer id
        // first. The peer id comes from the URL the transport gave us, which
        // is the identity the connection authenticated, so a proof relayed
        // from an honest member does not verify here.
        // The version comes out of the message being verified rather than
        // from the constant, so that the peer's proof only verifies over the
        // version it actually advertised.
        let their_transcript = match transcript_for_urls(
            HELLO_PROOF_TAG,
            respond.proto_ver,
            &their_nonce,
            &our_nonce,
            &peer_url,
            &our_url,
        ) {
            Ok(transcript) => transcript,
            Err(err) => {
                tracing::debug!(
                    ?err,
                    ?peer_url,
                    "Abandoning a hello exchange that cannot be bound to a channel"
                );
                self.fail(&peer_url);
                return;
            }
        };
        if !verify_hello_proof(
            &self.hello_key,
            &their_transcript,
            &respond.proof_r,
        ) {
            tracing::debug!(
                ?peer_url,
                "A peer could not prove knowledge of the space secret, recording nothing"
            );
            self.fail(&peer_url);
            return;
        }

        self.grant(&peer_url);

        // Our own proof is over our own transcript: our nonce and peer id
        // first, which is what makes the two proofs of one exchange different
        // bytes and stops either being reflected back at its author.
        // Our own proof binds the version our `Initiate` advertised, which is
        // always the one we speak.
        let our_transcript = match transcript_for_urls(
            HELLO_PROOF_TAG,
            HELLO_PROTO_VER,
            &our_nonce,
            &their_nonce,
            &our_url,
            &peer_url,
        ) {
            Ok(transcript) => transcript,
            Err(err) => {
                tracing::debug!(
                    ?err,
                    ?peer_url,
                    "Abandoning a hello exchange that cannot be bound to a channel"
                );
                self.forget(&peer_url);
                return;
            }
        };

        // Now that they have proven, it is safe to disclose.
        let confirm = Confirm {
            proof_i: hello_proof(&self.hello_key, &our_transcript),
            agent_infos_i: self.local_agent_infos().await,
        };

        {
            let mut state = self.state.lock().expect("poison");
            if let Some(entry) = state.get_mut(&peer_url) {
                entry.exchange = Some((Exchange::AwaitingAck, Instant::now()));
            }
        }

        if let Err(err) = self.send(&peer_url, HelloMsg::Confirm(confirm)).await
        {
            tracing::debug!(?err, ?peer_url, "Could not send a hello confirm");
            self.forget(&peer_url);
        }
    }

    async fn handle_confirm(
        &self,
        peer_url: Url,
        our_url: Url,
        confirm: Confirm,
    ) {
        let (our_nonce, their_nonce, their_proto_ver) = {
            let state = self.state.lock().expect("poison");
            match state.get(&peer_url).and_then(|e| e.exchange.as_ref()) {
                Some((
                    Exchange::Responding {
                        our_nonce,
                        their_nonce,
                        their_proto_ver,
                    },
                    _,
                )) => (*our_nonce, *their_nonce, *their_proto_ver),
                _ => {
                    tracing::debug!(
                        ?peer_url,
                        "Dropping a hello confirm we did not ask for"
                    );
                    return;
                }
            }
        };

        // `Confirm` carries no version of its own: the initiator's proof binds
        // the version its `Initiate` advertised, remembered when we answered.
        let their_transcript = match transcript_for_urls(
            HELLO_PROOF_TAG,
            their_proto_ver,
            &their_nonce,
            &our_nonce,
            &peer_url,
            &our_url,
        ) {
            Ok(transcript) => transcript,
            Err(err) => {
                tracing::debug!(
                    ?err,
                    ?peer_url,
                    "Abandoning a hello exchange that cannot be bound to a channel"
                );
                self.fail(&peer_url);
                return;
            }
        };
        if !verify_hello_proof(
            &self.hello_key,
            &their_transcript,
            &confirm.proof_i,
        ) {
            tracing::debug!(
                ?peer_url,
                "A peer could not prove knowledge of the space secret, recording nothing"
            );
            self.fail(&peer_url);
            return;
        }

        self.grant(&peer_url);
        self.ingest_agent_infos(&peer_url, confirm.agent_infos_i)
            .await;

        let ack = Ack {
            agent_infos_r: self.local_agent_infos().await,
        };
        self.forget(&peer_url);

        if let Err(err) = self.send(&peer_url, HelloMsg::Ack(ack)).await {
            tracing::debug!(?err, ?peer_url, "Could not send a hello ack");
        }
    }

    async fn handle_ack(&self, peer_url: Url, ack: Ack) {
        {
            let state = self.state.lock().expect("poison");
            if !matches!(
                state.get(&peer_url).and_then(|e| e.exchange.as_ref()),
                Some((Exchange::AwaitingAck, _))
            ) {
                tracing::debug!(
                    ?peer_url,
                    "Dropping a hello ack we did not ask for"
                );
                return;
            }
        }

        self.ingest_agent_infos(&peer_url, ack.agent_infos_r).await;
        self.forget(&peer_url);
    }

    /// Expire exchanges that have run out of time, then retry the peers whose
    /// backoff has elapsed.
    async fn expire_and_retry(&self) {
        let timeout = self.config.exchange_timeout();
        let backoff_max = self.config.retry_backoff_max();
        let mut retry = Vec::new();

        {
            let mut state = self.state.lock().expect("poison");
            let now = Instant::now();

            for (peer_url, entry) in state.iter_mut() {
                let expired = entry
                    .exchange
                    .as_ref()
                    .map(|(_, started_at)| {
                        now.duration_since(*started_at) >= timeout
                    })
                    .unwrap_or(false);
                if expired {
                    tracing::debug!(
                        ?peer_url,
                        "A hello exchange timed out, it will be retried"
                    );
                    entry.exchange = None;
                    entry.retry_after = Some(now + entry.backoff);
                    entry.backoff = (entry.backoff * 2).min(backoff_max);
                }
            }

            state.retain(|peer_url, entry| {
                if entry.exchange.is_some() {
                    return true;
                }
                match entry.retry_after {
                    // Due for a retry. The entry is kept, minus its gate, so
                    // that the backoff accumulated so far survives the retry.
                    Some(at) if at <= now => {
                        entry.retry_after = None;
                        retry.push(peer_url.clone());
                        true
                    }
                    Some(_) => true,
                    // Nothing in flight and nothing to wait for: a peer we
                    // have no reason to remember.
                    None => false,
                }
            });
        }

        for peer_url in retry {
            self.initiate(peer_url, false).await;
        }
    }

    /// Record that a peer proved knowledge of the space secret.
    fn grant(&self, peer_url: &Url) {
        tracing::debug!(
            ?peer_url,
            "A peer proved knowledge of the space secret, granting access"
        );
        if let Err(err) = self.access_state.set_access_decision(
            peer_url.clone(),
            PeerAccess {
                decision: AccessDecision::Granted,
                decided_at: Timestamp::now(),
            },
        ) {
            tracing::warn!(
                ?err,
                ?peer_url,
                "Could not record an access decision for a peer that proved knowledge of the space secret"
            );
        }
    }

    /// Drop all state for a peer without penalising it.
    fn forget(&self, peer_url: &Url) {
        self.state.lock().expect("poison").remove(peer_url);
    }

    /// Drop the exchange with a peer and gate the next attempt behind a
    /// doubling backoff.
    fn fail(&self, peer_url: &Url) {
        let mut state = self.state.lock().expect("poison");
        if let Some(entry) = state.get_mut(peer_url) {
            entry.exchange = None;
            entry.retry_after = Some(Instant::now() + entry.backoff);
            entry.backoff =
                (entry.backoff * 2).min(self.config.retry_backoff_max());
        }
    }

    /// The encoded agent infos of our local agents, which is all this node
    /// discloses in an exchange.
    async fn local_agent_infos(&self) -> Vec<String> {
        let local_agents = match self.local_agent_store.get_all().await {
            Ok(local_agents) => local_agents,
            Err(err) => {
                tracing::debug!(
                    ?err,
                    "Could not read the local agent store to disclose agent infos"
                );
                return Vec::new();
            }
        };

        let mut out = Vec::with_capacity(local_agents.len());
        for local_agent in local_agents {
            let info =
                match self.peer_store.get(local_agent.agent().clone()).await {
                    Ok(Some(info)) => info,
                    Ok(None) => continue,
                    Err(err) => {
                        tracing::debug!(
                            ?err,
                            "Could not read a local agent info to disclose it"
                        );
                        continue;
                    }
                };
            match info.encode() {
                Ok(encoded) => out.push(encoded),
                Err(err) => {
                    tracing::warn!(
                        ?err,
                        "Could not encode a local agent info to disclose it"
                    );
                }
            }
        }
        out
    }

    /// Wait, bounded, for at least one local agent info to be available to
    /// disclose.
    async fn await_local_agent_infos(&self) {
        const POLL: Duration = Duration::from_millis(25);
        const LIMIT: Duration = Duration::from_secs(5);

        let start = Instant::now();
        while start.elapsed() < LIMIT {
            if !self.local_agent_infos().await.is_empty() {
                return;
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// Store the agent infos a peer disclosed after proving itself.
    async fn ingest_agent_infos(&self, peer_url: &Url, encoded: Vec<String>) {
        let mut infos = Vec::with_capacity(encoded.len());
        for encoded in encoded {
            match AgentInfoSigned::decode(&self.verifier, encoded.as_bytes()) {
                Ok(info) => {
                    if info.space != self.space_id {
                        tracing::warn!(
                            ?peer_url,
                            space = ?info.space,
                            "Dropping an agent info a peer disclosed for another space"
                        );
                        continue;
                    }
                    infos.push(info);
                }
                Err(err) => {
                    tracing::warn!(
                        ?err,
                        ?peer_url,
                        "Could not decode an agent info a peer disclosed"
                    );
                }
            }
        }

        if infos.is_empty() {
            return;
        }
        tracing::debug!(
            ?peer_url,
            count = infos.len(),
            "Storing agent infos disclosed in a hello exchange"
        );
        if let Err(err) = self.peer_store.insert(infos).await {
            tracing::warn!(
                ?err,
                ?peer_url,
                "Could not store the agent infos a peer disclosed"
            );
        }
    }

    async fn send(&self, peer_url: &Url, msg: HelloMsg) -> K2Result<()> {
        let Some(transport) = self.transport.upgrade() else {
            return Err(K2Error::other("Transport dropped"));
        };
        let data = K2HelloMessage::new(msg).encode_msg()?;
        transport
            .send_module(
                peer_url.clone(),
                self.space_id.clone(),
                HELLO_MOD_NAME.to_string(),
                data,
            )
            .await
    }
}

/// The transport module handler that feeds incoming hello messages to the
/// module.
#[derive(Debug)]
struct HelloMessageHandler {
    inner: Arc<HelloInner>,
}

impl TxBaseHandler for HelloMessageHandler {}

impl TxModuleHandler for HelloMessageHandler {
    fn recv_module_msg(
        &self,
        peer: Url,
        _space_id: SpaceId,
        _module: String,
        data: bytes::Bytes,
    ) -> K2Result<()> {
        let msg = K2HelloMessage::decode_msg(data).map_err(|err| {
            K2Error::other_src(
                format!("could not decode a hello message from {peer}"),
                err,
            )
        })?;

        let inner = self.inner.clone();
        tokio::task::spawn(async move {
            inner.handle(peer, msg).await;
        });

        Ok(())
    }
}

fn fresh_nonce() -> HelloNonce {
    let mut nonce = [0_u8; HELLO_NONCE_LEN];
    rand::rng().fill_bytes(&mut nonce);
    nonce
}

fn to_nonce(bytes: &bytes::Bytes) -> Option<HelloNonce> {
    HelloNonce::try_from(&bytes[..]).ok()
}
