# Hello/PoK Access Module — Implementation Plan (agent-executable)

Companion to `hello-pok-access.md` (the design; read it first — it is the source of
truth for *why*; this document is the source of truth for *what and in which
order*). Scope: **kitsune2 workspace only.** Holochain-side work (fingerprint
SpaceId derivation, preflight slim-down) and the mDNS branch rework are explicitly
out of scope for this plan.

## Ground rules

- Follow `CLAUDE.md`: edition 2024; mutex poisoning handled with
  `.expect("poison")`; logging levels per the conventions there (dropped messages
  from ungranted peers are `debug`, protocol violations `warn`, never `warn`/
  `error` for transient conditions).
- Wire messages are protobuf. Add `.proto` sources; **never** hand-edit files
  under `proto/gen/`; regenerate with `cargo make proto`.
- Test layering per `CLAUDE.md`: unit tests for everything reachable, functional
  tests on the in-memory transport, at most a couple of real-transport
  integration tests in `crates/kitsune2`.
- After each phase: `cargo fmt`, `cargo make static`, then tests for the changed
  crate plus downstream consumers. Full `cargo make test` after phases 4–6.
- Breaking API changes are fine (this targets `main`), but keep `kitsune2_api`
  free of tokio-specific and crypto-heavy dependencies: HMAC/KDF code lives in
  `core`, not `api`.

## Existing code to read before starting

- `crates/api/src/access.rs` — `AccessDecision`, `PeerAccess`, `PeerAccessState`.
- `crates/core/src/factories/core_access.rs` — `CorePeerAccessState` and its
  peer-store listener; the hello module extends this state rather than replacing it.
- `crates/api/src/transport.rs` — `check_message_permitted`, `is_peer_blocked`,
  the `Preflight` exemption comment, and the send-path checks (~lines 280–360 and
  754–822).
- `crates/core/src/factories/core_space.rs` — `is_any_agent_at_url_blocked`
  (the unknown=blocked default at ~line 300), space factory wiring.
- `crates/core/src/factories/core_publish.rs` + `crates/core/proto/publish.proto`
  — the pattern for a module: factory, module-message handler registration,
  protobuf encode/decode.
- Issues #263, #265, #347 (summarized in the design doc).

## Phase 1 — API surface (`crates/api`)

1. `crates/api/src/access.rs`, additions:
   - `trait SpaceSecret` exactly as specified in the design doc §2: a single
     `derive_key(space_id, purpose) -> BoxFut<'static, K2Result<Bytes>>`
     method; plus `DynSpaceSecret`. Kitsune2 calls it once per (space, purpose)
     and caches the result; all protocol crypto (HMAC, future bootstrap
     signing) lives in kitsune2, keyed by derived material. No proof-shaped
     methods in v1.
   - `trait SpaceSecretFactory` following the existing factory pattern (see
     `BootstrapFactory` for shape): `default_config`, `validate_config`,
     `create(builder, space_id) -> BoxFut<'static, K2Result<DynSpaceSecret>>`;
     plus `DynSpaceSecretFactory`.
   - Extend `PeerAccessState` with a setter and a remover so the hello module
     can record results and state can be reset:
     `fn set_access_decision(&self, peer_url: Url, access: PeerAccess)
     -> K2Result<()>;` and
     `fn remove_access_decision(&self, peer_url: Url) -> K2Result<()>;`
     (the remover is the primitive behind decision pruning and is what Phase 5
     test 5 uses to simulate state loss). Update `CorePeerAccessState`
     accordingly (decisions map write/remove; explicit `Blocked` entries from
     the blocks listener must not be overwritten by a later `Granted` from
     hello — blocks win; encode that rule in `set_access_decision`'s impl and
     unit-test it).
2. `crates/api/src/builder.rs`: add `pub space_secret: DynSpaceSecretFactory` to
   `Builder`, threaded like the other factories.
3. Constant for the module id, e.g. `pub const HELLO_MOD_NAME: &str = "hello";`
   placed near the other module-name constants (grep for how `fetch`/`publish`
   name theirs and match).
4. `crates/api/src/transport.rs`: add all three new `TxSpaceHandler` methods
   here, each with a default impl so downstream phases compile independently
   and existing implementors don't break:
   - `fn access_module_id(&self) -> String` — default: `HELLO_MOD_NAME.into()`;
   - `fn is_access_granted(&self, peer_url: &Url) -> K2Result<bool>` — default:
     `Ok(false)` (the safe direction: ungranted until a real implementation
     overrides);
   - `fn ungranted_message_dropped(&self, peer: Url)` — fire-and-forget,
     default: no-op.
   The `core_space.rs` overrides land where they are used: Phase 3 implements
   `ungranted_message_dropped` (forward to the hello module), Phase 4
   implements `is_access_granted` (against the access state).

Tests: unit tests in `access.rs`/`core_access.rs` for the blocks-beat-granted
precedence rule. `cargo test -p kitsune2_api -p kitsune2_core`.

## Phase 2 — Wire protocol (`crates/core`)

1. New file `crates/core/proto/hello.proto`, package `kitsune2.hello`, messages
   per the design doc: `Initiate`, `Respond`, `Confirm`, `Ack`, wrapped in a
   oneof envelope message (follow `publish.proto`'s envelope style). Fields:
   `proto_ver` (uint32), `nonce_*` (bytes, 32), `proof_*` (bytes), agent infos as
   repeated encoded `AgentInfoSigned` strings (match how publish/preflight encode
   agent infos today — grep for `AgentInfoSigned::encode`).
2. Run `cargo make proto`; commit generated code unmodified.
3. `crates/core/src/factories/core_hello/proto_helpers.rs` (or similar): the
   transcript builder — a pure function
   `transcript(tag, nonce_self, nonce_peer, peer_id_self, peer_id_peer) -> Bytes`
   with fixed-length framing (nonces are exactly 32 bytes; peer ids are
   length-prefixed since they are variable-length strings across transports —
   do NOT bare-concatenate variable-length fields).
   **Bind peer ids, not full URLs**: use the `peer_id()` path segment of each
   side's kitsune2 `Url`. Full URLs must not enter the transcript — an iroh
   node holds several URLs at once (global + per-space relays) and the two
   sides will not reliably agree on one; the peer id is the component the
   transport cryptographically authenticates (see the design doc §3). The
   verifying side must take the peer id from the `peer: Url` the transport
   passed to the module handler (connection-derived), never from message
   contents.
   `Url::peer_id()` returns `Option<&str>` (`crates/api/src/url.rs:197`): if
   either side's peer id resolves to `None` — for its own URL or the peer's —
   abort the exchange, log at `debug`, and record nothing. (A peer-id-less URL
   should never reach a module handler, but the rule must not depend on
   "should never".)

Tests: transcript determinism, distinctness under swapped roles, length-prefix
ambiguity test (two different (peer_id_self, peer_id_peer) pairs whose bare
concatenation would collide must produce different transcripts).

## Phase 3 — Core implementations (`crates/core`)

1. `crates/core/src/factories/core_space_secret.rs`:
   - `CoreSpaceSecret`: `derive_key(space, purpose)` =
     `HKDF-SHA256(secret, purpose)`. The per-space secret comes from module
     config; default when unset: the space id bytes (which makes the space
     "open to anyone who knows the space id" — today's semantics — so this is
     the production default, not a special public mode). Dependencies (`hmac`,
     `sha2`, `hkdf`) go in `kitsune2_core`'s `Cargo.toml`, workspace-versioned
     in the root `Cargo.toml` dependency section per the existing layout.
   - Hello proof crypto (HMAC-SHA256 over the transcript, keyed by the cached
     `"k2-hello-v1"` derived key) lives in the hello module, NOT in the trait
     impl — the trait only derives keys.
   - `NoopSpaceSecret` (test convenience): returns a fixed key; combined with a
     hello-module test config that skips proof verification if needed. The
     **test builder** (`default_test_builder` in `crates/core`) and the
     production builder both register `CoreSpaceSecret`; tests that want
     access-denied scenarios configure mismatched secrets instead of a special
     mode.
2. `crates/core/src/factories/core_hello.rs` — the module proper:
   - Registered as a transport **module handler** for `HELLO_MOD_NAME` per space
     (follow `core_publish.rs` for registration and message dispatch).
   - State: `HashMap<Url, ExchangeState>` behind a mutex, where `ExchangeState`
     is `Challenging { our_nonce, started_at }`; one in-flight exchange per URL.
   - **Simultaneous-initiate tie-break (fully specified):** if an `Initiate`
     arrives from a peer we currently have a `Challenging` entry for, compare
     the two **peer ids** bytewise (never full URLs — they are unstable across
     relay failover, see design doc open question 3). The side whose peer id is
     bytewise-**lower** keeps its initiator role; the higher side abandons its
     own initiated exchange (discards its nonce and pending state) and answers
     the surviving `Initiate` as responder. Both sides compute the same
     comparison, so exactly one exchange survives deterministically. Unit-test
     both orderings (lower-id initiates first, higher-id initiates first).
   - Protocol handling per the design doc §3, including: verify-before-disclose
     ordering; on success, `set_access_decision(Granted)` + insert received agent
     infos into the peer store; on verification failure, record nothing (leave
     the peer ungranted), log at `debug`, drop the exchange.
   - Triggers per design doc §4:
     - a peer-store listener (reuse `register_peer_update_listener`) challenging
       new URLs with no decision;
     - a hook called on `local_agent_join` (wire from `core_space.rs`) that
       challenges all URLs in the peer store and all transport-connected peers
       (`get_connected_peers`). The hello module must **tolerate
       `get_connected_peers` errors** (log at `debug`, proceed with the
       peer-store URLs) — transports may legitimately not implement it;
     - the **drop-triggered challenge**: when the enforcement path (Phase 4)
       drops an incoming non-hello message from an ungranted (not explicitly
       blocked) URL, it notifies the hello module via
       `TxSpaceHandler::ungranted_message_dropped` (trait method added in
       Phase 1; implement the `core_space.rs` override here, forwarding to the
       hello module), which initiates an exchange toward that peer unless one
       is already pending or the per-URL retry backoff says otherwise. Rate
       limiting is the existing per-URL `ExchangeState`/backoff — no separate
       limiter. Must NOT fire for explicitly blocked URLs (denylist wins).
   - Config (`CoreHelloConfig`, follow the existing mod-config pattern):
     `exchange_timeout_ms` (default 10_000), `retry_backoff_min_ms` (30_000),
     `retry_backoff_max_ms` (300_000), `max_concurrent_exchanges` (32).
   - A tokio interval task expires timed-out exchanges and drives retries;
     abort on drop (follow the `CorePeerAccessState` pruning-task pattern,
     including the `Drop` impl).
3. **Implement `get_connected_peers` for the mem transport.** It currently
   returns an error (`crates/core/src/factories/mem_transport.rs:159`), which
   would kill the join-trigger path that Phase 5 test 1 depends on (space-B
   peer store empty, bootstrap disabled → connected peers are the only trigger
   source). The transport's task loop already maintains a `con_pool` of open
   in-memory connections; expose the connected peer URLs from it. Add a small
   unit test in `mem_transport/test.rs`.
4. Wire into `CoreSpaceFactory::create` in `core_space.rs`: build the space
   access impl and hello module alongside the existing modules; hand the hello
   module the peer store, access state, transport (weak ref per the
   `WeakDynTransport` guidance in `transport.rs`), and local agent store.

Tests (unit, in-crate): full happy-path handshake between two in-process module
instances over a stub transport; wrong-secret rejection; reflection attempt
(echoing a proof back) rejected; relay simulation (proof computed by peer C,
bound to C's peer id, presented over a connection authenticated as peer M)
rejected via channel binding; timeout expiry and retry
backoff; simultaneous-initiate tie-break; default-secret (space id) peers grant
each other within one exchange.

## Phase 4 — Enforcement (`crates/api/src/transport.rs`)

1. The three `TxSpaceHandler` methods already exist with defaults from
   Phase 1. Here: implement the `core_space.rs` override of `is_access_granted`
   against the access state, and change the `is_any_agent_at_url_blocked` call
   sites to the three-valued rule from design doc §5 (keep the existing method
   name/semantics for the blocks check).
2. `check_message_permitted` (incoming) and `send_space_notify`/`send_module`
   (outgoing): precedence blocks → granted → exempt-types/access-module-id →
   drop. Blocked-message counters keep working for dropped messages (both the
   blocks case and the ungranted case; if cheap, count them separately — the
   stats struct may gain an `ungranted` counter, but do not let this balloon the
   phase).
3. The `Preflight` exemption comment block (~line 294) must be updated to
   document the hello exemption and its rationale.

Tests: extend `crates/core/src/factories/mem_transport/test.rs` — ungranted peer:
hello module messages pass, publish module messages drop; granted peer: all pass;
blocked peer: everything but exempt wire types drops even if previously granted.

## Phase 5 — Functional tests (the reason this work exists)

In `crates/core` (in-memory transport, real modules), add a test module, e.g.
`crates/core/tests/hello_access.rs`:

1. **Moss multi-cell regression test (the headline):** two nodes, both with local
   agents in space A, granted and gossiping. Node 1 joins space B first; later
   node 2 joins space B *while the connection already exists*. Assert: node 2
   becomes granted at node 1 in space B via the hello exchange (not via
   bootstrap — run with bootstrap disabled/`test` bootstrap that returns
   nothing), agent infos are exchanged, and a message (publish or gossip round)
   subsequently flows in space B. Assert a generous but bounded wall-clock
   (seconds, not minutes).
2. **Read-protection test (per #265 AC):** node 3 configured with a *wrong*
   secret for space B attempts hello; verify it is never granted, gossip/fetch/
   notify messages to and from it are dropped, and — critically — no agent infos
   for space B are ever sent to it.
3. **Blocks precedence:** grant a peer via hello, then block one of its agents;
   assert traffic stops.
4. **Default-secret space (today's semantics):** with no secret configured
   (secret = space id), an unknown peer's first non-hello message is still
   dropped until the (instant) hello completes — assert the default path grants
   within one exchange and traffic flows.
5. **Asymmetric-state self-heal (drop-triggered challenge):** complete a hello
   between two nodes, then wipe node 1's access decision for node 2 via
   `PeerAccessState::remove_access_decision` (the Phase 1 remover; simulates
   pruning/restart) while node 2 keeps its grant. Node 2 sends a message; assert
   node 1 drops it, a re-hello runs (triggered by the drop, with bootstrap
   disabled and no join/insert events), and traffic flows again — bounded by
   seconds. Also assert the negative: an explicitly blocked peer's messages do
   NOT trigger challenges.

## Phase 6 — Integration (`crates/kitsune2`)

1. Register `CoreSpaceSecret` + hello in the default production builder.
2. One integration test with the real transport (follow the style of
   `crates/kitsune2/tests/blocks.rs`, which already covers adjacent behavior and
   **will need updating** for the new enforcement rule — expect and fix breakage
   there rather than weakening the new rule).
3. Update `crates/kitsune2_showcase` only if it fails to build; the #265 AC
   showcase demo is a follow-up, not this pass.

## Definition of done

- `cargo make verify` passes (includes fmt, clippy `--deny=warnings`, doc-check,
  taplo, full test suite).
- The Phase 5 moss-scenario test passes deterministically (run it 20× locally:
  `for i in $(seq 20); do cargo test -p kitsune2_core --test hello_access || break; done`).
- No hand edits under any `proto/gen/`.
- New public API items have doc comments (doc-check runs with warnings denied).
- A short `README.md` section or module-level doc comment on `core_hello`
  describing the wire protocol and state machine (mermaid state diagram welcome,
  following `crates/gossip/README.md` precedent).

## Explicitly out of scope (do not do these even if tempting)

- Holochain-side changes (fingerprint SpaceId, preflight slim-down).
- mDNS branch rework.
- Fingerprint rotation / time-bucketing.
- Changing the unknown-URL default on release branches.
- Encrypted bootstrap payloads.
