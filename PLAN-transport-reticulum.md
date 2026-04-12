# Plan: `kitsune2_transport_reticulum`

A third `Transport` implementation for kitsune2, built on [Reticulum](https://reticulum.network/) via [`reticulum-rs-transport`](https://crates.io/crates/reticulum-rs-transport) v0.2.0 (the LXMF-rs / FreeTAKTeam Rust implementation), plugging into the same `TxImp` seam that `transport_tx5` and `transport_iroh` already use.

> **Crate choice.** Two Rust Reticulum implementations were spiked ([spikes/](spikes/)). `reticulum-rs-transport` v0.2.0 (crate name `rns_transport`) was chosen over `reticulum` v0.1.0 (Beechat) because it has Resource support for large payloads, `name_hash` on `AnnounceEvent` for aspect filtering, built-in `AnnounceTable` dedup, a `test_bridge` module, and richer metadata on announce events (hops, interface). Beechat lacks Resources entirely and has weaker announce metadata.

## Goal and non-goals

**Goal.** Make it possible to run a kitsune2 node that carries all of its gossip, fetch, publish, and module traffic over a Reticulum network instead of Iroh/QUIC, selected at `Builder` time. This requires one small *additive* change to `kitsune2_api` (accepting a new `ret` URL scheme); no breaking changes.

**Non-goals for v1.**
- Running on LoRa radios. Target IP-based Reticulum interfaces (TCPClient/TCPServer, AutoInterface) first. LoRa is a downstream concern once the adapter works.
- Browser participation. Neither Iroh nor Reticulum runs in browsers; we inherit that limitation and do not try to fix it here.
- Replacing Iroh as default. This is a pluggable alternative, not a migration.
- A custom Reticulum transport node. v1 assumes deployers bring their own Reticulum nodes with `enable_transport = yes` where needed.

## Key design decisions

### 1. URL scheme

Kitsune2's [`Url`](crates/api/src/url.rs) type has a strict parser: only `ws`/`wss`/`http`/`https` are accepted today, host is required, port must be explicit or have a known-default, and the path must have exactly one `/` — canonical form `scheme://host:port/peer-id` ([url.rs:118-160](crates/api/src/url.rs#L118-L160)).

**Decision: add `ret` to the accepted schemes list** in [crates/api/src/url.rs](crates/api/src/url.rs), with a hard-coded known-default port (e.g. `1`), confirmed as an acceptable additive change. Canonical form:

```
ret://reticulum:1/<identity-hash-hex>
```

Two things to notice:

1. **The host and port are constants — `reticulum:1` — and are ignored by the transport.** Reticulum has nothing meaningful to put in host/port slots: destinations are reached by hash through the announce-built routing tables, not by IP rendezvous. The `reticulum:1` authority exists purely to satisfy the `Url` parser's invariants.
2. **The path carries the peer's Reticulum Identity hash, *not* a destination hash.** See section 2 — because kitsune2 uses per-space destinations (aspect `kitsune2/<space-hash>`), a single node has *many* destination hashes, one per space it has joined. The kitsune2 api gives the transport exactly one URL per node (via `self.imp.url()`), so that URL has to be stable across all spaces. The node's Identity hash is the right stable handle: given a peer's Identity and a space id, the transport can derive the per-space destination hash on demand. **Spike confirmed:** destination hashes are deterministic — `hash = SHA256(name_hash[..10] || identity_address_hash[..16])[..16]` — and can be computed offline using only the peer's public `Identity` (obtained from an announce via `DestinationAnnounce::validate()`) plus the aspect string. See `new_out(pub_identity, "kitsune2", space_hash)` in the spike.

A `url.rs` module analogous to [crates/transport_iroh/src/url.rs](crates/transport_iroh/src/url.rs) handles `Url ↔ identity-hash` conversion. Unlike Iroh's version, there is no host/port extraction — those fields are constants and discarded.

### 2. Reticulum object model → kitsune2 transport model

| kitsune2 concept | `rns_transport` concept | Concrete API |
|---|---|---|
| Local node identity | `PrivateIdentity` (X25519 + Ed25519) | `PrivateIdentity::new_from_rand(OsRng)` |
| Public identity (from announces) | `Identity` | `announce.validate()` → `info.destination.identity` |
| Local endpoint for space *S* | `SingleInputDestination`, aspect `kitsune2/<space_hash(S)>` | `transport.add_destination(identity, DestinationName::new("kitsune2", space_hash))` |
| Offline destination hash derivation | `SingleOutputDestination` | `new_out(pub_identity, "kitsune2", space_hash).desc.address_hash` |
| Network attachment | `InterfaceManager` + TCP/UDP/Serial interfaces | `iface::tcp_client`, `iface::tcp_server`, `iface::udp` |
| Peer discovery | Per-space announces | `dest.announce(OsRng, app_data)`, `transport.recv_announces()` |
| Peer connection for space *S* | `Link` to peer's per-space destination | `transport.link(dest_desc)` → `Arc<Mutex<Link>>` |
| Send small (≤ MDU) | `Link::data_packet` | `link.data_packet(payload)?` → `transport.send_packet(packet)` |
| Send large (> MDU) | `Resource` (automatic chunking) | `transport.send_resource(link_id, data, metadata)` |
| Receive data | Link events via transport | `transport.resource_events()` for Resources |
| Link lifecycle | `LinkStatus` enum | `Pending` / `Handshake` / `Active` / `Stale` / `Closed` |
| Packet MDU | `PACKET_MDU` constant | ~383 bytes (after headers) |

**Per-space destinations, not a single global destination.** Every space gets its own Reticulum `Destination` via `transport.add_destination(identity, DestinationName::new("kitsune2", space_hash))`. Each is announced independently, and a kitsune2 node filters incoming announces for its joined spaces by comparing `AnnounceEvent.name_hash` against precomputed `DestinationName::new("kitsune2", space_hash).as_name_hash_slice()` values (spike confirmed: `name_hash` is available on `AnnounceEvent` in `rns_transport` v0.2.0, though not in Beechat — one of the reasons we chose this crate). Corollary: **a `Link` is 1:1 with a `(peer, space)` pair**, not with a peer. Two nodes sharing two spaces will have two independent Links between them.

**There is no "seed" or "home relay" concept.** A Reticulum node attaches to the network via one or more `Interface`s. Destinations are made reachable by being *announced*: the announce propagates through every node with `enable_transport = yes`, which builds routing tables along the way. To reach a destination, you need only the destination hash; the transport layer figures out the path through whatever interfaces exist. Unlike Iroh, where a peer address *has* to embed a relay URL for NAT rendezvous.

#### Impedance mismatch with `TxImp::send` — and how we handle it

Kitsune2's `TxImp` trait assumes one connection per peer `Url`:

- [`TxImp::send(peer, data)`](crates/api/src/transport.rs#L445) takes no space id. The space id *is* inside the encoded `K2Proto` payload (`data` is already serialized), but the trait doesn't expose it.
- [`TxImpHnd::peer_connect(peer)`](crates/api/src/transport.rs#L66) and [`TxImpHnd::peer_disconnect(peer, reason)`](crates/api/src/transport.rs#L112) fire once per peer, not per `(peer, space)`.
- [`TxImpHnd::new_listening_address(url)`](crates/api/src/transport.rs#L41) broadcasts one URL to all space handlers.

Per-space Reticulum links need to plug into that api without an api change. The adapter:

1. **Connection map is two-level**: `HashMap<Url, PeerState>`, and `PeerState` contains both a per-peer `preflight_state` and an inner `HashMap<SpaceId, LinkContext>` for each active `(peer, space)` Link.
2. **`TxImp::send` extracts the space id from the encoded `K2Proto` with a partial decode.** `K2Proto` is a prost-generated protobuf; we already depend on it transitively through `kitsune2_api`, and its `space_id` field is a top-level tag, so extracting it is a cheap tag-walk rather than a full decode-and-reencode. Preflight messages carry `space_id: None` — see point 5 below for how those are routed.
3. **Per-peer `peer_connect` fires on first-link-open, `peer_disconnect` on last-link-close.** The transport keeps a reference count in the outer `PeerState` map; the first inner Link triggers `TxImpHnd::peer_connect(url)` and the preflight exchange, and the last one being torn down triggers `TxImpHnd::peer_disconnect(url, reason)`.
4. **Preflight is per-peer, not per-link.** Preflight validation is kept in `PeerState.preflight_state`, outside the per-space link map. Once a peer has been preflight-validated over *any* per-space link, subsequent per-space links to the same peer skip the exchange and go straight to data. This mirrors kitsune2's current expectation that preflight is a property of the peer URL, not of each individual link.
5. **Preflight routing.** Preflight has `space_id: None`, so step 2's extraction yields nothing. The adapter holds preflight bytes in a staging area and sends them over the *first* per-space link that opens to a given peer — which in practice is the same link that's about to carry the first real message, since kitsune2's `send_space_notify` / `send_module` calls naturally precede the first use of that space. If we need to open a link purely to deliver preflight (no real message queued yet), we pick an arbitrary joined space as the carrier. This is an ugly detail but entirely internal to the transport.

This model is more complicated than the Iroh transport's, but the complication is load-bearing for per-space announces. It is worth the cost. It will be isolated in one module (`peer_state.rs`) so the rest of the transport doesn't have to think about it.

Reticulum `Link`s are otherwise the closest analogue to an Iroh `Connection` — 1:1 with their destination, carrying a session key after a 1-RTT handshake, emitting connected / disconnected / idle events.

### 3. Framing

The Iroh transport puts its own preflight+data framing on top of QUIC streams because QUIC streams are raw byte streams. Reticulum **already** frames messages — each `Link::send` corresponds to one delivered payload boundary — so we do **not** need a length-prefixed framing layer.

What we *do* need:
- **Preflight exchange, per-peer.** The kitsune2 `TxImp` layer requires a preflight round-trip before real messages flow. Reuse the same idea as Iroh: send one preflight packet, wait (10 s timeout) for the remote's preflight packet, then flip `PeerState.preflight_state` to ready. Define `ReticulumFrame::{Preflight, Data}` as a single tag byte plus the kitsune2-encoded payload. **Preflight is per-peer**, carried on whichever per-space link happens to be the first one opened (see §2). Subsequent per-space links to the same peer skip the exchange. The frame itself is much smaller than the Iroh `Frame` struct and — unlike Iroh's preflight — does **not** need to carry a URL field. The remote's URL is deterministic from its Identity hash, and `Link::peer_identity()` exposes that Identity directly, so there is nothing to exchange.
- **MTU handling.** Spike confirmed: `PACKET_MDU` is ~383 bytes. The `Resource` abstraction (automatic chunking + reassembly via `transport.send_resource(link_id, data, metadata)`) handles payloads exceeding the MDU. The transport should:
  - Use `link.data_packet(payload)` + `transport.send_packet(packet)` when the payload fits in `PACKET_MDU`.
  - Use `transport.send_resource(link_id, data, None)` for anything larger, up to a configured `max_frame_bytes`. Receiver gets `ResourceEvent` via `transport.resource_events()`.
  - kitsune2's gossip layer has historically produced payloads that are very large by Reticulum standards, and a recent kitsune2 commit bumped the Iroh transport's frame size ceiling to accommodate them. For Reticulum, **this needs to go the other way**: we document that running over Reticulum requires the operator (or the gossip layer) to be tuned for smaller frames, and that bringing the two into alignment is a follow-up work item beyond v1 of the transport. For now, `max_frame_bytes` is a hard cap validated in `validate_config`, and the default is set to something plausible — not matched to kitsune2's worst case.

### 4. Async / runtime

Reticulum-rs is Tokio-based, same as kitsune2, so no bridging is needed. Tasks:

**Global, one per transport:**
- `global_announce_consumer_task` — subscribes to announces across all joined-space aspects, updates the local `identity_cache: HashMap<IdentityHash, Identity>` used by `TxImp::send` to derive per-space destination hashes for a peer.
- `listening_address_emitter` — one-shot: once Reticulum confirms the local identity is loaded and the transport is ready, emit a single `TxImpHnd::new_listening_address(ret://reticulum:1/<our-identity-hash>)`. The local URL is deterministic and never changes during a run; no ongoing watch loop is needed.

**Per joined space, spawned on `register_space_handler`:**
- `space_accept_task_S` — owns the inbound-link channel for the `kitsune2/<space_hash(S)>` Destination. On each new `Link`, spawns a `link_reader_task_S_peer` and inserts a `LinkContext` into `PeerState.links[space_id]`, bumping the per-peer refcount so this triggers `peer_connect` if it's the first link to that peer.
- `space_announce_task_S` — periodically re-announces the local destination for space *S* on the Reticulum network, at the configured `announce_interval_s`.
- `space_announce_listener_S` — consumes the global `transport.recv_announces()` broadcast stream, filtering by comparing `AnnounceEvent.name_hash` against the precomputed `DestinationName::new("kitsune2", space_hash).as_name_hash_slice()` for space *S*. For each matching announce, validates it with `DestinationAnnounce::validate()` and feeds both (a) the global `identity_cache` above and (b) a per-space peer-candidate queue consumed by the `Bootstrap` implementation (see §5). The transport-level `AnnounceTable` handles dedup internally (spike confirmed: rand_hash + signature + configurable capacity).

**Per active per-space link:**
- `link_reader_task_S_peer` — owns the Link's recv side, decodes `ReticulumFrame`, dispatches into `TxImpHnd::recv_data` / `TxImpHnd::preflight_validate_incoming`. On disconnect, decrements the per-peer refcount in `PeerState` and fires `peer_disconnect` if it was the last one.

### 5. Peer discovery: `ReticulumBootstrap`

Reticulum's announce system *is* the discovery layer. kitsune2's [`Bootstrap`](crates/api/src/) trait is the right place to plug it in — **the Reticulum crate exposes both a `ReticulumTransportFactory` and a `ReticulumBootstrapFactory`**, which share state. The bootstrap factory drains the per-space announce queues populated by `space_announce_listener_S` (see §4), turning each new peer into whatever the kitsune2 `Bootstrap` trait expects (an `AgentInfo`-equivalent, probably). The transport factory uses the identity cache to resolve peer URLs into per-space destination hashes when sending.

This replaces kitsune2's HTTP `bootstrap_client` entirely for Reticulum deployments. The `Builder` helper wires both factories in together and **does not** register a `kitsune2_bootstrap_client`.

Because the two factories must share state (identity cache, per-space announce queues), they cannot be independently constructed. Pattern: introduce a `ReticulumNode` struct that owns the shared state including the `rns_transport::Transport` instance, and both factory `create()` methods pull from a shared `Arc<ReticulumNode>` built at `Builder`-time. Spike confirmed that one `Transport` instance can host multiple destinations via repeated `transport.add_destination()` calls, each producing an independently-addressable destination with its own hash.

## Workspace integration

### New crate

`crates/transport_reticulum/` — `kitsune2_transport_reticulum`. Dependencies, mirroring [crates/transport_iroh/Cargo.toml](crates/transport_iroh/Cargo.toml):

```toml
[dependencies]
bytes = { workspace = true }
reticulum-rs-transport = { workspace = true }  # crate name in code: rns_transport
rand_core = { workspace = true }               # needed for PrivateIdentity::new_from_rand(OsRng)
kitsune2_api = { workspace = true }
serde = { workspace = true, features = ["derive"] }
tokio = { workspace = true, features = ["macros", "rt", "rt-multi-thread", "sync", "time", "io-util"] }
tracing = { workspace = true }
url = { workspace = true }
schemars = { workspace = true, optional = true }

# optional, for test-utils feature
kitsune2_core = { workspace = true, optional = true }
kitsune2_test_utils = { workspace = true, optional = true }

[features]
schema = ["dep:schemars"]
test-utils = ["dep:kitsune2_core", "dep:kitsune2_test_utils"]
```

`reticulum-rs-transport = "0.2.0"` and `rand_core = "0.6"` must be added to `[workspace.dependencies]` in [Cargo.toml](Cargo.toml). Because `kitsune2_api` stays lean, the Reticulum dependency lives *only* in this crate — never leaked upward.

### Crate layout

```
crates/transport_reticulum/
├── Cargo.toml
├── README.md                    # architecture + config reference
└── src/
    ├── lib.rs                   # ReticulumTransport, ReticulumBootstrap, factories, TxImp impl, task management
    ├── node.rs                  # ReticulumNode shared state (identity cache, announce queues, per-space dest map)
    ├── config.rs                # ReticulumTransportConfig + ModConfig
    ├── url.rs                   # Url ↔ identity-hash conversion
    ├── frame.rs                 # Preflight/Data tag-byte framing (small)
    ├── peer_state.rs            # PeerState two-level map: per-peer refcount + per-space LinkContext
    ├── link.rs                  # LinkContext, link_reader task, preflight handshake state machine
    ├── announce.rs              # per-space announce listener + publisher tasks
    ├── bootstrap.rs             # ReticulumBootstrapFactory, draining announce queues into peer store
    ├── destination.rs           # Destination/Link/Send/Recv abstraction traits (for unit-testability, mirrors endpoint.rs)
    ├── test_utils/              # feature="test-utils" — in-memory Destination, harness
    │   ├── mod.rs
    │   └── harness.rs
    └── tests/
        ├── mod.rs
        ├── frame.rs
        ├── url.rs
        ├── peer_state.rs        # two-level map refcount semantics, preflight-per-peer bookkeeping
        └── link.rs              # handshake, idle timeout, disconnect
```

### Abstraction for testability

Match the Iroh pattern: a private trait layer (see [crates/transport_iroh/src/endpoint.rs](crates/transport_iroh/src/endpoint.rs)) so unit tests can swap in a fake without needing a real Reticulum network. The real implementations are thin wrappers over `rns_transport` types:

| Trait | Real impl wraps | Key methods |
|---|---|---|
| `Endpoint` | `rns_transport::Transport` | `add_destination()`, `link()`, `send_packet()`, `send_resource()`, `recv_announces()` |
| `Destination` | `Arc<Mutex<SingleInputDestination>>` | `announce()`, `desc.address_hash` |
| `Link` | `Arc<Mutex<rns_transport::Link>>` | `data_packet()`, `status()`, `peer_identity()`, `teardown()` |

For functional tests, `rns_transport` provides `InterfaceManager::new_channel()` for synthetic channels and a `test_bridge` module for inter-daemon testing. These can wire two real `Transport` instances together without a network.

### Integration crate wiring

In [crates/kitsune2/](crates/kitsune2/), add an optional re-export + `Builder` helper, gated behind a `reticulum-transport` feature, analogous to however `transport_iroh` is currently exposed. The helper wires in **both** the `ReticulumTransportFactory` and `ReticulumBootstrapFactory`, sharing a single `ReticulumNode` under the hood, and skips the HTTP `kitsune2_bootstrap_client` entirely. **Do not** change the default builder. Consumers opt in.

## Configuration

Mirrors [IrohTransportConfig](crates/transport_iroh/src/lib.rs#L222):

```rust
pub struct ReticulumTransportConfig {
    /// Reticulum interfaces to bring up on startup. At least one must be specified.
    /// v1 supports TCPClient / TCPServer / AutoInterface; LoRa is deferred.
    pub interfaces: Vec<ReticulumInterfaceConfig>,

    /// Path to the Reticulum identity file on disk. If None, a fresh identity
    /// is generated on startup, which means the local URL (derived from the
    /// identity hash) changes every run. Persisting the identity is strongly
    /// recommended for anything but ephemeral tests.
    pub identity_path: Option<std::path::PathBuf>,

    /// Max kitsune2 frame size in bytes. Validated at config time;
    /// see `Framing` section for why this is a hard cap for this transport.
    /// Default: 1 MiB (intentionally lower than kitsune2 gossip's worst case —
    /// see open question #5 and the "follow up work" note in §3).
    pub max_frame_bytes: usize,

    /// Link-establishment timeout (Reticulum 1-RTT + kitsune2 preflight round-trip).
    /// Default: 30 seconds.
    pub connect_timeout_s: u32,

    /// How often (seconds) to re-announce each joined-space destination.
    /// Default: 300 seconds. Applied per-space — if the node has joined N
    /// spaces, it emits N announces every `announce_interval_s`.
    pub announce_interval_s: u32,

    /// How long an idle Link is kept alive before tearing down (seconds).
    /// Default: 600 seconds. Keeping it high amortises the per-Link 1-RTT cost —
    /// this is the mitigation for the "gossip pattern" disadvantage vs. Iroh streams.
    /// Note that a single (peer, space) link being idle does *not* imply the peer
    /// is idle — other per-space links to the same peer may still be in use.
    pub link_idle_timeout_s: u32,
}
```

Note the absence of any "advertised seed" / "home relay" field. There is no such concept in Reticulum — see section 2.

`validate_config` must:
- Reject empty `interfaces`.
- Reject `max_frame_bytes > 16 MiB` (sanity cap).
- If `identity_path` is set and the parent directory does not exist, reject.

## Public API

The public surface is the two factories, a shared node type, and config:

```rust
pub struct ReticulumNode { /* shared state: identity cache, per-space dest map, announce queues */ }
impl ReticulumNode {
    pub async fn new(config: ReticulumTransportConfig) -> K2Result<Arc<Self>> { /* ... */ }
}

pub struct ReticulumTransportFactory { node: Arc<ReticulumNode> }
impl ReticulumTransportFactory {
    pub fn create(node: Arc<ReticulumNode>) -> DynTransportFactory { /* ... */ }
}
impl TransportFactory for ReticulumTransportFactory { /* ... */ }

pub struct ReticulumBootstrapFactory { node: Arc<ReticulumNode> }
impl ReticulumBootstrapFactory {
    pub fn create(node: Arc<ReticulumNode>) -> DynBootstrapFactory { /* ... */ }
}
impl BootstrapFactory for ReticulumBootstrapFactory { /* ... */ }

pub mod config {
    pub struct ReticulumTransportConfig { /* ... */ }
    pub struct ReticulumTransportModConfig { pub reticulum_transport: ReticulumTransportConfig }
}
```

Everything else (`ReticulumTransport`, `PeerState`, `LinkContext`, frame types, the destination trait layer) stays `pub(crate)`. Consumers who want the fast path should use the `Builder` helper in `crates/kitsune2/` that constructs the shared `ReticulumNode` and wires both factories automatically.

## Testing plan

Per the *Testing strategy* section of CLAUDE.md — unit tests are the default layer, functional tests use in-memory implementations, integration tests live in the top-level `kitsune2` crate.

### Unit tests (in `transport_reticulum`)

1. **`frame.rs`** — round-trip encode/decode of `Preflight` and `Data` tag bytes, malformed input rejection.
2. **`url.rs`** — round-trip `Url ↔ identity-hash`, reject malformed inputs (wrong scheme, bad hex, wrong path shape, non-canonical host/port).
3. **`peer_state.rs`** — the two-level map:
   - First link for a peer triggers `peer_connect` and stages preflight.
   - Second link for the same peer (different space) does *not* re-trigger `peer_connect`, and skips preflight.
   - Last link close triggers `peer_disconnect`; penultimate close does not.
   - `TxImp::send` space-id extraction from a partially-decoded `K2Proto`.
4. **`link.rs`** — against a fake `Destination` / `Link` trait implementation:
   - Successful preflight handshake moves the per-peer `preflight_state` to ready.
   - Preflight timeout fires `peer_disconnect` and drops the peer state.
   - Inbound `Data` frame before preflight completes is dropped.
   - Graceful disconnect propagates a disconnect reason into `TxImpHnd::peer_disconnect`.
5. **`announce.rs`** — announces published on registration, filtered announce listener accepts matching aspects and rejects non-matching.
6. **`bootstrap.rs`** — draining an announce queue produces `AgentInfo`-equivalent records, one-per-announce, deduped per peer.
7. **`lib.rs`** — `TxImp::send` semantics with a fake destination: link reuse, concurrent-send lock behaviour, idempotent `disconnect`, per-space link opening on first send.
8. **Config** — `validate_config` rejects each of the invalid combinations above.

### Functional tests (in `transport_reticulum`)

Two `ReticulumNode`s wired together using `rns_transport`'s `InterfaceManager::new_channel()` synthetic channels and/or the `test_bridge` module for inter-daemon testing. Cover:

- Full preflight handshake over a real-ish stack, across the first-opened per-space link.
- Send → receive of a small notify message, same space on both sides.
- Two-space test: both nodes join two spaces, verify two independent per-space links are established and messages routed correctly.
- Send of a payload that exceeds the Link MDU, exercising the Resource path.
- Announce → bootstrap discovery: node A joins space X, node B joins space X, verify A's `ReticulumBootstrap` surfaces B as a discovered peer without any external coordination.
- Link idle timeout behaviour on one per-space link does not tear down other per-space links to the same peer.

### Integration tests (in `crates/kitsune2`)

One genuine integration test that builds a two-node setup via the `Builder` with `ReticulumTransportFactory` + `ReticulumBootstrapFactory`, joins an agent in a space, and runs a gossip round end-to-end. This is deliberately *one* test — per CLAUDE.md, integration tests with real transports belong here but should be few. It gives confidence that the factories wire through `Builder` correctly and that gossip timing is acceptable over Reticulum Links.

## Task breakdown

Steps are ordered so each one is independently testable against the previous. The spike (step 0) is complete — see [spikes/](spikes/) — and all seven questions were validated. The plan below incorporates those findings.

1. **Scaffold crate.** Create `crates/transport_reticulum/` with empty `lib.rs`, Cargo.toml, README, add to workspace members. `cargo make verify` passes with the empty crate.
2. **Add `rns_transport` dependency.** Add `reticulum-rs-transport = "0.2.0"` and `rand_core = "0.6"` to `[workspace.dependencies]` in the root [Cargo.toml](Cargo.toml), and add the crate-level deps. Verify it builds.
3. **`kitsune2_api` scheme addition.** In [crates/api/src/url.rs](crates/api/src/url.rs), add `ret` to the accepted schemes list with a hard-coded known-default port. Extend the existing url unit tests to cover the new scheme. Confirmed as additive; decision on backport to `release-X.Y` lines is deferred to PR review.
4. **`config.rs`.** Define the config structs + `default_config` + `validate_config` on the factory. Unit test `validate_config`. No runtime yet.
5. **`url.rs`.** Identity-hash encode/decode against the `ret://reticulum:1/<hash>` canonical form, where `<hash>` is the hex-encoded `Identity.address_hash`. Unit tests.
6. **`frame.rs`.** Two-variant tag-byte framing. Unit tests.
7. **`destination.rs` trait layer.** Define traits wrapping the `rns_transport` types:
   - `Endpoint` — wraps `rns_transport::Transport` (`add_destination`, `link`, `send_packet`, `send_resource`, `recv_announces`)
   - `Destination` — wraps `Arc<Mutex<SingleInputDestination>>` (`announce`, `desc.address_hash`)
   - `Link` — wraps `Arc<Mutex<rns_transport::Link>>` (`data_packet`, `status`, `peer_identity`, `teardown`)
   
   Plus a fake implementation of each for tests.
8. **`peer_state.rs`.** The two-level connection map + per-peer preflight bookkeeping + `K2Proto` space-id extraction helper. Unit tests using plain fixtures, no Reticulum dependency.
9. **`link.rs`.** `LinkContext` + preflight handshake state machine + `link_reader_task`, built against the trait layer. Unit tests using the fake destination.
10. **`announce.rs`.** Per-space announce publisher (`dest.announce(OsRng, app_data)` on interval) and announce listener that filters `AnnounceEvent.name_hash` against precomputed `DestinationName::new("kitsune2", space_hash).as_name_hash_slice()`. Validates matching announces with `DestinationAnnounce::validate()` to extract the full `Identity`. Unit tests using the fake.
11. **`node.rs`.** `ReticulumNode` shared state: identity cache (`HashMap<AddressHash, Identity>`), per-space destination map, announce queues. Wires everything in steps 7–10 to a fake backend for tests.
12. **`bootstrap.rs` — `ReticulumBootstrapFactory`.** Drains announce queues and produces `AgentInfo`-equivalent records. Dedup is handled at the transport level by `AnnounceTable` (spike confirmed), but we still dedup at the bootstrap level by identity hash so the peer store doesn't churn. Unit test against the fake.
13. **`lib.rs` — `ReticulumTransport` + `ReticulumTransportFactory`.** Task management (`listening_address_emitter`, `global_announce_consumer_task`, per-space tasks spawned on `register_space_handler`), `TxImp` impl using `peer_state.rs`. Wire up `TxImpHnd` calls (`new_listening_address`, `peer_connect`, `peer_disconnect`, `recv_data`, `set_unresponsive`). Unit tests against the fake. During this step, verify that kitsune2 never triggers `peer_connect` before any space is joined — the `NoLocalAgentsDuringPreflight` check in [transport.rs:82-96](crates/api/src/transport.rs#L82-L96) suggests this is the case.
14. **Real `rns_transport` backend implementation.** Thin wrappers that implement the `destination.rs` traits over real `rns_transport` types. Compile-only; behavioural correctness is covered by functional + integration tests below.
15. **Functional tests.** Two-node harness wired via `InterfaceManager::new_channel()` synthetic channels and/or `test_bridge`, exercising handshake, per-space link independence, Resource path, idle timeout, announce-driven discovery.
16. **Integration test in `crates/kitsune2`.** Two-node gossip round via `Builder` + both Reticulum factories.
17. **README.md.** Architecture diagrams (ASCII, same style as [crates/transport_iroh/src/lib.rs](crates/transport_iroh/src/lib.rs)), config reference, known limitations (no browser, LoRa deferred, `max_frame_bytes` caveats and the "align with kitsune2 gossip frame size" follow-up item).
18. **Workspace polish.** Ensure `cargo make verify` passes end-to-end: `fmt`, `clippy --deny=warnings`, `doc-check` (nightly, `--cfg docsrs`), `taplo`, `test`. Verify the doc-check succeeds with `#![deny(missing_docs)]` on `lib.rs`.

## Spike results

Spikes completed in [spikes/](spikes/) against both `reticulum-rs-transport` v0.2.0 (LXMF-rs) and `reticulum` v0.1.0 (Beechat). All seven questions validated for the LXMF-rs crate:

| # | Question | Result | Plan impact |
|---|---|---|---|
| 1 | Multiple destinations per instance? | **YES** — `transport.add_destination()` per aspect, each gets unique `address_hash` | §2 per-space model validated |
| 2 | Offline destination-hash derivation? | **YES** — `new_out(pub_identity, app, aspect).desc.address_hash` matches actual, deterministic `SHA256(name_hash[..10] \|\| identity_hash[..16])[..16]` | §1 identity-hash URL validated |
| 3 | Full identity in announces? | **YES** — `DestinationAnnounce::validate()` reconstructs full `Identity` (X25519 + Ed25519 keys) | Identity cache from announces is feasible |
| 4 | Aspect-filtered announce subscription? | **PARTIAL** — `recv_announces()` gives all announces, but `AnnounceEvent.name_hash` enables cheap userspace filter against precomputed `DestinationName.as_name_hash_slice()` | §4 announce listener does userspace filter, not built-in |
| 5 | Link::send + Resource? | **YES** — `link.data_packet()` for ≤ `PACKET_MDU` (~383 bytes), `transport.send_resource()` for larger payloads with automatic chunking | §3 framing validated, no length-prefix needed |
| 6 | Loopback/in-memory interface? | **NO** dedicated loopback, but `InterfaceManager::new_channel()` provides synthetic channels and `test_bridge` provides inter-daemon testing | Functional tests use channels + test_bridge; unit tests use trait-based fakes |
| 7 | Announce dedup metadata? | **YES** — `rand_hash` (5 random + 5 timestamp bytes), Ed25519 signature, hops, interface fields. Transport-level `AnnounceTable` handles dedup internally | Bootstrap layer dedup is a bonus, not a necessity |

Beechat v0.1.0 was rejected: no Resource support, no `name_hash` on `AnnounceEvent`, no `AnnounceTable`, no `test_bridge`, no channel system, weaker announce metadata, requires protoc.

## What we are explicitly **not** doing

- Not writing a custom Reticulum transport-node / bootstrap server. Deployers bring their own Reticulum infrastructure.
- Not building LoRa support. The transport compiles against Reticulum-rs, which supports LoRa interfaces, but we don't test or tune for radio in v1.
- Not making breaking `kitsune2_api` changes. The `ret` URL scheme addition is strictly additive.
- Not tuning kitsune2's gossip layer to match Reticulum frame-size realities. That is a separate follow-up work item; for v1 we document the mismatch and set a conservative `max_frame_bytes` default.
- Not replacing the Iroh default. This is additive.
