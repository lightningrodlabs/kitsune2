# Plan: Beechat backend for `kitsune2_transport_reticulum`

Add a second Reticulum backend to `kitsune2_transport_reticulum` using [Beechat's `reticulum` crate](https://github.com/BeechatNetworkSystemsLtd/Reticulum-rs), switchable via Cargo feature flags. The existing LXMF-rs (`rns_transport`) backend remains the default. This follows the precedent set by `transport_tx5`, which uses feature flags to select between WebRTC backends.

## Motivation

Two independent Rust Reticulum implementations exist, each with distinct strengths:

| | LXMF-rs (`rns_transport` v0.2.0) | Beechat (`reticulum` v0.1.0) |
|---|---|---|
| PACKET_MDU | 464 bytes | **2048 bytes** |
| Resource chunking | Built-in | None |
| Multihop routing | Basic | **PathTable + PathRequests + retransmit** |
| Link restart/stale | Basic | **Configurable (`restart_outlinks`, timers)** |
| `request_path()` | No | **Yes** |
| `announce_forever` | No | **Yes** |
| `name_hash` on AnnounceEvent | Direct field | Not surfaced (must lock dest) |
| `peer_identity()` on Link | Public getter | Private field |
| `rusqlite` dep | Yes (requires patch) | **No** |
| Active development | Yes (FreeTAKTeam upstream) | Yes (pushed 2026-04-13) |

Making the backend swappable lets us evaluate both in production without committing to either, and lets downstream consumers choose based on their deployment topology (direct peers → LXMF-rs is fine; mesh/multihop → Beechat's routing is superior).

## Precedent: `transport_tx5`

`transport_tx5` uses feature flags to select a WebRTC backend, but its pattern is simpler than ours — it delegates entirely to the upstream `tx5` crate's `backend` module, so there's no local `#[cfg]` branching. Key patterns to adopt:

1. **Feature propagation chain.** The crate defines `backend-go-pion = ["tx5/backend-go-pion"]`, the top-level `kitsune2` crate re-exports that as `transport-tx5-backend-go-pion = ["kitsune2_transport_tx5/backend-go-pion"]`, and the showcase app chains through it. We should follow the same pattern.
2. **Default feature.** tx5 sets `default = ["backend-go-pion"]`. We should set `default = ["backend-lxmf"]` so the existing backend remains opt-out.
3. **Config is backend-agnostic.** tx5's `Tx5TransportConfig` works regardless of backend. Our `ReticulumTransportConfig` already achieves this — it references no `rns_transport` types.

What differs from tx5: we have two separate upstream crates with different APIs, not one crate with pluggable internals. So we *do* need local `#[cfg]` branching, concentrated in the backend module and in type re-exports.

## Design

### 1. Feature flags

```toml
# crates/transport_reticulum/Cargo.toml
[features]
default = ["backend-lxmf"]

# LXMF-rs / FreeTAKTeam backend (current)
backend-lxmf = ["dep:reticulum-rs-transport"]

# Beechat backend
backend-beechat = ["dep:reticulum"]

schema = ["dep:schemars"]
test-utils = ["dep:kitsune2_core", "dep:kitsune2_test_utils"]

[dependencies]
# Backend-specific deps are optional; exactly one must be enabled
reticulum-rs-transport = { workspace = true, optional = true }
reticulum = { workspace = true, optional = true }
# ... rest unchanged
```

Add `reticulum = { git = "https://github.com/BeechatNetworkSystemsLtd/Reticulum-rs", branch = "main" }` (or a lightningrodlabs fork) to `[workspace.dependencies]` in the root `Cargo.toml`.

Enforcement: `lib.rs` uses a compile-time check:

```rust
#[cfg(all(feature = "backend-lxmf", feature = "backend-beechat"))]
compile_error!("Only one Reticulum backend may be enabled at a time");

#[cfg(not(any(feature = "backend-lxmf", feature = "backend-beechat")))]
compile_error!("A Reticulum backend must be enabled: backend-lxmf or backend-beechat");
```

### 2. Type unification

Both crates define structurally identical types (same crypto, same hash sizes, same Reticulum protocol). The types that leak outside `backend.rs` are:

| Type | Used in | Both crates have it? |
|---|---|---|
| `AddressHash` (`[u8; 16]`) | destination.rs, node.rs, routers.rs, url.rs, announce.rs, lib.rs | Yes, identical layout |
| `Identity` (`{PublicKey, VerifyingKey, AddressHash}`) | destination.rs, node.rs, announce.rs, test_utils | Yes, identical layout |
| `PrivateIdentity` | node.rs | Yes, identical layout |
| `DestinationName` | destination.rs, node.rs, test_utils | Yes, identical layout + `as_name_hash_slice()` |

**Strategy: conditional re-exports from a `types.rs` module.**

```rust
// src/types.rs

#[cfg(feature = "backend-lxmf")]
pub(crate) use rns_transport::hash::AddressHash;
#[cfg(feature = "backend-beechat")]
pub(crate) use reticulum::hash::AddressHash;

#[cfg(feature = "backend-lxmf")]
pub(crate) use rns_transport::identity::{Identity, PrivateIdentity};
#[cfg(feature = "backend-beechat")]
pub(crate) use reticulum::identity::{Identity, PrivateIdentity};

#[cfg(feature = "backend-lxmf")]
pub(crate) use rns_transport::destination::DestinationName;
#[cfg(feature = "backend-beechat")]
pub(crate) use reticulum::destination::DestinationName;
```

All other modules import from `crate::types` instead of directly from `rns_transport`. This is a mechanical find-and-replace; the types are layout-identical so no logic changes.

### 3. Backend module split

Rename `backend.rs` → `backend_lxmf.rs`, create `backend_beechat.rs`. Gate them:

```rust
// src/lib.rs (or src/backend/mod.rs)
#[cfg(feature = "backend-lxmf")]
mod backend_lxmf;
#[cfg(feature = "backend-lxmf")]
use backend_lxmf as backend;

#[cfg(feature = "backend-beechat")]
mod backend_beechat;
#[cfg(feature = "backend-beechat")]
use backend_beechat as backend;
```

Both modules export the same public surface consumed by `node.rs`:

```rust
pub(crate) struct RealEndpoint { ... }
impl Endpoint for RealEndpoint { ... }
// + RealLink, RealDestination as impl Link, impl Destination
```

### 4. `node.rs` transport creation

The `from_config` method in `node.rs` calls `rns_transport::transport::TransportConfig::new(...)` and `rns_transport::transport::Transport::new(...)`, then creates interface spawners. This is backend-specific.

**Strategy:** Move the transport construction into a backend-specific factory function gated by `#[cfg]`, called from `from_config`. The `from_rns_transport` escape hatch (which accepts a caller-owned `rns_transport::Transport`) becomes `#[cfg(feature = "backend-lxmf")]`-only, with an equivalent `#[cfg(feature = "backend-beechat")]` `from_beechat_transport`.

Alternatively, `from_config` can call a backend function:

```rust
// In backend_lxmf.rs / backend_beechat.rs:
pub(crate) async fn create_endpoint(
    config: &ReticulumTransportConfig,
) -> K2Result<(DynEndpoint, AddressHash)> { ... }
```

This keeps `node.rs` backend-agnostic except for the one dispatch point.

### 5. Beechat-specific adaptations in `backend_beechat.rs`

#### 5a. AnnounceEvent → AnnounceInfo

Beechat's `AnnounceEvent` has `{ destination: Arc<Mutex<SingleOutputDestination>>, app_data: PacketDataBuffer }` — no `name_hash` or `hops`.

The announce bridge must lock the destination to extract the identity and derive the name hash:

```rust
fn spawn_announce_bridge(
    mut announce_rx: broadcast::Receiver<reticulum::transport::AnnounceEvent>,
    tx: broadcast::Sender<AnnounceInfo>,
) {
    tokio::spawn(async move {
        loop {
            match announce_rx.recv().await {
                Ok(ev) => {
                    let (identity, name_hash) = {
                        let dest = ev.destination.lock().await;
                        let identity = dest.desc.identity;
                        let mut nh = [0u8; 10];
                        nh.copy_from_slice(dest.desc.name.as_name_hash_slice());
                        (identity, nh)
                    };
                    let app_data = Bytes::copy_from_slice(ev.app_data.as_slice());
                    let _ = tx.send(AnnounceInfo {
                        identity,
                        app_data,
                        name_hash,
                        hops: 0, // Beechat doesn't surface hops on AnnounceEvent
                    });
                }
                // ... error handling same as LXMF-rs version
            }
        }
    });
}
```

This is slightly more expensive (one mutex acquisition per announce) but announces are infrequent.

#### 5b. Link → DynLink (peer identity)

Beechat's `Link` has a `peer_identity: Identity` field but no public getter. The `destination()` method returns `&DestinationDesc` which has `.identity`.

For **outbound** links, `destination.identity` is the remote peer's identity (set at construction from the `SingleOutputDestination`).

For **inbound** links, `destination.identity` is the remote peer's identity as seen in the link request.

So `link.destination().identity.address_hash` replaces `link.peer_identity().address_hash`. Verify this empirically in the Beechat tests before relying on it for both link directions. If it doesn't work for inbound links, fork Beechat and add `pub fn peer_identity(&self) -> &Identity`.

#### 5c. Link data events

Beechat delivers data via `LinkEvent::Data(LinkPayload)` through the `in_link_events()` / `out_link_events()` broadcast channels, where the `LinkEventData` carries a `LinkId`. It also has `received_data_events()` for non-link single-destination data.

The `spawn_inbound_link_bridge` and `spawn_received_data_bridge` will dispatch from `LinkEvent::Data` instead of from `received_data_events` + `resource_events` (since Beechat has no Resources). The data bridge is simpler:

```rust
// Listen to in_link_events + out_link_events, forward Data payloads
LinkEvent::Data(payload) => {
    let data = Bytes::copy_from_slice(payload.as_slice());
    let _ = data_tx.send((event.id, data));
}
```

No `resource_events` bridge, no `PacketContext` filtering — those are LXMF-rs-specific.

#### 5d. No `send_resource` — chunking or MDU-only?

Beechat has no Resource abstraction. `PACKET_MDU = 2048`. After encryption overhead, usable payload per packet is ~1900–2000 bytes.

**Phase 1 (this plan): fail sends that exceed MDU.**

The `Endpoint::send_resource` method returns an error for the Beechat backend. The `Endpoint::send_packet` method (which is currently unimplemented even for LXMF-rs — see `backend.rs:430`) becomes the primary send path. Since the MDU is 4x larger, most kitsune2 messages should fit. We document the limitation and measure actual gossip payload sizes in functional tests.

**Phase 2 (follow-up): add a chunking layer if needed.**

If gossip payloads routinely exceed ~1900 bytes, implement a simple chunking protocol in `frame.rs`:

```
ChunkedFrame:
  tag byte: 0x03
  sequence_id: u32 (4 bytes)
  fragment_index: u16 (2 bytes)
  fragment_count: u16 (2 bytes)
  payload: [u8]
```

Reassembly buffer in `routers.rs`, keyed by `(link_id, sequence_id)`. Timeout incomplete sequences after 30s. This is ~200 lines — far simpler than LXMF-rs's Resource protocol.

#### 5e. No `test_bridge` — TCP loopback for functional tests

Beechat's integration tests use real TCP connections on loopback (`127.0.0.1:port`). The functional tests in `tests/two_node_data.rs` would need a Beechat-specific harness that creates two transports connected via TCP. The Beechat test infrastructure (`hop_test.rs`) provides a `build_transport()` helper pattern that does exactly this.

The unit tests are unaffected — they use the `FakeEndpoint` / `FakeLink` / `FakeDestination` from `test_utils/`, which are backend-agnostic (they implement the `destination.rs` traits, not the Reticulum types directly).

#### 5f. Interface startup

Beechat's interface API is very similar to LXMF-rs:

| LXMF-rs | Beechat |
|---|---|
| `rns_transport::iface::tcp_client::TcpClient::new(target)` | `reticulum::iface::tcp_client::TcpClient::new(target, iface_manager)` |
| `rns_transport::iface::tcp_server::TcpServer::new(addr, iface_manager)` | `reticulum::iface::tcp_server::TcpServer::new(addr, iface_manager)` |
| `rns_transport::iface::udp::UdpInterface::new(addr)` | `reticulum::iface::udp::UdpInterface::new(addr)` |

The `start_interfaces` function in `node.rs` would be duplicated into backend-specific modules, or factored into the `create_endpoint` factory function since it needs the transport handle.

#### 5g. Beechat TransportConfig extras

Beechat's `TransportConfig` has options our LXMF-rs backend doesn't:

- `set_retransmit(bool)` — act as a transport node, forwarding packets for others
- `set_broadcast(bool)` — broadcast mode
- `set_reroute_eager(bool)` — prefer newer routes even if same hop count
- `set_restart_outlinks(bool)` — auto-restart closed outbound links
- `set_announce_forever(bool)` — keep retransmitting announces

These should be exposed as **optional fields** on `ReticulumTransportConfig`, documented as Beechat-only, and silently ignored when the LXMF-rs backend is active. This keeps the config struct shared.

## Workspace integration

### Root Cargo.toml

```toml
[workspace.dependencies]
# Existing:
reticulum-rs-transport = { git = "...", branch = "..." }

# New:
reticulum = { git = "https://github.com/BeechatNetworkSystemsLtd/Reticulum-rs", branch = "main" }
```

### Top-level `crates/kitsune2/Cargo.toml`

Add feature variants:

```toml
[features]
# Existing:
transport-reticulum = [
  "dep:kitsune2_transport_reticulum",
  "kitsune2_transport_reticulum/backend-lxmf",
]

# New:
transport-reticulum-beechat = [
  "dep:kitsune2_transport_reticulum",
  "kitsune2_transport_reticulum/backend-beechat",
]
```

## File changes summary

### New files

| File | Contents |
|---|---|
| `src/types.rs` | Conditional re-exports of `AddressHash`, `Identity`, `PrivateIdentity`, `DestinationName` |
| `src/backend_beechat.rs` | `RealEndpoint`, `RealDestination`, `RealLink` impl against `reticulum` crate |

### Renamed files

| From | To |
|---|---|
| `src/backend.rs` | `src/backend_lxmf.rs` |

### Modified files

| File | Change |
|---|---|
| `Cargo.toml` | Feature flags, optional deps |
| `src/lib.rs` | `#[cfg]` gating for backend modules, compile-time checks |
| `src/destination.rs` | `use crate::types::*` instead of `use rns_transport::*` |
| `src/node.rs` | `use crate::types::*`, backend factory dispatch in `from_config` |
| `src/announce.rs` | `use crate::types::*` |
| `src/routers.rs` | `use crate::types::*` |
| `src/url.rs` | `use crate::types::*` |
| `src/peer_state.rs` | `use crate::types::*` (test code only) |
| `src/config.rs` | Optional Beechat-specific fields |
| `src/test_utils/harness.rs` | `use crate::types::*` |

### Unchanged files

| File | Why |
|---|---|
| `src/frame.rs` | No Reticulum type dependencies |
| `src/routers.rs` (logic) | Works through trait layer |
| `src/bootstrap.rs` | Works through trait layer |
| `src/link.rs` | Works through trait layer |

## Task breakdown

### Phase 1: Type unification and backend split (no new code)

1. **Create `src/types.rs`** with `#[cfg]`-gated re-exports. Initially only the `backend-lxmf` arm exists.
2. **Mechanically replace** all `use rns_transport::{hash::AddressHash, identity::*, destination::DestinationName}` with `use crate::types::*` across all files except `backend.rs`.
3. **Rename** `backend.rs` → `backend_lxmf.rs`.
4. **Update `lib.rs`** with `#[cfg]` gating, compile-time checks, and `use backend_lxmf as backend`.
5. **Update `Cargo.toml`** — make `reticulum-rs-transport` optional behind `backend-lxmf`, add `default = ["backend-lxmf"]`.
6. **Verify** `cargo make verify` passes with `--features backend-lxmf`. All existing tests still pass.

### Phase 2: Beechat backend

7. **Add `reticulum` to workspace deps** in root `Cargo.toml`.
8. **Add `backend-beechat` arm** to `types.rs` re-exports. Verify the types are compatible (same field names, same public methods we use).
9. **Write `backend_beechat.rs`** — implement `RealEndpoint`, `RealDestination`, `RealLink` against `reticulum::*` types. Key differences from `backend_lxmf.rs`:
   - Announce bridge: lock destination to get `name_hash` and identity
   - Data bridge: `LinkEvent::Data` instead of `received_data_events` + `resource_events`
   - No resource bridge
   - `send_resource` returns error (or uses `send_packet` with MDU check)
   - `send_packet` actually implemented (Beechat's `Transport::send_packet` takes a `Packet` type — need to reconstruct it from bytes or change the trait)
   - Link creation: `transport.link(dest_desc)` API matches
   - `peer_identity_hash`: use `link.destination().identity.address_hash`
10. **Backend factory in `node.rs`** — `create_endpoint` for Beechat backend, gated behind `#[cfg(feature = "backend-beechat")]`. Wire up interface spawners using Beechat's interface API.
11. **Config additions** — add Beechat-only fields (`retransmit`, `announce_forever`, etc.) as `Option` fields, documented, default `None`.
12. **Compile check** — `cargo check --features backend-beechat` builds successfully.

### Phase 3: Testing

13. **Unit tests** — existing tests pass with both `--features backend-lxmf` and `--features backend-beechat` (unit tests use `FakeEndpoint`, so they're backend-agnostic).
14. **Functional test harness for Beechat** — TCP-loopback-based two-node setup, gated behind `#[cfg(feature = "backend-beechat")]`. Pattern from Beechat's `tests/hop_test.rs`.
15. **Functional tests** — two-node preflight, send/recv, announce discovery over real Beechat transport. Measure actual payload sizes to determine if chunking is needed.

### Phase 4: Follow-up (out of scope for this plan)

- Chunking layer if gossip payloads exceed Beechat's MDU.
- Expose Beechat's `request_path()` through a kitsune2 API.
- Beechat-specific integration test in `crates/kitsune2/`.
- Evaluate whether to make Beechat the default.

## Risks

1. **`link.destination().identity` may not work for inbound links.** Beechat's `Link` stores `destination: DestinationDesc` which is set at construction. For inbound links, the `destination` may be the *local* destination, not the peer's. If so, the `peer_identity` private field is the only source, and we'd need a Beechat fork to add a getter. Mitigation: verify in the Phase 3 functional test before building the full bridge. This is the highest-risk item.

2. **`protoc` build dependency.** Beechat uses `tonic-build` which requires `protoc` at build time. This must be available in CI. The kitsune2 workspace already uses `prost-build` for its own protos, so `protoc` may already be in the CI image — verify.

3. **Beechat is pre-1.0 and API-unstable.** Pinning to a git rev (not `branch = "main"`) mitigates this. Same situation we have with LXMF-rs today.

4. **MDU may be too small for gossip.** 2048 bytes is much better than 464, but kitsune2 gossip payloads can be large. The Phase 1 approach (fail on oversize) makes this visible early. The chunking layer is a known fallback.
