# Spike-0 Findings: Reticulum-rs Crate Evaluation

**Date:** 2026-04-12
**Spike for:** `PLAN-transport-reticulum.md`, Task 0

## Crates evaluated

There are two independent Rust implementations of the Reticulum Network Stack:

| | **reticulum-rs v0.2.0** (LXMF-rs) | **reticulum v0.1.0** (Beechat) |
|---|---|---|
| **Repository** | [FreeTAKTeam/LXMF-rs](https://github.com/FreeTAKTeam/LXMF-rs) | [BeechatNetworkSystemsLtd/Reticulum-rs](https://github.com/BeechatNetworkSystemsLtd/Reticulum-rs) |
| **Crate name** | `reticulum-rs` (umbrella), `reticulum-rs-transport`, `reticulum-rs-core` | `reticulum` |
| **Rust module** | `rns_transport`, `rns_core` | `reticulum` |
| **License** | MIT | MIT |
| **Build deps** | Standard (no protoc) | Requires `protoc` (prost-build for kaonic proto) |
| **Async runtime** | Tokio | Tokio |

The plan references `github.com/tragen-fr/reticulum-rs` which does not match either repository. Both crates share the same core architecture (Identity, Destination, Link, Packet) but diverge significantly in feature completeness.

## Spike questions — comparison

| # | Question | reticulum-rs v0.2.0 (LXMF-rs) | reticulum v0.1.0 (Beechat) |
|---|---|---|---|
| Q1 | Multiple Destinations per instance? | **YES** — `transport.add_destination()` per aspect, verified with code | **YES** — identical API |
| Q2 | Offline destination hash from (identity, aspect)? | **YES** — `SHA256(name_hash[..10] \|\| identity_hash[..16])[..16]`, verified: computed == actual | **YES** — same algorithm, verified |
| Q3 | Announces carry full public identity? | **YES** — X25519 pubkey (32B) + Ed25519 verifying key (32B) + name_hash + rand_hash + signature. Supports optional ratchet keys. | **YES** — same format, no ratchet support |
| Q4 | Announce subscription filtered by aspect? | **PARTIAL** — `recv_announces()` is unfiltered broadcast, but `AnnounceEvent` includes `name_hash: [u8; 10]` for userspace filtering | **WORSE** — `AnnounceEvent` has only `destination` + `app_data`, no `name_hash`/`hops`/`interface` fields |
| Q5 | Link::send + Resource? | **YES** — `link.data_packet()` for ≤464B, `transport.send_resource()` for large payloads with auto chunking/compression | **PARTIAL** — `link.data_packet()` only (MDU=2048B). **No Resource support.** No channel system. |
| Q6 | Loopback/in-memory interface? | **NO** built-in, but `InterfaceManager::new_channel()` + `test_bridge` module available | **NO** built-in, `new_channel()` works, no `test_bridge` |
| Q7 | Announce metadata for dedup? | **YES** — `AnnounceEvent` has `name_hash`, `hops`, `interface`. Built-in `AnnounceTable` handles caching/dedup. Raw packet has `rand_hash` with embedded timestamp. | **WEAKER** — raw packet has `rand_hash` but not surfaced on `AnnounceEvent`. No `AnnounceTable`. |

## Feature comparison

| Feature | reticulum-rs v0.2.0 | reticulum v0.1.0 |
|---|---|---|
| Resource (large payload transfer) | Yes | **No** |
| Channel system (typed messaging) | Yes | **No** |
| Ratchet / forward secrecy | Yes | **No** |
| AnnounceTable (dedup/caching) | Yes | **No** |
| Configurable timeouts | Yes (link idle, proof, path request, resource retry) | **No** (hardcoded) |
| `received_data_events()` | Yes | **No** |
| `test_bridge` module | Yes | **No** |
| Packet MDU | 464 bytes | 2048 bytes |
| `protoc` build dependency | No | **Yes** |
| Kaonic (gRPC interface) | No | Yes |
| `InterfaceDriver` trait | Yes | Not visible |
| Documentation coverage | ~1.5% (docs.rs) | ~11% (docs.rs) |

## Key findings

### PACKET_MDU difference is significant

- LXMF-rs: `PACKET_MDU = 464` bytes — matches the Python Reticulum spec (~500B after headers)
- Beechat: `PACKET_MDU = 2048` bytes — **non-standard**, may not interoperate with Python Reticulum nodes

The LXMF-rs value is spec-correct. The Beechat value appears to be a deviation. For kitsune2 gossip payloads (which can be very large), the Resource abstraction in LXMF-rs is the proper way to handle this, not an inflated MDU.

### Resource support is a hard requirement

kitsune2 gossip produces payloads much larger than 464 bytes. Without Resource support:
- Beechat would require us to implement our own chunking/reassembly layer
- LXMF-rs provides `transport.send_resource(link_id, data, metadata)` with automatic compression, chunking, checksumming, and retransmission

### Both share the same core protocol

The hash computation, identity model, destination addressing, link handshake, and announce format are identical between the two crates. They appear to be independent implementations of the same spec, with LXMF-rs being further along in transport-layer features.

## Plan validation

All plan assumptions are **validated** by both crates:

1. **§1 URL shape** (`ret://reticulum:1/<identity-hash-hex>`): **VALIDATED**. Destination hashes are deterministic from `(public_identity, aspect)` and can be computed offline. The identity hash in the URL is the stable handle.

2. **§2 Per-space link model**: **VALIDATED**. One Transport instance hosts multiple Destinations (one per space), each with an independent address hash. Links are per-destination, which maps to per-`(peer, space)`.

3. **§3 Framing model**: **VALIDATED**. Reticulum handles message boundaries — each `data_packet()` / Resource transfer is a discrete payload. No length-prefixed framing needed on top. The `Preflight`/`Data` tag byte framing in the plan is correct and minimal.

4. **§4 Announce listeners**: **MINOR ADJUSTMENT**. No built-in aspect filtering. The transport's `space_announce_listener_S` task will need to filter `AnnounceEvent`s in userspace by comparing `name_hash` (LXMF-rs) or `destination.desc.name` (Beechat). This is straightforward.

5. **§5 Bootstrap via announces**: **VALIDATED**. Announces carry full public identities, enabling the `ReticulumBootstrap` to reconstruct peer Identity and compute per-space destination hashes on demand.

## Recommendation

**Use `reticulum-rs` v0.2.0 (LXMF-rs)** for `kitsune2_transport_reticulum`. Reasons:

1. Resource support is a hard requirement for kitsune2 gossip payloads — LXMF-rs has it, Beechat does not.
2. Richer `AnnounceEvent` with `name_hash`, `hops`, `interface` makes aspect filtering and dedup straightforward.
3. Built-in `AnnounceTable` for announce caching/dedup.
4. Configurable timeouts (link idle, proof, resource retry) map directly to `ReticulumTransportConfig`.
5. Channel system provides an additional messaging layer if needed.
6. No `protoc` build dependency.
7. Spec-correct `PACKET_MDU = 464` bytes.

The main risk is that both crates are early-stage (v0.1–v0.2) with sparse documentation. LXMF-rs is the safer bet given its more complete feature set.

### Dependency spec for Cargo.toml

```toml
[workspace.dependencies]
reticulum-rs-transport = "0.2.0"
```

The umbrella `reticulum-rs` crate is not needed — depend on `reticulum-rs-transport` directly, which re-exports identity, hash, and destination types from its own modules (avoiding the type-mismatch issue between `rns_core` and `rns_transport` when using both sub-crates).

## Corrections to the plan

No structural corrections needed. Minor adjustments:

1. **§4 `space_announce_listener_S`**: Add userspace `name_hash` filtering on `AnnounceEvent`. Not aspect-filtered at the subscription level.

2. **Dependency name**: The crate is `reticulum-rs-transport` (module name `rns_transport`), not `reticulum`. Update `Cargo.toml` spec in §Workspace integration accordingly.

3. **`max_frame_bytes` context**: LXMF-rs `PACKET_MDU = 464` bytes. Resource handles larger payloads transparently. The `max_frame_bytes` config in the plan should cap what we pass to `send_resource()`, not to `data_packet()`.

## Spike code

- LXMF-rs spike: `/tmp/reticulum-spike/` (compiles and runs without protoc)
- Beechat spike: `/tmp/reticulum-spike-beechat/` (requires `nix-shell -p protobuf` for protoc)
