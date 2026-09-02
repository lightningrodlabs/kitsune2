# mDNS Bootstrap for Kitsune2

Status: Implemented (v1)
Branch: `feat/mdns-bootstrap` (based on `v0.4.0-dev.9`)

## Implemented v1 shape

- `crates/transport_iroh` — new `mdns` cargo feature, new
  `IrohTransportConfig.enable_lan_discovery` flag. All iroh API surface
  related to LAN discovery lives in `crates/transport_iroh/src/lan_discovery.rs`
  as a one-file wrapper.
- `crates/core` — `CompositeBootstrapFactory` stacks multiple
  `BootstrapFactory`s over one peer store.
- `crates/bootstrap_mdns` (new) — `MdnsBootstrapFactory` + wire protocol
  (`proto.rs`), TCP session (`session.rs`), mDNS announce/browse
  (`discovery.rs`).
- Integration tests (`tests/lan_exchange.rs`, gated behind `K2_MDNS_IT`)
  verify same-space discovery and cross-space isolation.

## Motivation

Enable LAN-local peer discovery without a WAN bootstrap server, leveraging
iroh's built-in mDNS support for direct dialability and adding a Kitsune2-level
mDNS bootstrap for signed agent-info distribution. Design must:

1. Avoid leaking `space_id` (DNA hash) to any non-member on the LAN.
2. Keep a clean migration path when iroh is bumped past 0.95.1.
3. Compose with (not replace) the existing WAN `CoreBootstrap`.

## Threat model

- **Passive LAN listener** sees all mDNS broadcasts. Must learn nothing useful.
- **Active LAN attacker** can spoof mDNS records. Must not be able to inject
  bogus peers into a peer store.
- **Adversary with a candidate space_id list** can precompute fingerprints and
  confirm presence. Accepted limitation; time-bucketed rotation can mitigate
  in a future iteration.
- **WAN bootstrap server compromise** is a separate, larger problem (see
  "Follow-on" section). Not in scope for this plan.

## Two-layer design

### Layer A — iroh LAN dialability

Turn on iroh's `LocalSwarmDiscovery` so that once two nodes know each other's
`NodeId`, they can dial over the LAN without a relay.

- Feature-gate behind a new `mdns` cargo feature on `kitsune2_transport_iroh`.
  Default off.
- Add a single thin wrapper function in
  `crates/transport_iroh/src/endpoint.rs`:

  ```rust
  #[cfg(feature = "mdns")]
  fn enable_lan_discovery(b: EndpointBuilder, node_id: NodeId) -> EndpointBuilder;
  ```

  All iroh API churn across version bumps lives in this one function.
- Add a single config knob `TransportIrohConfig::enable_lan_discovery: bool`
  (default `false`).

### Layer B — Kitsune2 mDNS bootstrap

New crate `kitsune2_bootstrap_mdns` implementing `BootstrapFactory` /
`Bootstrap` from `crates/api/src/bootstrap.rs`. Uses a standalone mDNS
implementation (`mdns-sd`) — **not** iroh's mDNS — so the announcement
format is decoupled from iroh version churn.

#### Announcement payload

mDNS TXT carries a commitment, not the secret:

```
space_fp   = H(space_id || "k2-mdns-v1")     // 32 bytes
node_id    = iroh NodeId (raw bytes)
proto_ver  = u8
```

No raw `space_id`, no `AgentInfoSigned`, no agent public key. Fits in one TXT
record without chunking.

Service name: `_kitsune2._udp.local`, instance = base64(random 16B) to avoid
hostname collisions.

#### Proof-of-knowledge + info exchange

After mDNS discovery matches on `space_fp`, peers connect via the normal iroh
transport and run a small handshake protocol before exchanging agent info.

1. Both sides send fresh 32-byte nonces `n_a`, `n_b`.
2. Each computes
   `proof = HMAC(space_id, "k2-mdns-proof-v1" || n_self || n_peer)`
   and sends it.
3. Each verifies the peer's proof. Mismatch ⇒ abort.
4. Exchange `Vec<AgentInfoSigned>` for this space.
5. Verify signatures (reuse `DynVerifier`); insert into `peer_store`.

Wire format: explicit schema (protobuf or msgpack). Do **not** round-trip iroh
types through the wire — keep iroh types out of our message definitions.

#### Composite bootstrap

The mDNS bootstrap is **additional**, not a replacement. Options:

- (preferred) A `CompositeBootstrapFactory` that fans out `put` to all
  configured bootstraps and merges discovered peers into the shared peer store.
- Alternative: leave the builder with a single `DynBootstrapFactory` and have
  `CompositeBootstrapFactory` be the single factory users opt into when they
  want both.

## Iroh migration-friendliness rules

These apply across this whole change:

1. **Do not use iroh's mDNS for our announcement payload.** Use `mdns-sd`
   directly. Isolates us from iroh's mDNS API churn and from its lack of
   user-data support in 0.95.1.
2. **One-file iroh surface.** All iroh-version-sensitive calls related to LAN
   discovery live in `endpoint.rs` behind the wrapper above.
3. **Feature flags independent.** `transport_iroh/mdns` (Layer A) and
   `kitsune2_bootstrap_mdns` (Layer B) can be enabled separately.
4. **No iroh types at module boundaries.** The bootstrap crate operates on
   opaque NodeId bytes and Kitsune2 `Url`s; conversion is the transport's job.
5. **Pinned wire format.** Explicit schema for handshake + info-exchange
   messages, versioned with `proto_ver`. Never serialize iroh-native types
   onto the wire.
6. **Upgrade smoke test.** One integration test that boots two nodes, does
   LAN discovery, completes the handshake, and gossips one op. Run on every
   iroh bump as the canary.

## Milestones

1. **Spike** (0.5d): prove iroh 0.95.1 `LocalSwarmDiscovery` works between
   two in-process endpoints on localhost. Confirms Layer A viability.
2. **Layer A** (0.5d): feature flag, config, endpoint wrapper, LAN
   integration test.
3. **Composite bootstrap** (0.5d): factory that stacks multiple bootstraps
   feeding one peer store.
4. **Layer B — wire protocol** (1d): define handshake + info-exchange
   messages; unit-test the HMAC proof and signature verification in
   isolation.
5. **Layer B — mDNS glue** (1d): `MdnsBootstrapFactory`, announce/browse
   loops with `mdns-sd`, wire it to the handshake protocol, peer-store
   insertion.
6. **Tests** (1d): two-process LAN test with no WAN bootstrap; tombstone /
   expiry; wrong-space filtering; active-spoof rejection.
7. **Docs** (0.5d): README snippet, config reference, known-limitations
   note about space-id precomputation attacks.

Total: ~5 days.

## Open questions

- Protobuf vs msgpack for the wire format. Protobuf if we expect third-party
  implementers; msgpack if we want to stay lightweight and Rust-internal.
- Should `agent_id` also be hidden in discovery / post-handshake? Current
  plan exposes it only after mutual proof. Deferring further hardening.
- Rotation strategy: plain `H(space_id || tag)` for v1. Add
  `HMAC(space_id, epoch)` with clock-synced rotation in v2 if precomputation
  becomes a real concern.

## Follow-on: bootstrap-server hardening

Out of scope for this branch, but worth scheduling separately. A compromised
WAN bootstrap server today leaks every `space_id` it has seen plus the agent
set per space, because clients PUT raw `AgentInfoSigned`. Applying the same
commitment approach server-side is a real protocol redesign — the hard part
isn't the hashing, it's retaining PUT anti-spam / anti-DoS when the server
can no longer parse the payload. Flagged as a separate design round.
