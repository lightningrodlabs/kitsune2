# Broadcast transports for Kitsune2

Status: draft / in progress
Branch: `broadcast-transport` (based on upstream `main`, kitsune2 `0.5.0-dev.6`,
compatible with holochain 0.7 / `develop`)

## 1. Motivation

Kitsune2's current transports (iroh, tx5) are connection-oriented: every peer
relationship is a managed 1:1 connection, with all the machinery that implies —
NAT traversal, relays, bootstrap servers, preflight exchange, unresponsive-peer
tracking, reconnection policy. Connection management is one of the deepest
sources of complexity and failure modes in the stack.

This design explores a different physical assumption: a **shared broadcast
medium** that nodes simply transmit into and listen on. No connections exist;
"reachability" degrades to "was heard recently". Candidate media:

1. Ultrasonic sound (speaker → microphone)
2. Optical (screen flashing → camera)
3. Bluetooth (BLE advertising / isochronous broadcast)
4. WiFi radio (UDP multicast on a LAN; raw 802.11 frame injection)

### Why Holochain's data model fits broadcast unusually well

- **Ops are self-authenticating.** Signed, content-addressed, validated on
  receipt. The receiver never needs to trust — or even identify — the
  transmitter. The incoming-ops pipeline already treats network data as
  hostile.
- **DHT state is a monotonic set.** Union is the only merge operation.
  Duplicate reception is idempotent, loss is healed later, ordering is
  irrelevant, replay attacks are no-ops. Lossy feedback-free media are
  therefore *safe*, just slow.
- **Source chains give nearly-free anti-entropy.** A chain head
  (`author pubkey + seq + head hash` ≈ 70 bytes) summarizes an agent's entire
  contribution to the DHT. A node hearing "agent X is at seq 900" while
  holding seq 850 knows precisely what it's missing — a contiguous chain
  segment — with no pairwise set-reconciliation round trips. Generic DHT sync
  needs Bloom/IBLT-style diffing; per-agent chains need a 70-byte beacon.

"Fast push, slow heal" maps directly: **push** = broadcast newly authored ops
into the air; **heal** = periodically broadcast chain heads and arc
fingerprints, and let whoever is behind ask. Every repair heals all listeners
at once, turning gossip's O(pairs) traffic into O(deficits).

This is also the offline-friendly principle taken to its limit: a silent or
jammed channel is indistinguishable from being offline, and the system already
must tolerate that.

## 2. Media survey (realistic numbers)

| Medium | Throughput | Range | Duplex | Best role |
|---|---|---|---|---|
| Ultrasonic (17–20 kHz MFSK, à la ggwave) | 10–100 B/s (~1 kB/s ideal) | 1–10 m, one room | half | proximity discovery/bootstrap side-channel; tiny spaces |
| Screen → camera (animated matrix codes, cimbar-class) | 1–90 kB/s | < 2 m, line of sight | **simplex** | one-way DHT seeding carousel; air-gap ingestion |
| Bluetooth (BT5 extended advertising; BIS/PAwR later) | 0.1–2 kB/s sustained | 10–100 m (coded PHY) | broadcast + listen | ambient neighborhood mesh; multi-hop via TTL re-advertise |
| WiFi (UDP multicast on LAN; raw injection à la wifibroadcast) | 0.1–10 MB/s | LAN / ~100 m | broadcast + listen | first shippable target; full-app viable |

Notes per medium:

- **Ultrasonic.** Commodity speakers/mics leave ~2 kHz of usable near-ultrasound
  spectrum. A ~500-byte signed op takes 5–60 s — not a DHT transport. But a
  70-byte chain-head beacon or a "here is my agent info + my faster-medium
  address" bootstrap frame fits in seconds. Killer role: co-present discovery
  feeding the other transports. Half-duplex forces suppression discipline.
- **Screen/camera.** Physically simplex per device pair; a return channel does
  not scale past two devices. Model it as a **fountain-coded carousel**: a node
  continuously cycles its op store as animated codes; any camera that watches
  long enough ingests ops. The monotonic-set model makes one-way ingestion
  always valid. Likely ships as tooling (`emit`/`ingest`) rather than a live
  transport.
- **Bluetooth.** BT5 extended advertising gives connectionless ~255 B PDUs
  (chainable to ~1.6 kB) — one chain-head beacon fits in a single
  advertisement. BLE-mesh-style managed flooding (re-advertise heard frames,
  TTL-limited) buys multi-hop for free because frames are already
  self-authenticating and idempotent. Platform order: Linux/BlueZ first,
  Android next; iOS background advertising is too constrained.
- **WiFi.** UDP multicast (group/port derived from config, ultimately from the
  space) is a real megabit broadcast medium today on LANs and batman-adv
  meshes, and is where the protocol gets validated at speed. Raw 802.11
  injection (the wifibroadcast/OpenHD lineage) offers association-free
  broadcast at Mbps with FEC for dedicated (root, monitor-mode) nodes.

## 3. Architecture

One core crate, thin media backends:

```
crates/transport_broadcast        kitsune2_transport_broadcast
  src/medium.rs                   BroadcastMedium trait (the physical layer)
  src/frame.rs                    wire format: src/dst ids, tags, chunk headers
  src/chunking.rs                 fragmentation/reassembly (pure, unit-tested;
                                  adapted from the reticulum transport work)
  src/peers.rs                    virtual-connection table (phase 1)
  src/lib.rs                      TxImp + TransportFactory
  src/mediums/mem.rs              in-process shared-air test medium
  src/mediums/udp_multicast.rs    UDP multicast medium
crates/transport_switch           kitsune2_transport_switch
  src/lib.rs                      runtime-selectable multi-backend factory
```

### 3.1 The physical abstraction

```rust
pub trait BroadcastMedium: 'static + Send + Sync + std::fmt::Debug {
    /// Largest frame this medium can carry in one transmission.
    fn mtu(&self) -> usize;
    /// Rough sustained throughput, bytes/sec — used to scale timers.
    fn est_bytes_per_sec(&self) -> u32;
    /// True if the medium cannot listen while transmitting (sound, screen).
    fn half_duplex(&self) -> bool;
    /// Fire a frame into the air. Best-effort; no delivery guarantee.
    fn transmit(&self, frame: bytes::Bytes) -> BoxFut<'_, K2Result<()>>;
    /// Stream of every frame heard on the medium (including, on some
    /// media, our own transmissions — the frame layer filters those).
    fn frames(&self) -> futures::stream::BoxStream<'static, bytes::Bytes>;
}
```

A medium knows nothing about kitsune2 — no peers, no spaces, no connections.
Everything above it is shared across all four media.

### 3.2 Frame layer

Every frame carries a fixed header:

```
+-------+---------+------------+------------+-----+---------+
| magic | version | src NodeId | dst NodeId | tag | payload |
| 2 B   | 1 B     | 8 B        | 8 B        | 1 B | var     |
+-------+---------+------------+------------+-----+---------+
```

- `NodeId` is an ephemeral 8-byte random id generated at transport start;
  it appears as the peer-id path segment of the node's URL.
- `dst = 0x00..00` is the broadcast address (phase 2 native mode); phase 1
  unicast emulation always addresses a specific node, and non-addressees
  drop the frame at the header check.
- Tags: `PREFLIGHT`, `DATA`, `CHUNK`, `GOODBYE` (best-effort disconnect
  notice), reserved space for phase-2 `BEACON` / `WANT` / `REPAIR`.
- Payload over the medium MTU is fragmented by the chunking layer
  (`sequence_id`, `fragment_index`, `fragment_count`), reassembled per
  `(src NodeId, sequence_id)` with timeout-based eviction. This is the
  reticulum chunking design with link ids replaced by sender ids.

Encryption (space-keyed AEAD so only members can read the air, HMAC space
tags so outsiders cannot correlate traffic to a DNA) is deliberately deferred
to phase 2: phase 1 media (in-proc, LAN multicast) are for protocol
validation. The header reserves the version byte to make that change
non-breaking.

### 3.3 URL scheme

`ws://<medium>.bcast:1/<node-id-hex>` — e.g.
`ws://udpm.bcast:1/1a2b3c4d5e6f7081`.

Host = medium name under a reserved `.bcast` label, port is a constant `1`
(broadcast media have no ports; the kitsune2 `Url` type requires one), path
segment = ephemeral `NodeId`, so `Url::peer_id()` works unchanged. Using the
`ws` scheme follows the precedent of core's `MemTransport`
(`ws://stub.tx:42/<id>`) and — deliberately — avoids modifying
`kitsune2_api`'s `Url` validator, keeping this crate buildable against the
published `kitsune2_api 0.5.0-dev.6` that holochain `develop` pins. A
dedicated `bcast` scheme (the one-line validator change the reticulum branch
made for `ret`) can come later when upstreaming.

### 3.4 Phase 1 — unicast emulation (compatibility)

Implements `TxImp` exactly, so gossip/fetch/publish/bootstrap run unmodified
and the existing test suites become the conformance harness.

- A **virtual connection** is an entry in a peer table keyed by `NodeId`,
  created on first frame heard from (or first send to) a peer.
  - Outgoing open: `TxImpHnd::peer_connect(url)` → transmit the returned
    preflight frame addressed to the peer.
  - Incoming open: first frame from an unknown `NodeId` → same dance.
  - Idle timeout (scaled by `est_bytes_per_sec`) → `peer_disconnect`.
  - `disconnect()` transmits a best-effort `GOODBYE` frame.
- `send()` = look up / open virtual connection, chunk if needed, transmit
  frames addressed to the peer.
- `get_connected_peers()` = peers heard within the idle window — which is all
  "connected" ever means on a broadcast medium.
- Media access: listen-before-talk with randomized backoff lives in the frame
  scheduler, not the medium (a broadcast node is always listening anyway).

Known, accepted limitations of phase 1: no per-frame FEC or retransmission
(fine on in-proc and LAN multicast; documented as unfit for lossy media until
the fountain-coding layer lands), and the same preflight-loss sensitivity any
connection emulation has.

### 3.5 Phase 2 — native broadcast (the payoff)

Adds an optional capability to `kitsune2_api` (mirroring how per-space hooks
were added for the reticulum transport):

```rust
pub trait TxBroadcastImp {
    fn broadcast(&self, space: SpaceId, data: bytes::Bytes)
        -> BoxFut<'_, K2Result<()>>;
}
// plus TxSpaceHandler::recv_space_broadcast(src_hint, space, data)
```

and three module replacements that exploit it:

- **`broadcast_publish`** — authoring emits ops once into the air instead of
  per-peer fan-out; overheard validation receipts count for every listener.
- **`broadcast_gossip`** — Trickle-paced (RFC 6206) beacons carrying
  `{agent-info digest, chain heads held, DHT arc fingerprint}`; suppression
  keeps N nodes from melting a slow channel. Repair is SRM-style: a behind
  node schedules a `WANT` after a random delay and suppresses if it overhears
  an equivalent request; any holder schedules a `REPAIR` the same way. Every
  repair heals all listeners simultaneously. Mechanism details adopted from
  MemoryLAN (§7.3): repair replies are additionally gated by a
  density-scaled probability (~1/d, with d estimated per interface from
  overheard beacon ids), and set-fingerprint beacons re-salt their Bloom
  filter each round so hash-collision false negatives surface over time.
  Repair is served from a small FIFO **air cache** of recently heard pages
  with move-to-front on duplicate reception — so any node (or relay) can
  answer for content it merely overheard, and duplicates act as a
  popularity signal rather than waste.
- **Bootstrap = beacons.** No bootstrap server on-medium; hearing a beacon
  *is* discovery.

Multi-hop uses content-hash flooding, not TTL: a re-broadcasting node
suppresses forwarding when the frame's hash is in a fixed-size FIFO history
(MemoryLAN's loop-prevention; see §7.3). This is addressless, needs no hop
budget, and composes with idempotent self-validating pages.

Blocking shifts from the connection edge to ingestion: you cannot close a
connection that does not exist, so frames/ops attributable to blocked agents
are dropped at receipt instead.

Payloads larger than a few MTUs get fountain coding (RaptorQ) rather than
ACK/retransmit — the canonical answer for feedback-free broadcast, and what
makes the simplex optical medium usable at all.

### 3.6 Remote signals over broadcast

Holochain's remote signals (`send_remote_signal`) are already an
*API-level* multicast: one signal, a list of recipient agents. On the wire
today they are pure unicast fan-out — the ribosome builds a **per-recipient**
signed payload (the recipient's `cell_id` is baked into the signed
`ZomeCallParams`, sharing one nonce across the batch) and `holochain_p2p`
fires one `send_space_notify` per recipient, errors ignored. Two properties
make signals the ideal *first* consumer of the phase-2 broadcast capability:
the delivery contract is already fire-and-forget best-effort (exactly what a
broadcast medium provides), and there is no protocol state machine at all —
it is a pure fan-out replacement, simpler than publish or gossip.

Under phase 1, signals work unchanged but wastefully: an N-recipient signal
is N addressed frames on a medium where one transmission physically reaches
everyone. Phase 2 collapses this to one frame per signal, in **two modes**:

**Mode 1 — polite ignore.** The frame carries the signal body once plus an
advisory audience header: either a *listen* list or an *ignore* list —
whichever is shorter — with "empty ignore list" as the whole-space
broadcast. Every space member can physically read the body (it sits inside
the phase-2 space-keyed AEAD, so non-members cannot); non-addressed members
politely drop it at the header. Enforcement is etiquette, not cryptography.
This requires the signature change: instead of binding a per-recipient
`cell_id`, the sender signs `(signal, space, audience, nonce, expiry)` once.
Receiving conductors deliver to each hosted agent matching the audience and
dedupe by `(provenance, nonce)` — which also makes TTL-flooded re-broadcast
on multi-hop media (BLE relaying) safe. Note the semantic shift: today a
signal is readable only by its recipients (each unicast rides an encrypted
connection); polite-ignore makes it readable by the whole space. That is a
*visible* confidentiality change, so it must be opt-in at the HDK API, never
a transparent transport optimization.

**Mode 2 — encrypted.** Confidential to the recipient subset, not just the
space. Two sub-options, deliberately staged:

- *2a — per-recipient key wrapping (first).* Encrypt the body once under a
  fresh symmetric message key; append one wrapped copy of that key per
  recipient (crypto-box style). Frame size is `O(body) + O(recipients ×
  ~72 B)` rather than N full copies. Holochain already exposes the
  necessary primitives as host functions
  (`create_x25519_keypair`, `x_25519_x_salsa20_poly1305_encrypt` backed by
  lair), so recipients' x25519 encryption keys can be exchanged at the app
  layer today; a later refinement is publishing an encryption key alongside
  the agent key in `AgentInfo`. Stateless — right for ad-hoc one-shot
  signals.
- *2b — group ratchet (later).* For stable collaboration groups (a syn
  session, a chat room), a group-negotiated evolving key — sender-key
  ratchet à la Signal groups, or MLS-style tree agreement — amortizes the
  per-signal overhead to `O(1)` and adds forward secrecy, at the cost of
  session establishment and membership-change handling. Same frame
  audience header as mode 1, different key schedule. Design note: keep the
  ratchet strictly at the app/HDK layer over the same broadcast frame
  format, so the transport stays oblivious to group membership.

Signals are also the story for the slow media: a presence ping or cursor
update fits a single BLE advertisement or a few seconds of ultrasound, where
DHT sync never will. Mode 1 on a Trickle-paced channel is what makes those
media useful live rather than only for discovery.

## 4. Hot-switchable backends (single binary)

Requirement: one binary containing every compiled-in transport backend, with
the *active* backend chosen — and switchable — at runtime via config, and that
choice ultimately surfaced in UIs like Moss.

### 4.1 `kitsune2_transport_switch`

A `SwitchableTransportFactory` wraps a set of named inner
`TransportFactory` instances (e.g. `"iroh"`, `"bcast-udpm"`, `"bcast-mem"`,
later `"reticulum"`):

- Config key `transportSwitch.active` selects the backend to instantiate at
  `create()` time. All registered factories contribute their defaults to the
  merged config; validation validates the selected backend.
- The factory hands the *shared* `TxImpHnd` to whichever inner backend is
  active, wrapping its `TxImp` in a delegating `SwitchTxImp` whose active slot
  is swappable at runtime.
- **Runtime switch** (via the `Config` runtime-update callback mechanism that
  `kitsune2_api::config` already provides): construct the new backend's
  `TxImp`, swap the active slot, then fire
  `TxImpHnd::new_listening_address(new_url)`. Spaces already respond to that
  event by re-signing and re-publishing agent infos with the new URL, which is
  exactly what a transport change requires. Peers on the old transport age out
  through the normal unresponsive path; the old imp is torn down after a
  drain period.

A switch is deliberately modeled as "this node moved", because from the
network's perspective that is what happened. Running multiple backends
*concurrently* (hear on all, send on best) is a future extension; it requires
multi-URL agent infos and is out of scope here.

### 4.2 Plumbing to Holochain and Moss

The path, in order:

1. `holochain_p2p` builds the kitsune2 `Builder` — today it hardwires the
   iroh factory behind the `transport-iroh` cargo feature. It instead
   registers the switch factory with all compiled-in backends
   (features control *inclusion*; config controls *selection*).
2. `NetworkConfig` in the conductor config gains a `transport` section that
   maps onto the `transportSwitch` module config (serde-schema'd, so it
   appears in the generated conductor-config schema).
3. An admin-API call (`DumpNetworkStats` already exposes
   `TransportStats.backend`; add a `SetNetworkTransport` or piggyback on
   runtime config update) exposes read + switch.
4. Moss reads/writes that admin call — a transport picker in the UI
   ("Internet (iroh) / Local network (WiFi broadcast) / Bluetooth / …"),
   showing the active backend from network stats.

Steps 1–2 are holochain-repo work on `develop`; 3–4 follow. This document
covers the kitsune2 side; the switch crate is designed so holochain's
integration is configuration, not code.

## 5. Threat model notes

- **Integrity**: unchanged — ops are signed and validated; the medium is
  untrusted by construction.
- **Confidentiality**: an open medium is recordable by anyone in range.
  Phase-2 space-keyed AEAD limits readability to holders of the network seed;
  that is bundle-possession membership, *not* membrane proof. Traffic timing
  and presence still leak.
- **Availability**: jamming a physical medium is trivially possible and
  unfixable at this layer; the system degrades to "offline", which it must
  already tolerate.
- **Replay/spoofing**: replayed ops are idempotent no-ops. Spoofed `NodeId`s
  can confuse the phase-1 virtual-connection table but cannot forge ops;
  phase-2 native mode barely cares about node identity at all.

## 6. Build order

1. **Core crate + in-proc medium + phase-1 emulation** — `BroadcastMedium`
   trait, frame/chunking layers, virtual-connection `TxImp`, and a
   configurable in-process "shared air" medium (loss, delay, partitions) for
   deterministic tests. Prove the plumbing against the standard transport
   usage patterns.
2. **UDP multicast medium** — first real backend; fast enough that protocol
   bugs are not hidden by bitrate. Shippable for LAN/mesh local-first use.
3. **`transport_switch`** — runtime backend selection over
   {iroh, bcast-udpm, bcast-mem}; switch test swaps backends mid-run and
   verifies re-announce.
4. **Phase-2 native broadcast** — `TxBroadcastImp` api extension, then its
   consumers in order of increasing complexity: **broadcast signals first**
   (§3.6 — pure fan-out replacement, mode 1 "polite ignore" then mode 2a
   encrypted; needs the audience-signature change on the holochain side),
   then Trickle beacons with chain-head repair and SRM suppression;
   benchmark against pairwise gossip in `kitsune2_showcase` (the O(pairs) →
   O(deficits) claim should be measurable).
5. **BLE extended-advertising medium** (BlueZ), then TTL flooding for
   multi-hop.
6. **Ultrasonic medium** scoped to beacon/bootstrap frames; wire as a
   discovery feeder for BLE/WiFi.
7. **Optical carousel** as emit/ingest tooling.

The chain-head-beacon gossip design (step 4) is worth having even on iroh: it
exploits a structural property of Holochain data that generic DHT sync
ignores.

## 7. Relationship to other in-flight lightningrodlabs work

### 7.1 Reticulum transport (`kitsune2-lrl` `transport-reticulum`)

That branch pioneered several pieces this design reuses: per-medium trait
abstraction for testability, announce-driven bootstrap replacing the HTTP
server, the pure chunking layer (copied here with link ids replaced by sender
node ids). This branch intentionally starts from upstream `main` rather than
that branch; the reticulum transport can later rebase onto this base and
potentially become a `BroadcastMedium`-style backend itself (Reticulum
interfaces are themselves broadcast-capable at the LoRa/packet-radio layer).

### 7.2 mDNS bootstrap (`kitsune2-lrl` `feat/mdns-bootstrap`, `holochain-lrl` `feat/mdns-bootstrap-0.6.1`)

The mdns work solves LAN *discovery* for the connection-oriented world:
`kitsune2_bootstrap_mdns` (a Bootstrap module announcing a privacy-preserving
space fingerprint over mDNS, with an HMAC proof-of-knowledge handshake before
agent infos are exchanged), `CompositeBootstrapFactory` in `kitsune2_core`
(stack WAN + LAN bootstrap over one peer store), and an iroh `mdns` feature
for relay-less LAN dialing. The holochain branch adds a `network.mdns`
conductor-config block that emits the `mdnsBootstrap` module config, and
composes the factory unconditionally — the module is a no-op unless enabled,
so a single binary carries the capability.

Influences adopted here:

- **Complementary, not competing.** mdns + iroh-LAN is the connectionless
  *discovery* / connection-oriented *data* answer for LANs (shipping against
  0.6.1); the broadcast transport removes the connection layer entirely. In
  the switchable world they compose: when the active backend is iroh, mdns
  bootstrap is the LAN discovery story; when it is a broadcast medium,
  hearing frames *is* discovery and mdns is unnecessary.
- **Composite pattern.** `CompositeBootstrapFactory` is exactly the
  factory-wrapping-factories shape the switchable transport uses at the
  transport layer, and phase 2's "beacons are the bootstrap" ships as another
  Bootstrap module composed alongside core + mdns rather than replacing them.
- **Privacy construction.** The mdns space commitment
  (`SHA-256(space_id || "k2-mdns-v1")`, domain-tagged, with HMAC
  proof-of-knowledge) sets the privacy bar for anything we put on a shared
  medium. Phase-2 broadcast space tags reuse the same construction and
  domain-separation style (`k2-bcast-v1`), including its known limitation
  (candidate-list confirmation) and planned time-bucketed rotation. Note
  that phase-1 frames carry cleartext `K2Proto` payloads whose space ids
  are visible on the air — one more reason phase 1 is a validation stage,
  not a production mode.
- **Single-binary gating.** The "compose unconditionally, no-op unless
  enabled via module config" trick is the same mechanism the switch factory
  uses to satisfy the no-separate-binaries requirement; cargo features
  control only what is *compiled in*, never which behavior is *active*.
- **Test gating.** The mdns branch gates real-multicast integration tests
  behind `K2_MDNS_IT`; the UDP multicast medium tests follow the same
  pattern (`K2_BCAST_IT`) so CI environments without multicast stay green.
- **Config plumbing.** The `network.mdns` → `insert_module_config` →
  kitsune2 module config path in `NetworkConfig::to_k2_config()` is the
  template for `network.transport` → `switchTransport` /
  `broadcastTransport` plumbing (§4.2), and the holochain-lrl branch's
  `[patch.crates-io]` + CI dev-build workflow is the proven route for
  getting these binaries into Moss.

Version-skew plan: the mdns branches are kitsune2-0.4.1 / holochain-0.6.1
based, while this branch tracks kitsune2 `main` (0.5.0-dev) for holochain
0.7. The mdns kitsune2 work (bootstrap_mdns crate + composite factory +
iroh mdns feature) is additive and small; the integration path is to
forward-port it onto this branch once the transport work lands, so a single
kitsune2-lrl base feeds the holochain-0.7 integration branch, which then
wires mdns, broadcast, and the switch through one `[patch.crates-io]` set.

### 7.3 MemoryLAN (Tschudin, 2026)

Christian Tschudin's *MemoryLAN: A Local Area Content Replication Mesh*
(Univ. of Basel, Jun–Jul 2026) is a formal treatment of a broadcast
replication mesh in the same design space as this work — it grew out of
conversations with the Holochain side and adopts Holochain's long-standing
"fast push / slow heal" framing (as "fast push / slow repair") for what
its switches were already doing. Its value to us is a *validated,
precisely specified mechanism set*: simulations show ~2× gain over
store-and-forward meshes (from broadcasting and from in-network repair),
with first LoRa / long-range-BLE deployments concurring. Its model:
**memory switches** — small-memory, client-agnostic store-and-forward
nodes — extend a physical broadcast domain into a mesh the way Ethernet
switches extend a bus, "operating below the routing threshold". The
service abstraction is addressless: no sender/receiver, just content
pages; `add` at one edge yields best-effort `update` at every edge. Fast
push floods new pages with loop prevention by a fixed-size FIFO hash
history (sized ~2× the content cache); slow repair beacons a Bloom
fingerprint of a FIFO content cache, and neighbors reply to detected
deficits with probability scaled by estimated neighbor density (1/d ..
1/d²) to prevent NACK implosion; duplicate reception moves a page to the
cache head ("keep hot content hot").

Mechanisms adopted directly into phase 2 (§3.5): hash-history flooding
instead of TTL for multi-hop; the FIFO air cache with move-to-front,
serving repair from overheard content; density-scaled reply probability
composed with SRM overhear-suppression; per-round Bloom re-salting. The
paper also names a failure mode our doc previously didn't — **sloshing**,
oscillating repair between nodes whose bounded caches disagree — and its
mitigation (novelty pressure; accept convergence on a *subset*). Note that
our chain-head repair layer is structurally immune: per-agent chains make
repair monotone (every repair is forward progress toward a known head), so
sloshing can only affect the unordered air-cache layer, which is
best-effort by construction.

The deepest architectural takeaway is the **switch as a first-class,
protocol-blind role**. A MemoryLAN switch never interprets pages — which
means it can cache and flood our space-AEAD-encrypted frames without being
a Holochain node, holding a key, or passing a membrane: dedicated cheap
relay hardware (ESP32-class, LoRa, BLE) can extend a space's broadcast
domain from *outside* the space. That suggests a future `bcast-relay`
profile: the frame/flooding/air-cache layers of `transport_broadcast`
compiled standalone, no kitsune2 above them. Two caveats the paper leaves
open that matter for us: a content-blind cache is poisonable (garbage
flooding evicts real content — our switches should at minimum admit only
frames bearing a known space's HMAC tag, and rate-limit per tag), and
cache units interact with fragmentation (caching fountain-coded symbols
rather than reassembled pages fits the model best: any k of n symbols
serve).

Where we intentionally go beyond MemoryLAN: its repair is content-blind
Bloom reconciliation because it assumes nothing about the data. Holochain
data is signed, monotone, per-agent chains — so our member-level repair
uses precise chain-head deltas (no false negatives, ~70-byte beacons)
and keeps Bloom reconciliation only for the unordered layers. In
MemoryLAN's terms, phase 2 is a *typed* memory LAN for members, riding on
an untyped one that anyone — member or relay — can serve. The tinySSB
integration reported in the paper (an unmodified CRDT sync protocol
running transparently over the mesh) is the same claim our phase 1 makes
for unmodified kitsune2 modules, which is encouraging precedent from a
deployed system.
