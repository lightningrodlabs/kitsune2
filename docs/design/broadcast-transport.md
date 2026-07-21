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

`bcast://<medium>:1/<node-id-hex>` — e.g. `bcast://udpm:1/1a2b3c4d5e6f7081`.

Host = medium name, port is a constant `1` (broadcast media have no ports;
the kitsune2 `Url` type requires one), path segment = ephemeral `NodeId`, so
`Url::peer_id()` works unchanged. This requires adding `bcast` to the allowed
schemes in `kitsune2_api`'s `Url` validator — the same one-line api change the
reticulum transport made for `ret`.

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
  repair heals all listeners simultaneously.
- **Bootstrap = beacons.** No bootstrap server on-medium; hearing a beacon
  *is* discovery.

Blocking shifts from the connection edge to ingestion: you cannot close a
connection that does not exist, so frames/ops attributable to blocked agents
are dropped at receipt instead.

Payloads larger than a few MTUs get fountain coding (RaptorQ) rather than
ACK/retransmit — the canonical answer for feedback-free broadcast, and what
makes the simplex optical medium usable at all.

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
4. **Phase-2 native broadcast** — `TxBroadcastImp` api extension, Trickle
   beacons with chain-head repair, SRM suppression; benchmark against
   pairwise gossip in `kitsune2_showcase` (the O(pairs) → O(deficits) claim
   should be measurable).
5. **BLE extended-advertising medium** (BlueZ), then TTL flooding for
   multi-hop.
6. **Ultrasonic medium** scoped to beacon/bootstrap frames; wire as a
   discovery feeder for BLE/WiFi.
7. **Optical carousel** as emit/ingest tooling.

The chain-head-beacon gossip design (step 4) is worth having even on iroh: it
exploits a structural property of Holochain data that generic DHT sync
ignores.

## 7. Relationship to the reticulum transport work

The `transport-reticulum` branch pioneered several pieces this design reuses:
per-medium trait abstraction for testability, announce-driven bootstrap
replacing the HTTP server, the pure chunking layer (copied here with link ids
replaced by sender node ids), and the one-line `Url` scheme addition. This
branch intentionally starts from upstream `main` rather than that branch; the
reticulum transport can later rebase onto this base and potentially become a
`BroadcastMedium`-style backend itself (Reticulum interfaces are themselves
broadcast-capable at the LoRa/packet-radio layer).
