# kitsune2_transport_reticulum

Kitsune2 `Transport` + `Bootstrap` implementations backed by
[Reticulum](https://reticulum.network/).

This crate is an alternative to `kitsune2_transport_iroh` and
`kitsune2_transport_tx5`. It carries gossip, fetch, publish, and module
traffic over a Reticulum network, with peer discovery driven by
Reticulum announces rather than an HTTP bootstrap server.

## Backends

Two Rust Reticulum implementations are supported, selected via
mutually-exclusive Cargo features. Exactly one must be enabled; a
compile-time check in `lib.rs` rejects zero or both. `backend-lxmf`
is the default.

| Feature            | Upstream                                                                         | Crate module     | Characteristics                                                                                                           |
| ------------------ | -------------------------------------------------------------------------------- | ---------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `backend-lxmf`     | [LXMF-rs](https://github.com/lightningrodlabs/LXMF-rs) (`reticulum-rs-transport`) | `rns_transport` | Mature. `PACKET_MDU = 464`. Built-in Resource chunking handles arbitrary-size payloads. Uses rusqlite.                    |
| `backend-beechat`  | [Beechat](https://github.com/lightningrodlabs/Reticulum-rs) (`reticulum`)        | `reticulum`     | `PACKET_MDU = 2048`. Richer routing (PathTable, PathRequests, retransmit). Configurable link restart. No rusqlite. No built-in chunking (see [`PLAN-beechat-backend.md`](../../PLAN-beechat-backend.md) §5d). |

Both backends expose the same public surface (`ReticulumNode`,
`ReticulumTransportFactory`, `ReticulumBootstrapFactory`,
`ReticulumTransportConfig`, …) — consumer code is backend-agnostic.
Backend-specific tuning (e.g. Beechat's `retransmit`, `announce_forever`)
lives in `ReticulumTransportConfig::beechat` and is silently ignored
by the other backend.

Cargo usage:

```toml
# default (LXMF)
kitsune2_transport_reticulum = "…"

# explicit LXMF
kitsune2_transport_reticulum = { version = "…", default-features = false, features = ["backend-lxmf"] }

# Beechat
kitsune2_transport_reticulum = { version = "…", default-features = false, features = ["backend-beechat"] }
```

The top-level `kitsune2` crate re-exposes these as
`transport-reticulum` (LXMF) and `transport-reticulum-beechat`
(Beechat) so downstream consumers can follow the same naming
convention as `transport-tx5-backend-go-pion` and friends.

## URL scheme

```text
ret://reticulum:1/<identity-hash-hex>
```

The host (`reticulum`) and port (`1`) are constants — Reticulum routes
by destination hash, not IP. The path is the hex-encoded peer
`Identity.address_hash`, which is stable across runs once the identity
is persisted.

## Architecture

Each joined kitsune2 space gets its own Reticulum `Destination`
(aspect `kitsune2/<space_hash>`). A node announces and listens on every
space it has joined, independently.

```text
          ┌────────────────────────────────┐
          │        ReticulumNode           │
          │  (identity + shared state)     │
          └───────────────┬────────────────┘
                          │
      ┌───────────────────┴───────────────────┐
      │                                       │
      ▼                                       ▼
 ┌────────────────────┐             ┌──────────────────────┐
 │ ReticulumTransport │             │ ReticulumBootstrap   │
 └──────────┬─────────┘             └──────────┬───────────┘
            │                                  │
   per-space Destination              drains announce queues,
   + Link preflight state             produces AgentInfo records
   + packet / Resource send           for the peer store
```

Data-plane frames:

- `≤ fernet-aware MDU` → `Link::data_packet` (single packet, encrypted
  by Reticulum)
- `>  MDU`             → backend-dependent — LXMF uses
  `Transport::send_resource` (Resource auto-chunks); Beechat has no
  Resource abstraction today (chunking layer tracked in
  [`PLAN-beechat-backend.md`](../../PLAN-beechat-backend.md)).

A one-byte framing tag distinguishes preflight from data so the
receiver can demultiplex without a length prefix.

## Trait abstraction

All I/O goes through traits in `destination.rs`
(`Endpoint`, `Destination`, `Link`), mirroring the Iroh transport's
endpoint abstraction. Unit tests swap in fakes; the real backends
live in `backend_lxmf.rs` / `backend_beechat.rs`, each implementing
the same trait surface against its respective upstream crate.
Shared type aliases (`AddressHash`, `Identity`, `DestinationName`)
come from `types.rs`, conditionally re-exported from whichever
backend is selected.

Functional tests wire two real instances of the active backend
through an in-process loopback: LXMF uses
`InterfaceManager::new_channel()`; Beechat uses TCP loopback on
`127.0.0.1`.

## Configuration

See [`ReticulumTransportConfig`](src/config.rs). Key fields:

| Field                  | Default     | Notes                                                                         |
|------------------------|-------------|-------------------------------------------------------------------------------|
| `interfaces`           | *required*  | One or more `TcpClient` / `TcpServer` / `Udp`.                                |
| `identity_path`        | `None`      | Persist Identity to keep a stable URL.                                        |
| `max_frame_bytes`      | `1 MiB`     | LXMF only. Cap handed to `send_resource()`.                                   |
| `connect_timeout_s`    | `30`        | Reticulum 1-RTT + kitsune2 preflight.                                         |
| `announce_interval_s`  | `300`       | Per-space re-announce cadence.                                                |
| `link_idle_timeout_s`  | `600`       | LXMF only; Beechat uses compile-time timers and ignores this.                 |
| `beechat.*`            | all unset   | Beechat-only: `retransmit`, `broadcast`, `rerouteEager`, `restartOutlinks`, `announceForever`. `None` keeps the upstream default; LXMF silently ignores. |

`ReticulumTransportConfig::validate()` rejects empty interfaces,
oversize `max_frame_bytes`, and non-existent identity-path parents.

## Builder

The top-level `kitsune2` crate exposes `reticulum_builder()` (behind
either `transport-reticulum` or `transport-reticulum-beechat`) that
wires `ReticulumNode` to both the transport and bootstrap factories.
See
[`crates/kitsune2/tests/reticulum_integration.rs`](../kitsune2/tests/reticulum_integration.rs)
for a complete two-node gossip example.

## Known limitations

- **No browser / WASM target.** Reticulum is native-only.
- **LoRa untested.** `rns_transport` supports LoRa interfaces but this
  crate does not test or tune for radio links.
- **`max_frame_bytes` vs. gossip frame size.** kitsune2 gossip does not
  currently negotiate frame sizes with the transport. The default
  1 MiB cap is conservative for TCP/UDP Reticulum; aligning gossip's
  frame budget with real Reticulum MDUs is tracked as follow-up work.
- **No HTTP bootstrap.** Discovery happens via Reticulum announces.
  Deployers bring their own Reticulum infrastructure (or use
  `AutoInterface` on a LAN).
