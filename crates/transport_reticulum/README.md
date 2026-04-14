# kitsune2_transport_reticulum

Kitsune2 `Transport` + `Bootstrap` implementations backed by
[Reticulum](https://reticulum.network/), via the
[`reticulum-rs-transport`](https://crates.io/crates/reticulum-rs-transport)
crate (module name `rns_transport`).

This crate is an alternative to `kitsune2_transport_iroh` and
`kitsune2_transport_tx5`. It carries gossip, fetch, publish, and module
traffic over a Reticulum network, with peer discovery driven by
Reticulum announces rather than an HTTP bootstrap server.

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
- `>  MDU`             → `Transport::send_resource` (Reticulum
  auto-chunks)

A one-byte framing tag distinguishes preflight from data so the
receiver can demultiplex without a length prefix.

## Trait abstraction

All I/O goes through traits in `destination.rs`
(`Endpoint`, `Destination`, `Link`), mirroring the Iroh transport's
endpoint abstraction. Unit tests swap in fakes; the real backend is a
thin set of wrappers over `rns_transport` in `backend.rs`. Functional
tests wire two real `rns_transport` instances through
`InterfaceManager::new_channel()` for in-process loopback.

## Configuration

See [`ReticulumTransportConfig`](src/config.rs). Key fields:

| Field                  | Default     | Notes                                          |
|------------------------|-------------|------------------------------------------------|
| `interfaces`           | *required*  | One or more `TcpClient` / `TcpServer` / `Udp`. |
| `identity_path`        | `None`      | Persist Identity to keep a stable URL.         |
| `max_frame_bytes`      | `1 MiB`     | Cap handed to `send_resource()`.               |
| `connect_timeout_s`    | `30`        | Reticulum 1-RTT + kitsune2 preflight.          |
| `announce_interval_s`  | `300`       | Per-space re-announce cadence.                 |
| `link_idle_timeout_s`  | `600`       | Per-space Link idle teardown.                  |

`ReticulumTransportConfig::validate()` rejects empty interfaces,
oversize `max_frame_bytes`, and non-existent identity-path parents.

## Builder

The top-level `kitsune2` crate exposes `reticulum_builder()` (behind
the `transport-reticulum` feature) that wires `ReticulumNode` to both
the transport and bootstrap factories. See
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
