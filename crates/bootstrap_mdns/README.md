# kitsune2_bootstrap_mdns

mDNS-based LAN peer discovery and bootstrap for Kitsune2.

This crate lets two Kitsune2 nodes on the same local network find each
other and exchange signed agent info without a WAN bootstrap server —
while **never** broadcasting the raw `SpaceId` (DNA hash) on the wire.

## Privacy-preserving discovery

Plain mDNS-based service discovery would require broadcasting the space
identifier on every LAN the node attaches to, letting any passive
observer learn which Holochain networks (DNAs) a device participates in.
To avoid that leak:

1. **mDNS announcements carry only a commitment** — `SHA-256(space_id ||
   "k2-mdns-v1")`, 32 bytes — under the TXT key `spacefp`. The raw
   `SpaceId` never appears on the wire.
2. **A mutual HMAC challenge/response** over TCP proves both peers know
   the real `space_id` before either sends any `AgentInfoSigned`. A
   listener with a matching fingerprint but no real `space_id` cannot
   complete the handshake.
3. **Signed agent info is only exchanged after the proof succeeds.**
   Records are then verified by the kitsune2 builder's `Verifier` and
   inserted into the peer store via the normal flow.

### What this protects against

| Threat | Protected? |
| --- | --- |
| Passive LAN listener learning `space_id` | Yes |
| Active LAN attacker injecting fake peer info | Yes (proof + signature) |
| Active attacker impersonating a member | Yes (needs `space_id`) |
| Adversary with a list of candidate `space_id`s confirming presence | **No** — they can precompute fingerprints. Mitigated in a future iteration via time-bucketed rotation. |
| Compromised WAN bootstrap server leaking space lists | Out of scope — see `docs/plans/mdns-bootstrap.md`, "Follow-on" section. |

## Typical setup

```rust
use std::sync::Arc;
use kitsune2_api::{BootstrapFactory, DynBootstrapFactory};
use kitsune2_bootstrap_mdns::MdnsBootstrapFactory;
use kitsune2_core::factories::{CompositeBootstrapFactory, CoreBootstrapFactory};

// Run WAN bootstrap and LAN mDNS bootstrap side by side, sharing one
// peer store. `put` fans out to both; discoveries from either flow into
// the same store.
let bootstrap: DynBootstrapFactory = CompositeBootstrapFactory::create(vec![
    CoreBootstrapFactory::create(),
    MdnsBootstrapFactory::create(),
]);
```

Then enable mDNS in config (disabled by default):

```json
{
  "mdnsBootstrap": {
    "enabled": true,
    "serviceType": "_kitsune2._udp.local.",
    "refreshIntervalMs": 30000
  }
}
```

## Pairing with iroh LAN dialability

For LAN-only operation (no relay), also enable iroh's mDNS discovery in
the transport. This is a separate mechanism — it gives iroh the addresses
it needs to dial a peer by `NodeId`, while this crate distributes the
Kitsune2 `AgentInfoSigned` that contains the `NodeId` in the first place.

```toml
# In the consumer's Cargo.toml
kitsune2_transport_iroh = { version = "...", features = ["mdns"] }
```

```json
{
  "irohTransport": { "enableLanDiscovery": true },
  "mdnsBootstrap":  { "enabled": true }
}
```

## Testing

Unit tests run with `cargo test -p kitsune2_bootstrap_mdns` and do not
require multicast. The two LAN integration tests in `tests/lan_exchange.rs`
actually exercise mDNS and are gated behind the `K2_MDNS_IT` env var:

```sh
K2_MDNS_IT=1 cargo test -p kitsune2_bootstrap_mdns --test lan_exchange
```

## Non-goals

- **This is not a gossip transport.** It only distributes `AgentInfoSigned`
  on the LAN; all subsequent ops go through the configured kitsune2
  transport.
- **Not a replacement for WAN bootstrap** when nodes aren't on the same
  LAN. Compose it with `CoreBootstrapFactory` for mixed deployments.

## Wire format compatibility

The protocol carries a `proto_ver` byte in the first handshake message.
Peers running a newer, incompatible version MUST reject older versions
cleanly. Current version: `1`.
