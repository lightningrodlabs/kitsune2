# kitsune2_bootstrap_mdns

mDNS-based LAN peer discovery for Kitsune2.

This crate lets Kitsune2 nodes on the same local network find each other
without a WAN bootstrap server — while **never** broadcasting the raw
`SpaceId` (DNA hash) on the wire — and does nothing else. It announces
this node's peer URL under a commitment to the space, and dials the peers
it hears announcing the same commitment. Everything after the dial is
someone else's job.

## How it works

1. **Announce.** Once a local agent has a peer URL (delivered through
   `Bootstrap::put`), the node registers `_kitsune2._udp.local.` with two
   TXT fields: `spacefp`, the hex of `SHA-256(space_id || "k2-mdns-v1")`,
   and `url`, the kitsune2 peer URL. The instance name is a random token,
   the port is zero, and the record names every non-loopback interface
   address. The record is replaced when the URL changes.
2. **Browse.** Every resolved record that carries our fingerprint and a
   parseable peer URL — and is not our own instance or our own URL — is a
   candidate.
3. **Dial.** Candidates are handed to `Transport::dial(url)`, at most once
   per URL per `dialCooldownMs` and with at most `maxConcurrentDials` in
   flight. That opens a transport connection and runs its preflight.
4. **Hand over.** The space's access module (the *hello* module in
   `kitsune2_core`) sees the new connection, challenges the peer to prove
   knowledge of the space secret, proves the same in return, and only then
   exchanges signed agent infos into the peer store.

This crate never inserts anything into the peer store, never verifies a
signature, and carries no wire protocol of its own.

## Privacy and trust

- The raw `SpaceId` is never sent over mDNS; only the fingerprint is. The
  peer URL is public by nature — it is what bootstrap servers hand out.
- mDNS is unauthenticated, so discovery decides nothing. A spoofed
  announcement can at most cause one rate-limited dial to a peer that then
  fails the access exchange.
- An adversary holding a list of candidate space ids can hash each one and
  confirm which of them are present on the LAN. This is an inherent limit
  of any discovery scheme that matches on a shared identifier.

| Threat | Outcome |
| --- | --- |
| Passive LAN listener learning `space_id` | Learns only the fingerprint |
| Active LAN attacker injecting fake peer info | Nothing to inject: no peer info travels over mDNS |
| Active attacker impersonating a member | Fails the access module's proof-of-knowledge |
| Announcement flood | Bounded by the per-URL cooldown and the in-flight cap |
| Adversary with a candidate `space_id` list confirming presence | **Not prevented** |

## Typical setup

```rust
use kitsune2_api::DynBootstrapFactory;
use kitsune2_bootstrap_mdns::MdnsBootstrapFactory;
use kitsune2_core::factories::{CompositeBootstrapFactory, CoreBootstrapFactory};

// Run WAN bootstrap and LAN mDNS discovery side by side. `put` fans out
// to both. If the LAN side cannot start on this host it is logged and
// skipped; the WAN bootstrap carries on alone.
let bootstrap: DynBootstrapFactory = CompositeBootstrapFactory::create(vec![
    CoreBootstrapFactory::create(),
    MdnsBootstrapFactory::create(),
]);
```

Then enable discovery in config (disabled by default):

```json
{
  "mdnsBootstrap": {
    "enabled": true,
    "serviceType": "_kitsune2._udp.local.",
    "dialCooldownMs": 60000,
    "maxConcurrentDials": 4
  }
}
```

## Pairing with iroh LAN dialability

A dial only helps if the transport can reach the peer without a relay.
For the iroh transport that means enabling its own mDNS address lookup,
which is a separate mechanism: it makes a peer dialable by endpoint id,
while this crate is what says which peers to dial for a given space.

```toml
# In the consumer's Cargo.toml
kitsune2_transport_iroh = { version = "...", features = ["mdns"] }
```

```json
{
  "irohTransport": { "enableLanDiscovery": true },
  "mdnsBootstrap": { "enabled": true }
}
```

## Testing

Unit tests run with `cargo test -p kitsune2_bootstrap_mdns` and do not
require multicast: the browse loop is fed from an injected event channel
and dials a mock transport. The end-to-end test that actually exercises
mDNS between two full nodes lives in `crates/kitsune2/tests/mdns_lan.rs`
and is gated behind an environment variable, since CI runners rarely have
a multicast-capable interface:

```sh
KITSUNE2_LAN_TEST=1 cargo test -p kitsune2 --features mdns --test mdns_lan
```

## Non-goals

- **Not a peer-info channel.** No `AgentInfoSigned` travels over mDNS or
  through this crate; the access module exchanges it over the
  authenticated transport connection.
- **Not a replacement for WAN bootstrap** when nodes are not on the same
  LAN. Compose it with `CoreBootstrapFactory` for mixed deployments.
- **Not live to interface changes.** The addresses in the announcement are
  fixed when the bootstrap starts.
