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
   `Bootstrap::put`), the space registers `_kitsune2._udp.local.` with two
   TXT fields: `spacefp`, the url-safe base64 of the key the space secret
   derives for the purpose `"k2-mdns-v1"`, and `url`, the kitsune2 peer
   URL. The instance name is a random token per space, the port is zero,
   and the daemon fills in and maintains the host's interface addresses.
   The record is replaced when the URL changes. All spaces of one factory
   share one daemon, one browse, one reconciliation ticker and one
   hostname; the daemon serves the `serviceType` of the space that
   started it, and a space configured for another is refused. A space created
   while the daemon cannot start joins it from a later put once it can;
   a failed start is not tried again for a minute.
2. **Browse.** Every resolved record that carries our fingerprint and a
   parseable peer URL — and is not our own instance or our own URL — is a
   candidate. Records are tracked by name, so a restarted peer whose old
   record lingers next to its new one stays known until both are gone;
   the latest record of every name heard is kept (64 per fingerprint,
   256 fingerprints) and replayed to a space that joins later, since mDNS
   delivers a record only once.
3. **Dial.** Once the space has a URL of its own (before that it cannot be
   dialled back or preflight), a candidate is handed to
   `Transport::dial(space, url)` when it is first heard. While the LAN
   keeps announcing it and the transport reports no connection to it, it
   is dialled again after one `redialIntervalMs`, then two, four, up to
   sixteen intervals between dials; a fresh announcement restarts the
   short schedule, and a peer the space blocks is retried on the longest
   schedule only, so a lifted block is noticed without a re-announcement.
   At most `maxConcurrentDials` dials are in flight and at most 256 URLs
   are remembered per space — past that, the URLs that keep failing make
   room first and a peer that connected last. A dial opens a transport
   connection and runs its preflight.
4. **Hand over.** The space's access module (the *hello* module in
   `kitsune2_core`) sees the new connection, challenges the peer to prove
   knowledge of the space secret, proves the same in return, and only then
   exchanges signed agent infos into the peer store.

This crate never inserts anything into the peer store, never verifies a
signature, and carries no wire protocol of its own.

## Privacy and trust

- The raw `SpaceId` is never sent over mDNS; only the fingerprint is, and
  the fingerprint is derived from the space secret, so a non-member cannot
  compute it. The peer URL is public by nature — it is what bootstrap
  servers hand out.
- mDNS is unauthenticated, so discovery decides nothing. A spoofed
  announcement can at most cause bounded dials to a peer that then fails
  the access exchange.
- When no space secret is configured, kitsune2 uses the space id as the
  secret, and an adversary holding a list of candidate space ids can then
  derive each fingerprint and confirm which spaces are present on the LAN.
  That is a limit of the open-space default.

| Threat | Outcome |
| --- | --- |
| Passive LAN listener learning `space_id` | Learns only the fingerprint |
| Active LAN attacker injecting fake peer info | Nothing to inject: no peer info travels over mDNS |
| Active attacker impersonating a member | Fails the access module's proof-of-knowledge |
| Announcement flood | Each URL is dialled once on first sighting, then at most once per `redialIntervalMs` with exponential backoff (up to 16 intervals) while unconnected, under the `maxConcurrentDials` in-flight cap; at most 256 URLs are kept per space |
| Adversary with a candidate `space_id` list confirming presence | **Not prevented** for spaces with no configured secret |

## Typical setup

```rust
use kitsune2_api::DynBootstrapFactory;
use kitsune2_bootstrap_mdns::MdnsBootstrapFactory;
use kitsune2_core::factories::{CompositeBootstrapFactory, CoreBootstrapFactory};

// Run WAN bootstrap and LAN mDNS discovery side by side. `put` fans out
// to both. A daemon that cannot start when a space is created (no
// network yet, no multicast on the interface) does not cost the space
// anything: the mDNS bootstrap stays detached and retries the join on
// later puts, which keep coming as agent infos are re-signed. This is
// the intended wiring, and the one Holochain uses.
let bootstrap: DynBootstrapFactory = CompositeBootstrapFactory::create(vec![
    CoreBootstrapFactory::create(),
    MdnsBootstrapFactory::create(),
]);
```

A fingerprint that cannot be derived from the space secret is a
misconfiguration, and `create` fails on it. An embedder that would rather
run such a space without LAN discovery wraps the factory in
`OptionalBootstrapFactory::create(MdnsBootstrapFactory::create())`, which
turns that error into a warning and a no-op.

Then enable discovery in config (disabled by default):

```json
{
  "mdnsBootstrap": {
    "enabled": true,
    "serviceType": "_kitsune2._udp.local.",
    "redialIntervalMs": 30000,
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

## Rejected alternatives

- **Carrying the fingerprint in iroh's own mDNS user-data.** iroh's
  address lookup already announces every endpoint on the LAN, and its
  records can carry user data, so the fingerprint could ride along and
  this crate could disappear. Rejected: this module is transport-agnostic
  by design, and the hello layer it hands over to assumes discovery that
  does not depend on the transport, so binding "which peers to dial" to
  iroh's record format would tie both to one transport's mDNS.

## Known limitations

- The transport's mDNS lookup joins the multicast group only on the
  interfaces present when it is built. `transport_iroh` rebuilds it when
  the endpoint's local IP set changes (debounced by one second), so a node
  that starts without a network and gains one later becomes dialable on
  the LAN without a restart; a change iroh's own address watcher does not
  surface is not covered.
- The early peer URL that a node announces before its relay handshake
  completes covers only the global `relay_url`; a per-space relay only
  yields its URL once the relay handshake has run, so a space on a
  per-space relay is announced later.
- With per-space relays the announced URL can differ from the URL the
  transport keys its connection by, in which case the peer looks
  unconnected every round and is dialled again each interval. Holochain
  does not use per-space relays.
- `Transport::dial` returning `DialOutcome` and `TxImp::dial` being a
  required method break external transport implementors. That is accepted
  on this fork branch.

## Follow-ups

- A `peer_connect`-driven hello challenge. A bare kitsune2 node puts
  nothing in the peer store when an mDNS dial lands, so today the hello
  exchange waits for gossip's starvation sweep; challenging on connect
  would make LAN discovery independent of gossip's timing.

## Non-goals

- **Not a peer-info channel.** No `AgentInfoSigned` travels over mDNS or
  through this crate; the access module exchanges it over the
  authenticated transport connection.
- **Not a replacement for WAN bootstrap** when nodes are not on the same
  LAN. Compose it with `CoreBootstrapFactory` for mixed deployments.
