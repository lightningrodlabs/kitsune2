# mDNS Bootstrap for Kitsune2

Status: Implemented (discovery-only)
Branch: `feat/mdns-bootstrap-hello` (based on the hello access module line)

## Shape

- `crates/transport_iroh` — `mdns` cargo feature and
  `IrohTransportConfig.enable_lan_discovery`. Attaches iroh's mDNS address
  lookup so a peer is dialable by endpoint id without a relay; announces
  the peer URL from the configured relay alone so a node whose relay is
  unreachable is still addressable; lets a LAN path through the
  relay-down guard. All iroh surface for this lives in
  `crates/transport_iroh/src/lan_discovery.rs`.
- `crates/api` — `Transport::dial(space, url) -> DialOutcome`: open (or
  reuse) a connection and run the preflight, sending nothing else; a peer
  blocked in the space is not dialled and the outcome says so.
  `BootstrapFactory::create` receives the space's transport.
- `crates/core` — `CompositeBootstrapFactory` stacks several
  `BootstrapFactory`s over one peer store; an inner factory that fails
  fails the space, as it would alone, and an empty composite is an error.
  `OptionalBootstrapFactory` wraps a factory whose failure to start the
  space may survive: a warning and a no-op bootstrap.
- `crates/bootstrap_mdns` — `MdnsBootstrapFactory`: announce the space
  fingerprint and our peer URL, browse for the same, `dial` what matches.
  A daemon that cannot start leaves the space detached, retrying the join
  on later puts; a fingerprint that cannot be derived fails `create`. The
  intended wiring, which Holochain uses, is `Composite([Core, Mdns])`;
  `Optional(Mdns)` is for embedders that want the derive error tolerated
  too.
- `crates/kitsune2/tests/mdns_lan.rs` (feature `mdns`, gated on
  `KITSUNE2_LAN_TEST=1`) — two production-wired nodes, unreachable relay,
  no bootstrap server, each ends up with the other's agent info.

## Motivation

Enable LAN-local peer discovery without a WAN bootstrap server, without
leaking which spaces (DNA hashes) a device participates in to anyone on
the LAN, and composing with — not replacing — the existing WAN bootstrap.

## Why discovery-only

An earlier design carried its own TCP session protocol: an HMAC
proof-of-knowledge handshake followed by an exchange of signed agent infos,
inserted into the peer store by this crate. That was needed when nothing
else authenticated a LAN peer. The base this line builds on has the
*hello* access module (`crates/core/src/factories/core_hello.rs`), which
already does exactly that for every transport connection: it challenges a
new peer to prove knowledge of the space secret, discloses agent infos only
after verifying the proof, and gates gossip, fetch and publish on the
result.

Running a second proof protocol next to it would duplicate the security
argument and give the LAN path a peer-store insert of its own to reason
about. So mDNS now only has to get a *connection* up. The chain is:

```
mDNS record (spacefp, url)  ->  Transport::dial(url)  ->  preflight
   ->  hello sweep / peer_updated  ->  proof exchange  ->  agent infos stored
```

Nothing in this crate touches the peer store.

### Trigger for the hello exchange

The hello module challenges a peer when a local agent joins, when a new
URL appears in the peer store, when a message to or from an ungranted peer
is dropped, and when gossip reports it found nobody to gossip with (a
sweep over the peer store *and every connected peer*, floored at 5 s).

A connection that mDNS opens does not by itself put anything in the peer
store unless the embedding application's preflight does, so on a bare
kitsune2 node the sweep is what picks the new connection up. The e2e test
therefore shortens gossip's initiate interval; an application whose
preflight exchanges agent infos (Holochain's does) gets the faster
`peer_updated` path for free.

## Threat model

- **Passive LAN listener** sees all mDNS broadcasts. Learns the space
  fingerprint and a peer URL, both of which are already public to any
  bootstrap server.
- **Active LAN attacker** can spoof mDNS records. Can cause bounded dials
  to a peer that then fails the hello exchange: once when the URL is first
  heard, then at most once per `redialIntervalMs` with exponential backoff
  (up to sixteen intervals) while it stays announced and unconnected, at
  most `maxConcurrentDials` in flight and at most 256 URLs remembered per
  space, the ones that keep failing making room first and a peer that
  connected last. A peer the space blocks is retried on the longest
  schedule only, so a lifted block is noticed without a re-announcement.
  Cannot put anything in a peer store.
- **Adversary with a candidate space_id list** can derive fingerprints
  and confirm presence for spaces that configure no secret (the open-space
  default, where the space id is the secret). Spaces with a real secret
  are not affected.
- **WAN bootstrap server compromise** is a separate problem (see below).

## Announcement

Service type `_kitsune2._udp.local.`, one record per space under a random
16-byte instance name, port 0, the host's interface addresses as
enumerated and maintained by `mdns-sd` (`enable_addr_auto`), and two TXT
fields:

```
spacefp = base64url(space_secret.derive_key(space_id, "k2-mdns-v1"))
url     = <kitsune2 peer URL>
```

The fingerprint is purpose-scoped key material from the same space secret
the hello proof uses, so a non-member cannot compute it. With no secret
configured kitsune2 falls back to the space id, and the candidate-list
attack in the threat model applies to that default.

One `mdns-sd` daemon per factory serves every space it creates: one
browse, one reconciliation ticker (each space reconciling against its own
transport's connections), one hostname, and a registry that routes each
resolved record to the space whose fingerprint it carries. The daemon
serves the service type of the space that started it; a space configured
for another type is refused. A daemon that fails to start is not retried for a
minute, so many spaces created together cost one attempt; each of those
spaces stays detached and tries to join again on its later puts, so a host
that gains a network after its spaces were created recovers LAN discovery
without recreating them.

Records are tracked by name per space, so a restarted peer whose old record
lingers next to its new one stays known until both are gone. The browse
loop keeps the latest record of every name it hears — up to 64 per
fingerprint and 256 fingerprints, a registered space's never making room
for others' — and replays the matching ones to a space that joins later,
since mDNS delivers an unchanged record only once. Which space must forget
a withdrawn record is asked of the spaces, not of that cache.

The URL is taken from the local agent infos delivered through
`Bootstrap::put`; browsing starts immediately, announcing — and dialling,
since before the first URL the space has no handler registered and no
agent to preflight with — waits for the first put that carries a URL, and
the record is re-registered when the URL changes. Tombstones are ignored.

## Configuration

```json
{
  "mdnsBootstrap": {
    "enabled": false,
    "serviceType": "_kitsune2._udp.local.",
    "redialIntervalMs": 30000,
    "maxConcurrentDials": 4
  },
  "irohTransport": { "enableLanDiscovery": false }
}
```

Both flags default off. Enabling `mdnsBootstrap` without
`enableLanDiscovery` produces dials the transport cannot complete without
a relay.

## Iroh migration rules

1. This crate uses `mdns-sd` directly for its own announcement; iroh's
   address lookup is only used for dialability.
2. All iroh surface for LAN discovery lives in one file in the transport.
3. `transport_iroh/mdns` and `kitsune2_bootstrap_mdns` are independently
   enableable.
4. No iroh types cross module boundaries: this crate sees `Url`s only.
5. The e2e test is the canary to run on every iroh bump.

## Rejected alternatives

- **Fingerprint in iroh's mDNS user-data.** iroh's address lookup records
  can carry user data, so the fingerprint could ride on the transport's
  own announcement and `kitsune2_bootstrap_mdns` could go. Rejected: the
  module is transport-agnostic, and access5's hello layer assumes a
  transport-independent discovery; the transport's mDNS stays a
  dialability mechanism only (migration rule 1).

## Known limitations

- The early URL announce (`transport_iroh`, on `enable_lan_discovery`)
  derives the peer URL from the global `relay_url` alone. Per-space relays
  get their URL from the relay handshake, so a space on a per-space relay
  is not addressable on the LAN until that handshake completes.
- With per-space relays the announced URL can differ from the URL the
  transport keys its connection by; such a peer looks unconnected every
  round and is dialled again each interval. Holochain does not use
  per-space relays.
- The relay-down LAN bypass in `transport_iroh` only trusts LAN-scoped
  addresses (RFC 1918, IPv4 link-local, CGNAT, IPv6 ULA). IPv6 link-local
  is dropped because the lookup delivers it without a scope id, and a LAN
  numbered with global-unicast IPv6 is not recognised; iroh's unfiltered
  in-connect lookup on the relay-up path is unaffected.
- The api change (`TxImp::dial` required, `Transport::dial` returning
  `DialOutcome`) breaks external transport implementors. Accepted on this
  fork branch.

## Follow-ups

- A `peer_connect`-driven hello challenge, so that a bare kitsune2 node
  does not depend on gossip's starvation sweep to start the proof
  exchange after an mDNS dial. The e2e's shortened gossip interval is a
  stand-in for this.

## Follow-on: bootstrap-server hardening

Out of scope here. A compromised WAN bootstrap server leaks every
`space_id` it has seen plus the agent set per space, because clients PUT
raw `AgentInfoSigned`. Applying the commitment approach server-side is a
protocol redesign — retaining PUT anti-spam when the server can no longer
parse the payload is the hard part.
