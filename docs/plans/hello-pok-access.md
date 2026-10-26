# Hello/PoK Access Module — Design for Review

Status: **draft, requesting feedback**
Relates to: [#263](https://github.com/holochain/kitsune2/issues/263) (Access module),
[#265](https://github.com/holochain/kitsune2/issues/265) (Restrict network access),
[#347](https://github.com/holochain/kitsune2/issues/347) (Agent info publish broken by blocks logic),
PR [#340](https://github.com/holochain/kitsune2/pull/340) (Respect blocks when sending).
Prototype ancestry: [`kitsune2-lrl` branch `feat/mdns-bootstrap`](https://github.com/lightningrodlabs/kitsune2/tree/feat/mdns-bootstrap)  (fingerprint + HMAC
proof-of-knowledge handshake; not in production, will be modified to match this design).

## Summary

One mechanism — a space-scoped **hello** module that performs a challenge/response
**proof-of-knowledge (PoK)** handshake — resolves three currently entangled problems:

1. **The missing introduction mechanism (#347).** Since PR #340, a peer URL with no
   agent infos in a space's peer store is treated as blocked, and all non-preflight
   messages from it are dropped. Kitsune2's own agent-info publish is one of the
   dropped messages, so the mechanism that would introduce a new peer is gated on the
   peer already being introduced. In multi-space deployments (Holochain conductors
   with multiple cells), where connections outlive space membership changes and
   preflight never re-runs, a newly joined agent is invisible to existing members
   until their bootstrap poll fires (up to 5 minutes; unboundedly worse if bootstrap
   is unreachable). This is a critical UX defect for downstream apps (Moss group
   joining).
2. **The missing access gate (#263/#265).** The current "unknown URL = blocked"
   default *looks* like access control but is not: the admission criterion (a signed
   agent info at your URL) is a self-issued credential anyone can mint for any space
   ID they know. There is no hook that asks the host whether a peer is authorized.
3. **Space-ID disclosure.** The space ID (in Holochain: the DNA hash) is the de facto
   read capability, yet it is advertised in cleartext to parties who are not members:
   the bootstrap server sees every space ID, and (in Holochain) the connection
   preflight broadcasts agent infos — containing raw space IDs — for arbitrary
   spaces to every connected peer, member or not.

The design principle tying these together: **the label a space is advertised and
rendezvoused under must be a public commitment (fingerprint) to a host-held secret,
and every path from that label to data must require proving knowledge of the
secret.** The hello module is that proof path, and it doubles as the introduction
mechanism, because a successful proof exchange ends with mutual agent-info exchange.

## Access model

Two tiers, deliberately:

- **Write protection** stays where it is today: application-level validation of
  signed ops (in Holochain: membranes/validation rules), plus the explicit `Blocks`
  denylist. Nothing in this design weakens or depends on it.
- **Read protection** becomes: *knowledge of the space secret*. A peer that cannot
  prove knowledge is never gossiped with, never served fetch requests, and never
  told who the members are. Finer-grained read control (e.g. membrane-proof-based
  credentials) is a planned upgrade via the same host hook, not a redesign.

## Components

### 1. Host secret and fingerprint (host-side; Holochain companion work)

The host derives the kitsune2 `SpaceId` as a **fingerprint** of its secret rather
than passing the secret itself:

```
space_id = H(host_secret ‖ "hc-space-v1")
```

Kitsune2 never holds the secret. The bootstrap protocol is **unchanged**: agent
infos are signed over the fingerprint space, so server-side signature/expiry/
tombstone validation keeps working, and the server never learns the secret. The
host keeps a `space_id → (identity, secret)` map for routing. (Non-Holochain
hosts that don't care about space-ID secrecy simply don't set a secret; see §2's
default.)

**The host secret should be an independent, rotatable value — not the DNA
hash.** In Holochain terms: generate a space secret alongside the network seed
at group creation (carried in the same invite; the network seed itself is
unchanged and keeps its role as DNA identity). Binding the fingerprint to an
independent secret means **rotation is cheap**: a new secret yields a new
fingerprint/SpaceId/auth keypair — a rendezvous migration — while the DNA, the
cells, and every conductor's existing data stay put; the host remaps the new
space id to the same DNA and gossip re-converges from data members already
hold. This converts ex-member exclusion and leaked-secret recovery from "new
DNA, migrate the group" into "rotate the secret, abandon the old space id."
Rotation requires no kitsune2 support beyond what this design already has
(per-space `derive_key`, `unregister_space`); coordination is host-side (§
Companion changes).

**Hard ordering constraint:** fingerprint-as-SpaceId must not ship before (or
without) the PoK gate. The fingerprint is public by construction; if space
membership is still granted to anyone who presents a signed agent info, shipping
the fingerprint first merely publishes the new capability. Gate first, or same
release.

### 2. Host trait (kitsune2 API)

The host hands kitsune2 **purpose-scoped derived keys**, once per space — not the
root secret, and not per-handshake proofs:

```rust
pub trait SpaceSecret: 'static + Send + Sync + std::fmt::Debug {
    /// Derive purpose-scoped key material from the host-held space secret.
    /// Called once per (space, purpose); kitsune2 caches the result.
    fn derive_key(&self, space_id: SpaceId, purpose: &str)
        -> BoxFut<'static, K2Result<Bytes>>;
}
```

Kitsune2 requests `"k2-hello-v1"` (the hello HMAC key) and — for the bootstrap
hardening follow-on below — `"k2-bootstrap-auth-v1"` (an ed25519 seed for the
space auth keypair), and runs all protocol crypto itself.

Why this shape, and not the two obvious alternatives:

- *Not host-computed proofs:* host and kitsune2 are one process and one address
  space — there is no trust boundary between them, so routing every handshake
  through a host callback buys no security. It also becomes actively awkward once
  the bootstrap client (a blocking client under `spawn_blocking`) must sign every
  PUT/GET: per-request async host round-trips versus deriving `auth_sk` once and
  signing locally.
- *Not the root secret:* handing kitsune2 only derived keys is compartmentalization
  with teeth, unlike the trust-boundary framing. KDF one-wayness plus domain
  separation means an accidentally disclosed derived key (a debug log, a stats
  dump) reveals neither the root secret — which is the read capability *and* the
  space identity — nor any other derived key. The root secret exists in exactly
  one subsystem: the host.
- Richer credential schemes (membrane proofs, lair-held agent-key signatures —
  where a real *process* boundary exists that kitsune2 can never call across) are
  deliberately deferred: a proof-shaped companion trait can be **added** later as
  a non-breaking optional extension, rather than shaping v1 around a speculative
  need. This still satisfies #263's "host decides validation" trajectory.
- Core default implementation: `secret = space_id`, i.e. `derive_key` =
  `KDF(space_id, purpose)`. This makes a space "open to anyone who knows the
  space ID" — exactly today's semantics — so public spaces are the default
  configuration, not a special mode (this is also #263's per-space opt-out,
  realized as configuration rather than a separate mechanism). A
  skip-verification no-op survives only as a test convenience in the test
  builder.

### 3. The hello module (kitsune2 core)

A new module (module id `"hello"`) registered per space on the transport. Its
messages are **exempt** from the access/blocks gate, exactly as `Preflight` is
today — this is #263's "network messages to or from any module other than the
loaded access module are blocked" rule.

Wire protocol (protobuf, versioned with `proto_ver`; 4 messages):

```
Initiate  { proto_ver, nonce_i }                       I → R
Respond   { proto_ver, nonce_r, proof_r }              R → I
Confirm   { proof_i, agent_infos_i }                   I → R
Ack       { agent_infos_r }                            R → I
```

Proofs are `HMAC-SHA256(k_hello, T)` where `k_hello` is the space's
`"k2-hello-v1"` derived key (§2), computed over a transcript `T` that binds the
session **and the channel**:

```
T_r = "k2-hello-proof-v1" ‖ nonce_r ‖ nonce_i ‖ peer_id_r ‖ peer_id_i
T_i = "k2-hello-proof-v1" ‖ nonce_i ‖ nonce_r ‖ peer_id_i ‖ peer_id_r
```

Protocol rules:

- Nonces are fresh 32-byte values per exchange, never reused.
- Self-nonce-first ordering makes the two proofs distinct bytes, preventing
  reflection.
- Proofs bind both nonces and both **authenticated peer ids** — the `peer_id()`
  path segment of the kitsune2 `Url`, never the full URL (defeats relaying;
  full URLs would false-negative — see Security analysis).
- Agent infos are disclosed only **after** verifying the counterparty's proof:
  the responder proves first (in `Respond`, no infos); the initiator proves and
  discloses in `Confirm`; the responder discloses in `Ack`.
- On successful verification each side records `Granted` for (space, peer URL)
  in the access state and inserts the received agent infos into the peer store —
  which is the #347 fix: introduction completes in two round trips instead of
  waiting for a bootstrap poll.

Transport contract:

- The `peer: Url` a transport passes to module handlers must carry a peer-id
  segment derived from the connection's authenticated remote identity (for
  iroh, the TLS-authenticated `EndpointId`), never from data the remote claimed
  in a payload. The iroh and in-memory transports both satisfy this; the
  requirement gets documented on the transport handler traits.

### 4. Triggers

Hello exchanges are initiated:

- when a **local agent joins a space** — toward all currently connected peers and
  all URLs already in the space's peer store (this is the Moss multi-cell case);
- when a **new URL appears in the peer store** (e.g. via bootstrap) with no access
  decision;
- when an **incoming non-hello message from an ungranted URL is dropped** — the
  drop stays, but it triggers a (rate-limited) challenge toward that peer. Not an
  optimization: grant state is in-memory, hourly-pruned, and restart-lossy —
  always asymmetrically — and without this trigger that asymmetry is silent
  deafness until the next join or insert; with it, the first dropped message
  heals the pair in one round trip (cost bounds in Security analysis);
- on **gossip initiation with no eligible peers** (per #263's AC);
- on receiving an `Initiate` (respond path).

Failed or unanswered exchanges retry on a backoff, and retry immediately when a
new agent info for that URL arrives (per #263: "the retry will happen when the
unresponsive agent produces a new agent info").

### 5. Enforcement changes

`check_message_permitted` / the outgoing send checks change from consulting only
blocks to a three-valued rule, in precedence order:

1. explicit `Blocks` entry for any agent at the URL → **drop** (denylist always wins);
2. access state `Granted` for (space, URL) → **allow**;
3. otherwise → **drop, except** `Preflight`/`Unspecified`/`Disconnect` wire types
   and module messages whose module id equals the space's access module id
   (`"hello"`), and trigger the challenge per §4.

"Unknown" is thus still not trusted — but it is actively resolvable in one round
trip rather than a dead state.

## Companion changes

- **Holochain preflight slim-down.** With hellos doing per-space introduction, the
  preflight returns to compat-checking only and stops broadcasting agent infos
  (this also removes the current cross-space DNA-hash disclosure to non-members,
  and moots the existing last-writer-wins preflight cache bug in `holochain_p2p`).
  Gated by the existing `NetworkCompatParams` version bump.
- **mDNS bootstrap (`feat/mdns-bootstrap`).** Simplifies: mDNS keeps advertising
  the fingerprint + node id for discovery, but drops its bespoke session protocol;
  after connecting, the standard hello module performs PoK and info exchange. The
  PoK secret becomes the host secret (not the SpaceId), consistent everywhere.
- **Rotatable space secret (Holochain/app side).** Per §1: store a per-cell
  space secret (generated with the network seed; carried in invites alongside
  it), keep the `space_id ↔ DNA` mapping dynamic, and support a rotation flow —
  distribute the new secret encrypted to each remaining member's agent key
  (envelopes can be published in the *old* space: the excluded party sees only
  ciphertext), with members dual-homing old and new space ids for a transition
  window (kitsune2 already supports multiple spaces per DNA mapping).
- **Interim mitigations (independent of this design, for current release lines):**
  bootstrap `backoffMaxMs` reduction, and optionally flipping the unknown-URL
  default to allow (pure denylist semantics) until this module ships.

## Security analysis

- **Relay resistance and channel binding.** Without channel binding, a relay
  attack works: an attacker challenged by A forwards A's nonce to an honest
  member C, obtains C's valid proof, and presents it to A as its own. Binding
  each proof to both peer ids defeats this: the transport authenticates the
  peer id at the connection layer (iroh mutually authenticates `EndpointId`s in
  its TLS handshake), so the verifier checks the proof against the identity the
  channel actually proved — which the relay attacker cannot satisfy without the
  victim's private key. The binding must use the peer id and never the full
  URL: an iroh node legitimately holds several URLs at once (global-relay plus
  per-space relay URLs), the relay half changes on failover, and for incoming
  connections the responder has no URL for the initiator at accept time — only
  the authenticated `remote_id()` — so full-URL binding would false-negative
  between honest peers in exactly the multi-space deployments this design
  targets. (The `feat/mdns-bootstrap` handshake lacks channel binding entirely
  and will be updated.) Possible future hardening: TLS-exporter channel binding
  (RFC 9266 style) if iroh exposes exporter keying material; peer-id binding is
  sufficient today because the transport already authenticates peer ids.
- **Non-transferable credentials** (#263 requirement): follows directly from the
  above — proofs are bound to fresh nonces of both parties and both
  authenticated peer ids, so both replay and relay fail.
- **Oracle resistance and DoS.** An attacker can request proofs from honest
  nodes at will, but every proof is bound to the attacker-visible session and
  the honest node's peer id, so harvested proofs are unusable elsewhere. HMAC
  per challenge is cheap; per-URL rate limiting and a bounded pending-exchange
  table cap the state. The drop-triggered challenge (§4) adds no amplification —
  the sender's peer id is connection-authenticated, so challenges flow only
  back to the actual sender, 1:1, limited by the per-URL retry state — and no
  added disclosure, since anyone can already elicit a membership-revealing
  `Respond` by sending an `Initiate` (accepted below).
- **Accepted limitations (unchanged from the mDNS design):**
  - An adversary with a *candidate* secret list can precompute fingerprints and
    confirm a space's existence/membership. Time-bucketed rotating fingerprints
    are a possible v2; they complicate bootstrap indexing.
  - The fingerprint is a stable pseudonym → cross-peer/cross-time linkability
    (secret rotation per §1 refreshes the pseudonym, bounding the window).
  - The bootstrap operator sees per-fingerprint cohorts and timing. The
    bootstrap-hardening follow-on below reduces what those cohorts contain
    (URL hints instead of agent rosters) and gates queries on proof of
    knowledge; traffic-analysis residuals remain inherent to running a
    rendezvous service.
  - Granularity is the **conductor** (peer URL), not the agent: proving knowledge
    admits the whole remote instance, and data synced there is available to all
    its tenants. This is the honest trust boundary; #263 says the same.

## Migration

Breaking change; lands on `main` for the next minor per the project's branching
policy. Old and new nodes will not interoperate in a space once the host adopts
fingerprint SpaceIds (they rendezvous under different labels); Holochain's compat
params already hard-reject mismatched protocol versions, giving a clean cut.

For embedders that stay on the default `secret = space_id` (§2), access
*semantics* are unchanged — anyone who knows the space ID can get in — but the
mechanics improve: an unknown peer now completes a one-round-trip hello before
non-exempt traffic flows, instead of today's unknown=blocked dead state that
only a bootstrap poll could escape.

## Follow-on: bootstrap hardening (now simple)

The lrl mDNS design doc flagged bootstrap-server hardening as "a real protocol
redesign — the hard part isn't the hashing, it's retaining PUT anti-spam when the
server can no longer parse the payload." Two moves, enabled by this design,
dissolve that without encrypting anything:

1. **Self-certifying space route via a derived signing keypair.** Every member
   derives the same keypair from the space secret:
   `(auth_sk, auth_pk) = ed25519(derive_key(space, "k2-bootstrap-auth-v1"))`.
   The bootstrap route for the space *is* `auth_pk`, and every PUT/GET must be
   signed with `auth_sk` (GETs sign a server-issued nonce to prevent replay; the
   existing `AuthMaterial`/`blocking_get_auth` plumbing is the precedent). The
   server verifies against the route's own key — proof-of-knowledge the server
   can check without holding the secret. No registration, no TOFU, no squatting
   (a different key is a different route real members never consult). Anti-spam
   becomes *simpler* than today: one signature check plus per-route quotas,
   instead of parsing agent infos.
2. **Store URL hints, not agent infos.** Once introduction is PoK-gated, a
   joiner only needs somewhere to connect; the hello exchange delivers
   authenticated agent infos peer-to-peer immediately after. Server records
   shrink to signed `(peer_url, ttl)` — no agent public keys, no signed rosters.
   A compromised or curious operator holds pseudonymous cohorts of transport
   URLs (which churn with iroh's per-process endpoint keys), not identities.

A derived pubkey is a commitment like the hash, plus third-party-verifiable
possession proofs — which is what lets the server refuse spectators without
holding the secret. And agent infos are the right thing to remove from the
server: they are permanent identity (rosters compose into a group-membership
graph) and self-signed membership evidence, while URL hints churn on restart.

Residuals, stated honestly: the operator still sees traffic patterns and cohort
sizes per pseudonymous route; any secret-holder still learns member URLs (that is
what discovery is); rotating a leaked secret means abandoning the old space id —
cheap with the rotatable secret of §1 (rendezvous migration, not data migration).
These are the same residuals a full encrypted-payload design would leave, at a
fraction of the complexity. Note this couples bootstrap to hello (bare URLs are
useless without a PoK exchange to learn who's there) — acceptable, since the
ordering constraint in §1 already requires them to ship as a unit.

## Open questions for reviewers

1. Derived-key trait sufficiency: does anyone foresee a credential scheme that
   *cannot* be expressed as derived key material (see §2's rationale and
   deferral) soon enough that the proof-shaped companion trait should be
   designed now rather than added later (it is non-breaking to add)?
2. Message count: is deferring the responder's agent infos to `Ack` worth the 4th
   message, or should they ride in `Respond` accepting disclosure-before-proof
   from the responder side only?
3. Access-state keying: decisions are currently keyed by full `Url`, so a relay
   switch orphans a peer's `Granted` entry and forces a re-hello (one RTT;
   tolerable). Should access decisions instead be keyed by peer id within a
   space, matching what the proof actually binds?
4. Does the interim unknown-URL default flip belong on `release-0.x` lines while
   this ships on `main`, or is the bootstrap-backoff mitigation enough?
5. Naming: `hello` vs `access` for the module id (the module *is* #263's access
   module; "hello" describes the wire behavior).
6. Fingerprint form — hash or pubkey? §1 derives the SpaceId as a plain hash,
   while the bootstrap follow-on derives a signing keypair whose `auth_pk` could
   itself serve as *the* fingerprint everywhere (mDNS record, bootstrap route,
   potentially the SpaceId). One commitment, in the variant that also supports
   third-party-verifiable possession proofs — but it changes §1's derivation,
   so it is much cheaper to decide before Holochain-side work starts.
