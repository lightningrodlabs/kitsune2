# Moss Group Joining Delays and Kitsune/Bootstrap fixes

## Context

After last week's Moss test where many people couldn't join, Gregory shared his sense that much of the problem was likely due to how kitsune currently handles blocking agents as documented in these three issues:  [#263](https://github.com/holochain/kitsune2/issues/263) [#265](https://github.com/holochain/kitsune2/issues/265) & [#347](https://github.com/holochain/kitsune2/issues/347)

I looked at these issues and it struck me that this problem is related to a number of other access issues that have been bothering me for a while:

1. The bootstrap server leaks DNA hashes and AgentInfos and shouldn't.

2. mdns discovery has the same issue, you don't really want to broadcast the DNA hash on the local network. I had already done an initial implementation of a fix for this which included a ProofOfKnowledge handshake to manage this [here](https://github.com/lightningrodlabs/kitsune2/tree/feat/mdns-bootstrap-0.4.1)

3. One kitsune node leaks agent-infos and DNA hashes to another kitsune node that has some shared DNAs but not all.

4. There's been no good or easy way to revoke access to a Holochain network other than a full fork and migrate by all the people who agree they want to do that.

5. Read access has thus been hard to achieve without something like the centralized auth server that's connected to the iroh relay.

## Design

The DNA hash has always been the de facto read capability; the problem is that everything leaks it. So, two mechanisms:

1. Treat the **DNA hash as the group secret**: the kitsune `space_id` is derived from it one-way, so it can be published freely, and the hash itself never goes on the wire, to the bootstrap server, or over mDNS.
2. Replace the admission criterion the transport already enforces — presence of a signed agent info — with **proof of knowledge of that secret**, since a self-signed info over a public `space_id` proves nothing. A new **hello module** supplies the proof; its own messages are exempt from the check, as `Preflight` is today, which makes it the only module that can speak before a grant exists — so it carries the peer introduction in the same exchange.

### Keys

```
group_secret         = dna_hash                            // host-held, never published
(space_sk, space_id) = ed25519(KDF(group_secret, "k2-space-v1"))
k_hello              = KDF(group_secret, "k2-hello-v1")
k_enc                = KDF(group_secret, "k2-agent-enc-v1")
```

`space_id` is the only publishable identifier — wire envelope, bootstrap route, mDNS TXT record; the wire format is unchanged, only the meaning of the bytes. Everything else is derivable only by members, so the host hands kitsune purpose-scoped keys via `derive_key(space_id, purpose)` and kitsune never sees the DNA hash.

`KDF` must be pinned exactly, since two conductors that derive differently cannot talk: HKDF-SHA256, 32-byte output, `ikm = group_secret`, `info = purpose`, `salt = space_id` — which fixes the derivation order: `space_id` first (its own derivation is unsalted), then everything else salted by it. And a footgun to name: kitsune's built-in default when a host configures no secret is `secret = space_id`, which under this design means "open to anyone who knows the space_id". That is the intended degenerate semantics for bare embedders, but a host deriving `space_id` from a real secret must always supply its own `derive_key` — Holochain does, since only it holds the DNA hash.

It is an ed25519 **verifying key** rather than a hash for one reason: the bootstrap server holds no secret and must verify possession proofs against it. Between members both sides hold the secret, so hello uses HMAC — **symmetric proofs between members, asymmetric only at the boundary with a non-member.** Signing hello transcripts would instead make every proof transferable evidence of a node's membership, verifiable by anyone holding the public `space_id`.

Read protection is exactly as strong as the DNA hash is guessable: with public zomes the entropy is the network seed's, so seeds should be generated random, not human-chosen. A separate rotatable secret (revocation by re-keying) was considered and deferred because of the **straggler problem**: the rotated secret can be distributed in-band to every member who comes online during the transition — encrypted to each remaining member's agent key, published where the excluded party sees only ciphertext — but a member offline past the window returns to an abandoned rendezvous with no one left to fetch from, and can only be recovered out-of-band. So rotation is only as good as the group's manual relations, which is workable for small networks and unworkable past that. Deferring is cheap to reverse — `space_id` stays an opaque key on the wire, so switching a group to an independent secret later is a rendezvous migration, not a data migration — and revocation meanwhile uses the block list (below).

### Hello handshake

1. Initiator sends `nonce_i`
2. Responder responds with `(proof_r, nonce_r)`
3. Initiator confirms with `(proof_i, agent_infos_i)`
4. Responder acknowledges with `(agent_infos_r)`

`proof_x` is `HMAC-SHA256(k_hello, T_x)` over a transcript of **fresh per-exchange nonces** that binds both the session and the channel, and is role-asymmetric so neither proof can be reflected as the other:

```
T_r = "k2-hello-proof-v1" ‖ proto_ver ‖ nonce_r ‖ nonce_i ‖ peer_id_r ‖ peer_id_i
T_i = "k2-hello-proof-v1" ‖ proto_ver ‖ nonce_i ‖ nonce_r ‖ peer_id_i ‖ peer_id_r
```

- Bind the **peer id, not the full URL** — an iroh node holds several URLs at once and they change on relay failover. The verifier must take the peer id from the connection's authenticated remote identity, never from the payload, or a relay attack replays an honest member's proof.
- Agent infos are disclosed only after the counterparty's proof verifies — what the 4th message buys — and on success each side records `Granted` for (space, peer id) and inserts them. That is the #347 fix: introduction in two round trips instead of waiting on a bootstrap poll.
- **Crossing initiates are the normal case, not the corner case** — two peers joining a space discover each other symmetrically — so the resolution is part of the wire protocol: the peer with the bytewise-lower peer id keeps the initiator role, the other abandons its own exchange and answers; and the winner **repeats its initiate with the same nonce** rather than staying silent, since its first one may have arrived before the loser had the space and been dropped. Silence there deadlocks both sides into timeout-and-backoff.

### Triggers

An ungranted URL stays untrusted but stops being a dead end: dropping its message now triggers an exchange rather than ending there. Hellos are initiated:

- when a **local agent joins a space** ("I'm new here"): initiated with all currently connected peers and everything already in the peer store (this is the Moss multi-cell case)
- when a **new agent info is added to the peer store** ("they're new here", e.g. via bootstrap or mDNS) — in the sharded case, only for peers whose arc overlaps ours
- when an **incoming non-hello message from an ungranted URL is dropped** ("one of us forgot"): grant state is lossy and asymmetric, and this heals the pair (rate-limited, and never for explicitly blocked peers)
- on **gossip initiation with no eligible peers** ("both of us forgot"): every other trigger is event-driven, and in symmetric grant loss no events occur — gossip's timer is the only thing still firing

### Revocation

Revocation is the **existing block list**, which already beats grants everywhere. Honest scope: blocking makes every honest node refuse service, but it cannot withdraw knowledge — and since blocks are keyed by identity while the read capability is knowledge, a determined excluded party can re-key and hello again. This is a practical deterrent, not cryptographic revocation; true capability withdrawal (secret rotation) is deferred future work.

Be explicit about one consequence: an excluded party still holds the group secret, so they can still derive `space_sk` and `k_enc` — and the bootstrap server, which authenticates the *space*, not the member, cannot tell them apart from anyone else. **Blocking stops service from honest nodes; it does not stop an ex-member from reading the encrypted roster off the bootstrap server, indefinitely.** That is the strongest standing argument for eventually un-deferring the rotatable secret.

### Bootstrap

The route is `space_id` itself, and PUT/GET are signed with the derived `space_sk` (GETs sign a server-issued nonce against replay — a challenge in the reject response, not a separate endpoint), so the server verifies membership against the route's own key while holding no secret — anti-spam becomes one signature check plus per-route quotas. Note the auth is space-granular by construction: the server cannot rate-limit *per member*, so one misbehaving member can exhaust its own group's quota — a griefing limit we accept.

What's stored is the **full agent info, encrypted under `k_enc`**, with cleartext ttl. This keeps arcs visible to members *before* connecting — in the sharded case you choose whom to hello based on arc coverage — while the operator and non-members see neither identities nor rosters. Encryption is symmetric, so any member could forge a blob, but the inner agent info is self-signed and members verify it after decrypting.

Replacement (a fresh info superseding an agent's old record) needs a record key the server can match without reading the payload, and there is a real trade hiding in it: a key stable per agent gives the server a per-agent pseudonym and its update cadence; a key random per publisher orphans records across restarts. We take the stable side, scoped so it links to nothing outside the space:

```
record_key = KDF(group_secret, "k2-record-v1" ‖ agent_pubkey)
```

The server sees stable opaque keys and their update timing within one route — accepted — and cannot link them across spaces or to any real identity. Tombstones are just replacements whose inner info says so.

## Consequences

- **Kitsune peer-id.** Identification and access-decision keying move from full URL to peer id.
- **Grant state stands alone.** A grant is independent state with its own lifetime, not a shadow of peer-store contents, so the existing peer-store listener stops deriving and revoking decisions from agent-info presence — and with hello and the blocks listener both writing that map, blocks must explicitly win.
- **Host derived-key trait.** New `derive_key(space_id, purpose)` with the usual factory/builder plumbing.
- **Holochain preflight slim-down.** Preflight returns to compat-checking only and stops broadcasting agent infos.
- **mDNS simplification.** Keep advertising `space_id` + node id, drop the bespoke session protocol, use hello — keyed by `k_hello` like everywhere else.
- **Bootstrap server simplification.** Payloads become opaque blobs; signature/expiry/tombstone validation moves from the server to members after decryption, keyed for replacement as above.
- **Space mapping.** The host keeps a `space_id ↔ DNA` map; the DNA hash stays the app-facing identity, `space_id` is network-internal.
- **Invites are unchanged** — bundle plus network seed, exactly as today; no new distribution channel is introduced.
- **Ships as a unit,** since a derived `space_id` without the gate delivers none of the read protection it advertises. Breaking change, `main`, next minor.
