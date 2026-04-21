# Plan: integrate chunking layer with multicast fixes

Hand-off for a fresh agent session. Integrates the WIP chunking branch
with the recent UDP multicast fixes so the Beechat backend becomes
actually usable and the LXMF backend stops flooding the LAN.

## Context

Two-machine Volla LAN test on `volla` branch `reticulum` reached
"peers discover each other" but then the LXMF-backed reticulum traffic
saturated the local network to the point that other comms (SSH, vite
HMR, etc.) broke. Root cause suspected: kitsune2 gossip frames exceed
the Reticulum plaintext MDU and are either dropped, endlessly retried,
or fragmented in a way that overwhelms LXMF's event channel. The
chunking WIP branch addresses exactly this by framing all > MDU
traffic into paced `Link::send_small` fragments with a
`TAG_CHUNKED = 0x02` tag and a reassembly state machine.

## Starting branch state

| Repo                    | Branch                            | Head                                                                       | Notes |
| ----------------------- | --------------------------------- | -------------------------------------------------------------------------- | ----- |
| `kitsune2-lrl`          | `transport-reticulum`             | `a61a7ac fix(transport_reticulum): make UDP multicast 'group' actually multicast` | Local == origin. Includes the 0.5 → 0.4.0-dev.11 walkback (`102c86c`). |
| `kitsune2-lrl`          | `transport-reticulum-chunking`    | `b449c22 wip(transport_reticulum): chunking layer for oversized Data frames` | Origin only. Single WIP commit on top of `d2f501d` (older reticulum tip). Does **not** include multicast fix or walkback. |
| `../Reticulum-rs-lrl`   | `udp-multicast`                   | `60da33d feat(udp): multicast-aware socket bind`                           | Local only. Sitting on top of `8ff571f`, which is the git rev the Beechat backend currently pins in `kitsune2-lrl`'s `[patch.crates-io]`. |
| `../holochain-lrl`      | `transport-reticulum`             | `c03719f82 chore: pin kitsune2 at 0.4.0-dev.11 ...`                        | Pins match kitsune2-lrl. |
| `../volla`              | `reticulum`                       | `c47c7c4 Chore: simplify reticulum UDP config ...`                         | Uses LXMF via the `transport-reticulum` feature. Has env-var controls: `VOLLA_RUST_LOG`, `VOLLA_RETICULUM_LISTEN`/`DIAL`, `VOLLA_RETICULUM_ANNOUNCE_INTERVAL_S`. |
| `../tauri-plugin-holochain` | `main-0.6.1`                  | (unchanged)                                                                | Local, used via a path override in volla's `src-tauri/Cargo.toml`. |

## Integration goal

Fold the chunking WIP into `transport-reticulum` so a single branch has
both fixes, then extend outward: ensure the local `udp-multicast` fix
in `Reticulum-rs-lrl` is reachable from Beechat, verify both backends
compile and pass their round-trip tests, and confirm the resulting
build eliminates the LAN-spam observed on the current LXMF test.

## Steps

1. **Rebase chunking onto transport-reticulum.** In `kitsune2-lrl`:
   ```sh
   git checkout transport-reticulum-chunking
   git rebase transport-reticulum
   ```
   Expect conflict points at most in `config.rs` (chunking adds
   `chunk_reassembly_timeout_s`; multicast fix touches `Udp { bind, group }`
   docs + adds `resolve_udp_addrs` / `is_multicast_addr` helpers), and
   possibly in both `backend_lxmf.rs` and `backend_beechat.rs` where the
   `ReticulumInterfaceConfig::Udp` arm now funnels through the helper.
   Keep the multicast-fix semantics (group promotion + beechat warn)
   intact, merge the chunking additions alongside.

2. **Validate LXMF compile + tests.**
   ```sh
   cargo check -p kitsune2_transport_reticulum --features backend-lxmf
   cargo test  -p kitsune2_transport_reticulum --features backend-lxmf
   ```
   The two-node Beechat data-roundtrip test already exists; make sure
   the chunker's 50 KiB regression test still passes under LXMF if it's
   not beechat-gated.

3. **Wire Reticulum-rs-lrl udp-multicast.** Simplest path: swap the
   Beechat `reticulum` dep from a pinned git rev to the local sibling
   checkout in `kitsune2-lrl/Cargo.toml`:
   ```toml
   # workspace deps
   reticulum = { path = "../Reticulum-rs-lrl" }
   ```
   (Instead of the current
   `reticulum = { git = "...", rev = "8ff571f" }`.)
   Check the `udp-multicast` branch of `Reticulum-rs-lrl` is the active
   local HEAD before building.

4. **Validate Beechat compile + tests.**
   ```sh
   cargo check -p kitsune2_transport_reticulum --no-default-features --features backend-beechat
   cargo test  -p kitsune2_transport_reticulum --no-default-features --features backend-beechat
   ```
   Specifically verify the chunker's 50 KiB end-to-end Beechat round-trip
   test passes on real TCP loopback.

5. **Downstream verify from volla.**
   ```sh
   cd ../volla
   nix develop
   cargo check --manifest-path src-tauri/Cargo.toml --features holochain_bundled
   ```
   Should build clean. Then run the two-machine LAN test
   (`npm run start:desktop` with `VOLLA_RUST_LOG=warn,kitsune2_transport_reticulum=debug,rns_transport=debug`
   and `VOLLA_RETICULUM_ANNOUNCE_INTERVAL_S=60`) and confirm via
   `tcpdump -i eth0 -nn 'udp port 4242'` that packet rate stays modest
   (say <30 pps sustained) instead of the current flood.

6. **Swap volla to beechat for the same LAN test.** One-line edit in
   `volla/src-tauri/Cargo.toml` on the holochain dep: `transport-reticulum`
   → `transport-reticulum-beechat`. Rebuild, re-run. Confirms the
   integrated stack works end-to-end under Beechat — the goal state.

7. **Push / finalize.** After (6) is green: decide whether to
   merge the chunking integration into `transport-reticulum` proper or
   leave it on a dedicated branch (e.g. `transport-reticulum-integrated`).
   Update `holochain-lrl` / `volla` pins only if anything in the
   kitsune2 public API changed.

## Validation signals

- **Kitsune2 tests**: both `backend-lxmf` and `backend-beechat` feature
  matrices pass `cargo test`.
- **Chunker regression**: 50 KiB / 26-fragment Beechat roundtrip test
  from the chunking WIP passes.
- **Volla LXMF LAN**: two-machine discovery still works (peers see
  each other) AND tcpdump pps on the reticulum multicast port stays
  reasonable (no flood).
- **Volla Beechat LAN**: with the feature swap, same two-machine test
  passes with even lower per-packet overhead (Beechat's 2048 B MDU vs
  LXMF's ~464 B).

## Known gotchas

- **Don't bump kitsune2 back to 0.5.** The walkback was deliberate so
  `tauri-plugin-holochain`'s 0.4 pin stays satisfied via `[patch.crates-io]`
  in volla. The chunking WIP was authored before the walkback; its
  `Cargo.toml` may still say `0.5.0-dev.0` in places the walkback's
  sed didn't cover because the branch didn't exist yet. Sweep.
- **Feature exclusivity**: `backend-lxmf` and `backend-beechat` have a
  `compile_error!` guard in the transport_reticulum crate. Don't enable
  both.
- **Reticulum-rs path dep vs git rev**: the path dep gives fast iteration
  but anyone else building will need the sibling checkout at the expected
  location. Document this in the compile instructions if switching
  persistently.
- **Beechat event-channel cap**: the chunking commit adds 1ms pacing
  between fragments to work around Beechat's 16-slot broadcast channel
  dropping events on burst. If the integrated build shows fragment
  drops anyway, the workaround belongs upstream in `Reticulum-rs-lrl`
  (widen the channel, or use a different event distribution).

## Starting prompt for the fresh session

> Pick up the integration described in `kitsune2-lrl/PLAN-integrate-chunking.md`.
> Start from step 1 (rebase `transport-reticulum-chunking` onto
> `transport-reticulum` in kitsune2-lrl). Report at each validation
> signal in the plan.
