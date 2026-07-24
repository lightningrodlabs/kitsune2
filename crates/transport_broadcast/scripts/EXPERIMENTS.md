# Multicast measurement matrix

Runbook for the experiments that pin down the LAN multicast loss
mechanisms before the phase-1 pacer design is finalized. Each experiment
lists the exact command per machine (via `probe-lab.sh`, this directory)
and the interpretation fork.

Machine roles used below:

- **W1, W2** — wired machines
- **F** — the WiFi laptop under test (the lossy path in all data so far)
- **F2** — a second WiFi device, if available

All commands accept extra `bcast_probe` flags after the listed arguments
(e.g. a non-default `--group`/`--port` to avoid live traffic). Every
receiver prints host-wide kernel udp drop counters
(`InErrors`/`RcvbufErrors` deltas) whenever they are non-zero — **watch
these on the receiving side in every experiment**: network loss and
local kernel loss look identical in the loss column, and the counters
are what tell them apart.

Background from the first campaign (see design doc §3.4 "Measured"):
wired↔wired ~684 kB/s clean; WiFi uplink ~lossless to ~0.9 MB/s; WiFi
downlink ~320 pps size-independent cap when F's radio is quiet,
collapsing to ~110 pps while F transmits 500 fps. All of the following
exists to explain those three numbers and their interactions.

---

## 1. Is the ~320 pps downlink "policer" real, or our own receive buffer?

The kernel accounts buffered datagrams by allocation size (~2 KiB/packet
regardless of payload), so an `SO_RCVBUF` overflow at F would *also*
look like a size-independent pps cap. Everything downstream depends on
which it is.

| machine | command |
| --- | --- |
| F | `./probe-lab.sh bigbuf-listen` |
| W1 | `./probe-lab.sh bigbuf-sweep` |

Then repeat with plain `listen` / `sweep` (OS-default buffer) for the
comparison.

- Cap unchanged at ~320 pps with the big buffer, and F shows **no**
  `RcvbufErrors` → the cap is in the network (AP policer confirmed).
- Cap moves/vanishes with the big buffer, or default-buffer runs show
  `RcvbufErrors` climbing in lockstep with loss → it was us all along;
  the medium's `rcvbuf` config (now available) is the real fix and the
  pacer numbers need re-measuring.

## 2. Where on the path is the loss? (free control receiver)

Multicast delivers to every receiver, so a wired listener during the
same sweep is a zero-cost control.

| machine | command |
| --- | --- |
| F | `./probe-lab.sh listen` |
| W2 | `./probe-lab.sh listen` |
| W1 | `./probe-lab.sh sweep` |

W1's matrix reports per-receiver rows for both listeners.

- W2 clean while F lossy → loss is on the WiFi leg (radio or AP
  forwarding), as assumed.
- W2 lossy too → loss is at the sender or switch, and the whole
  downlink story needs rework.

## 3. Per-source or aggregate policing? And is it fair?

The pacer's premise (per-node budgets) is sufficient only if the
bottleneck polices per source. Two sources at 200 fps vs the known
single-source behavior at 400 fps:

| machine | command |
| --- | --- |
| F | `./probe-lab.sh listen 90` |
| W1 | `./probe-lab.sh load 200 1400 60` |
| W2 | `./probe-lab.sh load 200 1400 60` (start within a few seconds of W1) |

Read F's per-sender rows (and final summary):

- Both senders ~0% → policing is **per-source**: per-node pacing under
  320 pps each is sufficient by itself.
- Both senders ~20% (like a single 400 fps sender) → policing is
  **aggregate**: per-node pacing cannot alone protect anyone; phase-2
  budget-sharing becomes a correctness requirement, and pacer defaults
  must assume multiple peers.
- Loss splits *unevenly* (one sender clean, one hammered) → channel
  capture exists; suppression/fairness machinery moves up the priority
  list.

## 4. Does the bottleneck see sources, hosts, or just packets?

Same aggregate as #3 but both senders on ONE host (same IP, same MAC):

| machine | command |
| --- | --- |
| F | `./probe-lab.sh listen 90` |
| W1 | `./probe-lab.sh dual-load 200 1400 60` |

Compare F's result with #3:

- Same as #3 → the bottleneck only counts packets (or keys per host and
  #3 was per-host too — distinguishable because #3 used two hosts).
- Different from #3 (e.g. here 20% each but in #3 clean) → policing
  keys on source identity; multi-conductor hosts need to share a budget.

## 5. Realistic scale: several modest senders

The "small office of nodes" case: does aggregate demand from N polite
senders break the downlink?

| machine | command |
| --- | --- |
| F | `./probe-lab.sh listen 90` |
| W1 | `./probe-lab.sh load 100 1400 60` |
| W2 | `./probe-lab.sh load 100 1400 60` |
| F2 (if available) | `./probe-lab.sh load 100 1400 60` |

Aggregate 300 fps from three sources, just under the quiet-radio cap.
Clean → per-node budgets of cap/N are workable. Lossy well below the
single-sender cap → contention costs more than arithmetic suggests
(airtime, not just the policer), and pacer defaults need margin for
peer count.

## 6. How much of "self-deafening" was the radio vs. F's own kernel?

Repeat of the bidirectional 500 fps run that measured 78% downlink
loss, now with drop counters (and optionally the big buffer):

| machine | command |
| --- | --- |
| F | `./probe-lab.sh load 500 200 60` |
| W1 | `./probe-lab.sh load 500 200 60` |

Watch F's kernel counter lines while its loss row runs:

- `RcvbufErrors` ~0 while loss is 78% → genuinely the radio
  (half-duplex deafness confirmed); slow-heal carries the load.
- `RcvbufErrors` accounts for a big share → F's own receive path was
  starving; medium-level `rcvbuf` + receive-loop hardening recover real
  capacity, and the radio penalty is smaller than measured.
- Optionally repeat with `bigbuf`-variants to see how much capacity a
  bigger buffer buys back.

## 7. Bottleneck queue depth (calibrates the pacer's burst allowance)

Bursts of n back-to-back full-MTU frames, once per second — loss vs n
maps how deep the bottleneck's queue is:

| machine | command |
| --- | --- |
| F | `./probe-lab.sh listen 40` |
| W1 | `./probe-lab.sh burst 8` — then `burst 32`, then `burst 128` |

At 1 burst/sec the average rate is far under every measured cap, so any
loss is pure queue overflow. The largest clean n (times frame size) ≈
the bottleneck's buffer, which is the number the token bucket's burst
capacity should sit under. If even `burst 128` is clean, buffers are
deep and burst sizing is a non-issue on this network.

---

## Recording results

Paste each run's final summary / matrix (and any kernel-counter lines)
into the design doc's §3.4 "Measured" section, per experiment. The
decision forks above map measurements directly onto pacer design
choices: budget defaults, per-node vs shared budgeting, burst capacity,
and whether receive-path hardening (rcvbuf) belongs in the default
medium config.
