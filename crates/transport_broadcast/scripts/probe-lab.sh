#!/usr/bin/env bash
# probe-lab: convenience wrapper around the bcast_probe example for the
# multicast measurement matrix in EXPERIMENTS.md (same directory).
#
# Run from anywhere inside the kitsune2 checkout, on each machine taking
# part in an experiment. Every subcommand maps to one machine-role in the
# runbook. Positional arguments come first; any remaining `--flag` args
# are passed straight through to bcast_probe (e.g.
# `probe-lab.sh listen 60 --port 25000`).
set -euo pipefail

cd "$(dirname "$0")/../../.."

PROBE=(cargo run -q -p kitsune2_transport_broadcast --example bcast_probe --)

usage() {
  cat >&2 <<'EOF'
probe-lab.sh <command> [args...] [--extra bcast_probe flags...]

commands:
  listen  [secs]              listen only (default: until ctrl-c), print
                              per-sender loss + kernel drop counters
  sweep                       serialized rate x size sweep; prints a loss
                              matrix from receiver reports
  bigbuf-sweep                sweep with an 8 MB SO_RCVBUF (pair with a
                              bigbuf-listen receiver to test the
                              kernel-drop hypothesis)
  bigbuf-listen [secs]        listen with an 8 MB SO_RCVBUF
  load <fps> <size> [secs]    steady background load / free-run beacon
                              (default 60s), final summary at the end
  dual-load <fps> <size> [secs]
                              TWO probe instances from this one machine,
                              each at <fps>, to test per-source vs
                              aggregate policing (default 60s)
  burst <n> [size] [secs]     n back-to-back frames once per second
                              (default size 1400, 30s) — queue-depth probe
  help                        this text

examples (see EXPERIMENTS.md for the full matrix):
  ./probe-lab.sh listen                       # control receiver
  ./probe-lab.sh bigbuf-sweep                 # experiment 1, sender side
  ./probe-lab.sh load 200 1400                # experiment 3, each sender
  ./probe-lab.sh dual-load 200 1400           # experiment 4
  ./probe-lab.sh burst 32                     # experiment 7
EOF
  exit 2
}

BIGBUF=8388608

# Consume the next argument if it exists and is not a --flag.
positional() {
  if [ $# -gt 0 ] && [ "${1#--}" = "$1" ]; then
    echo "$1"
  fi
}

cmd="${1:-help}"
shift || true

case "$cmd" in
  listen | bigbuf-listen)
    secs="$(positional "$@")"
    [ -n "$secs" ] && shift
    flags=()
    [ "$cmd" = "bigbuf-listen" ] && flags+=(--rcvbuf "$BIGBUF")
    [ -n "$secs" ] && flags+=(--for "$secs")
    exec "${PROBE[@]}" "${flags[@]}" "$@"
    ;;
  sweep)
    exec "${PROBE[@]}" --sweep "$@"
    ;;
  bigbuf-sweep)
    exec "${PROBE[@]}" --sweep --rcvbuf "$BIGBUF" "$@"
    ;;
  load)
    fps="${1:?usage: load <fps> <size> [secs]}"
    size="${2:?usage: load <fps> <size> [secs]}"
    shift 2
    secs="$(positional "$@")"
    [ -n "$secs" ] && shift || secs=60
    exec "${PROBE[@]}" --send "$fps" --size "$size" --for "$secs" "$@"
    ;;
  dual-load)
    fps="${1:?usage: dual-load <fps> <size> [secs]}"
    size="${2:?usage: dual-load <fps> <size> [secs]}"
    shift 2
    secs="$(positional "$@")"
    [ -n "$secs" ] && shift || secs=60
    # Build once so the two runs don't race the compiler.
    cargo build -q -p kitsune2_transport_broadcast --example bcast_probe
    bin=target/debug/examples/bcast_probe
    "$bin" --send "$fps" --size "$size" --for "$secs" "$@" &
    pid1=$!
    "$bin" --send "$fps" --size "$size" --for "$secs" "$@" &
    pid2=$!
    trap 'kill $pid1 $pid2 2>/dev/null || true' INT TERM
    wait "$pid1" "$pid2"
    ;;
  burst)
    n="${1:?usage: burst <n> [size] [secs]}"
    shift
    size="$(positional "$@")"
    [ -n "$size" ] && shift || size=1400
    secs="$(positional "$@")"
    [ -n "$secs" ] && shift || secs=30
    exec "${PROBE[@]}" --burst "$n" --size "$size" --for "$secs" "$@"
    ;;
  *)
    usage
    ;;
esac
