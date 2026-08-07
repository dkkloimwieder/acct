#!/usr/bin/env bash
# The v3.2 cadence-vs-load soak (acct-qm7o.6): open-loop load on the direct +
# staging paths, the feed consumer, N recalc workers, and the G1/G2 gauge
# sampler all running concurrently IN ONE PROCESS (ledger-bench soak), then
# quiesce → conservation/oracle verify → orchestrated close → immutability
# probes. Everything is in-process, so there are no orphan shells to sweep.
#
# The soak owns the ledger_feed slot: stop any external run-feed.sh /
# run-recalc.sh loops before starting (the slot has one cursor).
#
# Usage:
#   bash poc/ledger-v3.2/scripts/soak.sh                  # bounded smoke (60s)
#   SOAK_SECS=300 SOAK_RATE=800 bash .../soak.sh          # bigger run
#   SOAK_PROFILE=trend SOAK_BACKDATE_PCT=10 bash .../soak.sh
#   SOAK_PAUSE_WORKERS=60:120 bash .../soak.sh            # quiet-backlog demo
#
# Env knobs (all optional):
#   SOAK_DSN, SOAK_POOLS (300), SOAK_SECS (60), SOAK_RATE (400 trx/s per path;
#   0 = closed loop), SOAK_DIRECT_CALLERS (24), SOAK_STAGING_CALLERS (8),
#   SOAK_WORKERS (4), SOAK_PROFILE (med: low|med|high|trend),
#   SOAK_BACKDATE_PCT (5), SOAK_BACKDATE_WINDOW (3600s),
#   SOAK_PAUSE_WORKERS / SOAK_PAUSE_FEED ("A:B" seconds, off by default),
#   SOAK_MIDRUN_CLOSE_AT (seconds; off), SOAK_OUT (bench/out), SOAK_SEED (42)

set -euo pipefail

WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$WORKSPACE_DIR"

DSN="${SOAK_DSN:-postgres://acct:acct_dev@localhost:5111/poc_v3_2}"
ARGS=(
    --dsn "$DSN" soak
    --pools "${SOAK_POOLS:-300}"
    --duration-secs "${SOAK_SECS:-60}"
    --rate "${SOAK_RATE:-400}"
    --direct-callers "${SOAK_DIRECT_CALLERS:-24}"
    --staging-callers "${SOAK_STAGING_CALLERS:-8}"
    --workers "${SOAK_WORKERS:-4}"
    --profile "${SOAK_PROFILE:-med}"
    --backdate-pct "${SOAK_BACKDATE_PCT:-5}"
    --backdate-window-secs "${SOAK_BACKDATE_WINDOW:-3600}"
    --seed "${SOAK_SEED:-42}"
    --out-dir "${SOAK_OUT:-bench/out}"
)
[ -n "${SOAK_PAUSE_WORKERS:-}" ] && ARGS+=(--pause-workers "$SOAK_PAUSE_WORKERS")
[ -n "${SOAK_PAUSE_FEED:-}" ] && ARGS+=(--pause-feed "$SOAK_PAUSE_FEED")
[ -n "${SOAK_MIDRUN_CLOSE_AT:-}" ] && ARGS+=(--midrun-close-at "$SOAK_MIDRUN_CLOSE_AT")

echo "==> ledger-bench ${ARGS[*]}"
exec cargo run --release -p ledger-bench -- "${ARGS[@]}"
