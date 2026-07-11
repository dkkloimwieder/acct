#!/usr/bin/env bash
# Open-loop saturation sweep (acct-0at4.8 methodology): ramp the offered rate
# in rungs against one hot path and report throughput-at-SLO — the max
# sustainable rate with p99 under the SLO and achieved ≥ keep-up × offered.
# Latency is coordinated-omission-free (measured from intended send-time).
#
# The bench host is a noisy workstation: absolute numbers move with ambient
# load — check /proc/loadavg and running browsers before trusting them, and
# prefer the structural ratios (direct vs staging at the same rung).
#
# Usage:
#   bash poc/ledger-v3.2/scripts/slo-sweep.sh                # direct path
#   SWEEP_PATH=staging bash .../slo-sweep.sh
#   SWEEP_RUNGS="200 400 800 1600 3200" SWEEP_SECS=30 bash .../slo-sweep.sh
#
# Env knobs:
#   SWEEP_DSN, SWEEP_PATH (direct|staging), SWEEP_RUNGS, SWEEP_SECS (20),
#   SWEEP_CALLERS (32), SWEEP_POOLS (300), SWEEP_PROFILE (med),
#   SWEEP_SLO_P99_US (10000 = 10ms), SWEEP_KEEPUP_PCT (95),
#   SWEEP_OUT (bench/out/slo-<path>.json)

set -euo pipefail

WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$WORKSPACE_DIR"

DSN="${SWEEP_DSN:-postgres://acct:acct_dev@localhost:5111/poc_v3_2}"
SWEEP_PATH="${SWEEP_PATH:-direct}"
RUNGS="${SWEEP_RUNGS:-200 400 800 1600 3200}"
SECS="${SWEEP_SECS:-20}"
CALLERS="${SWEEP_CALLERS:-32}"
POOLS="${SWEEP_POOLS:-300}"
PROFILE="${SWEEP_PROFILE:-med}"
SLO_US="${SWEEP_SLO_P99_US:-10000}"
KEEPUP="${SWEEP_KEEPUP_PCT:-95}"
OUT="${SWEEP_OUT:-bench/out/slo-${SWEEP_PATH}.json}"

mkdir -p "$(dirname "$OUT")"
echo "==> building ledger-bench (release)"
cargo build --release -p ledger-bench 2>&1 | tail -1
BIN=target/release/ledger-bench

echo "==> seeding $POOLS pools"
"$BIN" --dsn "$DSN" seed --pools "$POOLS" >/dev/null

echo "==> sweep path=$SWEEP_PATH rungs=[$RUNGS] ${SECS}s each; SLO p99<${SLO_US}µs keep-up ${KEEPUP}%"
echo "[" > "$OUT"
first=1
best_rate=0
for rate in $RUNGS; do
    report="$("$BIN" --dsn "$DSN" load --path "$SWEEP_PATH" --rate "$rate" \
        --callers "$CALLERS" --duration-secs "$SECS" --pools "$POOLS" \
        --profile "$PROFILE" --backdate-pct 0 || true)"
    [ "$first" = 1 ] || echo "," >> "$OUT"
    first=0
    echo "$report" >> "$OUT"
    achieved="$(echo "$report" | grep -o '"achieved_rate": [0-9.]*' | grep -o '[0-9.]*$')"
    p99="$(echo "$report" | grep -o '"p99_us": [0-9]*' | grep -o '[0-9]*$')"
    keep_ok="$(awk "BEGIN {print ($achieved >= $rate * $KEEPUP / 100) ? 1 : 0}")"
    slo_ok="$(awk "BEGIN {print ($p99 < $SLO_US) ? 1 : 0}")"
    verdict="FAIL"
    if [ "$keep_ok" = 1 ] && [ "$slo_ok" = 1 ]; then
        verdict="PASS"
        best_rate="$rate"
    fi
    printf "rung %6s trx/s: achieved %8.0f, p99 %8sµs  → %s\n" "$rate" "$achieved" "$p99" "$verdict"
done
echo "]" >> "$OUT"
echo "==> throughput-at-SLO ($SWEEP_PATH): $best_rate trx/s (p99<${SLO_US}µs, keep-up ${KEEPUP}%)"
echo "==> per-rung reports: $OUT"
