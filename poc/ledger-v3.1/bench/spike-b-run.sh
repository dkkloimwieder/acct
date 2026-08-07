#!/usr/bin/env bash
# SPIKE-B (acct-0at4.11.2) campaign runner: RMW baseline (direct-per-call,
# ledger_submit_trx_c) vs the single-statement commutative variant
# (direct-single, ledger_submit_trx_single_c) on one scenario.
#
# Interleaves the two flavors rep-by-rep under identical conditions so the
# direct-single / direct-per-call THROUGHPUT (and p99) RATIO is robust to the
# noisy bench host (Chrome, load drift). Emits one JSON summary line per run to
# results/spike-b-<scenario>-cap<cap>.jsonl.
#
# Usage: bench/spike-b-run.sh <scenario> <depth> <reseed:each|once> [reps] [dur] [cap]
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
BIN=./target/release/ledger-harness

SCEN="${1:?scenario e.g. s5}"
DEPTH="${2:?depth e.g. 10}"
RESEED="${3:?each|once}"
REPS="${4:-3}"
DUR="${5:-15s}"
CAP="${6:-400}"

SEED_ARGS="--method-mix all-fifo --seed-count 10000 --seed-skus 1000 --seed-locations 10 --seed-depth ${DEPTH}"
OUT="results/spike-b-${SCEN}-cap${CAP}.jsonl"; : > "$OUT"

if [ "$RESEED" = "once" ]; then
  echo "== seed ${SCEN} depth=${DEPTH} (once) =="
  # shellcheck disable=SC2086
  $BIN run --scenario "$SCEN" --mode direct-per-call $SEED_ARGS --duration 2s --no-sampler --max-callers "$CAP" >/dev/null 2>&1 || true
  SEED_ARGS=""
fi

for r in $(seq 1 "$REPS"); do
  for MODE in direct-per-call direct-single; do
    echo "== ${SCEN} rep ${r}/${REPS} ${MODE} (depth=${DEPTH}, dur=${DUR}, cap=${CAP}) =="
    # shellcheck disable=SC2086
    $BIN run --scenario "$SCEN" --mode "$MODE" $SEED_ARGS --duration "$DUR" \
        --no-sampler --max-callers "$CAP" 2>/dev/null | tail -1 | tee -a "$OUT"
  done
done
echo "== done: $OUT =="
