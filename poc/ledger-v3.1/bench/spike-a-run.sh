#!/usr/bin/env bash
# SPIKE-A (acct-0at4.11.1) campaign runner: routed vs staging on one scenario.
#
# Interleaves routed/staging reps back-to-back under identical conditions so the
# staging/routed THROUGHPUT RATIO is robust to the noisy bench host (Chrome, load
# drift) even when absolutes bounce. Emits one JSON summary line per run to
# results/spike-a-<scenario>.jsonl.
#
# Usage: bench/spike-a-run.sh <scenario> <depth> <reseed:each|once> [reps] [dur] [cap]
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
OUT="results/spike-a-${SCEN}.jsonl"; : > "$OUT"

if [ "$RESEED" = "once" ]; then
  echo "== seed ${SCEN} depth=${DEPTH} (once) =="
  # shellcheck disable=SC2086
  $BIN run --scenario "$SCEN" --mode staging $SEED_ARGS --duration 2s --no-sampler --max-callers "$CAP" >/dev/null 2>&1 || true
  SEED_ARGS=""
fi

for r in $(seq 1 "$REPS"); do
  for MODE in routed staging; do
    echo "== ${SCEN} rep ${r}/${REPS} ${MODE} (depth=${DEPTH}, dur=${DUR}, cap=${CAP}) =="
    # shellcheck disable=SC2086
    $BIN run --scenario "$SCEN" --mode "$MODE" $SEED_ARGS --duration "$DUR" \
        --no-sampler --max-callers "$CAP" 2>/dev/null | tail -1 | tee -a "$OUT"
  done
done
echo "== done: $OUT =="
