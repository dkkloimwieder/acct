#!/usr/bin/env bash
# acct-zm69 — H+ext FIFO path comparison sweep.
#
# Runs paths 1/2/3 at 4 representative shapes each.

set -euo pipefail
cd "$(dirname "$0")/.."

DURATION="${POC_BENCH_DURATION_SECS:-60}"
WORKERS="${POC_BENCH_WORKERS:-20}"
ISSUE_PCT="${POC_BENCH_ISSUE_PCT:-70}"
OUT_DIR="${POC_OUT_DIR:-/tmp/h_paths_$(date +%s)}"
mkdir -p "$OUT_DIR"
echo "Output dir: $OUT_DIR"

run_one() {
    local fn="$1"
    local batch="$2"
    local groups="$3"
    local label="${fn}_b${batch}_g${groups}"
    local log="$OUT_DIR/${label}.log"
    echo
    echo "=== $label ==="
    POC_BENCH_FUNCTION="$fn" \
    POC_BENCH_BATCH_SIZE="$batch" \
    POC_BENCH_GROUPS="$groups" \
    POC_BENCH_GROUP_QTY="${POC_BENCH_GROUP_QTY:-1000000000}" \
    POC_BENCH_DURATION_SECS="$DURATION" \
    POC_BENCH_WORKERS="$WORKERS" \
    POC_BENCH_ISSUE_PCT="$ISSUE_PCT" \
        cargo test --release --test bench_h_batched -- \
            --ignored --nocapture 2>&1 | tee "$log" | tail -15
}

# Pick which functions to run.
FUNCTIONS="${POC_BENCH_FUNCTIONS:-post_batch_h_ext_layer_shmem post_batch_h_ext_deferred}"

for fn in $FUNCTIONS; do
    run_one "$fn" 100   50
    run_one "$fn" 1000  50
    run_one "$fn" 1000  1
    run_one "$fn" 1000  5000
done

echo
echo "All shapes complete. Logs in $OUT_DIR/"
