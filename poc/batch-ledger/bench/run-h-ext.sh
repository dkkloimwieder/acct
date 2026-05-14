#!/usr/bin/env bash
# acct-zm69 / zm69.h6-followup — H+ext bench sweep driver.
#
# Same shape sweep as run-h-batched.sh but POC_BENCH_FUNCTION=post_batch_h_ext
# (extension-mediated invariant via h_arena shmem). Default RC isolation
# since invariant lives outside MVCC/SSI.

set -euo pipefail
cd "$(dirname "$0")/.."

DURATION="${POC_BENCH_DURATION_SECS:-60}"
WORKERS="${POC_BENCH_WORKERS:-20}"
ISSUE_PCT="${POC_BENCH_ISSUE_PCT:-70}"
OUT_DIR="${POC_OUT_DIR:-/tmp/h_ext_$(date +%s)}"
mkdir -p "$OUT_DIR"
echo "Output dir: $OUT_DIR"

run_one() {
    local batch="$1"
    local groups="$2"
    local label="post_batch_h_ext_b${batch}_g${groups}"
    local log="$OUT_DIR/${label}.log"
    echo
    echo "=== Running $label ==="
    POC_BENCH_FUNCTION=post_batch_h_ext \
    POC_BENCH_BATCH_SIZE="$batch" \
    POC_BENCH_GROUPS="$groups" \
    POC_BENCH_GROUP_QTY="${POC_BENCH_GROUP_QTY:-1000000000}" \
    POC_BENCH_DURATION_SECS="$DURATION" \
    POC_BENCH_WORKERS="$WORKERS" \
    POC_BENCH_ISSUE_PCT="$ISSUE_PCT" \
        cargo test --release --test bench_h_batched -- \
            --ignored --nocapture 2>&1 | tee "$log"
}

# Mirror run-h-batched.sh shapes. 50 groups + 5000 + 1 fan_in cover the spectrum.
run_one 100   50
run_one 1000  50
run_one 10000 50
run_one 1000  1
run_one 1000  5000

echo
echo "All shapes complete. Logs in $OUT_DIR/"
