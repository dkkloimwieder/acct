#!/usr/bin/env bash
# acct-zm69 / zm69.h6 — Batched-H bench sweep driver.
#
# Six shapes:
#   1. post_batch_h     batch=100    groups=50    (FOR-EACH-ROW; small batch, realistic groups)
#   2. post_batch_h     batch=1000   groups=50    (FOR-EACH-ROW; LOAD-BEARING — match resume plan)
#   3. post_batch_h     batch=10000  groups=50    (FOR-EACH-ROW; large batch amortization)
#   4. post_batch_h_app batch=1000   groups=50    (app-level batch check; trigger-cost comparison)
#   5. post_batch_h     batch=1000   groups=1     (fan_in; direct comparison to A2's 37.4 baseline)
#   6. post_batch_h     batch=1000   groups=5000  (fan_out; ceiling probe at scale matching A2's bench)

set -euo pipefail
cd "$(dirname "$0")/.."

DURATION="${POC_BENCH_DURATION_SECS:-60}"
WORKERS="${POC_BENCH_WORKERS:-20}"
ISSUE_PCT="${POC_BENCH_ISSUE_PCT:-70}"
OUT_DIR="${POC_OUT_DIR:-/tmp/h_batched_$(date +%s)}"
mkdir -p "$OUT_DIR"
echo "Output dir: $OUT_DIR"

run_one() {
    local fn="$1"
    local batch="$2"
    local groups="$3"
    local label="${fn}_b${batch}_g${groups}"
    local log="$OUT_DIR/${label}.log"
    echo
    echo "=== Running $label ==="
    POC_BENCH_FUNCTION="$fn" \
    POC_BENCH_BATCH_SIZE="$batch" \
    POC_BENCH_GROUPS="$groups" \
    POC_BENCH_DURATION_SECS="$DURATION" \
    POC_BENCH_WORKERS="$WORKERS" \
    POC_BENCH_ISSUE_PCT="$ISSUE_PCT" \
        cargo test --release --test bench_h_batched -- \
            --ignored --nocapture 2>&1 | tee "$log"
}

run_one post_batch_h     100   50
run_one post_batch_h     1000  50
run_one post_batch_h     10000 50
run_one post_batch_h_app 1000  50
run_one post_batch_h     1000  1
run_one post_batch_h     1000  5000

echo
echo "All shapes complete. Logs in $OUT_DIR/"
