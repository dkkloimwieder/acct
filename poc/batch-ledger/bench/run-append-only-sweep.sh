#!/usr/bin/env bash
# 5×60s replicate sweep on post_batch_append_only, batch=1000 / 20w / 50 accounts.
# Output collected under /tmp/poc-append-only-sweep/ with per-run logs + aggregate.
#
# Per ezm methodology memory: many short runs with gaps detect small effects on
# a noisy system better than few long runs.

set -euo pipefail

cd "$(dirname "$0")/../../.."

OUT_DIR="${OUT_DIR:-/tmp/poc-append-only-sweep}"
RUNS="${RUNS:-5}"
GAP_SECS="${GAP_SECS:-30}"
DURATION="${DURATION:-60}"
WORKERS="${WORKERS:-20}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
ACCOUNTS="${ACCOUNTS:-50}"

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

echo "==> sweep: $RUNS × ${DURATION}s, gap=${GAP_SECS}s, batch=${BATCH_SIZE}, workers=${WORKERS}, accounts=${ACCOUNTS}"
echo "==> output: $OUT_DIR"

for i in $(seq 1 "$RUNS"); do
    echo
    echo "==> run $i / $RUNS"
    POC_BENCH_WORKERS="$WORKERS" \
    POC_BENCH_ACCOUNTS="$ACCOUNTS" \
    POC_BENCH_DURATION_SECS="$DURATION" \
    POC_BENCH_BATCH_SIZE="$BATCH_SIZE" \
        cargo test --manifest-path poc/batch-ledger/Cargo.toml \
        --release --test bench_p3_append_only -- \
        --ignored --nocapture --test-threads=1 p3_append_only_bench \
        2>&1 | tee "$OUT_DIR/run_$i.log"

    if [ "$i" -lt "$RUNS" ]; then
        echo "==> sleeping ${GAP_SECS}s before run $((i+1))"
        sleep "$GAP_SECS"
    fi
done

echo
echo "==> aggregating throughput across runs"
grep -hE "throughput: batches=" "$OUT_DIR"/run_*.log | tee "$OUT_DIR/throughput.txt"
echo
echo "==> aggregating per-run p99"
grep -hE "batch-latency" "$OUT_DIR"/run_*.log | tee "$OUT_DIR/latency.txt"
echo
echo "==> done. results in $OUT_DIR"
