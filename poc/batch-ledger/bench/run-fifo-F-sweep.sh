#!/usr/bin/env bash
# acct-ylmo — FIFO maximal F (sub 4 / acct-0450) bench sweep.
#
# Compares F-shmem (post_batch_fifo_maximal_F; mig 0024 + sub 4 bgworker
# drain) against the historical reference points:
#
#   fanin_fifo_mutable   : pools=1     post_batch_fifo            (mig 0020 plpgsql; ~370 tps baseline)
#   fanout_fifo_mutable  : pools=5000  post_batch_fifo            (~793 tps baseline)
#   fanin_fifo_inline    : pools=1     post_batch_fifo_maximal_inline (mig 0023; ~N/A baseline, single-rep)
#   fanout_fifo_inline   : pools=5000  post_batch_fifo_maximal_inline (~9,487 tps baseline)
#   fanin_fifo_F         : pools=1     post_batch_fifo_maximal_F  (mig 0024; F-shmem THIS EPIC)
#   fanout_fifo_F        : pools=5000  post_batch_fifo_maximal_F
#
# 3 replicates × 60s × 15s gaps for the F variants (the load-bearing
# new measurement). 1 replicate for mutable + inline anchors (history
# already has them; running 1 rep here just confirms we're on the
# same PG conf / mig set / dev box for an apples-to-apples compare).

set -euo pipefail

cd "$(dirname "$0")/../../.."

OUT_DIR="${OUT_DIR:-/tmp/poc-ylmo-bench}"
F_RUNS="${F_RUNS:-3}"
ANCHOR_RUNS="${ANCHOR_RUNS:-1}"
GAP_SECS="${GAP_SECS:-15}"
DURATION="${DURATION:-60}"
WORKERS="${WORKERS:-20}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
# Pure-issue workload: F-shmem caps MAX_LAYERS=64 per pool. The legacy
# 70/30 split would overflow the ring within the first batch on 1-pool
# fan-in (300 receipts × 1000 envelopes / batch → ~21 batches to cap;
# coalescing is sub 3 / acct-b8ub). For sub 4 we characterize the
# "issue-dominant warehouse outflow" shape where the ring stays small.
ISSUE_PCT="${ISSUE_PCT:-100}"
# Pre-seed each layer with 1B qty (5 layers × 5B = 5B qty/pool) so a
# 60s pure-issue run can't fully drain any layer. Keeps the ring at
# 5 layers throughout the bench → no overflow + no fully-drained
# layers (head advance) → tests the steady-state per-issue path.
LAYER_QTY="${LAYER_QTY:-1000000000}"

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

run_cell() {
    local label=$1 fn=$2 shape=$3 pools=$4 runs=$5
    mkdir -p "$OUT_DIR/$label"
    echo
    echo "==> $label : fn=$fn shape=$shape pools=$pools runs=$runs"
    for i in $(seq 1 "$runs"); do
        echo "--- run $i / $runs"
        POC_BENCH_WORKERS="$WORKERS" \
        POC_BENCH_DURATION_SECS="$DURATION" \
        POC_BENCH_BATCH_SIZE="$BATCH_SIZE" \
        POC_BENCH_FUNCTION="$fn" \
        POC_BENCH_SHAPE="$shape" \
        POC_BENCH_POOLS="$pools" \
        POC_BENCH_ISSUE_PCT="$ISSUE_PCT" \
        POC_BENCH_LAYER_QTY="$LAYER_QTY" \
            cargo test --manifest-path poc/batch-ledger/Cargo.toml \
            --release --test bench_fifo_fan -- \
            --ignored --nocapture --test-threads=1 \
            2>&1 | tee "$OUT_DIR/$label/run_$i.log"
        if [ "$i" -lt "$runs" ]; then sleep "$GAP_SECS"; fi
    done
}

# Anchors: 1 rep each.
run_cell fanin_fifo_mutable    post_batch_fifo                  fan_in   1    "$ANCHOR_RUNS"
run_cell fanin_fifo_inline     post_batch_fifo_maximal_inline   fan_in   1    "$ANCHOR_RUNS"
run_cell fanout_fifo_mutable   post_batch_fifo                  fan_out  5000 "$ANCHOR_RUNS"
run_cell fanout_fifo_inline    post_batch_fifo_maximal_inline   fan_out  5000 "$ANCHOR_RUNS"

# F-shmem (sub 4): the load-bearing new measurement.
run_cell fanin_fifo_F          post_batch_fifo_maximal_F        fan_in   1    "$F_RUNS"
run_cell fanout_fifo_F         post_batch_fifo_maximal_F        fan_out  5000 "$F_RUNS"

echo
echo "==> aggregating"
{
    printf "%-25s | %3s | %8s | %8s | %8s\n" "scenario" "run" "tps" "p50_ms" "p99_ms"
    echo "--------------------------+-----+----------+----------+---------"
    for sub in \
        fanin_fifo_mutable fanin_fifo_inline fanin_fifo_F \
        fanout_fifo_mutable fanout_fifo_inline fanout_fifo_F; do
        if [ ! -d "$OUT_DIR/$sub" ]; then continue; fi
        i=1
        for log in "$OUT_DIR/$sub"/run_*.log; do
            tps=$(awk '/transfers=[0-9]+\.[0-9]+\/s/ {match($0, /transfers=([0-9]+\.[0-9]+)/, m); print m[1]; exit}' "$log")
            p50_us=$(awk '/batch-latency/ {match($0, /p50=([0-9]+)/, m); print m[1]; exit}' "$log")
            p99_us=$(awk '/batch-latency/ {match($0, /p99=([0-9]+)/, m); print m[1]; exit}' "$log")
            printf "%-25s | %3d | %8.0f | %8d | %8d\n" "$sub" "$i" "${tps:-0}" "$((${p50_us:-0}/1000))" "$((${p99_us:-0}/1000))"
            i=$((i+1))
        done
    done
} | tee "$OUT_DIR/summary.txt"
echo
echo "==> done. results in $OUT_DIR"
