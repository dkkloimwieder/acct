#!/usr/bin/env bash
# acct-oqje — FIFO fan-in/fan-out bench: mutable (mig 0020) + maximal (mig 0021).
#
# Cells (4 total = 2 shapes × 2 functions):
#   fanin_fifo_mutable   : pools=1     post_batch_fifo            (mig 0020 plpgsql)
#   fanin_fifo_maximal   : pools=1     post_batch_fifo_maximal    (mig 0021 Rust dispatcher)
#   fanout_fifo_mutable  : pools=5000  post_batch_fifo
#   fanout_fifo_maximal  : pools=5000  post_batch_fifo_maximal
#
# Mirrors run-shmem-wac-maximal-sweep.sh but at 70% issue / 30% receipt
# (FIFO's expensive path is the issue walk, not the receipt). Each pool
# pre-seeded with 5 layers × 1M qty so workers cannot drain them in 60s.

set -euo pipefail

cd "$(dirname "$0")/../../.."

OUT_DIR="${OUT_DIR:-/tmp/poc-oqje-bench}"
RUNS="${RUNS:-3}"
GAP_SECS="${GAP_SECS:-15}"
DURATION="${DURATION:-60}"
WORKERS="${WORKERS:-20}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
ISSUE_PCT="${ISSUE_PCT:-70}"

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

run_cell() {
    local label=$1 fn=$2 shape=$3 pools=$4
    mkdir -p "$OUT_DIR/$label"
    echo
    echo "==> $label : fn=$fn shape=$shape pools=$pools"
    for i in $(seq 1 "$RUNS"); do
        echo "--- run $i / $RUNS"
        POC_BENCH_WORKERS="$WORKERS" \
        POC_BENCH_DURATION_SECS="$DURATION" \
        POC_BENCH_BATCH_SIZE="$BATCH_SIZE" \
        POC_BENCH_FUNCTION="$fn" \
        POC_BENCH_SHAPE="$shape" \
        POC_BENCH_POOLS="$pools" \
        POC_BENCH_ISSUE_PCT="$ISSUE_PCT" \
            cargo test --manifest-path poc/batch-ledger/Cargo.toml \
            --release --test bench_fifo_fan -- \
            --ignored --nocapture --test-threads=1 \
            2>&1 | tee "$OUT_DIR/$label/run_$i.log"
        if [ "$i" -lt "$RUNS" ]; then sleep "$GAP_SECS"; fi
    done
}

run_cell fanin_fifo_mutable   post_batch_fifo                  fan_in  1
run_cell fanin_fifo_maximal   post_batch_fifo_maximal          fan_in  1
run_cell fanin_fifo_inline    post_batch_fifo_maximal_inline   fan_in  1
run_cell fanout_fifo_mutable  post_batch_fifo                  fan_out 5000
run_cell fanout_fifo_maximal  post_batch_fifo_maximal          fan_out 5000
run_cell fanout_fifo_inline   post_batch_fifo_maximal_inline   fan_out 5000

echo
echo "==> aggregating"
{
    printf "%-25s | %3s | %8s | %8s | %8s\n" "scenario" "run" "tps" "p50_ms" "p99_ms"
    echo "--------------------------+-----+----------+----------+---------"
    for sub in fanin_fifo_mutable fanin_fifo_maximal fanin_fifo_inline fanout_fifo_mutable fanout_fifo_maximal fanout_fifo_inline; do
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
