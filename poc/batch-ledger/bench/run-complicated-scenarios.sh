#!/usr/bin/env bash
# Complicated-scenario sweep for sw4i signal validation.
# Three scenarios × two functions × 3 runs × 60s + sustained 10-min:
#   1. Fan-in single hot credit account (worst case for mutable balance UPDATE)
#   2. Fan-out 5000 accounts (low contention; baseline)
#   3. Sustained 10-min run at standard shape (catches checkpoint interference)

set -euo pipefail

cd "$(dirname "$0")/../../.."

OUT_DIR="${OUT_DIR:-/tmp/poc-complicated-scenarios}"
RUNS="${RUNS:-3}"
GAP_SECS="${GAP_SECS:-15}"
DURATION="${DURATION:-60}"
SUSTAINED_DURATION="${SUSTAINED_DURATION:-600}"
WORKERS="${WORKERS:-20}"
BATCH_SIZE="${BATCH_SIZE:-1000}"

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

run_one() {
    local bench=$1 fn=$2 dur=$3 accts=$4 outsub=$5
    mkdir -p "$OUT_DIR/$outsub"
    for i in $(seq 1 "$RUNS"); do
        echo "--- $bench / fn=$fn / run $i / $RUNS"
        POC_BENCH_WORKERS="$WORKERS" \
        POC_BENCH_ACCOUNTS="$accts" \
        POC_BENCH_DURATION_SECS="$dur" \
        POC_BENCH_BATCH_SIZE="$BATCH_SIZE" \
        POC_BENCH_FUNCTION="$fn" \
            cargo test --manifest-path poc/batch-ledger/Cargo.toml \
            --release --test "$bench" -- \
            --ignored --nocapture --test-threads=1 \
            2>&1 | tee "$OUT_DIR/$outsub/run_$i.log"
        if [ "$i" -lt "$RUNS" ]; then sleep "$GAP_SECS"; fi
    done
}

echo "==> Scenario 1: fan-in (mutable balance — worst case)"
run_one bench_fan_in post_batch "$DURATION" 50 fan_in_mutable

echo
echo "==> Scenario 1: fan-in (append-only — no UPDATE contention)"
run_one bench_fan_in post_batch_append_only "$DURATION" 50 fan_in_append_only

echo
echo "==> Scenario 2: fan-out (mutable balance, 5K accounts)"
run_one bench_fan_out post_batch "$DURATION" 5000 fan_out_mutable

echo
echo "==> Scenario 2: fan-out (append-only, 5K accounts)"
run_one bench_fan_out post_batch_append_only "$DURATION" 5000 fan_out_append_only

echo
echo "==> Scenario 3: sustained 10-min run (append-only, standard shape)"
mkdir -p "$OUT_DIR/sustained_10min"
POC_BENCH_WORKERS="$WORKERS" \
POC_BENCH_ACCOUNTS=50 \
POC_BENCH_DURATION_SECS="$SUSTAINED_DURATION" \
POC_BENCH_BATCH_SIZE="$BATCH_SIZE" \
    cargo test --manifest-path poc/batch-ledger/Cargo.toml \
    --release --test bench_p3_append_only -- \
    --ignored --nocapture --test-threads=1 p3_append_only_bench \
    2>&1 | tee "$OUT_DIR/sustained_10min/run_1.log"

echo
echo "==> aggregating"
{
    printf "%-30s | %3s | %8s | %8s | %8s\n" "scenario" "run" "tps" "p50_ms" "p99_ms"
    echo "-------------------------------+-----+----------+----------+---------"
    for sub in fan_in_mutable fan_in_append_only fan_out_mutable fan_out_append_only sustained_10min; do
        if [ ! -d "$OUT_DIR/$sub" ]; then continue; fi
        i=1
        for log in "$OUT_DIR/$sub"/run_*.log; do
            tps=$(awk '/transfers=[0-9]+\.[0-9]+\/s/ {match($0, /transfers=([0-9]+\.[0-9]+)/, m); print m[1]; exit}' "$log")
            p50_us=$(awk '/batch-latency/ {match($0, /p50=([0-9]+)/, m); print m[1]; exit}' "$log")
            p99_us=$(awk '/batch-latency/ {match($0, /p99=([0-9]+)/, m); print m[1]; exit}' "$log")
            printf "%-30s | %3d | %8.0f | %8d | %8d\n" "$sub" "$i" "${tps:-0}" "$((${p50_us:-0}/1000))" "$((${p99_us:-0}/1000))"
            i=$((i+1))
        done
    done
} | tee "$OUT_DIR/summary.txt"
echo
echo "==> done. results in $OUT_DIR"
