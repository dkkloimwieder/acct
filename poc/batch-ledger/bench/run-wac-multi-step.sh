#!/usr/bin/env bash
# Multi-step transactional perf — WAC dispatch under fan-in / fan-out shapes.
# Each batch = one txn containing N envelopes that hit a WAC pool.
# Fan-in: all envelopes target 1 pool (FOR UPDATE serializes; worst case).
# Fan-out: each envelope picks a unique pool from 5K (lowest contention).
# Compares pure receipt (no underflow risk), mixed 80/20 receipt+issue, mixed 50/50.

set -euo pipefail

cd "$(dirname "$0")/../../.."

OUT_DIR="${OUT_DIR:-/tmp/poc-wac-multi-step}"
RUNS="${RUNS:-3}"
GAP_SECS="${GAP_SECS:-15}"
DURATION="${DURATION:-60}"
WORKERS="${WORKERS:-20}"
BATCH_SIZE="${BATCH_SIZE:-1000}"

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

run_scenario() {
    local label=$1 shape=$2 pools=$3 issue_pct=$4
    mkdir -p "$OUT_DIR/$label"
    echo
    echo "==> $label : shape=$shape pools=$pools issue_pct=$issue_pct"
    for i in $(seq 1 "$RUNS"); do
        echo "--- run $i / $RUNS"
        POC_BENCH_WORKERS="$WORKERS" \
        POC_BENCH_POOLS="$pools" \
        POC_BENCH_DURATION_SECS="$DURATION" \
        POC_BENCH_BATCH_SIZE="$BATCH_SIZE" \
        POC_BENCH_ISSUE_PCT="$issue_pct" \
        POC_BENCH_SHAPE="$shape" \
            cargo test --manifest-path poc/batch-ledger/Cargo.toml \
            --release --test bench_wac_fan -- \
            --ignored --nocapture --test-threads=1 wac_fan_bench \
            2>&1 | tee "$OUT_DIR/$label/run_$i.log"
        if [ "$i" -lt "$RUNS" ]; then sleep "$GAP_SECS"; fi
    done
}

# Fan-in: 1 pool, all workers hammer it. FOR UPDATE serializes.
run_scenario fan_in_pure_receipt    fan_in  1    0
run_scenario fan_in_80r_20i         fan_in  1   20
run_scenario fan_in_50r_50i         fan_in  1   50

# Fan-out: 5K pools, low contention.
run_scenario fan_out_pure_receipt   fan_out 5000  0
run_scenario fan_out_80r_20i        fan_out 5000 20
run_scenario fan_out_50r_50i        fan_out 5000 50

echo
echo "==> aggregating"
{
    printf "%-25s | %3s | %8s | %8s | %8s\n" "scenario" "run" "tps" "p50_ms" "p99_ms"
    echo "--------------------------+-----+----------+----------+---------"
    for sub in fan_in_pure_receipt fan_in_80r_20i fan_in_50r_50i fan_out_pure_receipt fan_out_80r_20i fan_out_50r_50i; do
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
