#!/usr/bin/env bash
# FIFO + specific costing benches at varying pool cardinality.
# Mirrors the simple-transfer and WAC fan-in/fan-out shapes for the remaining
# cost methods.
#
# FIFO:
#   pools=1     -> fan-in (all workers contend cost_layers on same pool)
#   pools=20    -> standard baseline (existing P5 shape)
#   pools=5000  -> fan-out (low contention; many distinct cost_layers chains)
#
# Specific:
#   pools=1     -> fan-in (one pool, workers partition unit_id space)
#   pools=20    -> standard baseline (existing P4spec shape)
#   pools=1000  -> fan-out (limited by per-pool unit pre-seed cost)

set -euo pipefail

cd "$(dirname "$0")/../../.."

OUT_DIR="${OUT_DIR:-/tmp/poc-fifo-specific-fan}"
RUNS="${RUNS:-3}"
GAP_SECS="${GAP_SECS:-15}"
DURATION="${DURATION:-60}"
WORKERS="${WORKERS:-20}"
BATCH_SIZE="${BATCH_SIZE:-1000}"

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

run_one() {
    local bench=$1 label=$2 pools=$3 extra_env=$4
    mkdir -p "$OUT_DIR/$label"
    echo
    echo "==> $label : bench=$bench pools=$pools"
    for i in $(seq 1 "$RUNS"); do
        echo "--- run $i / $RUNS"
        # shellcheck disable=SC2086
        env $extra_env \
        POC_BENCH_WORKERS="$WORKERS" \
        POC_BENCH_POOLS="$pools" \
        POC_BENCH_DURATION_SECS="$DURATION" \
        POC_BENCH_BATCH_SIZE="$BATCH_SIZE" \
            cargo test --manifest-path poc/batch-ledger/Cargo.toml \
            --release --test "$bench" -- \
            --ignored --nocapture --test-threads=1 \
            2>&1 | tee "$OUT_DIR/$label/run_$i.log"
        if [ "$i" -lt "$RUNS" ]; then sleep "$GAP_SECS"; fi
    done
}

# FIFO scenarios
run_one bench_p5_fifo fifo_pools_1     1    ""
run_one bench_p5_fifo fifo_pools_20    20   ""
run_one bench_p5_fifo fifo_pools_5000  5000 ""

# Specific scenarios. units_per_pool tuned down for higher-pool counts.
run_one bench_p4spec_specific specific_pools_1    1    "POC_BENCH_UNITS_PER_POOL=200000"
run_one bench_p4spec_specific specific_pools_20   20   "POC_BENCH_UNITS_PER_POOL=100000"
run_one bench_p4spec_specific specific_pools_1000 1000 "POC_BENCH_UNITS_PER_POOL=5000"

echo
echo "==> aggregating"
{
    printf "%-25s | %3s | %8s | %8s | %8s\n" "scenario" "run" "tps" "p50_ms" "p99_ms"
    echo "--------------------------+-----+----------+----------+---------"
    for sub in fifo_pools_1 fifo_pools_20 fifo_pools_5000 specific_pools_1 specific_pools_20 specific_pools_1000; do
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
