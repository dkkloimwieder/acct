#!/usr/bin/env bash
# Dense batch-size sweep on post_batch_append_only.
# Tests how throughput / latency scale past the previously-declared knee at 1K.
# Each size gets 3×60s replicates with 15s gaps (denser than P3's 5×60s sweep
# but covers more sizes in similar wall-time).

set -euo pipefail

cd "$(dirname "$0")/../../.."

OUT_DIR="${OUT_DIR:-/tmp/poc-batch-size-sweep}"
SIZES="${SIZES:-1000 2000 4000 8000 16000 32000}"
RUNS_PER_SIZE="${RUNS_PER_SIZE:-3}"
GAP_SECS="${GAP_SECS:-15}"
DURATION="${DURATION:-60}"
WORKERS="${WORKERS:-20}"
ACCOUNTS="${ACCOUNTS:-50}"

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

echo "==> sweep: sizes={$SIZES}, $RUNS_PER_SIZE × ${DURATION}s, gap=${GAP_SECS}s, workers=${WORKERS}"
echo "==> output: $OUT_DIR"

for size in $SIZES; do
    echo
    echo "===> batch_size=$size"
    mkdir -p "$OUT_DIR/batch_$size"
    for i in $(seq 1 "$RUNS_PER_SIZE"); do
        echo "--- run $i / $RUNS_PER_SIZE @ batch=$size"
        POC_BENCH_WORKERS="$WORKERS" \
        POC_BENCH_ACCOUNTS="$ACCOUNTS" \
        POC_BENCH_DURATION_SECS="$DURATION" \
        POC_BENCH_BATCH_SIZE="$size" \
            cargo test --manifest-path poc/batch-ledger/Cargo.toml \
            --release --test bench_p3_append_only -- \
            --ignored --nocapture --test-threads=1 p3_append_only_bench \
            2>&1 | tee "$OUT_DIR/batch_$size/run_$i.log"
        if [ "$i" -lt "$RUNS_PER_SIZE" ]; then
            sleep "$GAP_SECS"
        fi
    done
done

echo
echo "==> aggregating throughput per size"
{
    printf "%10s | %3s | %8s | %8s | %8s\n" "batch_size" "run" "tps" "p50_ms" "p99_ms"
    echo "-----------+-----+----------+----------+---------"
    for size in $SIZES; do
        i=1
        for log in "$OUT_DIR/batch_$size"/run_*.log; do
            tps=$(awk -F'[=/]' '/transfers=[0-9]+\.[0-9]+\/s/ {for(i=1;i<=NF;i++) if($i ~ /^transfers$/) {print $(i+1); exit}}' "$log")
            p50_us=$(awk '/batch-latency/ {match($0, /p50=([0-9]+)/, m); print m[1]; exit}' "$log")
            p99_us=$(awk '/batch-latency/ {match($0, /p99=([0-9]+)/, m); print m[1]; exit}' "$log")
            printf "%10s | %3d | %8.0f | %8d | %8d\n" "$size" "$i" "${tps:-0}" "$((${p50_us:-0}/1000))" "$((${p99_us:-0}/1000))"
            i=$((i+1))
        done
    done
} | tee "$OUT_DIR/summary.txt"
echo
echo "==> done. results in $OUT_DIR"
