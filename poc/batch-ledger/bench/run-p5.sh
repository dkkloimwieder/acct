#!/usr/bin/env bash
# acct-1hps (P5 of acct-qdp5 PoC) — FIFO batch-size sweep.

set -euo pipefail
cd "$(dirname "$0")/.."

TAG="${BENCH_TAG:-p5_fifo}"
RUNS="${BENCH_RUNS:-5}"
GAP="${BENCH_GAP_SECS:-30}"
DURATION="${POC_BENCH_DURATION_SECS:-60}"
WORKERS="${POC_BENCH_WORKERS:-20}"
POOLS="${POC_BENCH_POOLS:-20}"
BATCH_SIZES="${BENCH_BATCH_SIZES:-1 10 100 1000 8000}"
DB_URL="${POC_DATABASE_URL:-postgres://acct:acct_dev@localhost:5111/acct_poc}"

OUT_BASE="bench/results/${TAG}"
mkdir -p "$OUT_BASE"

export POC_BENCH_DURATION_SECS="$DURATION"
export POC_BENCH_WORKERS="$WORKERS"
export POC_BENCH_POOLS="$POOLS"
export POC_DATABASE_URL="$DB_URL"

META="${OUT_BASE}/env.txt"
{
    echo "=== bench tag: ${TAG} ==="
    echo "=== started: $(date -Iseconds) ==="
    echo "=== runs: ${RUNS}, duration: ${DURATION}s, gap: ${GAP}s ==="
    echo "=== workers: ${WORKERS}, pools: ${POOLS} ==="
    echo "=== batch sizes: ${BATCH_SIZES} ==="
    echo; uname -a; echo
    psql "$DB_URL" -At -c "SHOW server_version;" 2>/dev/null
} > "$META" 2>&1

cat "$META"

cargo test --manifest-path "$(pwd)/Cargo.toml" --release --no-run --test bench_p5_fifo \
    >/dev/null 2>&1

for B in $BATCH_SIZES; do
    OUT_DIR="${OUT_BASE}/batch_${B}"
    mkdir -p "$OUT_DIR"
    export POC_BENCH_BATCH_SIZE="$B"
    echo
    echo "[bench] === batch_size=${B}, ${RUNS} replicates ==="
    for i in $(seq 1 "$RUNS"); do
        echo "[bench] run $i / $RUNS @ batch=${B}"
        LOG="${OUT_DIR}/run_${i}.log"
        cargo test --manifest-path "$(pwd)/Cargo.toml" --release \
            --test bench_p5_fifo \
            -- --ignored --nocapture --test-threads=1 p5_fifo_bench \
            2>&1 | tee "$LOG" | tail -15
        if [ "$i" -lt "$RUNS" ]; then sleep "$GAP"; fi
    done

    SUMMARY="${OUT_DIR}/summary.txt"
    {
        echo "=== batch_size=${B} summary ==="
        printf "%-5s  %-13s  %-11s  %-9s  %-9s  %-9s  %-9s  %-6s\n" \
            "run" "transfers/s" "batches/s" "p50us" "p95us" "p99us" "p99.9us" "dl"
        for i in $(seq 1 "$RUNS"); do
            LOG="${OUT_DIR}/run_${i}.log"
            tps=$(grep -E "^throughput:" "$LOG" | sed -E 's/.*transfers=([0-9.]+).*/\1/' || echo "0")
            bps=$(grep -E "^throughput:" "$LOG" | sed -E 's/.*batches=([0-9.]+).*/\1/' || echo "0")
            p50=$(grep -E "^batch-latency" "$LOG" | sed -E 's/.*p50=([0-9]+).*/\1/' || echo "0")
            p95=$(grep -E "^batch-latency" "$LOG" | sed -E 's/.*p95=([0-9]+).*/\1/' || echo "0")
            p99=$(grep -E "^batch-latency" "$LOG" | sed -E 's/.*p99=([0-9]+) p99\.9.*/\1/' || echo "0")
            p999=$(grep -E "^batch-latency" "$LOG" | sed -E 's/.*p99\.9=([0-9]+).*/\1/' || echo "0")
            dl=$(grep -E "^deadlocks delta:" "$LOG" | awk '{print $3}' || echo "0")
            printf "%-5s  %-13s  %-11s  %-9s  %-9s  %-9s  %-9s  %-6s\n" \
                "$i" "$tps" "$bps" "$p50" "$p95" "$p99" "$p999" "$dl"
        done
    } | tee "$SUMMARY"
done

CROSS="${OUT_BASE}/cross_summary.txt"
{
    echo "=== cross-batch summary (median transfers/s, runs 2-N) ==="
    printf "%-10s  %-13s\n" "batch_size" "median tps"
    for B in $BATCH_SIZES; do
        TPS_LIST=$(for i in $(seq 2 "$RUNS"); do
            grep -E "^throughput:" "bench/results/${TAG}/batch_${B}/run_${i}.log" \
                | sed -E 's/.*transfers=([0-9.]+).*/\1/'
        done | sort -n)
        COUNT=$(echo "$TPS_LIST" | wc -l)
        MID=$(( (COUNT + 1) / 2 ))
        MEDIAN=$(echo "$TPS_LIST" | sed -n "${MID}p")
        printf "%-10s  %-13s\n" "$B" "$MEDIAN"
    done
} | tee "$CROSS"
echo
echo "[bench] results in ${OUT_BASE}/"
