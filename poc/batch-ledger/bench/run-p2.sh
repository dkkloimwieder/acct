#!/usr/bin/env bash
# acct-zdrm (P2 of acct-qdp5 PoC) — pgledger-equivalent calibration.
#
# Runs the P2 bench 5×60s with 30s gaps. Captures env metadata once at the
# start. Aggregates per-run output into a summary table.

set -euo pipefail
cd "$(dirname "$0")/.."

TAG="${BENCH_TAG:-p2_baseline}"
RUNS="${BENCH_RUNS:-5}"
GAP="${BENCH_GAP_SECS:-30}"
DURATION="${POC_BENCH_DURATION_SECS:-60}"
WORKERS="${POC_BENCH_WORKERS:-20}"
ACCOUNTS="${POC_BENCH_ACCOUNTS:-50}"
DB_URL="${POC_DATABASE_URL:-postgres://acct:acct_dev@localhost:5111/acct_poc}"
ADMIN_URL="${POC_ADMIN_URL:-postgres://acct:acct_dev@localhost:5111/postgres}"

OUT_DIR="bench/results/${TAG}"
mkdir -p "$OUT_DIR"

export POC_BENCH_DURATION_SECS="$DURATION"
export POC_BENCH_WORKERS="$WORKERS"
export POC_BENCH_ACCOUNTS="$ACCOUNTS"
export POC_DATABASE_URL="$DB_URL"

META="${OUT_DIR}/env.txt"
{
    echo "=== bench tag: ${TAG} ==="
    echo "=== started: $(date -Iseconds) ==="
    echo "=== runs: ${RUNS}, duration: ${DURATION}s, gap: ${GAP}s ==="
    echo "=== workers: ${WORKERS}, accounts: ${ACCOUNTS} ==="
    echo
    echo "=== uname -a ==="; uname -a
    echo
    echo "=== lscpu | head -25 ==="; lscpu | head -25
    echo
    echo "=== free -h ==="; free -h
    echo
    echo "=== Docker postgres container ==="
    docker ps --filter "name=postgres" --format "table {{.Names}}\t{{.Image}}\t{{.Status}}" 2>/dev/null || true
    echo
    echo "=== psql server_version + key settings ==="
    psql "$DB_URL" -At -c "SHOW server_version;" 2>/dev/null || true
    psql "$DB_URL" -c "
        SELECT name, setting FROM pg_settings
         WHERE name IN ('shared_buffers','wal_buffers','max_connections',
                        'io_method','synchronous_commit','wal_level',
                        'effective_io_concurrency','max_wal_size','min_wal_size',
                        'checkpoint_timeout','checkpoint_completion_target',
                        'fsync','full_page_writes')
         ORDER BY name;" 2>/dev/null || true
} > "$META" 2>&1

cat "$META"

# Build once.
echo "[bench] building release binary..."
cargo test --manifest-path "$(pwd)/Cargo.toml" --release --no-run --test bench_p2_pgledger \
    >/dev/null 2>&1

# Run replicates.
for i in $(seq 1 "$RUNS"); do
    echo
    echo "[bench] === run $i / $RUNS ==="
    LOG="${OUT_DIR}/run_${i}.log"
    cargo test --manifest-path "$(pwd)/Cargo.toml" --release \
        --test bench_p2_pgledger \
        -- --ignored --nocapture --test-threads=1 p2_pgledger_baseline_bench \
        2>&1 | tee "$LOG"
    if [ "$i" -lt "$RUNS" ]; then
        echo "[bench] sleeping ${GAP}s ..."
        sleep "$GAP"
    fi
done

SUMMARY="${OUT_DIR}/summary.txt"
{
    echo "=== aggregated summary: ${TAG} ==="
    echo
    printf "%-5s  %-12s  %-12s  %-12s  %-8s  %-8s  %-8s  %-10s  %-6s\n" \
        "run" "attempted/s" "ok/s" "err" "p50us" "p95us" "p99us" "p99.9us" "dl"
    for i in $(seq 1 "$RUNS"); do
        LOG="${OUT_DIR}/run_${i}.log"
        att=$(grep -E "^throughput: attempted=" "$LOG" | sed -E 's/.*attempted=([0-9.]+).*/\1/' || echo "0")
        okps=$(grep -E "^throughput:" "$LOG" | sed -E 's/.*ok=([0-9.]+).*/\1/' || echo "0")
        errs=$(grep -E "^err:" "$LOG" | awk '{print $2}' || echo "0")
        p50=$(grep -E "^latency \(us\):" "$LOG" | sed -E 's/.*p50=([0-9]+).*/\1/' || echo "0")
        p95=$(grep -E "^latency \(us\):" "$LOG" | sed -E 's/.*p95=([0-9]+).*/\1/' || echo "0")
        p99=$(grep -E "^latency \(us\):" "$LOG" | sed -E 's/.*p99=([0-9]+) p99.9.*/\1/' || echo "0")
        p999=$(grep -E "^latency \(us\):" "$LOG" | sed -E 's/.*p99\.9=([0-9]+).*/\1/' || echo "0")
        dl=$(grep -E "^deadlocks delta:" "$LOG" | awk '{print $3}' || echo "0")
        printf "%-5s  %-12s  %-12s  %-12s  %-8s  %-8s  %-8s  %-10s  %-6s\n" \
            "$i" "$att" "$okps" "$errs" "$p50" "$p95" "$p99" "$p999" "$dl"
    done
} | tee "$SUMMARY"
echo
echo "[bench] results in ${OUT_DIR}/"
