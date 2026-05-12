#!/usr/bin/env bash
# acct-sw4i M9 — bench validation. Compare post_batch_shmem against
# post_batch (UPDATE accounts) and post_batch_append_only (INSERT-only)
# at the fan-in and fan-out shapes used elsewhere in the PoC.
#
# Shapes match results-complicated-scenarios.md so the deltas are
# directly comparable.

set -euo pipefail

cd "$(dirname "$0")/../../.."

OUT_DIR="${OUT_DIR:-/tmp/poc-shmem-apply}"
RUNS="${RUNS:-3}"
GAP_SECS="${GAP_SECS:-15}"
DURATION="${DURATION:-60}"
WORKERS="${WORKERS:-20}"
BATCH_SIZE="${BATCH_SIZE:-1000}"

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

run_one() {
    local bench=$1 label=$2 fn=$3 accts=$4
    mkdir -p "$OUT_DIR/$label"
    echo
    echo "==> $label : bench=$bench fn=$fn accts=$accts"
    for i in $(seq 1 "$RUNS"); do
        echo "--- run $i / $RUNS"
        POC_BENCH_WORKERS="$WORKERS" \
        POC_BENCH_DURATION_SECS="$DURATION" \
        POC_BENCH_BATCH_SIZE="$BATCH_SIZE" \
        POC_BENCH_FUNCTION="$fn" \
        POC_BENCH_ACCOUNTS="$accts" \
        POC_BENCH_DEBIT_ACCOUNTS="$accts" \
            cargo test --manifest-path poc/batch-ledger/Cargo.toml \
            --release --test "$bench" -- \
            --ignored --nocapture --test-threads=1 \
            2>&1 | tee "$OUT_DIR/$label/run_$i.log"
        if [ "$i" -lt "$RUNS" ]; then sleep "$GAP_SECS"; fi
    done
}

# Fan-in: 50 debit accts, 1 hot credit.
run_one bench_fan_in  fanin_mutable     post_batch              50
run_one bench_fan_in  fanin_append_only post_batch_append_only  50
run_one bench_fan_in  fanin_shmem       post_batch_shmem        50

# Fan-out: 5000 accounts.
run_one bench_fan_out fanout_mutable     post_batch              5000
run_one bench_fan_out fanout_append_only post_batch_append_only  5000
run_one bench_fan_out fanout_shmem       post_batch_shmem        5000

echo
echo "==> aggregating"
{
    printf "%-25s | %3s | %8s | %8s | %8s\n" "scenario" "run" "tps" "p50_ms" "p99_ms"
    echo "--------------------------+-----+----------+----------+---------"
    for sub in fanin_mutable fanin_append_only fanin_shmem \
               fanout_mutable fanout_append_only fanout_shmem; do
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
