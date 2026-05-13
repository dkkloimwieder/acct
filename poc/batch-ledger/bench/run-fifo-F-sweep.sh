#!/usr/bin/env bash
# acct-ylmo — FIFO maximal F (sub 4 / acct-0450) bench sweep.
#
# Compares F-shmem (post_batch_fifo_maximal_F; mig 0024 + sub 4 bgworker
# drain at MAX_LAYERS=256) against mutable + inline reference points
# under two workload shapes:
#
# **Pure-issue (issue_pct=100)** — F's optimal regime; ring stays at
# 5 layers throughout the bench (5 seeded + 0 receipts).
#
# **Mixed (issue_pct=70)** — realistic warehouse mix with bursty
# receipts. Fan-out only (fan-in concentrates all receipts on a single
# ring; at 20w × 60s × 30% receipts ≈ 7K receipts on one cell → cap
# overflow). Fan-out at 5000 pools distributes layer growth so peak
# per-pool stays under 256 across the 60s run.
#
# Anchors (1 rep): post_batch_fifo (mutable mig 0020) and
# post_batch_fifo_maximal_inline (mig 0023) at matching shapes for
# apples-to-apples compare on this PG / commit / dev box.

set -euo pipefail

cd "$(dirname "$0")/../../.."

OUT_DIR="${OUT_DIR:-/tmp/poc-ylmo-bench}"
F_RUNS="${F_RUNS:-3}"
ANCHOR_RUNS="${ANCHOR_RUNS:-1}"
GAP_SECS="${GAP_SECS:-15}"
DURATION="${DURATION:-60}"
WORKERS="${WORKERS:-20}"
BATCH_SIZE="${BATCH_SIZE:-1000}"

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

run_cell() {
    local label=$1 fn=$2 shape=$3 pools=$4 runs=$5 issue=$6 layer_qty=$7
    mkdir -p "$OUT_DIR/$label"
    echo
    echo "==> $label : fn=$fn shape=$shape pools=$pools issue=$issue layer_qty=$layer_qty runs=$runs"
    for i in $(seq 1 "$runs"); do
        echo "--- run $i / $runs"
        POC_BENCH_WORKERS="$WORKERS" \
        POC_BENCH_DURATION_SECS="$DURATION" \
        POC_BENCH_BATCH_SIZE="$BATCH_SIZE" \
        POC_BENCH_FUNCTION="$fn" \
        POC_BENCH_SHAPE="$shape" \
        POC_BENCH_POOLS="$pools" \
        POC_BENCH_ISSUE_PCT="$issue" \
        POC_BENCH_LAYER_QTY="$layer_qty" \
            cargo test --manifest-path poc/batch-ledger/Cargo.toml \
            --release --test bench_fifo_fan -- \
            --ignored --nocapture --test-threads=1 \
            2>&1 | tee "$OUT_DIR/$label/run_$i.log"
        if [ "$i" -lt "$runs" ]; then sleep "$GAP_SECS"; fi
    done
}

# Pure-issue layer qty: 1B/layer × 5 layers = 5B qty/pool. Issue avg
# qty 5; 20w × ~40 batches/s × 1000 envs × 60s × 100% issue = 48M
# issue-envelopes × 5 avg qty = 240M qty drain total. Spread across
# fan-out 5000 pools = 48K qty/pool; fan-in 1 pool = 240M qty/pool.
# At fan-in, 5B headroom comfortably outlasts.
PURE_QTY=1000000000
# Mixed-flow layer qty: 1M/layer is fine. At 70/30 fan-out, peak
# per-pool layer count stays around 100-200 over a 60s run — under
# the 256 cap.
MIX_QTY=1000000

# ── Pure-issue anchors ───────────────────────────────────────────
run_cell fanin_fifo_mutable_100    post_batch_fifo                  fan_in   1    "$ANCHOR_RUNS" 100 "$PURE_QTY"
run_cell fanin_fifo_inline_100     post_batch_fifo_maximal_inline   fan_in   1    "$ANCHOR_RUNS" 100 "$PURE_QTY"
run_cell fanout_fifo_mutable_100   post_batch_fifo                  fan_out  5000 "$ANCHOR_RUNS" 100 "$PURE_QTY"
run_cell fanout_fifo_inline_100    post_batch_fifo_maximal_inline   fan_out  5000 "$ANCHOR_RUNS" 100 "$PURE_QTY"

# ── F-shmem pure-issue (sub 4): optimal regime, 3 reps ───────────
run_cell fanin_fifo_F_100          post_batch_fifo_maximal_F        fan_in   1    "$F_RUNS"      100 "$PURE_QTY"
run_cell fanout_fifo_F_100         post_batch_fifo_maximal_F        fan_out  5000 "$F_RUNS"      100 "$PURE_QTY"

# ── Mixed-flow anchors (fan-out only) ────────────────────────────
run_cell fanout_fifo_mutable_70    post_batch_fifo                  fan_out  5000 "$ANCHOR_RUNS"  70 "$MIX_QTY"
run_cell fanout_fifo_inline_70     post_batch_fifo_maximal_inline   fan_out  5000 "$ANCHOR_RUNS"  70 "$MIX_QTY"

# ── F-shmem mixed (sub 4): realistic warehouse mix, 3 reps ───────
run_cell fanout_fifo_F_70          post_batch_fifo_maximal_F        fan_out  5000 "$F_RUNS"       70 "$MIX_QTY"

echo
echo "==> aggregating"
{
    printf "%-30s | %3s | %8s | %8s | %8s | %8s\n" "scenario" "run" "tps" "p50_ms" "p99_ms" "err"
    echo "-------------------------------+-----+----------+----------+----------+----------"
    for sub in \
        fanin_fifo_mutable_100 fanin_fifo_inline_100 fanin_fifo_F_100 \
        fanout_fifo_mutable_100 fanout_fifo_inline_100 fanout_fifo_F_100 \
        fanout_fifo_mutable_70 fanout_fifo_inline_70 fanout_fifo_F_70; do
        if [ ! -d "$OUT_DIR/$sub" ]; then continue; fi
        i=1
        for log in "$OUT_DIR/$sub"/run_*.log; do
            tps=$(awk '/transfers=[0-9]+\.[0-9]+\/s/ {match($0, /transfers=([0-9]+\.[0-9]+)/, m); print m[1]; exit}' "$log")
            p50_us=$(awk '/batch-latency/ {match($0, /p50=([0-9]+)/, m); print m[1]; exit}' "$log")
            p99_us=$(awk '/batch-latency/ {match($0, /p99=([0-9]+)/, m); print m[1]; exit}' "$log")
            err=$(awk '/batches_err:/ {print $2; exit}' "$log")
            printf "%-30s | %3d | %8.0f | %8d | %8d | %8s\n" "$sub" "$i" "${tps:-0}" "$((${p50_us:-0}/1000))" "$((${p99_us:-0}/1000))" "${err:-?}"
            i=$((i+1))
        done
    done
} | tee "$OUT_DIR/summary.txt"
echo
echo "==> done. results in $OUT_DIR"
