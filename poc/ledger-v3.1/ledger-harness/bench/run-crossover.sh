#!/usr/bin/env bash
# §11.4 crossover matrix: drive every scenario (S1-S21) under all three
# submission modes (direct-per-call, direct-batched, routed) and emit one JSON
# report per (scenario, mode). P5 reads these to map the region where each Path C
# configuration wins — in particular whether routed pulls meaningfully ahead of
# direct-batched-per-caller on hot/deep pools, or standard-tx batching captures
# most of routing's benefit.
#
# Per scenario the universe is reseeded to that scenario's expected depth
# (s1-s4 shallow/receipt-only → depth 0; s5/s6 → depth 10; s7/s8/s9 → depth
# 1000; s10-s21 Pareto family — receipts (s10/s12/s13) → depth 0, deplete
# variants (s14/s16-s18/s20-s21) → depth 10, high-vol (s11/s15/s19) → depth
# 100). The DSN is chosen per scenario (pooler for the 1000-caller s5-s9).
# s9 is s8's shape with multi-touch enabled (acct-34ce) — the coalesce-under-
# load case. s10-s21 are the Pareto 80/20 receipts/builds/mixed family block
# (acct-s90k). Each run is HARD-timeout-wrapped.
#
# NOTE: reseeding a large universe (SEED_COUNT) to depth 1000 for s7/s8 writes
# SEED_COUNT×1000 layer rows — the slow part of the bake-off; size SEED_COUNT to
# your run budget.
#
# Usage: bash bench/run-crossover.sh [duration] [scenarios...]
#   bash bench/run-crossover.sh 30s              # all S1-S21, all modes
#   bash bench/run-crossover.sh 30s s5 s7        # subset
#   bash bench/run-crossover.sh 30s s10 s14 s18  # Pareto family smoke

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

DURATION="${1:-30s}"
shift || true
SCENARIOS=("$@")
if [ "${#SCENARIOS[@]}" -eq 0 ]; then
    SCENARIOS=(s1 s2 s3 s4 s5 s6 s7 s8 s9 \
               s10 s11 s12 s13 \
               s14 s15 s16 s17 \
               s18 s19 s20 s21)
fi

SEED_COUNT="${SEED_COUNT:-10000}"
SEED_SKUS="${SEED_SKUS:-1000}"
SEED_LOCS="${SEED_LOCS:-10}"
BATCH_SIZE="${BATCH_SIZE:-50}"

depth_for() {
    case "$1" in
        s5|s6)                         echo 10 ;;
        s7|s8|s9)                      echo 1000 ;;
        s11|s15|s19)                   echo 100 ;;
        s14|s16|s17|s18|s20|s21)       echo 10 ;;
        *)                             echo 0 ;;
    esac
}
method_for() {
    case "$1" in
        s1|s2) echo all-wac ;;
        s3|s4) echo mixed ;;
        *)     echo all-fifo ;;
    esac
}

build_harness
restart_db

for sc in "${SCENARIOS[@]}"; do
    depth="$(depth_for "$sc")"
    method="$(method_for "$sc")"
    dsn="$(dsn_for_scenario "$sc")"
    echo "============================================================"
    echo "== scenario $sc (depth=$depth, method=$method, dsn=${dsn##*@})"
    echo "============================================================"

    # Reseed once for this scenario; the three modes share the seeded universe.
    echo "==> reseed $SEED_COUNT pools mix=$method depth=$depth"
    harness --dsn "$DIRECT_DSN" run \
        --scenario "$sc" --mode direct-per-call --duration 1s \
        --max-callers 1 --depth "$depth" \
        --method-mix "$method" --seed-count "$SEED_COUNT" \
        --seed-skus "$SEED_SKUS" --seed-locations "$SEED_LOCS" --seed-depth "$depth" \
        --no-sampler --output "$RESULTS_DIR/.seed-warmup-$sc.json" >/dev/null || true

    for mode in direct-per-call direct-batched routed; do
        ts="$(date -u +%Y-%m-%dT%H-%M-%S)"
        out="$RESULTS_DIR/${sc}-${mode}-${ts}.json"
        echo "==> $sc [$mode]"
        harness --dsn "$dsn" run \
            --scenario "$sc" --mode "$mode" --duration "$DURATION" \
            --batch-size "$BATCH_SIZE" --depth "$depth" \
            --output "$out" || echo "   (run $sc/$mode failed or timed out)"
    done
done

echo "==> crossover matrix complete. Reports in $RESULTS_DIR/"
