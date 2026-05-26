#!/usr/bin/env bash
# §11.4 crossover matrix: drive every scenario (S1-S8) under all three
# submission modes (direct-per-call, direct-batched, routed) and emit one JSON
# report per (scenario, mode). P5 reads these to map the region where each Path C
# configuration wins — in particular whether routed pulls meaningfully ahead of
# direct-batched-per-caller on hot/deep pools, or standard-tx batching captures
# most of routing's benefit.
#
# Per scenario the universe is reseeded to that scenario's expected depth
# (s1-s4 shallow/receipt-only → depth 0; s5/s6 → depth 10; s7/s8 → depth 1000).
# The DSN is chosen per scenario (pooler for the 1000-caller s5-s8). Each run is
# HARD-timeout-wrapped.
#
# NOTE: reseeding a large universe (SEED_COUNT) to depth 1000 for s7/s8 writes
# SEED_COUNT×1000 layer rows — the slow part of the bake-off; size SEED_COUNT to
# your run budget.
#
# Usage: bash bench/run-crossover.sh [duration] [scenarios...]
#   bash bench/run-crossover.sh 30s              # all S1-S8, all modes
#   bash bench/run-crossover.sh 30s s5 s7        # subset

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

DURATION="${1:-30s}"
shift || true
SCENARIOS=("$@")
if [ "${#SCENARIOS[@]}" -eq 0 ]; then
    SCENARIOS=(s1 s2 s3 s4 s5 s6 s7 s8)
fi

SEED_COUNT="${SEED_COUNT:-10000}"
SEED_SKUS="${SEED_SKUS:-1000}"
SEED_LOCS="${SEED_LOCS:-10}"
BATCH_SIZE="${BATCH_SIZE:-50}"

depth_for() {
    case "$1" in
        s5|s6) echo 10 ;;
        s7|s8) echo 1000 ;;
        *)     echo 0 ;;
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
