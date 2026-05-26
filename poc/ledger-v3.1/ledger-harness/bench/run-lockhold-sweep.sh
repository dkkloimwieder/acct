#!/usr/bin/env bash
# §11.2 headline measurement: per-trx lock-hold time is CONSTANT w.r.t. pool
# depth. For depth ∈ {10, 100, 1000} this reseeds a FIFO universe to that depth
# and drives an identical simple-FIFO-depletion workload (direct, per-call),
# capturing per-trx ack latency. If the latency percentiles stay flat as depth
# grows 100×, the architectural premise holds; if they grow, it has failed.
#
# Low caller count (LOCKHOLD_CALLERS) isolates the per-trx critical section from
# cross-caller lock contention, so the only thing varying across runs is pool
# depth — exactly what §11.2 asks to measure. Uses the direct DSN (low
# concurrency; no pooler needed). Outputs land in results/lockhold-d<DEPTH>-*.json.
#
# Usage: bash bench/run-lockhold-sweep.sh [duration]   (default 30s)

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

DURATION="${1:-30s}"
LOCKHOLD_CALLERS="${LOCKHOLD_CALLERS:-16}"
SWEEP_COUNT="${SWEEP_COUNT:-256}"     # modest universe → fast reseed per depth
SWEEP_SKUS="${SWEEP_SKUS:-256}"
SCENARIO="${SCENARIO:-s7}"            # simple FIFO depletions, Zipfian overlap

build_harness
restart_db

for depth in 10 100 1000; do
    ts="$(date -u +%Y-%m-%dT%H-%M-%S)"
    out="$RESULTS_DIR/lockhold-d${depth}-${ts}.json"
    echo "==> lock-hold sweep: depth=$depth (reseed $SWEEP_COUNT fifo pools, ${LOCKHOLD_CALLERS} callers)"
    harness --dsn "$DIRECT_DSN" run \
        --scenario "$SCENARIO" \
        --mode direct-per-call \
        --duration "$DURATION" \
        --max-callers "$LOCKHOLD_CALLERS" \
        --depth "$depth" \
        --method-mix all-fifo \
        --seed-count "$SWEEP_COUNT" \
        --seed-skus "$SWEEP_SKUS" \
        --seed-locations 1 \
        --seed-depth "$depth" \
        --no-sampler \
        --output "$out"
done

echo "==> lock-hold sweep complete. Compare ack_latency_us across:"
ls -1 "$RESULTS_DIR"/lockhold-d*.json | tail -3
echo "    (flat p50/p99 across depths 10→1000 confirms the §11.2 premise)"
