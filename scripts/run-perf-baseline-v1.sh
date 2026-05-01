#!/usr/bin/env bash
set -euo pipefail

# Phase 1 → Slice A perf re-bench. Drives all 13 shapes (A–M) from
# perf_baseline_v0.md using the same methodology v0 used (3 runs × 5
# min per shape, vmstat sidecar) so v1 medians compare apples-to-apples
# to v0 medians and any regression from the 11 added migrations
# (0022–0032) shows up cleanly.
#
# Total wall clock: ~3.5 hours. Runs sequentially; do not run other
# heavy workloads on the box during this.
#
# Logs land under /tmp/perf_v1_<timestamp>/<shape>/. Headline summary
# is gathered into perf_baseline_v1_raw.txt at the end; the human-
# readable perf_baseline_v1.md (with diff-vs-v0 columns) is built by
# hand or by a separate aggregator from those logs.

cd "$(dirname "$0")/.."

OUT_ROOT="${PERF_V1_OUT:-/tmp/perf_v1_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$OUT_ROOT"
echo "==> Perf v1 re-bench"
echo "    output: $OUT_ROOT"
echo "    methodology: 3 runs × 300s per shape (matches v0)"
echo

run_shape() {
  local label="$1"; shift
  local shape_dir="$OUT_ROOT/$label"
  mkdir -p "$shape_dir"
  echo "============================================================"
  echo "== Shape $label  ($(date -Is))"
  echo "============================================================"
  # All env vars after the label are forwarded to run-perf-baseline.sh.
  env "$@" \
    PERF_V1_LOG_DIR_OVERRIDE="$shape_dir" \
    ./scripts/run-perf-baseline.sh 2>&1 | tee "$shape_dir/driver.log"
  echo
}

# Shapes A–E share one driver invocation (matches v0's "single
# scripts/run-perf-baseline.sh call with the default 5-config matrix").
echo "== Shapes A-E (shared-credit, deadlock-freedom probe)"
mkdir -p "$OUT_ROOT/ABCDE"
T4_DURATION_SECS=300 T4_BASELINE_RUNS=3 \
  ./scripts/run-perf-baseline.sh 2>&1 | tee "$OUT_ROOT/ABCDE/driver.log"
echo

# Shape F: cross-account spread, realistic shape
run_shape "F" \
  T4_BINARY=load_realistic_workload \
  T4_CONFIGS="100:5:20" \
  T4_DURATION_SECS=300 T4_BASELINE_RUNS=3

# Shape G: outbox + 1 drainer
run_shape "G" \
  T4_BINARY=load_outbox_workload \
  T4_CONFIGS="100:5:20" \
  T4_DURATION_SECS=300 T4_BASELINE_RUNS=3 \
  T4_DRAIN_TIMEOUT_S=60

# Shape H: reservation interleaving (70 posters + 30 reservers)
run_shape "H" \
  T4_BINARY=load_reservation_interleave \
  T4_CONFIGS="100:5:20" \
  T4_DURATION_SECS=300 T4_BASELINE_RUNS=3 \
  T4_RESERVE_PCT=30

# Shape I: qty + multi-currency value, 50/50 split
run_shape "I" \
  T4_BINARY=load_value_workload \
  T4_CONFIGS="100:5:20" \
  T4_DURATION_SECS=300 T4_BASELINE_RUNS=3 \
  T4_VALUE_PCT=50

# Shape J: super-batched outbox + 1 drainer
run_shape "J" \
  T4_BINARY=load_outbox_super_batch \
  T4_CONFIGS="100:5:20" \
  T4_DURATION_SECS=300 T4_BASELINE_RUNS=3 \
  T4_DRAIN_TIMEOUT_S=90

# Shape K: super-batched outbox + 4 drainers (regression case, kept for repro)
run_shape "K" \
  T4_BINARY=load_outbox_super_batch \
  T4_CONFIGS="100:5:20" \
  T4_DURATION_SECS=300 T4_BASELINE_RUNS=3 \
  T4_DRAIN_TIMEOUT_S=90 \
  T4_DRAINERS=4

# Shape L: pseudo-sync via LISTEN/NOTIFY (v0 throughput peak)
run_shape "L" \
  T4_BINARY=load_outbox_pseudo_sync \
  T4_CONFIGS="100:5:20" \
  T4_DURATION_SECS=300 T4_BASELINE_RUNS=3 \
  T4_DRAIN_TIMEOUT_S=30

# Shape M: async outbox + hard-cap back-pressure (kept for repro)
run_shape "M" \
  T4_BINARY=load_outbox_back_pressure \
  T4_CONFIGS="100:5:20" \
  T4_DURATION_SECS=300 T4_BASELINE_RUNS=3 \
  T4_DRAIN_TIMEOUT_S=30 \
  T4_BP_CAP=200 T4_BP_POLL_MS=2

echo "============================================================"
echo "== Done at $(date -Is)"
echo "============================================================"
echo "Logs: $OUT_ROOT"
echo "Each shape's headline medians are in its driver.log under"
echo "the 'Cross-config medians' section. Aggregate manually into"
echo "perf_baseline_v1.md with diff-vs-v0 columns."
