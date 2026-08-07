#!/usr/bin/env bash
# sweep-p1al-nonregress.sh — acct-p1al Phase 1, hot-pool / deep-pool non-regression.
#
# The decisive spread pass (sweep-p1al-decisive.sh) showed router_pack_disjoint=ON
# recovers the multi-SKU spread 2x. Before recommending flipping it ON by default
# we must confirm it does NOT regress the aggregation cases the issue names (s5/s7).
#
# Structural expectation: pack_disjoint fuses only pool-DISJOINT components, so a
# packed group still does one pool_lock + one aggregate UPSERT per pool — no new
# same-pool contention. On a single hot pool (s5) there is one component → inert.
# On deep zipf (s7) the tail fragments like the spread → packing should help or be
# neutral, never hurt.
#
# Runs through run-batch-size-sweep.sh (same harness + seed recipe + named-wait
# diagnostic that produced the existing s5/s19 pack OFF/ON CSVs).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SWEEP="$HERE/run-batch-size-sweep.sh"

export COMMITTERS=4 SIZES="50 200 800" REPS=3

run() { # $1=scenario $2=pack
  echo "[nonregress] $(date -u +%H:%M:%S) START scenario=$1 pack=$2"
  SCENARIO="$1" PACK="$2" bash "$SWEEP" \
    && echo "[nonregress] $(date -u +%H:%M:%S) DONE  scenario=$1 pack=$2" \
    || echo "[nonregress] $(date -u +%H:%M:%S) FAIL  scenario=$1 pack=$2 (continuing)"
}

run s5 on    # vs existing batchdiag_s5_cc4_packoff.csv (shallow depth-10 seed, fast)
run s7 off   # deep depth-1000 seed
run s7 on    # deep depth-1000 seed
echo "[nonregress] $(date -u +%H:%M:%S) non-regression sweep complete"
