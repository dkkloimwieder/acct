#!/usr/bin/env bash
# §11.1 cross-flavor equivalence sweep: for each scenario shape, replay an
# identical deterministic input sequence through direct-c and routed-c and
# assert the resulting pool_state aggregate qty matches exactly (provisional
# unit_cost may differ — reported, not failed). Prints one JSON verdict line per
# scenario; exits non-zero if any scenario FAILs.
#
# Runs against the direct DSN at low caller counts (the equivalence universe is
# small and the replay is deterministic — no pooler needed).
#
# Usage: bash bench/run-equivalence.sh [scenarios...]   (default s5 s7 s8)

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

SCENARIOS=("$@")
if [ "${#SCENARIOS[@]}" -eq 0 ]; then
    SCENARIOS=(s5 s7 s8)
fi
CALLERS="${EQUIV_CALLERS:-8}"
PER_CALLER="${EQUIV_PER_CALLER:-50}"
DEPTH="${EQUIV_DEPTH:-5}"

build_harness
restart_db

fail=0
for sc in "${SCENARIOS[@]}"; do
    echo "==> equivalence $sc"
    if ! harness --dsn "$DIRECT_DSN" equivalence \
        --scenario "$sc" \
        --callers "$CALLERS" \
        --submissions-per-caller "$PER_CALLER" \
        --method-mix all-fifo \
        --depth "$DEPTH"; then
        echo "   FAIL: $sc"
        fail=1
    fi
done

if [ "$fail" -ne 0 ]; then
    echo "==> equivalence sweep: at least one scenario FAILED" >&2
    exit 1
fi
echo "==> equivalence sweep: all scenarios PASS (aggregate qty identical across flavors)"
