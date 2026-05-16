#!/usr/bin/env bash
# M9.2 (acct-4d4n.21) — statistical runner: N sweep × replication ×
# IQR + sampler perturbation (spec §5.3).
#
# Env knobs:
#   POC_M92_DURATION  — seconds per run     (default 60)
#   POC_M92_RUNS      — replications per N  (default 5)
#   POC_M92_SETTLE    — settle gap secs     (default 30)
#   POC_M92_NS        — comma-separated N   (default 1,2,4,8,16,32,64,128,256)
#   POC_M92_PERT_N    — N for perturbation  (default 32)
#   POC_M92_OUTPUT_MD — markdown path
#   POC_M92_OUTPUT_JS — JSON path
#
# Usage:
#   bash bench/run-m9-2-statistical.sh                     # full sweep + perturbation
#   POC_M92_DURATION=15 POC_M92_RUNS=2 POC_M92_NS=4,16 \
#     bash bench/run-m9-2-statistical.sh                   # smoke
#   POC_M92_SKIP_PERT=1 bash bench/run-m9-2-statistical.sh # sweep only
set -euo pipefail

CRATE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$CRATE_DIR"

echo "==> M9.2 statistical sweep"
echo "    N=${POC_M92_NS:-1,2,4,8,16,32,64,128,256}  runs=${POC_M92_RUNS:-5}  duration=${POC_M92_DURATION:-60}s  settle=${POC_M92_SETTLE:-30}s"
echo

cargo test --release --test bench_m9_statistical m9_statistical_fan_in_sweep \
    --features pg18 --no-default-features \
    -- --ignored --nocapture

if [ "${POC_M92_SKIP_PERT:-0}" != "1" ]; then
    echo
    echo "==> M9.2 sampler perturbation cell"
    cargo test --release --test bench_m9_statistical m9_sampler_perturbation_cell \
        --features pg18 --no-default-features \
        -- --ignored --nocapture
fi
