#!/usr/bin/env bash
# M9.1 (acct-4d4n.20) — workload generators for the 6 bake-off shapes
# (spec §5.2). Thin shell wrapper around the Rust `bench_m9_workloads`
# tokio harness.
#
# Args (env):
#   POC_M9_N         — backend count        (default 4)
#   POC_M9_DURATION  — seconds per shape   (default 60)
#   POC_M9_SHAPE     — name to run alone   (default: all 6)
#   POC_M9_OUTPUT    — markdown output path (default bench/results-m91-shapes.md)
#
# Examples:
#   bash bench/run-m9-1-workloads.sh
#   POC_M9_DURATION=15 bash bench/run-m9-1-workloads.sh
#   POC_M9_SHAPE=fan_in bash bench/run-m9-1-workloads.sh
set -euo pipefail

CRATE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$CRATE_DIR"

echo "==> M9.1 workload-generator harness"
echo "    N=${POC_M9_N:-4}  duration=${POC_M9_DURATION:-60}s  shape=${POC_M9_SHAPE:-all}"
echo

cargo test --release --test bench_m9_workloads m9_all_shapes \
    --features pg18 --no-default-features \
    -- --ignored --nocapture
