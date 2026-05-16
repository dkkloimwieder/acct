#!/usr/bin/env bash
# M9.3 (acct-4d4n.22) — GUC sweep + bottleneck classifier integration.
# Per spec §5.5: 18 GUC combos (3×3×2) × 2 shapes × {4,32,128} N × 5 runs.
# Total ≈ 108 cells. Wall-time ≈ 12.6h at the 5×60s + 30s settle methodology.
#
# Env knobs:
#   POC_M93_DURATION   — seconds per run               (default 60)
#   POC_M93_RUNS       — replications per cell         (default 5)
#   POC_M93_SETTLE     — settle gap secs               (default 30)
#   POC_M93_NS         — comma-separated N             (default 4,32,128)
#   POC_M93_BW         — comma-separated batch_window  (default 100,500,2000)
#   POC_M93_BS         — comma-separated batch_size    (default 64,1024,16384)
#   POC_M93_SC         — comma-separated 'on','off'    (default on,off)
#   POC_M93_SHAPES     — comma-separated names         (default fan_out,small_batch)
#   POC_M93_OUTPUT_MD  — markdown path
#   POC_M93_OUTPUT_JS  — JSON path
#
# Usage:
#   bash bench/run-m9-3-guc-sweep.sh                                 # full sweep
#   POC_M93_DURATION=10 POC_M93_RUNS=2 POC_M93_NS=4 \
#     POC_M93_BW=100,500 POC_M93_BS=64,1024 POC_M93_SC=on \
#     POC_M93_SHAPES=fan_out                                         # smoke
set -euo pipefail

CRATE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$CRATE_DIR"

echo "==> M9.3 GUC sweep"
echo "    shapes=${POC_M93_SHAPES:-fan_out,small_batch}  N=${POC_M93_NS:-4,32,128}  runs=${POC_M93_RUNS:-5}"
echo "    bw_us=${POC_M93_BW:-100,500,2000}  bs_max=${POC_M93_BS:-64,1024,16384}  sc=${POC_M93_SC:-on,off}"
echo "    per-run ${POC_M93_DURATION:-60}s  settle ${POC_M93_SETTLE:-30}s"
echo

cargo test --release --test bench_m9_guc_sweep m9_guc_sweep_all \
    --features pg18 --no-default-features \
    -- --ignored --nocapture
