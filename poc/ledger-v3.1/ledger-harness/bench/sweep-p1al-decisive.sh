#!/usr/bin/env bash
# sweep-p1al-decisive.sh — acct-p1al Phase 1, decisive 4-cell pass.
#
# Central question: does router_pack_disjoint=ON fuse the fragmented ~8-doc
# commit_groups on a multi-SKU spread back toward batch_size_max and recover the
# throughput that the committer-side write batching (acct-sczx/e95d/q6sx) was
# meant to deliver?
#
# All cells share the SAME spread: s2 (zipf), committer_count=4, 16 callers,
# 64 pools, batch_window_us=20000, affinity_scheme=0, all-fifo. The only knobs
# that move are router_pack_disjoint and batch_size_max. Re-baselines the OFF
# cell ourselves so every cell is mutually comparable on this host/seed.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="$HERE/run-sustained-5min.sh"
RDIR="$(cd "$HERE/../.." && pwd)/results"

# Fixed spread config for every cell.
export SCEN=s2 COMMITTER_COUNT=4 MAX_CALLERS=16 \
       SEED_COUNT=64 SEED_SKUS=64 SEED_LOCS=1 \
       BATCH_WINDOW_US=20000 AFFINITY=0 METHOD=all-fifo DUR=300

# cells: <tag> <pack_disjoint> <batch_size_max>
cells=(
  "off_bsm200 off 200"
  "on_bsm200  on  200"
  "on_bsm50   on  50"
  "on_bsm800  on  800"
)

echo "[sweep-p1al] $(date -u +%H:%M:%S) starting decisive 4-cell pass"
for c in "${cells[@]}"; do
  read -r tag pd bsm <<<"$c"
  base="$RDIR/p1al_s2_${tag}"
  echo "[sweep-p1al] $(date -u +%H:%M:%S) cell=$tag pack_disjoint=$pd batch_size_max=$bsm"
  PACK_DISJOINT="$pd" BATCH_SIZE_MAX="$bsm" \
    OUT="${base}.json" MD="${base}.md" CSV="${base}.csv" \
    bash "$RUNNER" \
    && echo "[sweep-p1al] $(date -u +%H:%M:%S) cell=$tag DONE -> ${base}.md" \
    || echo "[sweep-p1al] $(date -u +%H:%M:%S) cell=$tag FAILED (continuing)"
done
echo "[sweep-p1al] $(date -u +%H:%M:%S) sweep complete"
