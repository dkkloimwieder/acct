#!/usr/bin/env bash
# acct-0at4.10.4 (D) — statistics-discipline re-measurement of the crossover map.
#
# WHY THIS EXISTS
#   POC-REPORT §(b)'s crossover claims ("routed > direct on S5/S7/S8") and the A
#   pool-size sweep (acct-0at4.10.1) each reported ONE throughput number per
#   (scenario, mode) cell. One number on a noisy daily-driver host is not a
#   verdict — Chrome alone swings routed ~2x. A single run also can't say whether
#   a gap is real or sampling noise. This bake replaces each headline cell with a
#   DISTRIBUTION: N independent reps at distinct --seed values (acct-0at4.10.4's
#   Rust --seed makes the per-caller workload streams differ rep-to-rep, so reps
#   are genuinely independent samples — not replays of one fixed 0xDEADBEEF
#   stream), run in SHUFFLED order under the quiet-host gate. bench/stats.py then
#   reduces each cell to median +/- percentile-bootstrap 95% CI and tests
#   routed-vs-direct per scenario with Mann-Whitney U. The CI band — not a lone
#   median — is what the production decision consumes.
#
# DESIGN NOTES
#   * Cell order is shuffled WITHIN each scenario (the 10 = MODES x REPS runs go
#     out in random order) so a monotonic thermal/cache drift can't systematically
#     favor whichever mode always ran first. Scenarios are processed sequentially
#     because seeding a scenario TRUNCATEs every ledger table (--method-mix); a
#     mid-bake reseed would wipe a sibling scenario's universe. Cross-scenario
#     order is not a confound — each scenario compares only its own direct vs
#     routed.
#   * Reps within a cell share the rep's --seed across modes (seed = BASE + rep),
#     so direct-rep-r and routed-rep-r see comparable workload streams; the 5 reps
#     are the 5 samples the CI is built from.
#   * Universe is seeded ONCE per scenario via the DIRECT dsn (admin path; a tx
#     pooler can't carry the seed's DDL/large writes); the timed runs drive
#     through the pgbouncer POOLER dsn at the shipped default_pool_size, exactly
#     as §(b) / the A sweep measured. po_receipt is an inflow, so the aggregate
#     only grows across reps (O(1) per statement) — no depletion, reps stay valid.
#
# POSTURE (project_pocv3_bench_host_is_noisy_workstation)
#   Every timed run is gated on a quiet host (common.sh wait_for_quiet_host).
#   Absolute medians remain host-sensitive; the load-robust deliverables are the
#   WITHIN-scenario routed/direct comparison, the CI overlap, and the MWU p.
#
# USAGE / ENV
#   bash bench/crossover-stats.sh
#   SCENARIOS="s5 s2" REPS=5 DURATION=30s bash bench/crossover-stats.sh
#
#   SCENARIOS("s5 s7 s8 s2 s4"), MODES("direct-per-call routed"), REPS(5),
#   DURATION(30s), BASE_SEED(1000), METHOD(all-fifo),
#   SEED_COUNT(2000)/SEED_SKUS(500)/SEED_LOCS(10), SHIPPED_POOL(24),
#   OUTBASE(results/crossover-stats-<ts>) -> .csv (raw reps) + .md (stats report).

set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PGB_NAME="${PGBOUNCER_NAME:-poc-v3-1-pgbouncer}"

SCENARIOS_STR="${SCENARIOS:-s5 s7 s8 s2 s4}"
MODES="${MODES:-direct-per-call routed}"
REPS="${REPS:-5}"
DURATION="${DURATION:-30s}"
BASE_SEED="${BASE_SEED:-1000}"
METHOD="${METHOD:-all-fifo}"
SEED_COUNT="${SEED_COUNT:-2000}"
SEED_SKUS="${SEED_SKUS:-500}"
SEED_LOCS="${SEED_LOCS:-10}"
SHIPPED_POOL="${SHIPPED_POOL:-24}"

command -v python3 >/dev/null || { echo "FATAL: python3 required" >&2; exit 2; }
command -v shuf   >/dev/null || { echo "FATAL: shuf required"   >&2; exit 2; }

TS="$(date -u +%Y-%m-%dT%H-%M-%S)"
OUTBASE="${OUTBASE:-$RESULTS_DIR/crossover-stats-${TS}}"
CSV="$OUTBASE.csv"
MD="$OUTBASE.md"

# s5→depth 10, s7/s8→depth 1000; everything else shallow (design-v3.1 §11.4).
depth_for() { case "$1" in s5|s6) echo 10 ;; s7|s8|s9) echo 1000 ;; *) echo 0 ;; esac; }

# throughput errors duration — from a run report JSON.
extract() { python3 - "$1" <<'PY'
import json,sys
d=json.load(open(sys.argv[1]))
print(f"{d.get('throughput_trx_per_sec',0):.3f} {d.get('errors_total',0)} "
      f"{d.get('duration_secs',0):.1f}")
PY
}

echo "==> crossover statistics bake: scenarios=[$SCENARIOS_STR] modes=[$MODES] reps=$REPS dur=$DURATION"
echo "==> base_seed=$BASE_SEED method=$METHOD shipped_pool=$SHIPPED_POOL"
echo "==> host load: $(cat /proc/loadavg)"

build_harness
restart_db
assert_routed_gucs || { echo "FATAL: routed GUC drift — refusing to run" >&2; exit 2; }

# Confirm pgbouncer is at the shipped pool size so the cells match §(b). We do
# NOT recreate it here (that was the A sweep's job); just assert and warn.
pool_got="$(docker exec "$PGB_NAME" sh -c 'grep -iE "^\s*default_pool_size" /etc/pgbouncer/pgbouncer.ini' 2>/dev/null | grep -oE '[0-9]+' | head -1)"
if [ "${pool_got:-}" != "$SHIPPED_POOL" ]; then
    echo "    WARN: pgbouncer default_pool_size='$pool_got' (expected shipped $SHIPPED_POOL); cells may not match §(b)" >&2
else
    echo "    pgbouncer default_pool_size=$pool_got (shipped) ✓"
fi

echo "scenario,mode,seed,rep,throughput_trx_s,errors,duration_s" > "$CSV"

for sc in $SCENARIOS_STR; do
    depth="$(depth_for "$sc")"
    echo
    echo "############### scenario $sc (depth=$depth, method=$METHOD) ###############"
    # Seed this scenario's universe ONCE via the direct (admin) dsn. --method-mix
    # TRUNCATEs + reseeds, so this is also what isolates each scenario from the
    # previous one's universe. Timed reps below reuse this universe.
    echo "==> seed $SEED_COUNT pools depth=$depth via direct dsn"
    if ! harness --dsn "$DIRECT_DSN" run \
        --scenario "$sc" --mode direct-per-call --duration 1s --max-callers 1 \
        --depth "$depth" --method-mix "$METHOD" --seed-depth "$depth" \
        --seed-count "$SEED_COUNT" --seed-skus "$SEED_SKUS" --seed-locations "$SEED_LOCS" \
        --no-sampler --output "$RESULTS_DIR/.xover-seed-$sc.json" >/dev/null 2>&1; then
        echo "   FATAL: seed $sc failed — skipping scenario" >&2
        continue
    fi

    # Build the (mode, rep) work list for this scenario and shuffle it so no fixed
    # mode/rep order can alias onto a thermal drift.
    worklist=""
    for mode in $MODES; do
        for rep in $(seq 0 $((REPS - 1))); do
            worklist="$worklist$mode $rep"$'\n'
        done
    done
    shuffled="$(printf '%s' "$worklist" | grep -v '^$' | shuf)"

    printf '%-16s %-4s %-8s %-12s %-6s\n' "mode" "rep" "seed" "tput(trx/s)" "err"
    printf -- '------------------------------------------------\n'
    while read -r mode rep; do
        [ -z "$mode" ] && continue
        seed=$((BASE_SEED + rep))
        wait_for_quiet_host || true
        out="$RESULTS_DIR/.xover_${sc}_${mode}_r${rep}.json"
        if ! harness --dsn "$POOLER_DSN" run \
            --scenario "$sc" --mode "$mode" --duration "$DURATION" \
            --depth "$depth" --seed "$seed" \
            --no-sampler --output "$out" >/dev/null 2>&1; then
            echo "    FAIL $sc mode=$mode rep=$rep seed=$seed" >&2
            continue
        fi
        [ -f "$out" ] || { echo "    NO-OUTPUT $sc mode=$mode rep=$rep" >&2; continue; }
        read -r tput err dur <<<"$(extract "$out")"
        printf '%-16s %-4s %-8s %-12s %-6s\n' "$mode" "$rep" "$seed" "$tput" "$err"
        echo "$sc,$mode,$seed,$rep,$tput,$err,$dur" >> "$CSV"
    done <<<"$shuffled"
done

echo
echo "================ statistics (median +/- bootstrap CI, Mann-Whitney) ================"
python3 "$BENCH_DIR/stats.py" aggregate "$CSV" "$MD" \
    "scenarios=[$SCENARIOS_STR] modes=[$MODES] reps=$REPS dur=$DURATION method=$METHOD pool=$SHIPPED_POOL ts=$TS"

echo
echo "==> raw reps: $CSV"
echo "==> report:   $MD"
echo "==> host load: $(cat /proc/loadavg)"
