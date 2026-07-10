#!/usr/bin/env bash
# acct-0at4.10.1 (A) — pgbouncer pool-size confound sweep (FEEDBACK-TESTING #12).
#
# WHY THIS EXISTS
#   The §11.4 crossover map (POC-REPORT §(b)) measured S5/S7/S8 direct-vs-routed
#   throughput at ONE, unstated pgbouncer default_pool_size (64). The 1000-caller
#   scenarios multiplex onto default_pool_size server backends, so the DIRECT
#   flavor's contention is shaped by that pool size — not by 1000-way lock
#   queuing — while routed's fire-and-forget enqueue barely touches it. The
#   confound is ASYMMETRIC: shrinking the pool throttles direct far more than
#   routed, which FLATTERS routed. A single pool size therefore can't support the
#   crossover claim "routed > direct on S5/S7/S8".
#
#   This sweep re-measures the crossover across default_pool_size ∈ {25,50,100,
#   200} and reports, per scenario, whether routed stays ahead at every pool size
#   (crossover STABLE) or whether the winner flips somewhere (crossover as a
#   function of pool size). The pool size sits next to every number so the
#   POC-REPORT §(b)/§(c) claim is no longer measured at one arbitrary point.
#
# WHAT IT DRIVES
#   For each scenario S5/S7/S8 the universe is seeded ONCE to that scenario's
#   depth (s5→10, s7/s8→1000) via the DIRECT dsn (admin path; a transaction
#   pooler can't carry the seed's DDL/large writes). Then for each pool size we
#   recreate the pgbouncer container at that size and, from a clean committer
#   slate (docker restart), drive {direct-per-call, routed} through the POOLER
#   dsn. direct-per-call is the pool-size-sensitive flavor — the one the confound
#   acts on; routed is the challenger. Metric is achieved throughput_trx_per_sec
#   (+ errors) — the same axis the crossover map compares.
#
# POSTURE (noisy daily-driver host, project_pocv3_bench_host_is_noisy_workstation)
#   Absolute throughput swings with background load (Chrome ~2x). The load-robust
#   deliverable is the routed/direct RATIO measured back-to-back within a cell and
#   its STABILITY across pool sizes — reported alongside absolutes. Each timed run
#   is gated on a quiet host (common.sh wait_for_quiet_host). Full statistical
#   rigor (≥5 seeded reps, CIs, Mann-Whitney) is acct-0at4.10.4 (D), not here.
#
# USAGE / ENV
#   bash bench/pgbouncer-poolsize-sweep.sh
#   POOLS="25 100" SCENARIOS="s5 s7" DURATION=20s SEED_COUNT=2000 \
#       bash bench/pgbouncer-poolsize-sweep.sh
#
#   POOLS("25 50 100 200"), SCENARIOS("s5 s7 s8"), MODES("direct-per-call routed"),
#   DURATION(30s), SEED_COUNT(2000)/SEED_SKUS(500)/SEED_LOCS(10),
#   RESTORE_POOL(24 — pgbouncer size left running at exit),
#   OUTFILE(results/pgbouncer-poolsize-sweep-<ts>.json)

set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PGB_NAME="${PGBOUNCER_NAME:-poc-v3-1-pgbouncer}"

POOLS="${POOLS:-25 50 100 200}"
SCENARIOS_STR="${SCENARIOS:-s5 s7 s8}"
MODES="${MODES:-direct-per-call routed}"
DURATION="${DURATION:-30s}"
SEED_COUNT="${SEED_COUNT:-2000}"
SEED_SKUS="${SEED_SKUS:-500}"
SEED_LOCS="${SEED_LOCS:-10}"
RESTORE_POOL="${RESTORE_POOL:-24}"

TS="$(date -u +%Y-%m-%dT%H-%M-%S)"
OUTFILE="${OUTFILE:-$RESULTS_DIR/pgbouncer-poolsize-sweep-${TS}.json}"

command -v jq >/dev/null || { echo "FATAL: jq required" >&2; exit 2; }
command -v python3 >/dev/null || { echo "FATAL: python3 required" >&2; exit 2; }

# s5→depth 10, s7/s8→depth 1000 (design-v3.1 §11.4 / run-crossover.sh).
depth_for() { case "$1" in s5|s6) echo 10 ;; s7|s8|s9) echo 1000 ;; *) echo 0 ;; esac; }

# Recreate pgbouncer at the requested default_pool_size. `up` on an existing
# container only `docker start`s it (keeping the old baked-in DEFAULT_POOL_SIZE
# env), so we MUST `down` (rm -f) first, then `up` with the new env. Verify the
# effective size from the generated ini and warn loud on drift.
pgbouncer_set() {
    local size="$1"
    DEFAULT_POOL_SIZE="$size" bash "$BENCH_DIR/setup-pgbouncer.sh" down >/dev/null 2>&1 || true
    DEFAULT_POOL_SIZE="$size" bash "$BENCH_DIR/setup-pgbouncer.sh" up   >/dev/null 2>&1
    local got
    got="$(docker exec "$PGB_NAME" sh -c 'grep -iE "^\s*default_pool_size" /etc/pgbouncer/pgbouncer.ini' 2>/dev/null | grep -oE '[0-9]+' | head -1)"
    if [ "$got" != "$size" ]; then
        echo "    WARN: pgbouncer default_pool_size effective='$got' wanted='$size'" >&2
    fi
}

# throughput errors attempts trx_committed dropped — from the run report JSON.
# direct modes serialize `routed: null`, so coalesce to {} before .get.
extract() { python3 - "$1" <<'PY'
import json,sys
d=json.load(open(sys.argv[1]))
r=d.get('routed') or {}
print(f"{d.get('throughput_trx_per_sec',0):.1f} {d.get('errors_total',0)} "
      f"{d.get('attempts_total',0)} {r.get('trx_committed_total',0)} "
      f"{r.get('dropped_submissions_total',0)}")
PY
}

echo "==> pgbouncer pool-size sweep: scenarios=[$SCENARIOS_STR] pools=[$POOLS] modes=[$MODES] dur=$DURATION seed=$SEED_COUNT"
echo "==> host load: $(cat /proc/loadavg)"

build_harness
restart_db
assert_routed_gucs || { echo "FATAL: routed GUC drift — refusing to run" >&2; exit 2; }

rows="[]"
record() { # scenario pool mode tput errors attempts trx dropped
    rows=$(echo "$rows" | jq \
        --arg sc "$1" --argjson pool "$2" --arg mode "$3" --argjson tput "$4" \
        --argjson err "$5" --argjson att "$6" --argjson trx "$7" --argjson drop "$8" \
        '. + [{scenario:$sc, pool_size:$pool, mode:$mode, throughput_trx_per_sec:$tput,
               errors_total:$err, attempts_total:$att, trx_committed:$trx, dropped:$drop}]')
}

for sc in $SCENARIOS_STR; do
    depth="$(depth_for "$sc")"
    echo
    echo "############### scenario $sc (depth=$depth, method=all-fifo) ###############"
    # Seed this scenario's universe ONCE via the direct (admin) dsn; it survives
    # the per-cell docker restarts (data volume). Deep pools + huge per-layer
    # units mean the back-to-back timed cells can't run the aggregates dry.
    echo "==> seed $SEED_COUNT pools depth=$depth (direct dsn)"
    harness --dsn "$DIRECT_DSN" run \
        --scenario "$sc" --mode direct-per-call --duration 1s --max-callers 1 \
        --depth "$depth" --method-mix all-fifo --seed-depth "$depth" \
        --seed-count "$SEED_COUNT" --seed-skus "$SEED_SKUS" --seed-locations "$SEED_LOCS" \
        --no-sampler --output "$RESULTS_DIR/.seed-$sc.json" >/dev/null 2>&1 \
        || { echo "   FATAL: seed $sc failed" >&2; continue; }

    printf '%-6s %-5s %-16s %-12s %-8s %-10s %-8s\n' \
        "pool" "sc" "mode" "tput(trx/s)" "err" "trx_cmt" "drop"
    printf -- '--------------------------------------------------------------------------\n'
    for pool in $POOLS; do
        # Clean committer/shmem for this pool cell, THEN bring pgbouncer up at the
        # target size against a running server (fresh server-side conns).
        restart_db >/dev/null
        pgbouncer_set "$pool"
        assert_routed_gucs >/dev/null 2>&1 || echo "    WARN: routed GUC drift at pool=$pool" >&2
        for mode in $MODES; do
            wait_for_quiet_host || true
            out="$RESULTS_DIR/.poolsweep_${sc}_p${pool}_${mode}.json"
            harness --dsn "$POOLER_DSN" run \
                --scenario "$sc" --mode "$mode" --duration "$DURATION" \
                --depth "$depth" --no-sampler --output "$out" >/dev/null 2>&1 \
                || { echo "    FAIL $sc pool=$pool mode=$mode"; continue; }
            [ -f "$out" ] || { echo "    NO-OUTPUT $sc pool=$pool mode=$mode"; continue; }
            read -r tput err att trx drop <<<"$(extract "$out")"
            printf '%-6s %-5s %-16s %-12s %-8s %-10s %-8s\n' \
                "$pool" "$sc" "$mode" "$tput" "$err" "$trx" "$drop"
            record "$sc" "$pool" "$mode" "$tput" "$err" "$att" "$trx" "$drop"
        done
    done
done

# Restore pgbouncer to the shared default so later benches see the usual pool.
pgbouncer_set "$RESTORE_POOL"

mkdir -p "$(dirname "$OUTFILE")"
jq -n --arg ts "$TS" --arg scenarios "$SCENARIOS_STR" --arg pools "$POOLS" \
    --arg modes "$MODES" --arg dur "$DURATION" --argjson seed "$SEED_COUNT" \
    --argjson steps "$rows" \
    '{measurement:"pgbouncer_poolsize_crossover_sweep", ts:$ts, scenarios:$scenarios,
      pools:$pools, modes:$modes, duration:$dur, seed_count:$seed,
      metric:"throughput_trx_per_sec (achieved), closed-loop; pgbouncer recreated + committer restarted per pool cell; synchronous_commit=on",
      steps:$steps}' > "$OUTFILE"

echo
echo "================ crossover stability (routed / direct-per-call) ================"
# Per (scenario,pool): direct-per-call vs routed throughput, ratio, winner. A
# STABLE crossover keeps routed>direct (ratio>1, winner=routed) at every pool.
python3 - "$OUTFILE" <<'PY'
import json,sys
d=json.load(open(sys.argv[1]))
steps=d['steps']
by={}
for s in steps:
    by.setdefault((s['scenario'],s['pool_size']),{})[s['mode']]=s['throughput_trx_per_sec']
print(f"{'sc':<5}{'pool':>6}{'direct':>12}{'routed':>12}{'routed/direct':>16}{'winner':>10}")
print("-"*61)
cur=None
for (sc,pool) in sorted(by, key=lambda k:(k[0],k[1])):
    if sc!=cur: cur=sc
    m=by[(sc,pool)]
    dr=m.get('direct-per-call'); ro=m.get('routed')
    if dr and ro and dr>0:
        ratio=ro/dr; win='routed' if ro>dr else 'direct'
        print(f"{sc:<5}{pool:>6}{dr:>12.1f}{ro:>12.1f}{ratio:>16.2f}{win:>10}")
    else:
        print(f"{sc:<5}{pool:>6}{(dr or 0):>12.1f}{(ro or 0):>12.1f}{'n/a':>16}{'n/a':>10}")
print()
# Stability verdict per scenario: does routed win at EVERY measured pool?
for sc in sorted({k[0] for k in by}):
    ratios=[]
    for (s,p) in sorted(by):
        if s!=sc: continue
        m=by[(s,p)]; dr=m.get('direct-per-call'); ro=m.get('routed')
        if dr and ro and dr>0: ratios.append((p,ro/dr))
    if not ratios: continue
    allwin=all(r>1 for _,r in ratios)
    lo=min(r for _,r in ratios); hi=max(r for _,r in ratios)
    verdict="STABLE (routed wins at every pool)" if allwin else "FLIPS with pool size"
    print(f"{sc}: {verdict}; routed/direct ∈ [{lo:.2f}, {hi:.2f}] over pools {[p for p,_ in ratios]}")
PY

echo
echo "==> results: $OUTFILE"
echo "==> host load: $(cat /proc/loadavg)"
