#!/usr/bin/env bash
# Routed lever ablations (acct-0at4.9 deliverables C + D):
#   C. batch_size_max ∈ {1, 50, 200, 1000, ∞}  — maps the commit-group
#      amortization curve. At N in-flight callers the committer forms
#      ⌈N/batch_size_max⌉ commit groups (fsyncs) per drained wave; throughput
#      rises with batch_size_max until it saturates at the in-flight depth.
#   D. router_pack_disjoint on/off — whether the router only packs pool-disjoint
#      trx into a commit group (avoiding intra-group pool contention) or packs
#      regardless. Under Zipf overlap the two trade batch size against
#      contention; this measures which wins.
#
# WHY RESTART PER VALUE (not pg_reload_conf)
#   Both are GucContext::Sighup GUCs, but the routed committer/router BGWorkers
#   do NOT pick up a SIGHUP live (empirically: pg_reload_conf leaves throughput
#   flat; ALTER SYSTEM + restart yields batch=1 → ~1450 vs batch=200 → ~5400).
#   So each value is ALTER SYSTEM SET + `docker restart` — the BGWorker reads
#   the GUC at init from postgresql.auto.conf, and the restart also gives each
#   value a clean shmem-queue slate. ALTER SYSTEM persists across restart; the
#   seeded pool universe lives in the data volume and survives restarts.
#
# POSTURE
#   Noisy workstation: absolute rates swing with background load. Report the
#   STRUCTURAL shape (the batch curve, the pack delta) and same-invocation
#   comparisons; full statistical rigor is acct-0at4.10.
#
# USAGE / ENV
#   bash batch-pack-ablation.sh
#   CALLERS=200 DURATION=10s bash batch-pack-ablation.sh
#
#   DSN, SCENARIO(s2), CALLERS(200), DURATION(10s),
#   BATCH_VALUES("1 50 200 1000 100000"), PACK_VALUES("on off"),
#   SEED_COUNT(2000)/SEED_SKUS(500)/SEED_LOCATIONS(10), CONTAINER(acct-postgres),
#   OUTFILE(results/batch-pack-ablation-<ts>.json)

set -uo pipefail

DSN="${DSN:-postgres://acct:acct_dev@localhost:5111/poc_v3_1}"
SCENARIO="${SCENARIO:-s2}"
CALLERS="${CALLERS:-200}"
DURATION="${DURATION:-10s}"
BATCH_VALUES="${BATCH_VALUES:-1 50 200 1000 100000}"
PACK_VALUES="${PACK_VALUES:-on off}"
SEED_COUNT="${SEED_COUNT:-2000}"
SEED_SKUS="${SEED_SKUS:-500}"
SEED_LOCATIONS="${SEED_LOCATIONS:-10}"
CONTAINER="${CONTAINER:-acct-postgres}"

WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$WORKSPACE_DIR"
BIN="$WORKSPACE_DIR/target/release/ledger-harness"
TS="$(date -u +%Y-%m-%dT%H-%M-%S)"
OUTFILE="${OUTFILE:-$WORKSPACE_DIR/results/batch-pack-ablation-${TS}.json}"
DBNAME="${DSN##*/}"

command -v jq >/dev/null || { echo "FATAL: jq required" >&2; exit 2; }
[ -x "$BIN" ] || { echo "FATAL: harness not built ($BIN)" >&2; exit 2; }

pg_wait() { for _ in $(seq 1 90); do docker exec "$CONTAINER" pg_isready -U acct -q >/dev/null 2>&1 && return 0; sleep 1; done; return 1; }
setsys()  { docker exec "$CONTAINER" psql -U acct -d "$DBNAME" -qtAc "ALTER SYSTEM SET $1 = $2;" >/dev/null 2>&1; }
restart() { docker restart "$CONTAINER" >/dev/null && pg_wait; }
runtput() { "$BIN" run --scenario "$SCENARIO" --mode routed --duration "$DURATION" \
                --no-sampler --max-callers "$CALLERS" --dsn "$DSN" 2>/dev/null | tail -1; }

echo "==> routed ablations: scenario=$SCENARIO callers=$CALLERS duration=$DURATION"

# Fresh universe once (survives the per-value restarts).
echo "==> seed universe (all-wac, $SEED_COUNT pools)"
setsys ledger_routed_c.batch_size_max 200
setsys ledger_routed_c.router_pack_disjoint on
restart || { echo "FATAL: restart failed" >&2; exit 2; }
"$BIN" run --scenario "$SCENARIO" --mode routed --method-mix all-wac \
    --seed-count "$SEED_COUNT" --seed-skus "$SEED_SKUS" --seed-locations "$SEED_LOCATIONS" \
    --duration 2s --no-sampler --max-callers "$CALLERS" --dsn "$DSN" >/dev/null 2>&1 \
    || { echo "FATAL: seed failed" >&2; exit 2; }

rows="[]"
record() { # axis value throughput
    rows=$(echo "$rows" | jq --arg a "$1" --arg v "$2" --argjson t "${3:-null}" \
        '. + [{axis:$a, value:$v, throughput_trx_per_sec:$t}]')
}

echo
echo "== C. batch_size_max sweep (router_pack_disjoint=on) =="
printf '%-16s %-16s %s\n' "batch_size_max" "effective" "throughput(trx/s)"
printf -- '------------------------------------------------------\n'
setsys ledger_routed_c.router_pack_disjoint on
for v in $BATCH_VALUES; do
    setsys ledger_routed_c.batch_size_max "$v"; restart
    eff=$(docker exec "$CONTAINER" psql -U acct -d "$DBNAME" -tAc "SHOW ledger_routed_c.batch_size_max;" 2>/dev/null)
    line=$(runtput); t=$(echo "$line" | jq -r '.throughput_trx_per_sec // "null"')
    printf '%-16s %-16s %s\n' "$v" "$eff" "$t"
    record "batch_size_max" "$v" "$t"
done

echo
echo "== D. router_pack_disjoint toggle (batch_size_max=200) =="
printf '%-16s %-16s %s\n' "pack_disjoint" "effective" "throughput(trx/s)"
printf -- '------------------------------------------------------\n'
setsys ledger_routed_c.batch_size_max 200
for v in $PACK_VALUES; do
    setsys ledger_routed_c.router_pack_disjoint "$v"; restart
    eff=$(docker exec "$CONTAINER" psql -U acct -d "$DBNAME" -tAc "SHOW ledger_routed_c.router_pack_disjoint;" 2>/dev/null)
    line=$(runtput); t=$(echo "$line" | jq -r '.throughput_trx_per_sec // "null"')
    printf '%-16s %-16s %s\n' "$v" "$eff" "$t"
    record "router_pack_disjoint" "$v" "$t"
done

# Restore defaults.
setsys ledger_routed_c.batch_size_max 200
setsys ledger_routed_c.router_pack_disjoint on
restart

mkdir -p "$(dirname "$OUTFILE")"
jq -n --arg ts "$TS" --arg scenario "$SCENARIO" --argjson callers "$CALLERS" \
    --arg duration "$DURATION" --arg dsn "$DSN" --argjson steps "$rows" \
    '{measurement:"routed_batch_pack_ablation", ts:$ts, scenario:$scenario, callers:$callers, duration:$duration, dsn:$dsn, note:"ALTER SYSTEM + restart per value (Sighup GUC not picked up live by BGWorker); synchronous_commit=on", steps:$steps}' \
    > "$OUTFILE"
echo
echo "==> results: $OUTFILE"
