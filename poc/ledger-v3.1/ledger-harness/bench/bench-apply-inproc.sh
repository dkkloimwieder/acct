#!/usr/bin/env bash
# bench-apply-inproc.sh — acct-q6sx: in-process apply-path microbench.
#
# Drives ledger_routed_c_bench_apply() (committer.rs, bench_hooks builds only):
# an in-backend timing harness that calls plan_and_write directly inside a
# rolled-back subtransaction, with NO ingress (no staging ring, no router, no
# pool_lock/hydrate in the timed region). This isolates the committer's
# single-core *apply* ceiling — the number the end-to-end harness can't reach
# because the staging LWLock starves committers as caller count rises (acct-ruex).
#
# It is the Docker-native counterpart to a pgrx #[pg_bench]: `cargo pgrx bench`
# would need a separate pgrx-managed cluster + hand-injected base schema, whereas
# this runs against the real poc_v3_1 with the live schema + seeded pools.
#
# PREREQ: the bench_hooks .so must be installed:
#     WITH_BENCH_HOOKS=1 bash poc/ledger-v3.1/scripts/install-routed-c.sh
# After benching, reinstall the clean production .so:
#     bash poc/ledger-v3.1/scripts/install-routed-c.sh
#
# Bench-only. Cluster touch (DROP/CREATE poc_v3_1 + restart acct-postgres).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/common.sh"

SCEN="${SCEN:-s2}"
ITERS="${ITERS:-200}"
WARMUP="${WARMUP:-30}"
# Commit-group sizes to sweep. 183 ≈ the measure-apply-spans default cg (RBATCH
# 200 → cg 181.8) where the span-measured apply ~44 us/trx was taken; 480 ≈ the
# cc=1 single-push ingress ceiling. The small sizes show fixed-batch-overhead
# amortization.
BATCHES="${BATCHES:-1 8 32 96 183 480}"
OUT="${OUT:-${RESULTS_DIR}/apply_inproc.csv}"
LOGF="${RESULTS_DIR}/apply_inproc.log"
log() { echo "[apply-inproc] $*" | tee -a "$LOGF" >&2; }

psql_raw() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "$1" 2>/dev/null; }
psql_v3()  { psql_raw "$1" | tr -d '[:space:]'; }

# Cheap pre-check: is the bench_hooks build installed? (Inspect the extension SQL
# in the container — no DB work needed.)
EXTDIR="$(docker exec "$CONTAINER" pg_config --sharedir)/extension"
if ! docker exec "$CONTAINER" grep -q 'ledger_routed_c_bench_apply' \
        "$EXTDIR/ledger_routed_c--0.0.1.sql" 2>/dev/null; then
    log "ledger_routed_c_bench_apply NOT in the installed extension SQL."
    log "Install the bench build first:"
    log "    WITH_BENCH_HOOKS=1 bash poc/ledger-v3.1/scripts/install-routed-c.sh"
    exit 1
fi

clean_seed() {
    docker exec "$CONTAINER" psql -U acct -d postgres -c "DROP DATABASE IF EXISTS poc_v3_1 WITH (FORCE)" >&2
    docker exec "$CONTAINER" psql -U acct -d postgres -c "CREATE DATABASE poc_v3_1" >&2
    ( cd "$WS_DIR" && DATABASE_URL="$DIRECT_DSN" sqlx migrate run --source db/migrations >&2 )
    docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_direct_c CASCADE" >&2 || true
    docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_routed_c CASCADE" >&2
    restart_db
    # One direct-per-call receipt to materialize a fifo pool + its
    # posting_account_map (the apply path needs both). all-fifo matches the
    # method the span-measured apply ~44 us/trx was taken on.
    "$BIN" --dsn "$DIRECT_DSN" run --scenario "$SCEN" --mode direct-per-call --duration 1s \
        --max-callers 1 --method-mix all-fifo --seed-count 1 --seed-skus 1 --seed-locations 1 \
        --seed-depth 0 --no-sampler --output "${RESULTS_DIR}/.apply-inproc-seed.json" >/dev/null
}

build_harness
log "=== in-process apply microbench (acct-q6sx): iters=$ITERS warmup=$WARMUP batches=[$BATCHES] ==="
clean_seed

POOL="$(psql_v3 "SELECT id FROM pool WHERE method='fifo' ORDER BY id LIMIT 1")"
if [ -z "$POOL" ]; then
    log "no fifo pool was seeded — aborting"; exit 1
fi
log "seeded fifo pool_id=$POOL"

echo "batch,committed,us_per_trx,us_per_iter_mean,iter_ns_min,iter_ns_p50,iter_ns_mean,iter_ns_p99,iter_ns_max,iter_ns_stddev" > "$OUT"
printf '%8s %10s %11s %14s %10s %10s\n' batch committed 'us/trx' 'us/iter_mean' 'p50_us' 'p99_us' | tee -a "$LOGF" >&2

for b in $BATCHES; do
    wait_for_quiet_host || log "  NOTE: batch=$b on a busy host"
    json="$(psql_raw "SELECT ledger_routed_c_bench_apply($POOL, $b, $ITERS, $WARMUP)::text")"
    if [ -z "$json" ]; then log "  batch=$b: empty result"; continue; fi
    python3 - "$b" "$json" "$OUT" <<'PY' | tee -a "$LOGF" >&2
import json, sys
b = int(sys.argv[1]); d = json.loads(sys.argv[2]); out = sys.argv[3]
if "error" in d:
    print(f"  batch={b}: ERROR {d['error']}"); sys.exit(0)
ns = d["iter_ns"]
row = [b, d["committed_per_iter"], d["us_per_trx"], d["us_per_iter_mean"],
       ns["min"], ns["p50"], ns["mean"], ns["p99"], ns["max"], ns["stddev"]]
with open(out, "a") as f:
    f.write(",".join(f"{x:.3f}" if isinstance(x, float) else str(x) for x in row) + "\n")
print(f"{b:8d} {d['committed_per_iter']:10d} {d['us_per_trx']:11.2f} "
      f"{d['us_per_iter_mean']:12.1f} {ns['p50']/1e3:12.1f} {ns['p99']/1e3:12.1f}")
PY
done

log "=== done. CSV: $OUT (committed must == batch; us_per_trx at large batch ≈ span apply ~44) ==="
