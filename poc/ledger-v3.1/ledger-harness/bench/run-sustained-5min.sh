#!/usr/bin/env bash
# run-sustained-5min.sh — one sustained routed workload with detailed perf stats.
#
# Produces:
#   * throughput-rate DISTRIBUTION over the run (quartiles of trx/s, and the
#     derived trx_line/s + posting_line/s) from 1 Hz sampling of the committer's
#     trx_committed counter — the harness reports only MEAN throughput, so the
#     per-interval quartiles are sampled here.
#   * caller latency percentiles (ack + committed, p50/p95/p99) from the harness.
#   * committer TIME BREAKDOWN (pool_lock / hydrate / apply / commit-fsync / prep,
#     + the e95d prep refold decode/xact/dedup) from the routed block span counters.
#   * wait-event segmentation (busy / on-CPU / row-lock / shmem-LWLock) from the
#     pg_stat_activity committer sampler.
#
# Workload: s4 "production-like stress" — 200 callers, zipf(1.2), complex
# (multi-line) receipts, mixed cost methods, shallow — at PRODUCTION GUC defaults
# (committer_count=4, batch_size_max=50, batch_window_us=500). 200 callers route
# via pgbouncer (:6432) per project posture (acct-uix8 / pgbouncer memory).
#
# Bench-only. Cluster touch (DROP/CREATE poc_v3_1 + restart acct-postgres).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/common.sh"

# Defaults reproduce the e95d apply-span baseline (measure-apply-spans.sh) but run
# sustained for DUR with the full stat set: cc=1, single pool, single-push (1
# caller), all-fifo simple receipts, batch_size_max=200, batch_window_us=20000.
# This is the committer-bound regime where batching / SPI-call changes show up; it
# is the baseline to measure those changes against. Override any knob (SCEN, CALLERS,
# COMMITTER_COUNT, BATCH_SIZE_MAX, …) for the more-complex follow-up runs.
SCEN="${SCEN:-s2}"
DUR="${DUR:-300}"                 # sustained window, seconds
METHOD="${METHOD:-all-fifo}"
DEPTH="${DEPTH:-0}"
SEED_COUNT="${SEED_COUNT:-1}"
SEED_SKUS="${SEED_SKUS:-1}"
SEED_LOCS="${SEED_LOCS:-1}"
MAX_CALLERS="${MAX_CALLERS:-1}"
COMMITTER_COUNT="${COMMITTER_COUNT:-1}"
BATCH_SIZE_MAX="${BATCH_SIZE_MAX:-200}"
BATCH_WINDOW_US="${BATCH_WINDOW_US:-20000}"
AFFINITY="${AFFINITY:-0}"
PACK_DISJOINT="${PACK_DISJOINT:-off}"   # router_pack_disjoint (acct-xdwk) — on|off
STAMP="${STAMP:-sustained}"
OUT="${OUT:-${RESULTS_DIR}/sustained_${SCEN}.json}"
SAMPLES="${RESULTS_DIR}/sustained_${SCEN}_rate.samples"
SNAP0="${RESULTS_DIR}/.sustained_snap0"
SNAP1="${RESULTS_DIR}/.sustained_snap1"
CSV="${CSV:-${RESULTS_DIR}/sustained_${SCEN}.csv}"
MD="${MD:-${RESULTS_DIR}/sustained_${SCEN}.md}"
RUNLOG="${RESULTS_DIR}/sustained_${SCEN}.runlog"
LOGF="${RESULTS_DIR}/sustained_${SCEN}.log"
log() { echo "[sustained] $*" | tee -a "$LOGF" >&2; }

psql_v3() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "$1" 2>/dev/null | tr -d '[:space:]'; }
asys()   { docker exec "$CONTAINER" psql -U acct -d postgres -tAc "ALTER SYSTEM SET ledger_routed_c.$1 = $2" >/dev/null; }
reload() { docker exec "$CONTAINER" psql -U acct -d postgres -tAc "SELECT pg_reload_conf()" >/dev/null; }

# Restore production GUC defaults on exit (committer_count takes effect on the next
# postmaster restart; the SIGHUP ones reload immediately).
restore_gucs() {
    asys committer_count 4 2>/dev/null || true
    asys batch_size_max 200 2>/dev/null || true
    asys batch_window_us 500 2>/dev/null || true
    asys affinity_scheme 0 2>/dev/null || true
    asys router_pack_disjoint on 2>/dev/null || true
    reload 2>/dev/null || true
}
trap restore_gucs EXIT

# One observability snapshot: trx_line + posting_line cumulative inserts (cheap
# stats counters, no seqscan) + the committer prep-refold sub-span ns counters.
# All committer span counters are CUMULATIVE since committer start, so the
# breakdown is computed from a start/end delta here (the harness routed block is
# cumulative too — it includes the seed/canary). count(*) on the line tables gives
# the authoritative lines-per-trx for the window. Column order is fixed; the
# Python post-processor indexes by position.
snapshot() {
    docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "SELECT
        ledger_routed_c_committer_trx_committed_total(),
        ledger_routed_c_committer_drains_total(),
        ledger_routed_c_committer_pool_lock_acquisitions_total(),
        ledger_routed_c_committer_aggregate_upserts_total(),
        ledger_routed_c_committer_txn_ns_total(),
        ledger_routed_c_committer_pipeline_ns_total(),
        ledger_routed_c_committer_pool_lock_ns_total(),
        ledger_routed_c_committer_hydrate_ns_total(),
        ledger_routed_c_committer_apply_ns_total(),
        ledger_routed_c_committer_decode_ns_total(),
        ledger_routed_c_committer_xact_ns_total(),
        ledger_routed_c_committer_dedup_ns_total(),
        (SELECT count(*) FROM trx_line),
        (SELECT count(*) FROM posting_line)" 2>/dev/null | tr -d '[:space:]' | tr '|' ','
}

# 1 Hz sampler of the committer trx_committed counter (shmem read — instant, no DB
# load). Per-second deltas give the throughput-rate distribution.
sample_loop() {
    : > "$SAMPLES"
    while :; do
        local v; v="$(psql_v3 'SELECT ledger_routed_c_committer_trx_committed_total()')"
        echo "$(date +%s).${v:-NA}" >> "$SAMPLES"
        sleep 1
    done
}

clean_seed() {
    log "clean: DROP/CREATE poc_v3_1 + migrations + CREATE EXTENSION + restart"
    docker exec "$CONTAINER" psql -U acct -d postgres -c "DROP DATABASE IF EXISTS poc_v3_1 WITH (FORCE)" >&2
    docker exec "$CONTAINER" psql -U acct -d postgres -c "CREATE DATABASE poc_v3_1" >&2
    ( cd "$WS_DIR" && DATABASE_URL="$DIRECT_DSN" sqlx migrate run --source db/migrations >&2 )
    docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_direct_c CASCADE" >&2 || true
    docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_routed_c CASCADE" >&2
    restart_db
    log "seed: $SEED_COUNT pools, $SEED_SKUS skus, $SEED_LOCS locs, method=$METHOD depth=$DEPTH"
    "$BIN" --dsn "$DIRECT_DSN" run --scenario "$SCEN" --mode direct-per-call --duration 1s \
        --max-callers 1 --depth "$DEPTH" --method-mix "$METHOD" \
        --seed-count "$SEED_COUNT" --seed-skus "$SEED_SKUS" --seed-locations "$SEED_LOCS" \
        --seed-depth "$DEPTH" --no-sampler --output "${RESULTS_DIR}/.sustained-seed.json" >/dev/null
}

# Committer-readiness canary: a short routed run must drain (trx_committed>0), else
# the arena/committer is wedged and the timed run would be all-zero.
committer_ready() {
    local cj="${RESULTS_DIR}/.sustained-canary.json" mat tries=0
    while [ "$tries" -le 2 ]; do
        timeout 120 "$BIN" --dsn "$POOLER_DSN" run --scenario "$SCEN" --mode routed \
            --duration 3s --max-callers 8 --depth "$DEPTH" --no-sampler --output "$cj" >/dev/null 2>&1 || true
        mat="$(grep -oE '"trx_committed_total"[[:space:]]*:[[:space:]]*[0-9]+' "$cj" 2>/dev/null | head -1 | grep -oE '[0-9]+$' || echo 0)"
        [ "${mat:-0}" -gt 0 ] && { log "committer canary ok (trx_committed_total=$mat)"; return 0; }
        tries=$((tries+1)); log "canary drained 0 (try $tries) — restarting"; restart_db
    done
    log "WARN: committer never drained — timed run may be all-zero"; return 1
}

build_harness
log "=== sustained ${DUR}s routed run: scenario=$SCEN method=$METHOD callers=$MAX_CALLERS via pgbouncer ==="
log "    config: committer_count=$COMMITTER_COUNT batch_size_max=$BATCH_SIZE_MAX batch_window_us=$BATCH_WINDOW_US affinity=$AFFINITY pack_disjoint=$PACK_DISJOINT (e95d apply-span baseline)"
psql "$POOLER_DSN" -c 'SELECT 1' >/dev/null 2>&1 || { log "pgbouncer :6432 not reachable — run setup-pgbouncer.sh"; exit 1; }
# Set the GUCs BEFORE clean_seed: its restart applies committer_count (read at
# _PG_init) and the SIGHUP knobs together.
asys committer_count "$COMMITTER_COUNT"; asys batch_size_max "$BATCH_SIZE_MAX"
asys batch_window_us "$BATCH_WINDOW_US"; asys affinity_scheme "$AFFINITY"
asys router_pack_disjoint "$PACK_DISJOINT"; reload
clean_seed
committer_ready || true

GUCS="$(psql_v3 "SELECT string_agg(replace(name,'ledger_routed_c.','')||'='||setting, ',' ORDER BY name) FROM pg_settings WHERE name IN ('ledger_routed_c.committer_count','ledger_routed_c.batch_size_max','ledger_routed_c.batch_window_us','ledger_routed_c.affinity_scheme','ledger_routed_c.router_pack_disjoint')")"
log "GUCs: $GUCS"

wait_for_quiet_host || log "NOTE: starting on a busy host (load1=$(host_load1))"
log "load1 at start: $(host_load1)"

snapshot > "$SNAP0"
log "launching harness (routed, ${DUR}s, sampler ON) + 1 Hz rate sampler"
"$BIN" --dsn "$POOLER_DSN" run --scenario "$SCEN" --mode routed --duration "${DUR}s" \
    --max-callers "$MAX_CALLERS" --depth "$DEPTH" --output "$OUT" > "$RUNLOG" 2>&1 &
HPID=$!
sample_loop & SPID=$!
wait "$HPID"; HRC=$?
kill "$SPID" 2>/dev/null || true; wait "$SPID" 2>/dev/null || true
snapshot > "$SNAP1"
log "harness exited rc=$HRC; load1 at end: $(host_load1)"
[ "$HRC" -eq 0 ] || { log "harness FAILED — see $RUNLOG"; tail -5 "$RUNLOG" >&2; exit 1; }

# ── post-process: quartiles + breakdown ─────────────────────────────
python3 - "$OUT" "$SAMPLES" "$SNAP0" "$SNAP1" "$CSV" "$MD" "$SCEN" "$DUR" "$GUCS" <<'PY' | tee -a "$LOGF" >&2
import json, sys, statistics as st
jpath, spath, snap0, snap1, csv, md, scen, dur, gucs = sys.argv[1:10]
dur = float(dur)
R = json.load(open(jpath))
rt = R.get("routed") or {}

# --- throughput-rate distribution (per-second trx deltas) ---
rows = []
for line in open(spath):
    line = line.strip()
    if not line or "NA" in line: continue
    t, v = line.split(".", 1)
    try: rows.append((int(t), int(v)))
    except ValueError: continue
rates = []
for (t0, v0), (t1, v1) in zip(rows, rows[1:]):
    dt = t1 - t0
    if dt > 0 and v1 >= v0:
        rates.append((v1 - v0) / dt)
# Trim ramp-up (first 3) and drain tail (trailing seconds at <10% of median).
if len(rates) > 12:
    core = rates[3:]
    med = st.median(core) if core else 0
    while core and core[-1] < 0.10 * med:
        core.pop()
    rates = core

def q(xs, p):
    if not xs: return 0.0
    xs = sorted(xs); k = (len(xs) - 1) * p
    f = int(k); c = min(f + 1, len(xs) - 1)
    return xs[f] + (xs[c] - xs[f]) * (k - f)

def quart(xs):
    return dict(n=len(xs), min=min(xs) if xs else 0, q1=q(xs,.25), median=q(xs,.50),
                q3=q(xs,.75), p95=q(xs,.95), max=max(xs) if xs else 0,
                mean=st.mean(xs) if xs else 0, stdev=st.pstdev(xs) if len(xs)>1 else 0)

trx_q = quart(rates)

# --- per-run deltas from the start/end snapshots (counters are cumulative) ---
def rd(p): return [int(x) for x in open(p).read().strip().split(",")]
s0, s1 = rd(snap0), rd(snap1)
d = [b - a for a, b in zip(s0, s1)]
(d_trx, d_drains, d_polacq, d_agg, d_txn, d_pipe, d_pollock, d_hyd, d_apply,
 d_decode, d_xact, d_dedup, d_trxline, d_posting) = d
d_commit = max(d_txn - d_pipe, 0)                              # BEGIN/COMMIT/fsync
d_prep   = max(d_pipe - (d_pollock + d_hyd + d_apply), 0)      # decode+triage+dedup+line-decode
d_prepoth = max(d_prep - (d_decode + d_xact + d_dedup), 0)

trx_basis = d_trx if d_trx > 0 else 1
ratio_line = d_trxline / trx_basis
ratio_post = d_posting / trx_basis
cg_avg = (d_trx / d_drains) if d_drains else 0.0

def scaled(qd, r):
    return {k: (v*r if k != "n" else v) for k, v in qd.items()}
trxline_q = scaled(trx_q, ratio_line)
post_q    = scaled(trx_q, ratio_post)

# --- committer time breakdown (per-run delta) ---
txn = d_txn or 1
def us(ns): return ns / 1e3 / trx_basis
spans = [("pool_lock", d_pollock), ("hydrate", d_hyd), ("apply", d_apply),
         ("commit(fsync)", d_commit), ("prep", d_prep)]
refold = [("prep.decode", d_decode), ("prep.xact", d_xact),
          ("prep.dedup", d_dedup), ("prep.other", d_prepoth)]

aw = R.get("ack_latency_us",{}); cw = R.get("committed_latency_us",{})
thr_window = d_trx / dur

def fnum(x): return f"{x:,.1f}"

out = []
out.append(f"# Sustained routed workload — {scen} ({dur:.0f}s)\n")
out.append(f"Workload: {R.get('scenario','?')} · mode={R.get('mode')} · GUCs: {gucs}\n")
out.append(f"Throughput (window delta): {thr_window:,.0f} trx/s · "
           f"commit_group_size_avg={cg_avg:.1f} · drains={d_drains:,} · trx_committed={d_trx:,}\n")
out.append(f"lines/trx: trx_line={ratio_line:.2f}, posting_line={ratio_post:.2f}\n")

out.append("\n## Throughput-rate distribution (per-second, n={} samples, ramp/drain trimmed)\n".format(trx_q["n"]))
out.append("| rate | min | Q1 | median | Q3 | p95 | max | mean | stdev |")
out.append("|---|--:|--:|--:|--:|--:|--:|--:|--:|")
for label, qd in (("trx/s", trx_q), ("trx_line/s", trxline_q), ("posting_line/s", post_q)):
    out.append("| {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
        label, *[fnum(qd[k]) for k in ("min","q1","median","q3","p95","max","mean","stdev")]))

out.append("\n## Caller latency (µs)\n")
out.append("| | p50 | p95 | p99 |")
out.append("|---|--:|--:|--:|")
out.append(f"| ack (enqueue) | {aw.get('p50',0):,} | {aw.get('p95',0):,} | {aw.get('p99',0):,} |")
out.append(f"| committed (end-to-end) | {cw.get('p50',0):,} | {cw.get('p95',0):,} | {cw.get('p99',0):,} |")

out.append("\n## Committer time breakdown (where wall-time went, per-run delta, summed over all committers)\n")
out.append(f"Total committer txn wall-time over the window: {txn/1e9:.2f}s across {trx_basis:,} trx "
           f"(committer_count from GUCs; µs/trx is summed committer wall-time ÷ trx).\n")
out.append("| span | seconds | % of txn | µs/trx |")
out.append("|---|--:|--:|--:|")
for name, ns in spans:
    out.append(f"| {name} | {ns/1e9:.2f} | {100*ns/txn:.1f}% | {us(ns):.2f} |")
out.append("\n_prep refold (acct-e95d):_\n")
out.append("| sub-span | seconds | % of txn | µs/trx |")
out.append("|---|--:|--:|--:|")
for name, ns in refold:
    out.append(f"| {name} | {ns/1e9:.2f} | {100*ns/txn:.1f}% | {us(ns):.2f} |")

out.append("\n## Wait-event segmentation (committer pg_stat_activity sampler)\n")
out.append(f"- samples: {rt.get('committer_samples_total',0):,} (idle {rt.get('committer_idle_samples',0):,})\n")
out.append(f"- busy_frac: {rt.get('committer_busy_frac',0):.3f}  (committer utilization; low ⇒ ceiling is upstream/ingress)\n")
out.append(f"- of busy — on-CPU: {rt.get('committer_running_frac_of_busy',0):.3f} · "
           f"row-lock: {rt.get('committer_lock_frac_of_busy',0):.3f} · "
           f"shmem-LWLock: {rt.get('committer_lwlock_frac_of_busy',0):.3f}\n")

resilience = (f"\n## Resilience counters\n"
    f"- pool_lock_acquisitions={d_polacq:,} (window) · aggregate_upserts={d_agg:,} (window)\n"
    f"- (cumulative since restart) dedup_skips={rt.get('dedup_skips_total',0):,} · "
    f"dropped={rt.get('dropped_submissions_total',0):,} · tx_failures={rt.get('tx_failures_total',0):,} · "
    f"poisoned={rt.get('poisoned_total',0):,} · deadlock_retries={rt.get('deadlock_retries_total',0):,} · "
    f"takeover={rt.get('takeover_count',0):,}\n")
out.append(resilience)

open(md, "w").write("\n".join(out) + "\n")

# CSV: the rate distribution + key scalars.
with open(csv, "w") as f:
    f.write("metric,min,q1,median,q3,p95,max,mean,stdev\n")
    for label, qd in (("trx_per_s", trx_q), ("trx_line_per_s", trxline_q), ("posting_line_per_s", post_q)):
        f.write(label + "," + ",".join(f"{qd[k]:.2f}" for k in ("min","q1","median","q3","p95","max","mean","stdev")) + "\n")

print("\n".join(out))
print(f"\n[written] {md}\n[written] {csv}")
PY

log "=== done. report: $MD  csv: $CSV  json: $OUT ==="
