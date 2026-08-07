#!/usr/bin/env bash
# Wake-latency ablation — axis F (acct-0at4.15) + committer wake (acct-0at4.16).
#
# WHAT IT PRODUCES
#   The materialization-latency A/B that FEEDBACK #13 asks for, extended to the
#   FULL enqueue->materialize chain. The chain has two tick-cadence legs:
#     enqueue -> router      floored by the router's 50 ms wait_latch tick
#     router  -> committer   floored by the committer's 50 ms wait_latch tick
#   Two GUCs each collapse one leg toward wake-signal latency:
#     ledger_routed_c.wake_on_enqueue          (F)  SetLatch router on enqueue
#     ledger_routed_c.wake_committer_on_publish (this) SetLatch committers on
#                                                  commit_group publish
#   Metric is committed_latency (enqueue -> observed materialize), the full
#   pipeline surface — not ack. The harness reports it under open-loop pacing
#   (--target-rate, absolute-schedule arrivals, single-push --batch-size 1), so
#   latency is charged from INTENDED send time (no coordinated omission,
#   acct-0at4.8).
#
#   Three CUMULATIVE wake modes × batch_window_us ∈ {0, 500}:
#     off      both GUCs off. Both legs fall to their 50 ms tick — committed p50
#              sits near the two-tick floor (~60 ms).
#     enqueue  wake_on_enqueue on only (axis F). Closes enqueue->router; the
#              router->committer tick remains, so committed p50 halves to the
#              residual ~26-34 ms — HALF the chain (acct-0at4.15's finding).
#     both     both on. Closes the residual router->committer leg too; at
#              window=0 committed p50 should approach the pipeline+fsync floor
#              (single-digit ms) — the acct-0at4.16 result.
#   window=0 is the PURE wake floor (coalesce gate disabled). window=500 is the
#   SHIPPED default: the coalesce gate defers a lone group until its oldest member
#   ages past 500 µs, so a solitary submission still falls to a tick even wake-on
#   (documented, not fixed in this PoC); under a steady stream the wakes keep both
#   workers hot so the window — not the ticks — governs.
#
# WHY RESTART PER ARM (not pg_reload_conf)
#   wake_on_enqueue is read by the enqueue backend (a regular backend); batch_
#   window_us and wake_committer_on_publish are read by the router BGWorker. Both
#   BGWorker GUCs are Sighup, but routed Sighup GUCs are not reliably adopted live
#   by the BGWorkers (acct-0at4.9). A restart makes all three unambiguous — the
#   router re-reads them at init and fresh harness connections read wake_on_enqueue
#   — and gives each arm a clean shmem-queue + committer slate. ALTER SYSTEM
#   persists across restart; the seeded pool universe lives in the data volume.
#
# POSTURE
#   Noisy workstation: absolute latencies swing with background load. The
#   STRUCTURAL result — the off/enqueue/both committed-p50 collapse at window=0,
#   measured back-to-back — is the load-robust finding. Full statistical rigor is
#   acct-0at4.10. Routed is a characterized-but-superseded alternative (design-v3.1
#   §18); this closes its residual wake tick, it does not change the chosen path.
#
# USAGE / ENV
#   bash wake-ablation.sh
#   RATES="50 200" SCENARIO=s2 CALLERS=64 DUR=20s bash wake-ablation.sh
#
#   DSN, SCENARIO(s2), CALLERS(64), DUR(20s), RATES("50 200 800"),
#   WINDOWS("0 500"), WAKE_MODES("off enqueue both"),
#   SEED_COUNT(2000)/SEED_SKUS(500)/SEED_LOCS(10), CONTAINER(acct-postgres),
#   OUTFILE(results/wake-ablation-<ts>.json)

set -uo pipefail

DSN="${DSN:-postgres://acct:acct_dev@localhost:5111/poc_v3_1}"
SCENARIO="${SCENARIO:-s2}"
CALLERS="${CALLERS:-64}"
DUR="${DUR:-20s}"
RATES="${RATES:-50 200 800}"
WINDOWS="${WINDOWS:-0 500}"
WAKE_MODES="${WAKE_MODES:-off enqueue both}"
SEED_COUNT="${SEED_COUNT:-2000}"
SEED_SKUS="${SEED_SKUS:-500}"
SEED_LOCS="${SEED_LOCS:-10}"
CONTAINER="${CONTAINER:-acct-postgres}"

WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$WORKSPACE_DIR"
BIN="$WORKSPACE_DIR/target/release/ledger-harness"
TS="$(date -u +%Y-%m-%dT%H-%M-%S)"
OUTFILE="${OUTFILE:-$WORKSPACE_DIR/results/wake-ablation-${TS}.json}"
DBNAME="${DSN##*/}"

command -v jq >/dev/null || { echo "FATAL: jq required" >&2; exit 2; }
command -v python3 >/dev/null || { echo "FATAL: python3 required" >&2; exit 2; }
[ -x "$BIN" ] || { echo "FATAL: harness not built ($BIN)" >&2; exit 2; }

pg_wait() { for _ in $(seq 1 90); do docker exec "$CONTAINER" pg_isready -U acct -q >/dev/null 2>&1 && return 0; sleep 1; done; return 1; }
setsys()  { docker exec "$CONTAINER" psql -U acct -d "$DBNAME" -qtAc "ALTER SYSTEM SET $1 = $2;" >/dev/null 2>&1; }
restart() { docker restart "$CONTAINER" >/dev/null && pg_wait && sleep 3; }

# Map a cumulative wake mode to its (wake_on_enqueue, wake_committer_on_publish)
# GUC pair. off = neither leg woken; enqueue = only the enqueue->router leg
# (axis F); both = also the router->committer leg (acct-0at4.16).
mode_gucs() { case "$1" in
    off)     echo "off off" ;;
    enqueue) echo "on  off" ;;
    both)    echo "on  on"  ;;
    *)       echo "off off" ;;
esac; }

# committed p50/p99/p999 + ack p50/p99 + achieved tput + trx + drops from the
# nested --output JSON (LatencyPercentiles serialized as {p50,p95,p99,p999}).
extract() { python3 - "$1" <<'PY'
import json,sys
d=json.load(open(sys.argv[1]))
c=d['committed_latency_us']; a=d['ack_latency_us']; r=d.get('routed',{})
print(f"{d.get('throughput_trx_per_sec',0):.0f} {c['p50']} {c['p99']} {c.get('p999',c['p99'])} "
      f"{a['p50']} {a['p99']} {r.get('trx_committed_total',0)} {r.get('dropped_submissions_total',0)}")
PY
}

echo "==> wake ablation: scenario=$SCENARIO callers=$CALLERS dur=$DUR rates=[$RATES] windows=[$WINDOWS] modes=[$WAKE_MODES]"
echo "==> host load: $(cat /proc/loadavg)"

# Seed the universe once (all-wac keeps committer work O(1) so committed latency
# reflects the pipeline/tick, not cost compute). Survives the per-arm restarts.
echo "==> seed universe (all-wac, $SEED_COUNT pools)"
setsys ledger_routed_c.wake_on_enqueue off
setsys ledger_routed_c.wake_committer_on_publish off
setsys ledger_routed_c.batch_window_us 500
restart || { echo "FATAL: restart failed" >&2; exit 2; }
"$BIN" --dsn "$DSN" run --scenario "$SCENARIO" --mode routed --method-mix all-wac \
    --seed-count "$SEED_COUNT" --seed-skus "$SEED_SKUS" --seed-locations "$SEED_LOCS" \
    --duration 2s --no-sampler --max-callers "$CALLERS" >/dev/null 2>&1 \
    || { echo "FATAL: seed failed" >&2; exit 2; }

rows="[]"
record() { # window mode we wc rate tput cp50 cp99 cp999 ap50 ap99 trx drop
    rows=$(echo "$rows" | jq \
        --argjson w "$1" --arg mode "$2" --arg we "$3" --arg wc "$4" --argjson rate "$5" \
        --argjson tput "$6" --argjson cp50 "$7" --argjson cp99 "$8" --argjson cp999 "$9" \
        --argjson ap50 "${10}" --argjson ap99 "${11}" --argjson trx "${12}" --argjson drop "${13}" \
        '. + [{batch_window_us:$w, wake_mode:$mode, wake_on_enqueue:$we, wake_committer_on_publish:$wc,
               offered_rate:$rate, achieved_tput:$tput,
               committed_p50_us:$cp50, committed_p99_us:$cp99, committed_p999_us:$cp999,
               ack_p50_us:$ap50, ack_p99_us:$ap99, trx:$trx, dropped:$drop}]')
}

for win in $WINDOWS; do
    echo
    echo "================ batch_window_us=$win ================"
    printf '%-9s %-5s %-8s %-12s %-12s %-12s %-10s %-8s\n' \
        "mode" "rate" "tput" "cmt_p50(ms)" "cmt_p99(ms)" "cmt_p999(ms)" "ack_p50(ms)" "drop"
    printf -- '-------------------------------------------------------------------------------------\n'
    for mode in $WAKE_MODES; do
        read -r we wc <<<"$(mode_gucs "$mode")"
        setsys ledger_routed_c.batch_window_us "$win"
        setsys ledger_routed_c.wake_on_enqueue "$we"
        setsys ledger_routed_c.wake_committer_on_publish "$wc"
        restart
        eff_win=$(docker exec "$CONTAINER" psql -U acct -d "$DBNAME" -tAc "SHOW ledger_routed_c.batch_window_us;" 2>/dev/null)
        eff_we=$(docker exec "$CONTAINER" psql -U acct -d "$DBNAME" -tAc "SHOW ledger_routed_c.wake_on_enqueue;" 2>/dev/null)
        eff_wc=$(docker exec "$CONTAINER" psql -U acct -d "$DBNAME" -tAc "SHOW ledger_routed_c.wake_committer_on_publish;" 2>/dev/null)
        [ "$eff_win" = "$win" ] || echo "  WARN: batch_window_us effective=$eff_win wanted=$win"
        [ "$eff_we" = "$we" ] || echo "  WARN: wake_on_enqueue effective=$eff_we wanted=$we"
        [ "$eff_wc" = "$wc" ] || echo "  WARN: wake_committer_on_publish effective=$eff_wc wanted=$wc"
        for rate in $RATES; do
            out="$WORKSPACE_DIR/results/.wake_w${win}_${mode}_r${rate}.json"
            timeout 120 "$BIN" --dsn "$DSN" run --scenario "$SCENARIO" --mode routed \
                --duration "$DUR" --batch-size 1 --target-rate "$rate" --max-callers "$CALLERS" \
                --no-sampler --output "$out" >/dev/null 2>&1 \
                || { echo "  FAIL mode=$mode win=$win rate=$rate"; continue; }
            read -r tput cp50 cp99 cp999 ap50 ap99 trx drop <<<"$(extract "$out")"
            printf '%-9s %-5s %-8s %-12s %-12s %-12s %-10s %-8s\n' \
                "$mode" "$rate" "$tput" \
                "$(awk "BEGIN{printf \"%.1f\", $cp50/1000}")" \
                "$(awk "BEGIN{printf \"%.1f\", $cp99/1000}")" \
                "$(awk "BEGIN{printf \"%.1f\", $cp999/1000}")" \
                "$(awk "BEGIN{printf \"%.1f\", $ap50/1000}")" "$drop"
            record "$win" "$mode" "$we" "$wc" "$rate" "$tput" "$cp50" "$cp99" "$cp999" "$ap50" "$ap99" "$trx" "$drop"
        done
    done
done

# Restore defaults (both wakes off, shipped window).
setsys ledger_routed_c.wake_on_enqueue off
setsys ledger_routed_c.wake_committer_on_publish off
setsys ledger_routed_c.batch_window_us 500
restart

mkdir -p "$(dirname "$OUTFILE")"
jq -n --arg ts "$TS" --arg scenario "$SCENARIO" --argjson callers "$CALLERS" \
    --arg dur "$DUR" --arg dsn "$DSN" --argjson steps "$rows" \
    '{measurement:"routed_wake_ablation", ts:$ts, scenario:$scenario, callers:$callers, duration:$dur, dsn:$dsn, metric:"committed_latency_us (enqueue->materialize), open-loop paced; cumulative wake modes off/enqueue/both; ALTER SYSTEM + restart per arm; synchronous_commit=on", steps:$steps}' \
    > "$OUTFILE"

echo
echo "================ committed-p50 collapse (per window, per rate) ================"
# off -> enqueue -> both cumulative collapse. The load-robust finding is the
# within-window ratio measured back-to-back, not the absolute ms.
python3 - "$OUTFILE" <<'PY'
import json,sys
d=json.load(open(sys.argv[1]))
steps=d['steps']
by={}
for s in steps:
    by.setdefault((s['batch_window_us'],s['offered_rate']),{})[s['wake_mode']]=s['committed_p50_us']/1000.0
print(f"{'win':>4}{'rate':>6}{'off(ms)':>10}{'enqueue':>10}{'both':>10}{'off/both':>10}")
print("-"*50)
for (win,rate) in sorted(by):
    m=by[(win,rate)]
    off=m.get('off'); enq=m.get('enqueue'); both=m.get('both')
    ratio = f"{off/both:.1f}x" if (off and both and both>0) else "n/a"
    print(f"{win:>4}{rate:>6}{(off or 0):>10.1f}{(enq or 0):>10.1f}{(both or 0):>10.1f}{ratio:>10}")
PY
echo
echo "==> results: $OUTFILE"
echo "==> host load: $(cat /proc/loadavg)"
