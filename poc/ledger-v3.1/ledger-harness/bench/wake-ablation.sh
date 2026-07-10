#!/usr/bin/env bash
# Axis F (acct-0at4.15): SetLatch-on-enqueue vs the 50 ms router tick.
#
# WHAT IT PRODUCES
#   The materialization-latency A/B that FEEDBACK #13 asks for: does waking the
#   router BGWorker from the enqueue backend (ledger_routed_c.wake_on_enqueue)
#   collapse the tick-cadence materialization floor? Metric is committed_latency
#   (enqueue -> observed materialize), the FULL pipeline surface — not ack. The
#   harness reports it under open-loop pacing (--target-rate, absolute-schedule
#   arrivals, single-push --batch-size 1), so latency is charged from INTENDED
#   send time (no coordinated omission, acct-0at4.8).
#
#   Four arms = wake ∈ {off,on} × batch_window_us ∈ {0, 500}:
#     window=0    the PURE wake floor. With the coalesce gate disabled, wake-off
#                 materializes only on the next 50 ms tick; wake-on materializes
#                 as soon as the SetLatch is delivered. This is where the wake
#                 wins outright.
#     window=500  the SHIPPED default. The coalesce gate defers a group until its
#                 oldest member ages past 500 µs, so a lone submission still falls
#                 to the tick even with wake-on (the router re-parks after the
#                 defer). Under a steady arrival stream the wake keeps the router
#                 hot so the window — not the tick — governs. Measured to show the
#                 wake is NOT a free win at the shipped window without a bounded
#                 post-defer re-arm (documented, not fixed, in this PoC).
#
# WHY RESTART PER ARM (not pg_reload_conf)
#   wake_on_enqueue is read by the enqueue backend (a regular backend, live-
#   readable), but batch_window_us is read by the router BGWorker, and routed
#   Sighup GUCs are not reliably adopted live by the BGWorkers (acct-0at4.9). A
#   restart makes BOTH unambiguous — the router re-reads batch_window_us at init
#   and fresh harness connections read wake_on_enqueue — and gives each arm a
#   clean shmem-queue + committer slate. ALTER SYSTEM persists across restart; the
#   seeded pool universe lives in the data volume and survives.
#
# POSTURE
#   Noisy workstation: absolute latencies swing with background load. The
#   STRUCTURAL result — the wake-on/wake-off RATIO of committed p50 at window=0,
#   measured back-to-back — is the load-robust finding. Full statistical rigor is
#   acct-0at4.10.
#
# USAGE / ENV
#   bash wake-ablation.sh
#   RATES="50 200" SCENARIO=s2 CALLERS=64 DUR=20s bash wake-ablation.sh
#
#   DSN, SCENARIO(s2), CALLERS(64), DUR(20s), RATES("50 200 800"),
#   WINDOWS("0 500"), WAKES("off on"),
#   SEED_COUNT(2000)/SEED_SKUS(500)/SEED_LOCS(10), CONTAINER(acct-postgres),
#   OUTFILE(results/wake-ablation-<ts>.json)

set -uo pipefail

DSN="${DSN:-postgres://acct:acct_dev@localhost:5111/poc_v3_1}"
SCENARIO="${SCENARIO:-s2}"
CALLERS="${CALLERS:-64}"
DUR="${DUR:-20s}"
RATES="${RATES:-50 200 800}"
WINDOWS="${WINDOWS:-0 500}"
WAKES="${WAKES:-off on}"
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

echo "==> axis F wake ablation: scenario=$SCENARIO callers=$CALLERS dur=$DUR rates=[$RATES] windows=[$WINDOWS] wakes=[$WAKES]"
echo "==> host load: $(cat /proc/loadavg)"

# Seed the universe once (all-wac keeps committer work O(1) so committed latency
# reflects the pipeline/tick, not cost compute). Survives the per-arm restarts.
echo "==> seed universe (all-wac, $SEED_COUNT pools)"
setsys ledger_routed_c.wake_on_enqueue off
setsys ledger_routed_c.batch_window_us 500
restart || { echo "FATAL: restart failed" >&2; exit 2; }
"$BIN" --dsn "$DSN" run --scenario "$SCENARIO" --mode routed --method-mix all-wac \
    --seed-count "$SEED_COUNT" --seed-skus "$SEED_SKUS" --seed-locations "$SEED_LOCS" \
    --duration 2s --no-sampler --max-callers "$CALLERS" >/dev/null 2>&1 \
    || { echo "FATAL: seed failed" >&2; exit 2; }

rows="[]"
record() { # window wake rate tput cp50 cp99 cp999 ap50 ap99 trx drop
    rows=$(echo "$rows" | jq \
        --argjson w "$1" --arg wake "$2" --argjson rate "$3" --argjson tput "$4" \
        --argjson cp50 "$5" --argjson cp99 "$6" --argjson cp999 "$7" \
        --argjson ap50 "$8" --argjson ap99 "$9" --argjson trx "${10}" --argjson drop "${11}" \
        '. + [{batch_window_us:$w, wake:$wake, offered_rate:$rate, achieved_tput:$tput,
               committed_p50_us:$cp50, committed_p99_us:$cp99, committed_p999_us:$cp999,
               ack_p50_us:$ap50, ack_p99_us:$ap99, trx:$trx, dropped:$drop}]')
}

for win in $WINDOWS; do
    echo
    echo "================ batch_window_us=$win ================"
    printf '%-6s %-5s %-8s %-12s %-12s %-12s %-10s %-8s\n' \
        "wake" "rate" "tput" "cmt_p50(ms)" "cmt_p99(ms)" "cmt_p999(ms)" "ack_p50(ms)" "drop"
    printf -- '----------------------------------------------------------------------------------\n'
    for wake in $WAKES; do
        setsys ledger_routed_c.batch_window_us "$win"
        setsys ledger_routed_c.wake_on_enqueue "$wake"
        restart
        eff_win=$(docker exec "$CONTAINER" psql -U acct -d "$DBNAME" -tAc "SHOW ledger_routed_c.batch_window_us;" 2>/dev/null)
        eff_wake=$(docker exec "$CONTAINER" psql -U acct -d "$DBNAME" -tAc "SHOW ledger_routed_c.wake_on_enqueue;" 2>/dev/null)
        [ "$eff_win" = "$win" ] || echo "  WARN: batch_window_us effective=$eff_win wanted=$win"
        [ "$eff_wake" = "$wake" ] || echo "  WARN: wake_on_enqueue effective=$eff_wake wanted=$wake"
        for rate in $RATES; do
            out="$WORKSPACE_DIR/results/.wake_w${win}_${wake}_r${rate}.json"
            timeout 120 "$BIN" --dsn "$DSN" run --scenario "$SCENARIO" --mode routed \
                --duration "$DUR" --batch-size 1 --target-rate "$rate" --max-callers "$CALLERS" \
                --no-sampler --output "$out" >/dev/null 2>&1 \
                || { echo "  FAIL wake=$wake win=$win rate=$rate"; continue; }
            read -r tput cp50 cp99 cp999 ap50 ap99 trx drop <<<"$(extract "$out")"
            printf '%-6s %-5s %-8s %-12s %-12s %-12s %-10s %-8s\n' \
                "$wake" "$rate" "$tput" \
                "$(awk "BEGIN{printf \"%.1f\", $cp50/1000}")" \
                "$(awk "BEGIN{printf \"%.1f\", $cp99/1000}")" \
                "$(awk "BEGIN{printf \"%.1f\", $cp999/1000}")" \
                "$(awk "BEGIN{printf \"%.1f\", $ap50/1000}")" "$drop"
            record "$win" "$wake" "$rate" "$tput" "$cp50" "$cp99" "$cp999" "$ap50" "$ap99" "$trx" "$drop"
        done
    done
done

# Restore defaults.
setsys ledger_routed_c.wake_on_enqueue off
setsys ledger_routed_c.batch_window_us 500
restart

mkdir -p "$(dirname "$OUTFILE")"
jq -n --arg ts "$TS" --arg scenario "$SCENARIO" --argjson callers "$CALLERS" \
    --arg dur "$DUR" --arg dsn "$DSN" --argjson steps "$rows" \
    '{measurement:"routed_wake_on_enqueue_ablation", ts:$ts, scenario:$scenario, callers:$callers, duration:$dur, dsn:$dsn, metric:"committed_latency_us (enqueue->materialize), open-loop paced; ALTER SYSTEM + restart per arm; synchronous_commit=on", steps:$steps}' \
    > "$OUTFILE"
echo
echo "==> results: $OUTFILE"
echo "==> host load: $(cat /proc/loadavg)"
