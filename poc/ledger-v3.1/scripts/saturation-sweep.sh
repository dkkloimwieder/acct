#!/usr/bin/env bash
# Throughput-at-a-latency-SLO saturation sweep (acct-0at4.8).
#
# WHAT IT PRODUCES
#   The headline the open-loop methodology asks for: the MAX SUSTAINABLE offered
#   rate at which the end-to-end p99 latency still meets an SLO — not peak
#   throughput. Peak-only comparisons systematically flatter the batching
#   architectures (routed/staging trade latency for throughput); a
#   throughput-at-SLO headline removes that bias, so a direct-vs-staging-vs-routed
#   crossover is an apples-to-apples latency claim.
#
# HOW
#   Seeds a fixed pool universe ONCE, then drives the harness open-loop
#   (coordinated-omission-free, intended-send-time latency — see pacing.rs) at an
#   ascending ladder of offered rates. For each rate it records offered vs
#   ACHIEVED goodput and the SLO-headline latency (`slo_p99_us` = end-to-end p99:
#   ack for the synchronous direct modes, materialization for routed/staging).
#   The sustainable point is the highest offered rate where BOTH hold:
#       (a) slo_p99  <  SLO             (latency still met), and
#       (b) achieved >= KEEP_UP * offered   (not yet saturated — the generator
#                                            isn't just piling up backlog).
#   Past that point one of the two breaks; the sweep keeps going a couple of
#   steps so the degradation is visible, then reports the crossover.
#
# NOTE ON POSTURE
#   The bench host is a noisy workstation (project memory): absolute rates swing
#   with background load, so treat the ABSOLUTE sustainable rate as host-relative
#   and compare MODES within one sweep invocation (same host, same minute). Run
#   the same ladder for --mode direct-single / staging / routed to get the
#   crossover. Emits a JSON array of every step for the .9/.10 baseline work.
#
# USAGE
#   bash scripts/saturation-sweep.sh
#   MODE=staging SLO_P99_MS=25 bash scripts/saturation-sweep.sh
#   MODE=direct-single RATES="500 1000 1500 2000 2500 3000" bash scripts/saturation-sweep.sh
#
# ENV
#   MODE          direct-single | direct-per-call | staging | routed  (default direct-single)
#   SCENARIO      harness scenario id (§10.6)                         (default s1)
#   ARRIVAL       poisson | uniform                                   (default poisson)
#   SLO_P99_MS    latency SLO on the end-to-end p99, milliseconds      (default 10)
#   KEEP_UP       achieved/offered floor to still count as sustained   (default 0.95)
#   RATES         ascending offered-rate ladder (trx/s), space-sep     (default below)
#   DURATION      per-step drive time (must dominate warmup)           (default 10s)
#   CAP           --max-callers                                        (default 32)
#   METHOD_MIX    seeded pool method assignment                        (default all-fifo)
#   SEED_COUNT/SEED_SKUS/SEED_LOCATIONS/SEED_DEPTH  universe           (default 2000/500/10/5)
#   COMMITTERS    staging/routed drain connections                    (default 4)
#   DRAIN_BATCH   staging drain SKIP LOCKED LIMIT                      (default 200)
#   OUTFILE       sweep JSON array output                             (default results/saturation-<mode>-<ts>.json)
#   CONTAINER     docker container name                               (default acct-postgres)
#   DSN           Postgres DSN                                        (default poc_v3_1)
#   NO_RESTART    1 = skip the docker restart preflight               (default 0)

set -uo pipefail

MODE="${MODE:-direct-single}"
SCENARIO="${SCENARIO:-s1}"
ARRIVAL="${ARRIVAL:-poisson}"
SLO_P99_MS="${SLO_P99_MS:-10}"
KEEP_UP="${KEEP_UP:-0.95}"
RATES="${RATES:-250 500 1000 1500 2000 2500 3000 4000 5000 6000}"
DURATION="${DURATION:-10s}"
CAP="${CAP:-32}"
METHOD_MIX="${METHOD_MIX:-all-fifo}"
SEED_COUNT="${SEED_COUNT:-2000}"
SEED_SKUS="${SEED_SKUS:-500}"
SEED_LOCATIONS="${SEED_LOCATIONS:-10}"
SEED_DEPTH="${SEED_DEPTH:-5}"
COMMITTERS="${COMMITTERS:-4}"
DRAIN_BATCH="${DRAIN_BATCH:-200}"
CONTAINER="${CONTAINER:-acct-postgres}"
DSN="${DSN:-postgres://acct:acct_dev@localhost:5111/poc_v3_1}"
NO_RESTART="${NO_RESTART:-0}"

WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$WORKSPACE_DIR"
BIN="$WORKSPACE_DIR/target/release/ledger-harness"
SLO_US=$(( SLO_P99_MS * 1000 ))
TS="$(date -u +%Y-%m-%dT%H-%M-%S)"
OUTFILE="${OUTFILE:-results/saturation-${MODE}-${TS}.json}"

# Staging/routed take extra committer/drain flags; direct modes ignore them.
extra_flags=()
case "$MODE" in
    staging|routed) extra_flags=(--committers "$COMMITTERS" --drain-batch "$DRAIN_BATCH") ;;
esac

command -v jq >/dev/null || { echo "FATAL: jq required" >&2; exit 2; }

echo "==> saturation sweep: mode=$MODE scenario=$SCENARIO arrival=$ARRIVAL SLO(p99)<${SLO_P99_MS}ms keep-up>=${KEEP_UP}"
echo "==> rate ladder: $RATES"

if [ ! -x "$BIN" ]; then
    echo "==> building ledger-harness (release)"
    cargo build -p ledger-harness --release 2>&1 | tail -2 || { echo "build failed" >&2; exit 2; }
fi

if [ "$NO_RESTART" != "1" ]; then
    echo "==> restart $CONTAINER for a clean slate"
    docker restart "$CONTAINER" >/dev/null || { echo "docker restart failed" >&2; exit 2; }
    # Block until PG accepts connections.
    for _ in $(seq 1 90); do
        docker exec "$CONTAINER" pg_isready -U acct -q >/dev/null 2>&1 && break
        sleep 1
    done
fi

# Seed the universe ONCE (a 1s warmup run with --method-mix TRUNCATEs + seeds),
# so every rate step drives the SAME universe — the ramp is apples-to-apples.
echo "==> seed universe once (mix=$METHOD_MIX, ${SEED_COUNT} pools, depth ${SEED_DEPTH})"
"$BIN" run --scenario "$SCENARIO" --mode "$MODE" "${extra_flags[@]}" \
    --method-mix "$METHOD_MIX" --seed-count "$SEED_COUNT" --seed-skus "$SEED_SKUS" \
    --seed-locations "$SEED_LOCATIONS" --seed-depth "$SEED_DEPTH" \
    --duration 1s --no-sampler --max-callers "$CAP" \
    --target-rate 200 --arrival "$ARRIVAL" --dsn "$DSN" \
    --output /dev/null >/dev/null 2>&1 \
    || { echo "FATAL: seed/warmup run failed" >&2; exit 2; }

printf '\n%-9s  %-10s  %-10s  %-9s  %-9s  %-9s  %s\n' \
    "offered" "achieved" "keep-up" "p50(ms)" "p99(ms)" "p999(ms)" "verdict"
printf -- '---------------------------------------------------------------------------------\n'

rows="[]"
best=0
for rate in $RATES; do
    line=$("$BIN" run --scenario "$SCENARIO" --mode "$MODE" "${extra_flags[@]}" \
        --duration "$DURATION" --no-sampler --max-callers "$CAP" \
        --target-rate "$rate" --arrival "$ARRIVAL" --dsn "$DSN" 2>/dev/null | tail -1)
    if [ -z "$line" ] || ! echo "$line" | jq -e . >/dev/null 2>&1; then
        echo "  !! rate=$rate produced no JSON — aborting sweep" >&2
        break
    fi

    achieved=$(echo "$line" | jq -r '.throughput_trx_per_sec')
    p50=$(echo "$line" | jq -r '.p50_us // 0')
    p99=$(echo "$line" | jq -r '.slo_p99_us')
    p999=$(echo "$line" | jq -r '.slo_p999_us')

    # Sustained iff p99 within SLO AND achieved keeps up with offered.
    keep=$(awk -v a="$achieved" -v o="$rate" 'BEGIN{ printf (o>0)? a/o : 0 }')
    ok=$(awk -v p="$p99" -v slo="$SLO_US" -v k="$keep" -v floor="$KEEP_UP" \
        'BEGIN{ print (p<=slo && k>=floor) ? 1 : 0 }')
    if [ "$ok" = "1" ]; then verdict="SUSTAINED"; best="$rate"; else verdict="over-SLO"; fi

    printf '%-9s  %-10.0f  %-10.3f  %-9.2f  %-9.2f  %-9.2f  %s\n' \
        "$rate" "$achieved" "$keep" \
        "$(awk -v v="$p50" 'BEGIN{print v/1000}')" \
        "$(awk -v v="$p99" 'BEGIN{print v/1000}')" \
        "$(awk -v v="$p999" 'BEGIN{print v/1000}')" \
        "$verdict"

    rows=$(echo "$rows" | jq --argjson r "$rate" --argjson a "$achieved" \
        --argjson k "$keep" --argjson p50 "$p50" --argjson p99 "$p99" --argjson p999 "$p999" \
        --arg v "$verdict" \
        '. + [{offered_rate:$r, achieved_rate:$a, keep_up:$k, p50_us:$p50, slo_p99_us:$p99, slo_p999_us:$p999, verdict:$v}]')
done

echo
if [ "$best" -gt 0 ]; then
    echo "==> throughput-at-SLO (p99 < ${SLO_P99_MS}ms, achieved >= ${KEEP_UP}×offered): ${best} trx/s  [mode=$MODE]"
else
    echo "==> NO rate met the SLO — lower the first rung or relax SLO_P99_MS."
fi

mkdir -p "$(dirname "$OUTFILE")"
jq -n --arg mode "$MODE" --arg scenario "$SCENARIO" --arg arrival "$ARRIVAL" \
    --argjson slo_p99_us "$SLO_US" --arg keep_up "$KEEP_UP" --argjson sustained "$best" \
    --argjson steps "$rows" \
    '{mode:$mode, scenario:$scenario, arrival:$arrival, slo_p99_us:$slo_p99_us, keep_up:($keep_up|tonumber), throughput_at_slo:$sustained, steps:$steps}' \
    > "$OUTFILE"
echo "==> sweep detail: $OUTFILE"