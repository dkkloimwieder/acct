#!/usr/bin/env bash
# kill-9 postmaster recovery loop over the SPIKE-A staging table (acct-0at4.6,
# Part 2 — the surviving crash-recovery layer after the gate re-scope).
#
# WHAT IT PROVES
#   A hard postmaster crash (SIGKILL) mid-drain never yields torn writes, a
#   double-apply, or lost committed work that breaks conservation. The staging
#   committer `ledger_staging_drain_c` claims + applies + marks-done in ONE
#   transaction (ledger-direct-c/src/drain.rs §5), so WAL recovery makes the
#   outcome all-or-nothing: a drain that did not commit leaves its rows NOT done,
#   re-claimed cleanly on restart; the `trx` UNIQUE (trx_type, source_id) key
#   structurally forbids a second commit of the same submission. This loop crashes
#   the DB while drains are in flight, restarts it (crash recovery replays WAL),
#   drains the backlog to completion, and asserts the acct-0at4.5 conservation
#   sweep still finds NOTHING. Each iteration is an independent trial (the harness
#   run TRUNCATE+reseeds at start).
#
# NOTE ON POSTURE
#   Orchestration-heavy and the bench host is a noisy workstation — the signal is
#   PASS/FAIL, not throughput, so host noise is irrelevant. This is a manual
#   recovery-correctness harness, not a flaky CI gate. Run it when the staging
#   drain or its crash-safety changes.
#
# USAGE
#   bash scripts/kill9-staging-recovery.sh            # 5 iterations, s9, depth 10
#   ITERS=10 bash scripts/kill9-staging-recovery.sh
#   SCENARIO=s10 DEPTH=0 bash scripts/kill9-staging-recovery.sh
#
# ENV
#   ITERS         crash/recover iterations                 (default 5)
#   SCENARIO      harness scenario id (§10.6)              (default s9)
#   DEPTH         deep-seed layers per pool                (default 10)
#   METHOD_MIX    seeded pool method assignment            (default all-fifo)
#   SEED_COUNT/SEED_SKUS/SEED_LOCATIONS  universe size     (default 2000/500/10)
#   DURATION      harness run duration (must outlast the kill window) (default 30s)
#   COMMITTERS    drain-loop connections during load       (default 4)
#   DRAIN_BATCH   SKIP LOCKED LIMIT per drain call         (default 200)
#   CAP           --max-callers                            (default 200)
#   CONTAINER     docker container name                    (default acct-postgres)
#   DSN           Postgres DSN                             (default poc_v3_1)
#   FAIL_FAST     1 = stop at first failing iteration      (default 1)

set -uo pipefail

ITERS="${ITERS:-5}"
SCENARIO="${SCENARIO:-s9}"
DEPTH="${DEPTH:-10}"
METHOD_MIX="${METHOD_MIX:-all-fifo}"
SEED_COUNT="${SEED_COUNT:-2000}"
SEED_SKUS="${SEED_SKUS:-500}"
SEED_LOCATIONS="${SEED_LOCATIONS:-10}"
DURATION="${DURATION:-30s}"
COMMITTERS="${COMMITTERS:-4}"
DRAIN_BATCH="${DRAIN_BATCH:-200}"
CAP="${CAP:-200}"
CONTAINER="${CONTAINER:-acct-postgres}"
DSN="${DSN:-postgres://acct:acct_dev@localhost:5111/poc_v3_1}"
FAIL_FAST="${FAIL_FAST:-1}"

WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$WORKSPACE_DIR"
BIN="$WORKSPACE_DIR/target/release/ledger-harness"
LOGDIR="$(mktemp -d)"

# psql shim — the container always has psql + the acct role.
psql_c() {
    docker exec -e PGPASSWORD=acct_dev "$CONTAINER" \
        psql -h localhost -U acct -d poc_v3_1 -tAc "$1" 2>/dev/null
}

HARNESS_PID=""
cleanup() {
    [ -n "$HARNESS_PID" ] && kill "$HARNESS_PID" 2>/dev/null
    # Never leave the DB down for other sessions / the next run.
    docker start "$CONTAINER" >/dev/null 2>&1
    rm -rf "$LOGDIR"
}
trap cleanup EXIT

# Block until PG accepts connections (crash recovery replays WAL first, so this
# also gates on recovery completion).
wait_ready() {
    local deadline=$((SECONDS + 90))
    until docker exec "$CONTAINER" pg_isready -U acct -q >/dev/null 2>&1; do
        if [ "$SECONDS" -ge "$deadline" ]; then
            echo "==> FATAL: PG did not accept connections within 90s after restart" >&2
            return 1
        fi
        sleep 1
    done
}

# Block until the harness has left the reseed phase and callers are actually
# enqueueing — i.e. `ledger_inbox` is non-empty. This is what keeps the SIGKILL
# landing on the DRAIN path (the crash-safe unit under test) rather than on the
# harness's non-transactional bulk deep-seed. The inbox is pre-truncated before
# the run is backgrounded, and the reseed never writes to it, so a non-zero count
# unambiguously means load is flowing.
wait_load_started() {
    local deadline=$((SECONDS + 120))
    while :; do
        kill -0 "$HARNESS_PID" 2>/dev/null || {
            echo "==> harness exited before load started (log: $LOGDIR/iter-$iter.log)" >&2
            return 1
        }
        local c
        c="$(psql_c "SELECT count(*) FROM ledger_inbox")"
        [ -n "$c" ] && [ "$c" != "0" ] && return 0
        [ "$SECONDS" -ge "$deadline" ] && { echo "==> load did not start within 120s" >&2; return 1; }
        sleep 0.5
    done
}

# Drain every pending ledger_inbox row to completion, tolerating a few transient
# post-recovery errors. Echoes the number of rows recovered (drained here, i.e.
# rows a crashed drain had left NOT done).
drain_backlog() {
    local recovered=0 n tries
    while :; do
        tries=0
        n=""
        # ledger_staging_drain_c returns rows claimed this call; 0 == queue empty.
        while [ -z "$n" ] && [ "$tries" -lt 20 ]; do
            n="$(psql_c "SELECT ledger_staging_drain_c($DRAIN_BATCH)")"
            [ -z "$n" ] && { tries=$((tries + 1)); sleep 0.5; }
        done
        if [ -z "$n" ]; then
            echo "==> FATAL: drain query kept failing after recovery" >&2
            return 1
        fi
        [ "$n" = "0" ] && break
        recovered=$((recovered + n))
    done
    echo "$recovered"
}

# ── preflight ───────────────────────────────────────────────────────────────
echo "==> workspace: $WORKSPACE_DIR"
if [ ! -x "$BIN" ]; then
    echo "==> building ledger-harness (release)"
    cargo build -p ledger-harness --release 2>&1 | tail -2 || { echo "build failed" >&2; exit 2; }
fi

echo "==> restart $CONTAINER for a clean slate"
docker restart "$CONTAINER" >/dev/null || { echo "docker restart failed" >&2; exit 2; }
wait_ready || exit 2

echo "==> apply bench/spike-a-inbox.sql (idempotent)"
docker exec -i -e PGPASSWORD=acct_dev "$CONTAINER" \
    psql -h localhost -U acct -d poc_v3_1 -q < bench/spike-a-inbox.sql >/dev/null 2>&1

echo "==> baseline: seed + short run + inline sweep (catches a seed/sweep mismatch before the crash loop)"
if ! "$BIN" run --scenario "$SCENARIO" --mode staging \
        --method-mix "$METHOD_MIX" --seed-count "$SEED_COUNT" --seed-skus "$SEED_SKUS" \
        --seed-locations "$SEED_LOCATIONS" --seed-depth "$DEPTH" \
        --duration 2s --no-sampler --max-callers "$CAP" \
        --committers "$COMMITTERS" --drain-batch "$DRAIN_BATCH" \
        --verify --dsn "$DSN" 2>/dev/null | tail -2; then
    echo "==> FATAL: baseline sweep failed — DEPTH=$DEPTH seed shape is not conservation-clean; try DEPTH=0" >&2
    exit 2
fi

# ── crash / recover loop ────────────────────────────────────────────────────
reds=0
for iter in $(seq 1 "$ITERS"); do
    # Vary the kill instant (2..5s) so the crash lands at different points of a
    # drain — mid-claim, mid-write, mid-mark-done — across iterations.
    ka=$(( 2 + (iter % 4) ))
    echo
    echo "================================================================"
    echo "[iter $iter/$ITERS] load ${DURATION}, SIGKILL after ${ka}s"
    echo "================================================================"

    # Clear the inbox so `wait_load_started` can't false-trigger on a prior
    # iteration's rows during this iteration's (untouched-inbox) reseed phase.
    psql_c "TRUNCATE ledger_inbox RESTART IDENTITY" >/dev/null

    # Background the staging load (reseeds at start → independent trial).
    "$BIN" run --scenario "$SCENARIO" --mode staging \
        --method-mix "$METHOD_MIX" --seed-count "$SEED_COUNT" --seed-skus "$SEED_SKUS" \
        --seed-locations "$SEED_LOCATIONS" --seed-depth "$DEPTH" \
        --duration "$DURATION" --no-sampler --max-callers "$CAP" \
        --committers "$COMMITTERS" --drain-batch "$DRAIN_BATCH" \
        --dsn "$DSN" >"$LOGDIR/iter-$iter.log" 2>&1 &
    HARNESS_PID=$!

    # Wait out the reseed, then let the drain run for `ka`s before the crash.
    wait_load_started || { kill "$HARNESS_PID" 2>/dev/null; wait "$HARNESS_PID" 2>/dev/null; exit 2; }
    sleep "$ka"
    local_pending="$(psql_c "SELECT count(*) FROM ledger_inbox WHERE NOT done")"
    echo "-- pending inbox rows at kill: ${local_pending:-?}"

    echo "-- SIGKILL postmaster"
    docker kill --signal=SIGKILL "$CONTAINER" >/dev/null 2>&1

    # The load process is now erroring on dropped connections — reap it.
    kill "$HARNESS_PID" 2>/dev/null
    wait "$HARNESS_PID" 2>/dev/null
    HARNESS_PID=""

    echo "-- restart + crash recovery"
    docker start "$CONTAINER" >/dev/null || { echo "docker start failed" >&2; exit 2; }
    wait_ready || exit 2

    echo "-- drain backlog to completion"
    recovered="$(drain_backlog)" || exit 2
    still_pending="$(psql_c "SELECT count(*) FROM ledger_inbox WHERE NOT done")"
    committed="$(psql_c "SELECT count(*) FROM trx")"
    echo "-- recovered ${recovered} row(s); pending now ${still_pending:-?}; committed trx ${committed:-?}"

    echo "-- conservation sweep"
    sweep_out="$("$BIN" verify --dsn "$DSN" 2>/dev/null | tail -1)"
    if printf '%s' "$sweep_out" | grep -q '"verdict":"PASS"'; then
        echo "-- iter $iter: PASS  $sweep_out"
    else
        echo "-- iter $iter: FAIL  $sweep_out"
        reds=$((reds + 1))
        if [ "$FAIL_FAST" = "1" ]; then
            echo "==> FAIL_FAST=1; stopping at first failing iteration." >&2
            break
        fi
    fi
done

echo
echo "================================================================"
echo "summary: $((iter - reds))/$iter iterations conservation-clean after kill-9 recovery"
echo "================================================================"
[ "$reds" -gt 0 ] && exit 1
exit 0
