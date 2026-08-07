#!/usr/bin/env bash
# Run recalc workers against poc_v3_2: each worker is one connection looping
# ledger_recalc_step() — continuous-drain cadence (recalc-c §3). Workers claim
# dirty pools FOR UPDATE SKIP LOCKED, so any number can run concurrently; the
# default mirrors recalc-b D10 (4).
#
# Usage: bash poc/ledger-v3.2/scripts/run-recalc.sh
# Env (all optional): RECALC_DSN, RECALC_WORKERS (4), RECALC_POLL_MS (250)

set -euo pipefail

DSN="${RECALC_DSN:-postgres://acct:acct_dev@localhost:5111/poc_v3_2}"
WORKERS="${RECALC_WORKERS:-4}"
POLL_MS="${RECALC_POLL_MS:-250}"

worker() {
    local n="$1"
    while true; do
        local report
        report="$(psql "$DSN" -qtAc 'SELECT ledger_recalc_step()')" || {
            echo "worker $n: step failed; retrying" >&2
            sleep 1
            continue
        }
        if [[ "$report" == *'"claimed": false'* ]]; then
            sleep "$(awk "BEGIN {print $POLL_MS / 1000}")"
        else
            echo "worker $n: $report"
        fi
    done
}

echo "==> $WORKERS recalc worker(s), poll ${POLL_MS}ms, dsn $DSN"
for i in $(seq 1 "$WORKERS"); do
    worker "$i" &
done
wait
