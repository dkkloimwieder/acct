#!/usr/bin/env bash
# Shared environment + helpers for the ledger-v3.1 bench runners (design-v3.1
# §11). Source this from the run-*.sh scripts.
#
# Posture (acct-8cn2): the dev container's io_uring memlock ceiling can't hold
# 1000 direct PG backends, so the 1000-caller scenarios (S5/S7/S8) are driven
# through a pgbouncer transaction pool (bench/setup-pgbouncer.sh). Lower-caller
# scenarios use the direct DSN. Each harness invocation is HARD-timeout-wrapped
# (HARNESS_TIMEOUT) because a wedged backend once spun an LWLock for 68 min.

set -euo pipefail

# Resolve the workspace root (two levels up from bench/).
HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WS_DIR="$(cd "$HARNESS_DIR/.." && pwd)"

CONTAINER="${CONTAINER:-acct-postgres}"
# Direct connection (host port 5111 → container 5432).
DIRECT_DSN="${DIRECT_DSN:-postgres://acct:acct_dev@localhost:5111/poc_v3_1}"
# pgbouncer transaction pool (host port 6432). Used for ≥1000-caller scenarios.
POOLER_DSN="${POOLER_DSN:-postgres://acct:acct_dev@localhost:6432/poc_v3_1}"

# Per-run wall-time cap (seconds). A measurement run is `--duration` + drain +
# settle; this caps the whole invocation so nothing can wedge a session.
HARNESS_TIMEOUT="${HARNESS_TIMEOUT:-600}"

RESULTS_DIR="${RESULTS_DIR:-$WS_DIR/results}"
mkdir -p "$RESULTS_DIR"

BIN="${BIN:-$WS_DIR/target/release/ledger-harness}"

# Always (re)build so the binary tracks the current harness source — cargo is
# incremental, so this is near-free when nothing changed.
build_harness() {
    echo "==> building ledger-harness (release)"
    ( cd "$WS_DIR" && cargo build --release -p ledger-harness )
    BIN="$WS_DIR/target/release/ledger-harness"
}

# All measured caller/canary traffic goes through the pgbouncer transaction pool
# (acct-uix8): backends are bounded by default_pool_size regardless of caller
# count, which kills per-caller backend churn on the dev box and keeps absolutes
# comparable across scenarios. The admin/DDL/seed paths use DIRECT_DSN directly
# (not this helper) — CREATE DATABASE / ALTER SYSTEM / sqlx migrate can't run
# through a transaction pooler. $1 (scenario) is retained for call-site
# compatibility; routing no longer depends on it.
dsn_for_scenario() {
    echo "$POOLER_DSN"
}

# Restart the DB container for a clean shmem / committer / staging slate before a
# bake-off, and wait for readiness.
restart_db() {
    echo "==> restarting $CONTAINER for a clean slate"
    docker restart "$CONTAINER" >/dev/null
    for _ in $(seq 1 60); do
        if docker exec "$CONTAINER" pg_isready -U acct -d poc_v3_1 >/dev/null 2>&1; then
            echo "    ready"
            return 0
        fi
        sleep 1
    done
    echo "    WARNING: $CONTAINER not ready after 60s" >&2
}

# Run the harness with the hard timeout. Args passed through verbatim.
harness() {
    timeout "$HARNESS_TIMEOUT" "$BIN" "$@"
}

# Block until the host 1-min loadavg drops below LOAD_GATE (default 1.5) before a
# timed run. acct-postgres runs on a daily-driver workstation; external CPU load
# (Chrome, other agents) swings routed throughput ~2x, so identical runs return
# wildly different rates. Gating each timed run on a quiet host is load-bearing
# for trustworthy absolute numbers. Polls every 5s, logs while waiting, and gives
# up after LOAD_GATE_TIMEOUT seconds (default 600) so a permanently-busy host
# fails loud rather than hanging forever.
LOAD_GATE="${LOAD_GATE:-1.5}"
LOAD_GATE_TIMEOUT="${LOAD_GATE_TIMEOUT:-600}"
host_load1() { awk '{print $1}' /proc/loadavg; }
wait_for_quiet_host() {
    local waited=0 l
    while :; do
        l="$(host_load1)"
        if awk -v a="$l" -v g="$LOAD_GATE" 'BEGIN{exit !(a+0 < g+0)}'; then
            return 0
        fi
        if [ "$waited" -ge "$LOAD_GATE_TIMEOUT" ]; then
            echo "    [load-gate] STILL BUSY after ${LOAD_GATE_TIMEOUT}s (load1=$l >= $LOAD_GATE); proceeding anyway — mark this cell contended" >&2
            return 1
        fi
        echo "    [load-gate] host busy (load1=$l >= $LOAD_GATE); waiting…" >&2
        sleep 5
        waited=$((waited + 5))
    done
}
