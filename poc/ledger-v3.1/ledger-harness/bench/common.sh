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

build_harness() {
    if [ ! -x "$BIN" ]; then
        echo "==> building ledger-harness (release)"
        ( cd "$WS_DIR" && cargo build --release -p ledger-harness )
        BIN="$WS_DIR/target/release/ledger-harness"
    fi
}

# Pick the DSN for a scenario's caller count: pooler for the 1000-caller
# scenarios (s5/s6/s7/s8), direct otherwise.
dsn_for_scenario() {
    case "$1" in
        s5|s6|s7|s8) echo "$POOLER_DSN" ;;
        *)           echo "$DIRECT_DSN" ;;
    esac
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
