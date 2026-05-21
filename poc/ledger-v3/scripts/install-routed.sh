#!/usr/bin/env bash
# Install the pgrx-built ledger_routed (Path B) extension into the
# acct-postgres docker container.
#
# Mirrors install-direct.sh. The one structural difference: ledger_routed
# wires shmem regions in _PG_init via pg_shmem_init! and (post-zedi /
# post-usn2) registers router + committer BGWorkers — both of which only
# fire when the .so is loaded by the Postmaster via
# shared_preload_libraries. This script appends 'ledger_routed' to the
# live preload list (preserving existing entries like pg_stat_statements,
# pg_cron, ledger_extension, poc_ledger, poc_v21_ledger) and restarts
# the container if (and only if) the value changed.
#
# Usage:
#   bash poc/ledger-v3/scripts/install-routed.sh
#   WITH_TEST_HOOKS=1 bash poc/ledger-v3/scripts/install-routed.sh
#
# WITH_TEST_HOOKS=1 includes the test_hooks feature so any future
# ledger_routed_test_* pg_externs are exposed. Required by test
# binaries that need state-mutating helpers; default OFF for
# production .so builds.

set -euo pipefail

CONTAINER="${CONTAINER:-acct-postgres}"
CRATE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/ledger-routed"
SO_SRC="$CRATE_DIR/../target/release/libledger_routed.so"
CONTROL_SRC="$CRATE_DIR/ledger_routed.control"
SQL_SRC="$CRATE_DIR/sql/ledger_routed--0.0.1.sql"

FEATURES="pg18"
if [ "${WITH_TEST_HOOKS:-0}" = "1" ]; then
    FEATURES="pg18,test_hooks"
    echo "==> including test_hooks feature (test build)"
fi

mkdir -p "$CRATE_DIR/sql"

echo "==> rebuilding release .so + schema bindings (features=$FEATURES)"
(cd "$CRATE_DIR/.." && cargo build --release -p ledger-routed --features "$FEATURES" --no-default-features)
(cd "$CRATE_DIR" && cargo pgrx schema --release --features "$FEATURES" --no-default-features pg18 --out "$SQL_SRC")

if [ ! -f "$SO_SRC" ]; then
    echo "missing $SO_SRC — release build did not produce the cdylib"
    exit 1
fi
if [ ! -f "$CONTROL_SRC" ] || [ ! -f "$SQL_SRC" ]; then
    echo "missing control or sql artifact"
    exit 1
fi

PKGLIBDIR=$(docker exec "$CONTAINER" pg_config --pkglibdir)
EXTDIR=$(docker exec "$CONTAINER" pg_config --sharedir)/extension

echo "==> copying .so to $CONTAINER:$PKGLIBDIR/ledger_routed.so"
docker cp "$SO_SRC" "$CONTAINER:$PKGLIBDIR/ledger_routed.so"

echo "==> copying control + sql to $CONTAINER:$EXTDIR/"
docker cp "$CONTROL_SRC" "$CONTAINER:$EXTDIR/"
docker cp "$SQL_SRC" "$CONTAINER:$EXTDIR/"

# ── shared_preload_libraries plumbing ───────────────────────────────
#
# SHOW returns the live, merged value (postgresql.conf + postgresql.auto.conf
# + command-line) as a comma-separated identifier list, no spaces.
#
# CRITICAL GOTCHA: PG18's `ALTER SYSTEM SET shared_preload_libraries
# = '<comma_list>'` serializes the value to postgresql.auto.conf with
# the list double-quoted *inside* the single quotes — i.e. it writes
# `shared_preload_libraries = '"a,b,c"'`. On the next start PG treats
# the whole comma-list as a single bogus library name and refuses to
# launch. Use the unquoted identifier-list form instead, which
# serializes cleanly as `'a, b, c'`. Lib names are alphanumeric +
# underscore so unquoted parsing is safe.

CURRENT_PRELOADS=$(docker exec "$CONTAINER" psql -U acct -d postgres -tA -c "SHOW shared_preload_libraries")
echo "==> current shared_preload_libraries: $CURRENT_PRELOADS"

NEED_RESTART=0
if echo ",$CURRENT_PRELOADS," | grep -q ',ledger_routed,'; then
    echo "==> ledger_routed already preloaded; skipping ALTER SYSTEM"
else
    if [ -z "$CURRENT_PRELOADS" ]; then
        NEW_PRELOADS_LIST="ledger_routed"
    else
        NEW_PRELOADS_LIST="$(echo "$CURRENT_PRELOADS" | sed 's/,/, /g'), ledger_routed"
    fi
    echo "==> ALTER SYSTEM SET shared_preload_libraries = $NEW_PRELOADS_LIST"
    docker exec "$CONTAINER" psql -U acct -d postgres -c \
        "ALTER SYSTEM SET shared_preload_libraries = $NEW_PRELOADS_LIST"
    NEED_RESTART=1
fi

if [ "$NEED_RESTART" = "1" ]; then
    echo "==> restarting $CONTAINER to load ledger_routed"
    docker restart "$CONTAINER" >/dev/null
    until docker exec "$CONTAINER" pg_isready -U acct -d postgres >/dev/null 2>&1; do
        sleep 1
    done
    echo "==> $CONTAINER is back up"
fi

echo "==> verifying CREATE EXTENSION in poc_v3"
docker exec "$CONTAINER" psql -U acct -d poc_v3 -c \
    "DROP EXTENSION IF EXISTS ledger_routed; CREATE EXTENSION ledger_routed;"
docker exec "$CONTAINER" psql -U acct -d poc_v3 -c \
    "SELECT extname, extversion FROM pg_extension WHERE extname = 'ledger_routed'"
docker exec "$CONTAINER" psql -U acct -d poc_v3 -c \
    "SELECT ledger_routed_hello()"
docker exec "$CONTAINER" psql -U acct -d poc_v3 -c \
    "SELECT state, count FROM ledger_routed_staging_state_counts() ORDER BY state"

echo "==> install complete"
