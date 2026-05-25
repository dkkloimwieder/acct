#!/usr/bin/env bash
# Install the pgrx-built ledger_routed_c (Path C routed) extension into the
# acct-postgres docker container, targeting the poc_v3_1 database.
#
# Mirrors install-direct-c.sh, plus the shared_preload_libraries plumbing that
# ledger_routed_c needs: it wires shmem regions in _PG_init via pg_shmem_init!
# and registers router + committer + recovery BGWorkers, all of which only fire
# when the .so is loaded by the Postmaster via shared_preload_libraries. This
# script appends 'ledger_routed_c' to the live preload list (preserving existing
# entries) and restarts the container iff the value changed.
#
# Usage:
#   bash poc/ledger-v3.1/scripts/install-routed-c.sh
#   WITH_TEST_HOOKS=1 bash poc/ledger-v3.1/scripts/install-routed-c.sh

set -euo pipefail

CONTAINER="${CONTAINER:-acct-postgres}"
DB="${DB:-poc_v3_1}"
WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CRATE_DIR="$WORKSPACE_DIR/ledger-routed-c"
SO_SRC="$WORKSPACE_DIR/target/release/libledger_routed_c.so"
CONTROL_SRC="$CRATE_DIR/ledger_routed_c.control"
SQL_SRC="$CRATE_DIR/sql/ledger_routed_c--0.0.1.sql"

FEATURES="pg18"
if [ "${WITH_TEST_HOOKS:-0}" = "1" ]; then
    FEATURES="pg18,test_hooks"
    echo "==> including test_hooks feature (test build)"
fi

mkdir -p "$CRATE_DIR/sql"

echo "==> rebuilding release .so + schema bindings (features=$FEATURES)"
(cd "$WORKSPACE_DIR" && cargo build --release -p ledger-routed-c --features "$FEATURES" --no-default-features)
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

echo "==> copying .so to $CONTAINER:$PKGLIBDIR/ledger_routed_c.so"
docker cp "$SO_SRC" "$CONTAINER:$PKGLIBDIR/ledger_routed_c.so"

echo "==> copying control + sql to $CONTAINER:$EXTDIR/"
docker cp "$CONTROL_SRC" "$CONTAINER:$EXTDIR/"
docker cp "$SQL_SRC" "$CONTAINER:$EXTDIR/"

# ── shared_preload_libraries plumbing ───────────────────────────────
#
# Use the unquoted identifier-list form for ALTER SYSTEM: PG18 serializes
# `ALTER SYSTEM SET shared_preload_libraries = '<comma_list>'` (quoted) by
# wrapping the list in double quotes inside the single quotes, which then
# parses as one bogus library name on next start. The unquoted form
# (`= a, b, c`) serializes cleanly. Lib names are [a-z0-9_] so this is safe.
CURRENT_PRELOADS=$(docker exec "$CONTAINER" psql -U acct -d postgres -tA -c "SHOW shared_preload_libraries")
echo "==> current shared_preload_libraries: $CURRENT_PRELOADS"
CURRENT_NORMALIZED=$(echo "$CURRENT_PRELOADS" | tr -d ' ')

if echo ",$CURRENT_NORMALIZED," | grep -q ',ledger_routed_c,'; then
    echo "==> ledger_routed_c already preloaded; skipping ALTER SYSTEM"
else
    if [ -z "$CURRENT_NORMALIZED" ]; then
        NEW_PRELOADS_LIST="ledger_routed_c"
    else
        NEW_PRELOADS_LIST="$(echo "$CURRENT_NORMALIZED" | sed 's/,/, /g'), ledger_routed_c"
    fi
    echo "==> ALTER SYSTEM SET shared_preload_libraries = $NEW_PRELOADS_LIST"
    docker exec "$CONTAINER" psql -U acct -d postgres -c \
        "ALTER SYSTEM SET shared_preload_libraries = $NEW_PRELOADS_LIST"
fi

# Always restart after copying the .so. ledger_routed_c is a *preloaded*
# library: the postmaster loads its code + symbols at startup and every
# backend inherits that image via fork. Overwriting the .so file does NOT
# rebind a running cluster — the BGWorker bodies (router/committer) keep
# executing the old code, and a fresh `CREATE EXTENSION` fails to resolve any
# newly-added SQL symbols against the stale in-memory library ("could not find
# function ... in file"). A restart re-execs the postmaster against the new
# .so, picking up both the new worker code and the new symbols. (Cheap in dev;
# the data volume persists.)
echo "==> restarting $CONTAINER to load the freshly-built ledger_routed_c.so"
docker restart "$CONTAINER" >/dev/null
until docker exec "$CONTAINER" pg_isready -U acct -d postgres >/dev/null 2>&1; do
    sleep 1
done
echo "==> $CONTAINER is back up"

echo "==> verifying CREATE EXTENSION in $DB"
docker exec "$CONTAINER" psql -U acct -d "$DB" -c \
    "DROP EXTENSION IF EXISTS ledger_routed_c; CREATE EXTENSION ledger_routed_c;"
docker exec "$CONTAINER" psql -U acct -d "$DB" -c \
    "SELECT extname, extversion FROM pg_extension WHERE extname = 'ledger_routed_c'"
docker exec "$CONTAINER" psql -U acct -d "$DB" -c \
    "SELECT ledger_routed_c_hello()"
docker exec "$CONTAINER" psql -U acct -d "$DB" -c \
    "SELECT state, count FROM ledger_routed_c_staging_state_counts() ORDER BY state"

echo "==> install complete"
