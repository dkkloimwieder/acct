#!/usr/bin/env bash
# Install the pgrx-built ledger_direct_c (Path C direct) extension into the
# acct-postgres docker container, targeting the poc_v3_1 database.
#
# Modeled after poc/ledger-v3/scripts/install-direct.sh.
#
# Usage:
#   bash poc/ledger-v3.1/scripts/install-direct-c.sh
#   WITH_TEST_HOOKS=1 bash poc/ledger-v3.1/scripts/install-direct-c.sh
#
# WITH_TEST_HOOKS=1 includes the test_hooks feature so any future
# ledger_direct_c_test_* pg_externs are exposed; default OFF for
# production .so builds.

set -euo pipefail

CONTAINER="${CONTAINER:-acct-postgres}"
DB="${DB:-poc_v3_1}"
WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CRATE_DIR="$WORKSPACE_DIR/ledger-direct-c"
SO_SRC="$WORKSPACE_DIR/target/release/libledger_direct_c.so"
CONTROL_SRC="$CRATE_DIR/ledger_direct_c.control"
SQL_SRC="$CRATE_DIR/sql/ledger_direct_c--0.0.1.sql"

FEATURES="pg18"
if [ "${WITH_TEST_HOOKS:-0}" = "1" ]; then
    FEATURES="pg18,test_hooks"
    echo "==> including test_hooks feature (test build)"
fi

mkdir -p "$CRATE_DIR/sql"

echo "==> rebuilding release .so + schema bindings (features=$FEATURES)"
(cd "$WORKSPACE_DIR" && cargo build --release -p ledger-direct-c --features "$FEATURES" --no-default-features)
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

echo "==> copying .so to $CONTAINER:$PKGLIBDIR/ledger_direct_c.so"
docker cp "$SO_SRC" "$CONTAINER:$PKGLIBDIR/ledger_direct_c.so"

echo "==> copying control + sql to $CONTAINER:$EXTDIR/"
docker cp "$CONTROL_SRC" "$CONTAINER:$EXTDIR/"
docker cp "$SQL_SRC" "$CONTAINER:$EXTDIR/"

echo "==> verifying CREATE EXTENSION in $DB"
docker exec "$CONTAINER" psql -U acct -d "$DB" -c \
    "DROP EXTENSION IF EXISTS ledger_direct_c; CREATE EXTENSION ledger_direct_c;"
docker exec "$CONTAINER" psql -U acct -d "$DB" -c \
    "SELECT extname, extversion FROM pg_extension WHERE extname = 'ledger_direct_c'"
docker exec "$CONTAINER" psql -U acct -d "$DB" -c \
    "SELECT ledger_direct_c_hello()"

echo "==> install complete"
