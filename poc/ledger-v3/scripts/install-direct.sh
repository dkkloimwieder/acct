#!/usr/bin/env bash
# Install the pgrx-built ledger_direct (Path A) extension into the
# acct-postgres docker container.
#
# Modeled after poc/queue-extension-v21/scripts/install-into-container.sh.
#
# Usage:
#   bash poc/ledger-v3/scripts/install-direct.sh
#   WITH_TEST_HOOKS=1 bash poc/ledger-v3/scripts/install-direct.sh
#
# WITH_TEST_HOOKS=1 includes the test_hooks feature so any future
# ledger_direct_test_* pg_externs are exposed. Required by test
# binaries that need state-mutating helpers; default OFF for
# production .so builds.

set -euo pipefail

CONTAINER="${CONTAINER:-acct-postgres}"
CRATE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/ledger-direct"
SO_SRC="$CRATE_DIR/../target/release/libledger_direct.so"
CONTROL_SRC="$CRATE_DIR/ledger_direct.control"
SQL_SRC="$CRATE_DIR/sql/ledger_direct--0.0.1.sql"

FEATURES="pg18"
if [ "${WITH_TEST_HOOKS:-0}" = "1" ]; then
    FEATURES="pg18,test_hooks"
    echo "==> including test_hooks feature (test build)"
fi

mkdir -p "$CRATE_DIR/sql"

echo "==> rebuilding release .so + schema bindings (features=$FEATURES)"
(cd "$CRATE_DIR/.." && cargo build --release -p ledger-direct --features "$FEATURES" --no-default-features)
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

echo "==> copying .so to $CONTAINER:$PKGLIBDIR/ledger_direct.so"
docker cp "$SO_SRC" "$CONTAINER:$PKGLIBDIR/ledger_direct.so"

echo "==> copying control + sql to $CONTAINER:$EXTDIR/"
docker cp "$CONTROL_SRC" "$CONTAINER:$EXTDIR/"
docker cp "$SQL_SRC" "$CONTAINER:$EXTDIR/"

echo "==> verifying CREATE EXTENSION in poc_v3"
docker exec "$CONTAINER" psql -U acct -d poc_v3 -c \
    "DROP EXTENSION IF EXISTS ledger_direct; CREATE EXTENSION ledger_direct;"
docker exec "$CONTAINER" psql -U acct -d poc_v3 -c \
    "SELECT extname, extversion FROM pg_extension WHERE extname = 'ledger_direct'"
docker exec "$CONTAINER" psql -U acct -d poc_v3 -c \
    "SELECT ledger_direct_hello()"

echo "==> install complete"
