#!/usr/bin/env bash
# Install the pgrx-built ledger_direct extension into the acct-postgres docker
# container, targeting the poc_v3_2 database.
#
# Usage:
#   bash poc/ledger-v3.2/scripts/install-direct.sh

set -euo pipefail

CONTAINER="${CONTAINER:-acct-postgres}"
DB="${DB:-poc_v3_2}"
WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CRATE_DIR="$WORKSPACE_DIR/ledger-direct"
SO_SRC="$WORKSPACE_DIR/target/release/libledger_direct.so"
CONTROL_SRC="$CRATE_DIR/ledger_direct.control"
SQL_SRC="$CRATE_DIR/sql/ledger_direct--0.0.1.sql"

mkdir -p "$CRATE_DIR/sql"

# The workspace pins pgrx =0.18.1; the matching cargo-pgrx lives in a dedicated
# root (the globally-installed cargo-pgrx tracks other streams' versions):
#   cargo install cargo-pgrx --version 0.18.1 --locked --root ~/.cargo-pgrx/0.18.1
CARGO_PGRX="${CARGO_PGRX:-$HOME/.cargo-pgrx/0.18.1/bin/cargo-pgrx}"
if [ ! -x "$CARGO_PGRX" ]; then
    echo "missing $CARGO_PGRX — install with:"
    echo "  cargo install cargo-pgrx --version 0.18.1 --locked --root ~/.cargo-pgrx/0.18.1"
    exit 1
fi

echo "==> rebuilding release .so + schema bindings"
(cd "$WORKSPACE_DIR" && cargo build --release -p ledger-direct)
(cd "$CRATE_DIR" && "$CARGO_PGRX" pgrx schema --release pg18 --out "$SQL_SRC")

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

echo "==> verifying CREATE EXTENSION in $DB"
docker exec "$CONTAINER" psql -U acct -d "$DB" -c \
    "DROP EXTENSION IF EXISTS ledger_direct; CREATE EXTENSION ledger_direct;"
docker exec "$CONTAINER" psql -U acct -d "$DB" -c \
    "SELECT extname, extversion FROM pg_extension WHERE extname = 'ledger_direct'"
docker exec "$CONTAINER" psql -U acct -d "$DB" -c \
    "SELECT ledger_direct_hello()"

echo "==> install complete"
