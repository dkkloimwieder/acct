#!/usr/bin/env bash
# Install the pgrx-built poc_v21_ledger extension into the acct-postgres
# docker container.
#
# Mirrors poc/queue-extension/scripts/install-into-container.sh.
#
# Usage: bash poc/queue-extension-v21/scripts/install-into-container.sh

set -euo pipefail

CONTAINER="${CONTAINER:-acct-postgres}"
CRATE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SO_SRC="$CRATE_DIR/target/release/libpoc_v21_ledger.so"
CONTROL_SRC="$CRATE_DIR/poc_v21_ledger.control"
SQL_SRC="$CRATE_DIR/sql/poc_v21_ledger--0.0.1.sql"

echo "==> rebuilding release .so + schema bindings"
(cd "$CRATE_DIR" && cargo build --release --features pg18 --no-default-features)
(cd "$CRATE_DIR" && cargo pgrx schema pg18 --out "$SQL_SRC")

if [ ! -f "$SO_SRC" ]; then
    echo "missing $SO_SRC — run: cargo build --release --features pg18 --no-default-features"
    exit 1
fi
if [ ! -f "$CONTROL_SRC" ] || [ ! -f "$SQL_SRC" ]; then
    echo "missing control or sql artifact"
    exit 1
fi

PKGLIBDIR=$(docker exec "$CONTAINER" pg_config --pkglibdir)
EXTDIR=$(docker exec "$CONTAINER" pg_config --sharedir)/extension

echo "==> copying .so to $CONTAINER:$PKGLIBDIR/poc_v21_ledger.so"
docker cp "$SO_SRC" "$CONTAINER:$PKGLIBDIR/poc_v21_ledger.so"

echo "==> copying control + sql to $CONTAINER:$EXTDIR/"
docker cp "$CONTROL_SRC" "$CONTAINER:$EXTDIR/"
docker cp "$SQL_SRC" "$CONTAINER:$EXTDIR/"

echo "==> verifying"
docker exec "$CONTAINER" ls -la "$PKGLIBDIR/poc_v21_ledger.so"
docker exec "$CONTAINER" ls -la "$EXTDIR/poc_v21_ledger.control" "$EXTDIR/poc_v21_ledger--0.0.1.sql"
echo "==> install complete"
