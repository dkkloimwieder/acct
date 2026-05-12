#!/usr/bin/env bash
# Install the pgrx-built ledger_extension into the acct-postgres docker container.
#
# Host produces target/release/libledger_extension.so against PG18 dev headers
# matching the container's PG18 (postgresql-18 18.0-1.pgdg). Linux x86_64,
# glibc requirements topping out at GLIBC_2.34 — container's glibc 2.41
# is forward-compatible.
#
# Postgres loads dynamic libraries via the control file's module_pathname.
# Our control sets module_pathname = '$libdir/ledger_extension', so the .so
# must land under pkglibdir as `ledger_extension.so` (no `lib` prefix).
#
# Usage: bash poc/ledger-extension/scripts/install-into-container.sh

set -euo pipefail

CONTAINER="${CONTAINER:-acct-postgres}"
CRATE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SO_SRC="$CRATE_DIR/target/release/libledger_extension.so"
CONTROL_SRC="$CRATE_DIR/ledger_extension.control"
SQL_SRC="$CRATE_DIR/sql/ledger_extension--0.0.1.sql"

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

echo "==> copying .so to $CONTAINER:$PKGLIBDIR/ledger_extension.so"
docker cp "$SO_SRC" "$CONTAINER:$PKGLIBDIR/ledger_extension.so"

echo "==> copying control + sql to $CONTAINER:$EXTDIR/"
docker cp "$CONTROL_SRC" "$CONTAINER:$EXTDIR/"
docker cp "$SQL_SRC" "$CONTAINER:$EXTDIR/"

echo "==> verifying"
docker exec "$CONTAINER" ls -la "$PKGLIBDIR/ledger_extension.so"
docker exec "$CONTAINER" ls -la "$EXTDIR/ledger_extension.control" "$EXTDIR/ledger_extension--0.0.1.sql"
echo "==> install complete"
