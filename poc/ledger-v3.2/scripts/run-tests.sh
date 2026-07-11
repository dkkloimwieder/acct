#!/usr/bin/env bash
# Test runner for ledger-v3.2.
#
# Installs the ledger_direct extension into the acct-postgres container, then
# runs every acceptance_*/property_* binary under ledger-direct/tests/ against
# poc_v3_2. The extension is shmem-free, so no container restart between
# binaries is needed (tests/common reset_state isolates state within a binary;
# --test-threads=1 keeps binaries honest).
#
# Usage:
#   bash poc/ledger-v3.2/scripts/run-tests.sh
#   BINARIES="acceptance_direct_methods" bash poc/ledger-v3.2/scripts/run-tests.sh
#   FAIL_FAST=0 bash poc/ledger-v3.2/scripts/run-tests.sh
#   SKIP_INSTALL=1 bash poc/ledger-v3.2/scripts/run-tests.sh   # reuse installed .so
#
# Env:
#   PROPTEST_CASES  forwarded to property binaries (default per-binary: 100)

set -uo pipefail

FAIL_FAST="${FAIL_FAST:-1}"
SKIP_INSTALL="${SKIP_INSTALL:-0}"
WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$WORKSPACE_DIR"

# Also run the pure-Rust ledger-core unit/integration tests first — fast, no DB.
echo "==> ledger-core unit tests"
if ! cargo test -p ledger-core 2>&1 | tail -3; then
    echo "==> ledger-core tests failed"
    exit 1
fi

if [ "$SKIP_INSTALL" != "1" ]; then
    bash "$WORKSPACE_DIR/scripts/install-direct.sh"
fi

if [ -n "${BINARIES:-}" ]; then
    binaries="$BINARIES"
else
    binaries="$(ls ledger-direct/tests | grep -E '^(acceptance|property)_.*\.rs$' | sed 's/\.rs$//' | sort)"
fi

echo "==> pre-building test binaries"
cargo build --tests -p ledger-direct 2>&1 | tail -2

reds=()
greens=()
for bin in $binaries; do
    echo
    echo "================================================================"
    echo "[$bin]"
    echo "================================================================"
    if cargo test -p ledger-direct --test "$bin" -- --ignored --test-threads=1 --nocapture; then
        greens+=("$bin")
    else
        reds+=("$bin")
        if [ "$FAIL_FAST" = "1" ]; then
            echo "==> FAIL_FAST=1; stopping at first red binary."
            break
        fi
    fi
done

echo
echo "================================================================"
echo "summary: ${#greens[@]} green / ${#reds[@]} red"
echo "================================================================"
if [ ${#reds[@]} -gt 0 ]; then
    for r in "${reds[@]}"; do echo "  red: $r"; done
    exit 1
fi
