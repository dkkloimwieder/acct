#!/usr/bin/env bash
# Test runner for ledger-v3.2.
#
# Installs the ledger_direct extension into the acct-postgres container, then
# runs every acceptance_*/property_* binary under the DB-backed test crates
# (ledger-direct, ledger-feed) against poc_v3_2. The extension is shmem-free,
# so no container restart between binaries is needed (tests/common reset_state
# isolates state within a binary; --test-threads=1 keeps binaries honest).
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
TEST_CRATES="ledger-direct ledger-feed"
WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$WORKSPACE_DIR"

# Which test crate owns a binary of this name?
crate_of() {
    local bin="$1" crate
    for crate in $TEST_CRATES; do
        if [ -f "$crate/tests/$bin.rs" ]; then
            echo "$crate"
            return 0
        fi
    done
    return 1
}

# Also run the pure-Rust unit tests first — fast, no DB.
echo "==> ledger-core unit tests"
if ! cargo test -p ledger-core 2>&1 | tail -3; then
    echo "==> ledger-core tests failed"
    exit 1
fi
echo "==> ledger-bench unit tests"
if ! cargo test -p ledger-bench 2>&1 | tail -3; then
    echo "==> ledger-bench tests failed"
    exit 1
fi

if [ "$SKIP_INSTALL" != "1" ]; then
    bash "$WORKSPACE_DIR/scripts/install-direct.sh"
fi

if [ -n "${BINARIES:-}" ]; then
    binaries="$BINARIES"
else
    binaries="$(for crate in $TEST_CRATES; do
        ls "$crate/tests" | grep -E '^(acceptance|property)_.*\.rs$' | sed 's/\.rs$//'
    done | sort)"
fi

echo "==> pre-building test binaries"
# shellcheck disable=SC2046
cargo build --tests $(for c in $TEST_CRATES; do echo "-p $c"; done) 2>&1 | tail -2

reds=()
greens=()
for bin in $binaries; do
    crate="$(crate_of "$bin")" || { echo "unknown test binary: $bin"; reds+=("$bin"); continue; }
    echo
    echo "================================================================"
    echo "[$crate :: $bin]"
    echo "================================================================"
    if cargo test -p "$crate" --test "$bin" -- --ignored --test-threads=1 --nocapture; then
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
