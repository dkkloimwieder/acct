#!/usr/bin/env bash
# Cluster-per-binary test runner for ledger-v3.1 (Path C).
#
# Each test binary runs against a freshly-restarted acct-postgres container so
# cross-binary shmem / GUC / BGWorker state cannot leak. (Path C direct is
# shmem-free, so restart-between-binaries is precautionary; Path C routed's
# shmem queues + committer BGWorkers make it load-bearing.) Within a binary,
# --test-threads=1 + tests/common/mod.rs::reset_state keeps tests honest.
#
# Usage:
#   bash poc/ledger-v3.1/scripts/run-tests.sh                 # --path direct
#   bash poc/ledger-v3.1/scripts/run-tests.sh --path direct
#   bash poc/ledger-v3.1/scripts/run-tests.sh --path routed
#   bash poc/ledger-v3.1/scripts/run-tests.sh --path both
#   FAIL_FAST=0 bash ... run-tests.sh --path direct
#   BINARIES="acceptance_direct_methods" bash ... run-tests.sh --path direct
#
# Env:
#   CONTAINER     docker container name             (default acct-postgres)
#   FAIL_FAST     1 = exit on first red binary      (default 1)
#   RESTART_WAIT  seconds to sleep after restart    (default 5)
#   PROPTEST_CASES  forwarded to property binaries  (default per-binary)
#   BINARIES      space-separated test names; runs only those.

set -uo pipefail

CONTAINER="${CONTAINER:-acct-postgres}"
FAIL_FAST="${FAIL_FAST:-1}"
RESTART_WAIT="${RESTART_WAIT:-5}"
PATH_ARG="direct"

while [ $# -gt 0 ]; do
    case "$1" in
        --path) PATH_ARG="$2"; shift 2 ;;
        --path=*) PATH_ARG="${1#--path=}"; shift ;;
        -h|--help) sed -n '2,/^$/p' "$0"; exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

case "$PATH_ARG" in
    direct|routed|both) ;;
    *) echo "--path must be direct | routed | both (got: $PATH_ARG)" >&2; exit 2 ;;
esac

WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$WORKSPACE_DIR"

install_crate() {
    local crate="$1" script="$2"
    if [ ! -f "$script" ]; then
        echo "==> [$crate] install script not present ($script); skipping path"
        return 1
    fi
    echo "==> [$crate] installing test_hooks-enabled .so via $script"
    WITH_TEST_HOOKS=1 bash "$script" 2>&1 | tail -3
}

# discover_binaries <crate-dir> — acceptance_*.rs / property_*.rs under tests/.
discover_binaries() {
    local crate_dir="$1"
    [ -d "$crate_dir/tests" ] || return
    ls "$crate_dir/tests" \
        | grep -E '^(acceptance|property)_.*\.rs$' \
        | sed 's/\.rs$//' \
        | sort
}

reds=()
greens=()

run_one_path() {
    local crate="$1" crate_dir="$2" install_script="$3"

    if ! install_crate "$crate" "$install_script"; then
        return 0
    fi

    local binaries
    if [ -n "${BINARIES:-}" ]; then
        binaries="$BINARIES"
    else
        binaries="$(discover_binaries "$crate_dir")"
    fi
    if [ -z "$binaries" ]; then
        echo "==> [$crate] no test binaries discovered"
        return 0
    fi

    echo "==> [$crate] pre-building test artifacts (features=pg18,test_hooks)"
    (cd "$crate_dir" && cargo build --tests --features pg18,test_hooks --no-default-features 2>&1 | tail -3)

    local total=0
    for bin in $binaries; do
        total=$((total + 1))
        echo
        echo "================================================================"
        echo "[$crate #$total] $bin"
        echo "================================================================"
        docker restart "$CONTAINER" >/dev/null
        sleep "$RESTART_WAIT"
        if (cd "$crate_dir" && cargo test --features pg18,test_hooks --no-default-features \
                --test "$bin" -- --ignored --test-threads=1 --nocapture); then
            greens+=("$crate/$bin")
        else
            reds+=("$crate/$bin")
            if [ "$FAIL_FAST" = "1" ]; then
                echo "==> FAIL_FAST=1; stopping at first red binary."
                return 1
            fi
        fi
    done
}

case "$PATH_ARG" in
    direct|both)
        run_one_path ledger-direct-c "$WORKSPACE_DIR/ledger-direct-c" \
            "$WORKSPACE_DIR/scripts/install-direct-c.sh" || true
        ;;
esac
case "$PATH_ARG" in
    routed|both)
        run_one_path ledger-routed-c "$WORKSPACE_DIR/ledger-routed-c" \
            "$WORKSPACE_DIR/scripts/install-routed-c.sh" || true
        ;;
esac

echo
echo "================================================================"
echo "summary: ${#greens[@]} green / ${#reds[@]} red"
echo "================================================================"
if [ ${#reds[@]} -gt 0 ]; then
    echo "red binaries:"
    for r in "${reds[@]}"; do echo "  - $r"; done
    exit 1
fi
