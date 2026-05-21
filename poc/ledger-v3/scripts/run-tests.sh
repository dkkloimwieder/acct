#!/usr/bin/env bash
# Cluster-per-binary test runner for ledger-v3.
#
# Each test binary runs in its own freshly-restarted acct-postgres
# container so cross-binary shmem / GUC / BGWorker state cannot leak.
# (Path A is shmem-free so restart-between-binaries is precautionary;
# Path B's shmem queues and committer BGWorkers make it load-bearing.)
# Within a binary, --test-threads=1 + tests/common/mod.rs::reset_state
# keeps tests honest against each other.
#
# Usage:
#   bash poc/ledger-v3/scripts/run-tests.sh                  # --path direct
#   bash poc/ledger-v3/scripts/run-tests.sh --path direct
#   bash poc/ledger-v3/scripts/run-tests.sh --path routed
#   bash poc/ledger-v3/scripts/run-tests.sh --path both
#   FAIL_FAST=0 bash ... scripts/run-tests.sh --path direct
#   BINARIES="acceptance_direct_single_trx property_direct_pipeline" \
#       bash ... scripts/run-tests.sh --path direct
#
# Env:
#   CONTAINER     docker container name             (default acct-postgres)
#   FAIL_FAST     1 = exit on first red binary      (default 1)
#   RESTART_WAIT  seconds to sleep after restart    (default 5)
#   BINARIES      space-separated test names; if set, runs only those
#                 (path must still be specified so the right crate is
#                  installed and the right test dir is searched).

set -uo pipefail

CONTAINER="${CONTAINER:-acct-postgres}"
FAIL_FAST="${FAIL_FAST:-1}"
RESTART_WAIT="${RESTART_WAIT:-5}"
PATH_ARG="direct"

while [ $# -gt 0 ]; do
    case "$1" in
        --path) PATH_ARG="$2"; shift 2 ;;
        --path=*) PATH_ARG="${1#--path=}"; shift ;;
        -h|--help)
            sed -n '2,/^$/p' "$0"; exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

case "$PATH_ARG" in
    direct|routed|both) ;;
    *) echo "--path must be direct | routed | both (got: $PATH_ARG)" >&2; exit 2 ;;
esac

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# ── Per-crate setup ────────────────────────────────────────────────
# install_crate <crate-name> <install-script>
install_crate() {
    local crate="$1"
    local script="$2"
    if [ ! -x "$script" ]; then
        echo "==> [$crate] install script not present; skipping"
        return 1
    fi
    echo "==> [$crate] installing test_hooks-enabled .so via $script"
    WITH_TEST_HOOKS=1 bash "$script" 2>&1 | tail -3
}

# discover_binaries <crate-dir> <name-pattern>
#   echoes whitespace-separated test binary names matching
#   "^(acceptance_<pat>|property_<pat>)_.*\.rs$" under <crate-dir>/tests/.
discover_binaries() {
    local crate_dir="$1"
    local pat="$2"
    if [ ! -d "$crate_dir/tests" ]; then return; fi
    ls "$crate_dir/tests" \
        | grep -E "^(acceptance_${pat}|property_${pat})_.*\.rs$" \
        | sed 's/\.rs$//' \
        | sort
}

# run_one_path <crate-name> <crate-dir> <name-pattern> <install-script>
run_one_path() {
    local crate="$1"
    local crate_dir="$2"
    local pat="$3"
    local install_script="$4"

    if ! install_crate "$crate" "$install_script"; then
        echo "==> [$crate] not installable; skipping path"
        return 0
    fi

    local binaries
    if [ -n "${BINARIES:-}" ]; then
        binaries="$BINARIES"
    else
        binaries="$(discover_binaries "$crate_dir" "$pat")"
    fi

    if [ -z "$binaries" ]; then
        echo "==> [$crate] no test binaries discovered for pattern $pat"
        return 0
    fi

    echo "==> [$crate] pre-building test artifacts (features=pg18,test_hooks)"
    (cd "$crate_dir" && cargo build --tests --features pg18,test_hooks \
        --no-default-features 2>&1 | tail -3)

    local total=0
    for bin in $binaries; do
        total=$((total + 1))
        echo
        echo "================================================================"
        echo "[$crate #$total] $bin"
        echo "================================================================"

        docker restart "$CONTAINER" >/dev/null
        sleep "$RESTART_WAIT"

        if (cd "$crate_dir" && cargo test --features pg18,test_hooks \
                --no-default-features --test "$bin" -- \
                --ignored --test-threads=1 --nocapture); then
            greens+=("$crate/$bin")
        else
            reds+=("$crate/$bin")
            if [ "$FAIL_FAST" = "1" ]; then
                echo
                echo "==> FAIL_FAST=1; stopping at first red binary."
                return 1
            fi
        fi
    done
}

reds=()
greens=()

case "$PATH_ARG" in
    direct|both)
        run_one_path ledger-direct \
            "$REPO_ROOT/ledger-direct" \
            direct \
            "$REPO_ROOT/scripts/install-direct.sh" \
            || true
        ;;
esac

case "$PATH_ARG" in
    routed|both)
        run_one_path ledger-routed \
            "$REPO_ROOT/ledger-routed" \
            routed \
            "$REPO_ROOT/scripts/install-routed.sh" \
            || true
        ;;
esac

echo
echo "================================================================"
echo "summary: ${#greens[@]} green / ${#reds[@]} red"
echo "================================================================"
if [ ${#reds[@]} -gt 0 ]; then
    echo "red binaries:"
    for r in "${reds[@]}"; do
        echo "  - $r"
    done
    exit 1
fi
