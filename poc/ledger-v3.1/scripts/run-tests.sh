#!/usr/bin/env bash
# Cluster-per-binary test runner for ledger-v3.1 (Path C).
#
# Each test binary runs against a freshly-restarted acct-postgres container so
# cross-binary GUC / catalog state cannot leak. (Path C direct is shmem-free,
# so restart-between-binaries is precautionary.) Within a binary,
# --test-threads=1 + tests/common/mod.rs::reset_state keeps tests honest.
#
# Usage:
#   bash poc/ledger-v3.1/scripts/run-tests.sh                 # --path direct
#   bash poc/ledger-v3.1/scripts/run-tests.sh --path direct
#   FAIL_FAST=0 bash ... run-tests.sh --path direct
#   BINARIES="acceptance_direct_methods" bash ... run-tests.sh --path direct
#
# Env:
#   CONTAINER     docker container name             (default acct-postgres)
#   FAIL_FAST     1 = exit on first red binary      (default 1)
#   RESTART_WAIT  seconds to sleep after restart    (default 5)
#   PROPTEST_CASES  forwarded to property binaries  (default per-binary)
#   BINARIES      space-separated test names; runs only those.
#   SWEEP         1 = run the conservation-invariant sweep (acct-0at4.5) via
#                 `ledger-harness verify` after each green binary, as a
#                 cross-cutting CI teardown post-condition (default 0 — OPT-IN).
#                 Off by default because several binaries seed pool_state
#                 out-of-band (tests/common seed_aggregate) with no backing
#                 trx_line, which the qty/value reconciliation legitimately
#                 flags; enable only for suites that build all state through the
#                 ledger. The always-on wiring is the dedicated
#                 acceptance_conservation_sweep binaries, discovered like any
#                 other test.
#   DSN           Postgres DSN for the SWEEP verify (default poc_v3_1).

set -uo pipefail

CONTAINER="${CONTAINER:-acct-postgres}"
FAIL_FAST="${FAIL_FAST:-1}"
RESTART_WAIT="${RESTART_WAIT:-5}"
SWEEP="${SWEEP:-0}"
DSN="${DSN:-postgres://acct:acct_dev@localhost:5111/poc_v3_1}"
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
    direct) ;;
    *) echo "--path must be direct (got: $PATH_ARG)" >&2; exit 2 ;;
esac

WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$WORKSPACE_DIR"

# Build the harness once for the opt-in post-binary conservation sweep. A build
# failure disables SWEEP rather than sinking the whole run.
HARNESS=""
if [ "$SWEEP" = "1" ]; then
    echo "==> SWEEP=1: building ledger-harness for the post-binary conservation sweep"
    if (cargo build -p ledger-harness --release 2>&1 | tail -2); then
        HARNESS="$WORKSPACE_DIR/target/release/ledger-harness"
    else
        echo "==> ledger-harness build failed; disabling SWEEP"
        SWEEP=0
    fi
fi

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
            # Cross-cutting conservation post-condition on whatever state the
            # binary left (opt-in, acct-0at4.5).
            if [ "$SWEEP" = "1" ] && [ -n "$HARNESS" ]; then
                echo "-- conservation sweep after $crate/$bin --"
                if ! "$HARNESS" verify --dsn "$DSN"; then
                    echo "==> conservation sweep FAILED after $crate/$bin"
                    reds+=("$crate/$bin::sweep")
                    if [ "$FAIL_FAST" = "1" ]; then
                        echo "==> FAIL_FAST=1; stopping at first red binary."
                        return 1
                    fi
                fi
            fi
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
    direct)
        run_one_path ledger-direct-c "$WORKSPACE_DIR/ledger-direct-c" \
            "$WORKSPACE_DIR/scripts/install-direct-c.sh" || true
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
