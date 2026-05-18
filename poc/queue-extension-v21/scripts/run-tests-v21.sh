#!/usr/bin/env bash
# Run every v21 acceptance + property test in its own fresh cluster.
#
# Docker-restarts acct-postgres before each binary so cross-binary shmem
# state (staging ring, committer queue, spillover arena, router stats,
# GUCs) can't leak across tests. Within a binary, `--test-threads=1` +
# the existing `tests/common/mod.rs::reset_state` between-tests hook
# keeps tests honest against each other.
#
# Skips bench_m8_*, those are perf workloads not pass/fail regression.
#
# Usage:
#   bash poc/queue-extension-v21/scripts/run-tests-v21.sh
#   FAIL_FAST=0 bash poc/queue-extension-v21/scripts/run-tests-v21.sh
#   BINARIES="property_v21_enqueue_commit acceptance_v21_wo_complete" \
#       bash poc/queue-extension-v21/scripts/run-tests-v21.sh
#
# Env:
#   CONTAINER       docker container name (default acct-postgres)
#   FAIL_FAST       1 = exit on first red binary (default), 0 = run all
#   BINARIES        space-separated test binary names to run; default = all
#   RESTART_WAIT    seconds to sleep after docker restart (default 5)

set -uo pipefail

CONTAINER="${CONTAINER:-acct-postgres}"
FAIL_FAST="${FAIL_FAST:-1}"
RESTART_WAIT="${RESTART_WAIT:-5}"
CRATE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$CRATE_DIR"

if [ -z "${BINARIES:-}" ]; then
    BINARIES=$(ls tests/ | grep -E '^(acceptance_v21_|property_v21_).*\.rs$' | sed 's/\.rs$//' | sort)
fi

# Install the test_hooks-enabled .so so poc_v21_test_* pg_externs are
# exposed (acct-gx1z.1.2). Without this, every test fails on missing
# helper functions. The production .so build (without WITH_TEST_HOOKS)
# stays the default for install-into-container.sh.
echo "==> installing test_hooks-enabled .so"
WITH_TEST_HOOKS=1 bash "$(dirname "$0")/install-into-container.sh" 2>&1 | tail -3

# DROP + CREATE EXTENSION so the new SQL definitions land.
docker exec "$CONTAINER" psql -U acct -d acct_poc_queue_v21 \
    -c "DROP EXTENSION IF EXISTS poc_v21_ledger CASCADE; CREATE EXTENSION poc_v21_ledger;" 2>&1 | tail -3

# Build once up front so per-binary runs don't re-link.
echo "==> pre-building release artifacts (features=pg18,test_hooks)"
cargo build --release --features pg18,test_hooks --no-default-features 2>&1 | tail -3

reds=()
greens=()
total=0

for bin in $BINARIES; do
    total=$((total + 1))
    echo
    echo "================================================================"
    echo "[$total] $bin"
    echo "================================================================"

    docker restart "$CONTAINER" >/dev/null
    sleep "$RESTART_WAIT"

    if cargo test --release --features pg18,test_hooks --no-default-features \
            --test "$bin" -- --ignored --test-threads=1 --nocapture; then
        greens+=("$bin")
    else
        reds+=("$bin")
        if [ "$FAIL_FAST" = "1" ]; then
            echo
            echo "==> FAIL_FAST=1; stopping at first red binary."
            break
        fi
    fi
done

echo
echo "================================================================"
echo "summary: ${#greens[@]} green / ${#reds[@]} red / $total total"
echo "================================================================"
if [ ${#reds[@]} -gt 0 ]; then
    echo "red binaries:"
    for r in "${reds[@]}"; do
        echo "  - $r"
    done
    exit 1
fi
