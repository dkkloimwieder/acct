#!/usr/bin/env bash
# Tune the shared dev postgres for ledger-v3 Phase 3 runs.
#
# WARNING: this changes a CLUSTER-WIDE setting (max_connections) that
# affects all databases inside acct-postgres, not just poc_v3. The bump
# is harmless for the main acct database (no functional regression) but
# costs a few hundred MB of process slots. Idempotent — re-running is
# safe.
#
# Existing shared_buffers (8GB) and work_mem (64MB) are already
# sized for the workload; this script touches only max_connections.
#
# IO_URING MEMLOCK CEILING (observed 2026-05-21): bumping
# max_connections=1100 against io_method=io_uring exhausts the
# container's RLIMIT_MEMLOCK on each backend's io_uring ring setup
# ("could not setup io_uring queue: Cannot allocate memory"), and
# postgres refuses to start. Stay at TARGET=500 (fits ~200 callers +
# sampler/collector + headroom) until the container's memlock limit
# is raised or io_method is switched for the run. Filed as part of
# acct-7wye observations.

set -euo pipefail

CONTAINER="${CONTAINER:-acct-postgres}"
SUPERUSER="${SUPERUSER:-acct}"
TARGET="${TARGET:-500}"

current=$(docker exec "$CONTAINER" psql -U "$SUPERUSER" -d postgres -tAc "SHOW max_connections;")
echo "==> current max_connections=$current; target=$TARGET"

if [ "$current" -ge "$TARGET" ]; then
    echo "    already >= target; nothing to do"
    exit 0
fi

echo "==> ALTER SYSTEM SET max_connections = $TARGET"
docker exec "$CONTAINER" psql -U "$SUPERUSER" -d postgres -c \
    "ALTER SYSTEM SET max_connections = $TARGET;"

echo "==> restarting $CONTAINER"
docker restart "$CONTAINER" >/dev/null

echo "==> waiting for postgres"
for _ in $(seq 1 30); do
    if docker exec "$CONTAINER" pg_isready -U "$SUPERUSER" >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

new=$(docker exec "$CONTAINER" psql -U "$SUPERUSER" -d postgres -tAc "SHOW max_connections;")
echo "==> max_connections is now $new"
