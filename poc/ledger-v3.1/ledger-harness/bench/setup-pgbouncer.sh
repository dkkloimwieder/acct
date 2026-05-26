#!/usr/bin/env bash
# Stand up (or tear down) a pgbouncer transaction pool in front of poc_v3_1 so
# the 1000-caller scenarios (S5/S7/S8) can drive that many logical callers over
# a bounded set of server backends — the dev container's io_uring memlock
# ceiling can't hold 1000 direct backends (acct-8cn2).
#
# The pooler joins the acct_default docker network and connects to the
# acct-postgres container on its internal port 5432; it is published on host
# port 6432. Point the harness at it with:
#     --dsn postgres://acct:acct_dev@localhost:6432/poc_v3_1
# (the bench runners do this automatically for s5/s6/s7/s8 via common.sh).
#
# Transaction pooling means each in-flight tx borrows a server backend for the
# tx's duration only, so 1000 fire-and-forget enqueue callers (routed) or short
# per-call submits (direct) multiplex onto DEFAULT_POOL_SIZE backends.
#
# AUTH_TYPE=trust on the client side keeps this dev-only and avoids SCRAM
# verifier plumbing; the server-side connection still authenticates as acct.
#
# Usage:
#   bash bench/setup-pgbouncer.sh up      # start (idempotent)
#   bash bench/setup-pgbouncer.sh down    # stop + remove
#   bash bench/setup-pgbouncer.sh status  # show state + test a passthrough query

set -euo pipefail

IMAGE="${PGBOUNCER_IMAGE:-edoburu/pgbouncer:latest}"
NAME="${PGBOUNCER_NAME:-poc-v3-1-pgbouncer}"
NETWORK="${PGBOUNCER_NETWORK:-acct_default}"
PG_CONTAINER="${PG_CONTAINER:-acct-postgres}"
HOST_PORT="${PGBOUNCER_HOST_PORT:-6432}"
POOL_MODE="${POOL_MODE:-transaction}"
MAX_CLIENT_CONN="${MAX_CLIENT_CONN:-2000}"
DEFAULT_POOL_SIZE="${DEFAULT_POOL_SIZE:-64}"

cmd="${1:-up}"

case "$cmd" in
  up)
    if docker ps -a --format '{{.Names}}' | grep -qx "$NAME"; then
      echo "==> $NAME already exists; (re)starting"
      docker start "$NAME" >/dev/null
    else
      echo "==> starting pgbouncer ($IMAGE) on host port $HOST_PORT → $PG_CONTAINER:5432"
      docker run -d \
        --name "$NAME" \
        --network "$NETWORK" \
        -p "${HOST_PORT}:5432" \
        -e DB_HOST="$PG_CONTAINER" \
        -e DB_PORT=5432 \
        -e DB_USER=acct \
        -e DB_PASSWORD=acct_dev \
        -e POOL_MODE="$POOL_MODE" \
        -e AUTH_TYPE=trust \
        -e MAX_CLIENT_CONN="$MAX_CLIENT_CONN" \
        -e DEFAULT_POOL_SIZE="$DEFAULT_POOL_SIZE" \
        -e IGNORE_STARTUP_PARAMETERS="extra_float_digits,options" \
        "$IMAGE" >/dev/null
    fi
    echo "==> pgbouncer up: postgres://acct:acct_dev@localhost:${HOST_PORT}/poc_v3_1 (pool_mode=$POOL_MODE, pool_size=$DEFAULT_POOL_SIZE)"
    ;;
  down)
    echo "==> removing $NAME"
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    ;;
  status)
    docker ps --filter "name=$NAME" --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' || true
    echo "==> passthrough test:"
    PGPASSWORD=acct_dev psql "postgres://acct:acct_dev@localhost:${HOST_PORT}/poc_v3_1" \
      -tAc "SELECT 'pgbouncer→poc_v3_1 ok', current_database()" 2>&1 || \
      echo "    (passthrough query failed — is pgbouncer up and poc_v3_1 reachable?)"
    ;;
  *)
    echo "usage: $0 {up|down|status}" >&2
    exit 2
    ;;
esac
