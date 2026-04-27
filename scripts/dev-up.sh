#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

docker compose up -d --build

echo "Waiting for postgres to be ready..."
for i in {1..60}; do
  if docker compose exec -T postgres pg_isready -U acct -d acct >/dev/null 2>&1; then
    echo "Postgres ready."
    break
  fi
  if [[ $i -eq 60 ]]; then
    echo "ERROR: postgres did not become ready within 60s"
    docker compose logs postgres
    exit 1
  fi
  sleep 1
done

io_method=$(docker compose exec -T postgres psql -U acct -d acct -tAc "SHOW io_method" | tr -d '[:space:]')
echo "io_method = $io_method"
if [[ "$io_method" != "io_uring" ]]; then
  echo "ERROR: expected io_method=io_uring, got '$io_method'"
  exit 1
fi

echo "Installed extensions:"
docker compose exec -T postgres psql -U acct -d acct -c \
  "SELECT extname, extversion FROM pg_extension WHERE extname IN ('pg_stat_statements','pg_cron') ORDER BY extname"

echo
echo "Connect: psql 'postgres://acct:acct_dev@localhost:5111/acct'"
