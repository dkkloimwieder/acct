#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

: "${DATABASE_URL:=postgres://acct:acct_dev@localhost:5111/acct}"
export DATABASE_URL

exec sqlx migrate run --source db/migrations
