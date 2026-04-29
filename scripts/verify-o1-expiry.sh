#!/usr/bin/env bash
set -euo pipefail

# O1 verification — confirm the pg_cron-scheduled reservation expiry
# UPDATE is firing on the dev `acct` database. pg_cron only runs in
# the database named by `cron.database_name` (set to 'acct' in
# docker-compose.yml), so this check necessarily targets `acct` —
# the ephemeral acct_test / acct_ci DBs don't have pg_cron at all.
#
# What it does:
#   1. Inserts a sku, location, and sales_order into `acct`.
#   2. Inserts a reservation against that sku/location with
#      `expires_at` 1 minute in the past.
#   3. Waits ~35s (one pg_cron tick at a 30s cadence + a few seconds
#      slack for scheduling jitter).
#   4. Asserts the reservation flipped from 'active' to 'expired'.
#   5. Prints the most recent cron.job_run_details for visibility.
#   6. Cleans up: removes the reservation, sales_order, sku, location.
#
# Idempotent: every run uses fresh UUIDs and cleans up after itself.
# Safe to run on a populated `acct` DB; touches only its own rows.

cd "$(dirname "$0")/.."

PG_USER="${PG_USER:-acct}"
ADMIN_DB="${ADMIN_DB:-acct}"

psql_cmd() {
  docker compose exec -T postgres psql -U "$PG_USER" -d "$ADMIN_DB" -v ON_ERROR_STOP=1 "$@"
}

echo "==> Inserting test sku, location, sales_order, reservation"
TEST_IDS=$(psql_cmd -At <<'SQL'
WITH new_sku AS (
  INSERT INTO skus (code, uom, standard_cost)
  VALUES ('VERIFY-O1-' || substr(gen_random_uuid()::text, 1, 8), 'EA', 1)
  RETURNING id
),
new_loc AS (
  INSERT INTO locations (code, name)
  VALUES ('VERIFY-O1-' || substr(gen_random_uuid()::text, 1, 8), 'O1 verify location')
  RETURNING id
),
new_so AS (
  INSERT INTO sales_orders (status) VALUES ('open')
  RETURNING id
),
new_rsv AS (
  INSERT INTO inventory_reservations
    (sku_id, location_id, qty, so_id, so_line_id, expires_at)
  SELECT new_sku.id, new_loc.id, 1, new_so.id, gen_random_uuid(),
         clock_timestamp() - INTERVAL '1 minute'
    FROM new_sku, new_loc, new_so
  RETURNING id
)
SELECT
  (SELECT id FROM new_sku) || '|' ||
  (SELECT id FROM new_loc) || '|' ||
  (SELECT id FROM new_so)  || '|' ||
  (SELECT id FROM new_rsv);
SQL
)
SKU_ID=$(echo "$TEST_IDS" | cut -d'|' -f1)
LOC_ID=$(echo "$TEST_IDS" | cut -d'|' -f2)
SO_ID=$(echo "$TEST_IDS" | cut -d'|' -f3)
RSV_ID=$(echo "$TEST_IDS" | cut -d'|' -f4)
echo "    sku=$SKU_ID  loc=$LOC_ID  so=$SO_ID  rsv=$RSV_ID"

cleanup() {
  echo "==> Cleanup"
  docker compose exec -T postgres psql -U "$PG_USER" -d "$ADMIN_DB" >/dev/null 2>&1 <<SQL || true
DELETE FROM inventory_reservations WHERE id = '$RSV_ID';
DELETE FROM sales_orders            WHERE id = '$SO_ID';
DELETE FROM locations               WHERE id = '$LOC_ID';
DELETE FROM skus                    WHERE id = '$SKU_ID';
SQL
}
trap cleanup EXIT

echo "==> Waiting 35s for one cron tick (cadence is 30s + jitter)"
sleep 35

echo "==> Checking reservation status"
STATUS=$(psql_cmd -At -c "SELECT status::text FROM inventory_reservations WHERE id = '$RSV_ID';")
RESOLVED_AT=$(psql_cmd -At -c "SELECT resolved_at::text FROM inventory_reservations WHERE id = '$RSV_ID';")
echo "    status='$STATUS'  resolved_at='$RESOLVED_AT'"

if [ "$STATUS" != "expired" ]; then
  echo "FAIL: expected 'expired', got '$STATUS'" >&2
  exit 1
fi
if [ -z "$RESOLVED_AT" ]; then
  echo "FAIL: resolved_at is NULL on an expired row" >&2
  exit 1
fi

echo "==> Recent cron.job_run_details for reservation_expiry"
psql_cmd <<'SQL'
SELECT runid, status, return_message, start_time, end_time
  FROM cron.job_run_details
 WHERE jobid = (SELECT jobid FROM cron.job WHERE jobname = 'reservation_expiry')
 ORDER BY runid DESC LIMIT 5;
SQL

echo
echo "OK: O1 reservation expiry job is firing and flipping rows."
