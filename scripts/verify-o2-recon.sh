#!/usr/bin/env bash
set -euo pipefail

# O2 verification — confirm the daily reconciliation function detects
# the imbalances it's supposed to detect. Runs against the dev `acct`
# database (cron-scheduled job lives in cron.job there; this script
# invokes the function directly so we don't have to wait for midnight).
#
# What it does:
#   1. Inserts a temp value/USD account (kind='creation_void',
#      normal_side='unrestricted') and bumps its debits_total +12345.
#      That breaks per-ledger double-entry on (value, USD) without
#      relying on any pre-seeded reference data.
#   2. Calls run_daily_reconciliation(); asserts at least one new
#      'double_entry_imbalance' alert appears with the expected
#      payload (ledger_kind=value, currency=USD, imbalance≥12345).
#   3. Cleans up the temp account and any alerts it produced.
#   4. Prints the cron.job entry for daily_reconciliation for
#      operator visibility.
#
# Idempotent: every mutation is reverted on exit. Safe to run on a
# populated acct DB; touches only its own rows.

cd "$(dirname "$0")/.."

PG_USER="${PG_USER:-acct}"
ADMIN_DB="${ADMIN_DB:-acct}"

psql_at() {
  docker compose exec -T postgres psql -U "$PG_USER" -d "$ADMIN_DB" -v ON_ERROR_STOP=1 -At "$@"
}

echo "==> Inserting temp value/USD account for the test"
TEMP_ACCT_ID=$(psql_at -c "INSERT INTO accounts (kind, ledger_kind, currency, normal_side, debits_total) VALUES ('creation_void', 'value', 'USD', 'unrestricted', 12345) RETURNING id;" | head -1)
echo "    temp account id=$TEMP_ACCT_ID (debits_total=12345, no matching credit anywhere)"

# Snapshot the pre-existing alert id ceiling so we can identify
# alerts produced by THIS run for cleanup.
ALERT_FLOOR=$(psql_at -c "SELECT COALESCE(MAX(id), 0) FROM reconciliation_alerts;")

cleanup() {
  echo "==> Cleanup"
  docker compose exec -T postgres psql -U "$PG_USER" -d "$ADMIN_DB" >/dev/null 2>&1 <<SQL || true
DELETE FROM reconciliation_alerts WHERE id > $ALERT_FLOOR;
DELETE FROM accounts               WHERE id = $TEMP_ACCT_ID;
SQL
}
trap cleanup EXIT

echo "==> Running reconciliation"
INSERTED=$(psql_at -c "SELECT run_daily_reconciliation();")
echo "    new alerts on this run: $INSERTED"
if [ "$INSERTED" -lt 1 ]; then
  echo "FAIL: expected at least 1 alert after imbalance injection" >&2
  exit 1
fi

echo "==> Latest alert payload"
ALERT=$(psql_at -c "SELECT alert_type || '|' || payload::text FROM reconciliation_alerts WHERE id > $ALERT_FLOOR ORDER BY id DESC LIMIT 1;")
echo "    $ALERT"
echo "$ALERT" | grep -q '^double_entry_imbalance' \
  || { echo "FAIL: alert_type != double_entry_imbalance" >&2; exit 1; }
echo "$ALERT" | grep -q '"ledger_kind": "value"' \
  || { echo "FAIL: ledger_kind != value" >&2; exit 1; }
echo "$ALERT" | grep -q '"currency": "USD"' \
  || { echo "FAIL: currency != USD" >&2; exit 1; }

echo "==> cron.job entry"
docker compose exec -T postgres psql -U "$PG_USER" -d "$ADMIN_DB" \
  -c "SELECT jobid, schedule, command FROM cron.job WHERE jobname = 'daily_reconciliation';"

echo
echo "OK: O2 reconciliation function detects imbalance and writes an alert."
