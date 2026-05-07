-- Daily reconciliation: log-only alerts table + check function +
-- pg_cron schedule.
--
-- Two checks per Part IV §7 (the two that have schema groundwork):
--
--   1. Per-ledger double-entry: SUM(debits) = SUM(credits) per
--      (ledger_kind, currency). Violations indicate either bypass of
--      post_posting_lines or in-place corruption — neither should
--      happen in normal operation.
--
--   2. Reservation over-promise: per (sku, location), the
--      stock_available on-hand must be >= SUM(qty) of active
--      reservations. reserve_inventory() (0011) prevents this at
--      insert time; this check catches direct INSERTs that bypass the
--      function or any future drift.
--
-- Both checks INSERT a row per detected anomaly into
-- reconciliation_alerts. Log-only — no PagerDuty/Slack integration.
-- The schema is structured so future alerting can hang off
-- (alert_kind, payload, created_at) without further migrations.
--
-- Cadence: daily at 00:00 UTC. Table + function created here so the
-- recon function is testable in cargo test against acct_test. The
-- cron schedule is wrapped in DO/EXCEPTION so non-acct DBs migrate
-- cleanly (same pattern as 0001 / 0011).
--
-- close_period() in 0012 calls run_daily_reconciliation() with no
-- arguments and expects INT (alert count). plpgsql late-binding makes
-- the forward reference fine — the function body in 0012 only resolves
-- run_daily_reconciliation() at execution time.

CREATE TABLE reconciliation_alerts (
  id         BIGSERIAL PRIMARY KEY,
  alert_kind TEXT        NOT NULL,
  payload    JSONB       NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX reconciliation_alerts_created
  ON reconciliation_alerts (created_at DESC);

CREATE INDEX reconciliation_alerts_kind_created
  ON reconciliation_alerts (alert_kind, created_at DESC);

CREATE OR REPLACE FUNCTION run_daily_reconciliation() RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
  v_total INT := 0;
  v_step  INT;
BEGIN
  -- Check 1: per-ledger double-entry.
  WITH imbalances AS (
    SELECT ledger_kind, currency,
           SUM(debits_total)::BIGINT  AS dr,
           SUM(credits_total)::BIGINT AS cr
      FROM accounts
     GROUP BY ledger_kind, currency
    HAVING SUM(debits_total) <> SUM(credits_total)
  )
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'double_entry_imbalance',
         jsonb_build_object(
           'ledger_kind', ledger_kind,
           'currency',    currency,
           'debits',      dr,
           'credits',     cr,
           'imbalance',   dr - cr
         )
    FROM imbalances;
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 2: reservation over-promise.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'reservation_over_promise',
         jsonb_build_object(
           'sku_id',      a.sku_id,
           'location_id', a.location_id,
           'on_hand',     (a.debits_total - a.credits_total),
           'reserved',    COALESCE(r.total, 0),
           'deficit',     (a.debits_total - a.credits_total) - COALESCE(r.total, 0)
         )
    FROM accounts a
    LEFT JOIN (
      SELECT sku_id, location_id, SUM(qty)::BIGINT AS total
        FROM inventory_reservations
       WHERE status = 'active'
       GROUP BY sku_id, location_id
    ) r ON r.sku_id = a.sku_id AND r.location_id = a.location_id
   WHERE a.kind = 'stock_available'
     AND NOT a.is_closed
     AND (a.debits_total - a.credits_total) < COALESCE(r.total, 0);
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  RETURN v_total;
END;
$$;

DO $$
BEGIN
  PERFORM cron.schedule(
    'daily_reconciliation',
    '0 0 * * *',
    'SELECT run_daily_reconciliation();'
  );
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'pg_cron unavailable in % (%): %',
               current_database(), SQLSTATE, SQLERRM;
END $$;
