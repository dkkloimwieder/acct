DO $$
BEGIN
  PERFORM cron.unschedule('daily_reconciliation');
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'pg_cron unschedule skipped (%): %', SQLSTATE, SQLERRM;
END $$;

DROP FUNCTION IF EXISTS run_daily_reconciliation();

DROP INDEX IF EXISTS reconciliation_alerts_kind_created;
DROP INDEX IF EXISTS reconciliation_alerts_created;
DROP TABLE IF EXISTS reconciliation_alerts;
