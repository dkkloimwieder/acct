DO $$
BEGIN
  PERFORM cron.unschedule('daily_reconciliation');
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'pg_cron unavailable in % (%): %',
               current_database(), SQLSTATE, SQLERRM;
END $$;

DROP FUNCTION IF EXISTS run_daily_reconciliation();
DROP TABLE IF EXISTS reconciliation_alerts;
