DO $$
BEGIN
  PERFORM cron.unschedule('reservation_expiry');
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'pg_cron unavailable in % (%): %',
               current_database(), SQLSTATE, SQLERRM;
END $$;
