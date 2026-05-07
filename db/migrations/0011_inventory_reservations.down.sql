DO $$
BEGIN
  PERFORM cron.unschedule('reservation_expiry');
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'pg_cron unavailable in % (%): %',
               current_database(), SQLSTATE, SQLERRM;
END $$;

DROP FUNCTION IF EXISTS reserve_inventory(UUID, UUID, BIGINT, UUID, UUID, TIMESTAMPTZ, BIGINT);
DROP TABLE IF EXISTS inventory_reservations;
