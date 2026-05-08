-- Best-effort down (project convention).
--
-- Note: dropping the partitioned parent cascades to all 24 child
-- partitions automatically. Helper function and pg_cron job are
-- removed too. Tolerant pg_cron unschedule.

DO $$
BEGIN
  PERFORM cron.unschedule('inventory_movements_partition_rollover');
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'pg_cron unschedule skipped (%): %', SQLSTATE, SQLERRM;
END $$;

DROP FUNCTION IF EXISTS _create_inventory_movements_partition(DATE);

DROP INDEX IF EXISTS inventory_movements_posting_line;
DROP INDEX IF EXISTS inventory_movements_product_loc_date;
DROP TABLE IF EXISTS inventory_movements;

DROP TABLE IF EXISTS cost_books;
DROP TABLE IF EXISTS inventory_movement_event_types;
