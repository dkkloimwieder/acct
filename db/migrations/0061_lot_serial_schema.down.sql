-- Best-effort down (project convention; Phase 0/1 has no production data).

DO $$
BEGIN
  PERFORM cron.unschedule('inventory_unit_events_partition_rollover');
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

DROP FUNCTION IF EXISTS _create_inventory_unit_events_partition(DATE);

DROP INDEX IF EXISTS inventory_reservations_unit_ids;
ALTER TABLE inventory_reservations DROP COLUMN IF EXISTS unit_ids;

-- Drop tables (CASCADE drops partitions + indexes + triggers).
DROP TABLE IF EXISTS inventory_unit_events CASCADE;
DROP TABLE IF EXISTS inventory_units CASCADE;

DROP TYPE IF EXISTS inventory_unit_status;
