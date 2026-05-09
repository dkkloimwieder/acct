-- Best-effort down (project convention; Phase 0/1 has no production data).

-- pg_cron unschedule (tolerant for non-cron databases / fresh installs).
DO $$
BEGIN
  PERFORM cron.unschedule('inventory_lots_partition_rollover');
EXCEPTION WHEN OTHERS THEN
  NULL;
END $$;

-- Reservations.
DROP INDEX IF EXISTS inventory_reservations_lot;
ALTER TABLE inventory_reservations
  DROP CONSTRAINT IF EXISTS inventory_reservations_lot_specific_requires_lot;
ALTER TABLE inventory_reservations
  DROP COLUMN IF EXISTS lot_specific,
  DROP COLUMN IF EXISTS lot_id;

-- Accounts: restore the pre-E2.1 partial UKs without COALESCE(lot_id, 0).
DROP INDEX IF EXISTS accounts_lot_id;
DROP INDEX IF EXISTS accounts_value_loc_uk;
CREATE UNIQUE INDEX accounts_value_loc_uk
  ON accounts (kind, sku_id, location_id, currency)
  WHERE ledger_kind = 'value'
    AND kind IN ('inv_value_raw', 'inv_value_fg')
    AND sku_id IS NOT NULL
    AND NOT is_closed;
DROP INDEX IF EXISTS accounts_stock_avail_uk;
CREATE UNIQUE INDEX accounts_stock_avail_uk
  ON accounts (sku_id, location_id)
  WHERE kind = 'stock_available' AND NOT is_closed;
ALTER TABLE accounts DROP CONSTRAINT IF EXISTS accounts_lot_id_kind;
ALTER TABLE accounts DROP COLUMN IF EXISTS lot_id;

-- Lot subledger tables (drop CASCADE pulls the partitions).
DROP FUNCTION IF EXISTS _create_inventory_lot_events_partition(DATE);
DROP FUNCTION IF EXISTS _create_inventory_lots_partition(DATE);
DROP FUNCTION IF EXISTS _inventory_lot_remaining_qty(BIGINT, DATE);
DROP TABLE IF EXISTS inventory_lot_events CASCADE;
DROP TABLE IF EXISTS inventory_lots CASCADE;
DROP FUNCTION IF EXISTS block_inventory_lot_modifications();

-- skus.tracked_by + enum.
ALTER TABLE skus DROP COLUMN IF EXISTS tracked_by;
DROP TYPE IF EXISTS inventory_tracking;
