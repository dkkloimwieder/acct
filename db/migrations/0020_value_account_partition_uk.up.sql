-- acct-nfr: partition value accounts by the same dimensions as their
-- qty-side counterparts.
--
-- Per acct-5re's decision (option b) and doc §4.1's partition map:
--   inv_value_raw  -> (sku, location, currency)
--   inv_value_wip  -> (sku, routing_op, currency)
--   inv_value_fg   -> (sku, location, currency)
--   price_trueup_* -> (sku, currency)
--   currency-only kinds (cogs, cash, ar, ap, revenue, variance_*,
--                       labor_*, oh_*, fx_revaluation, inv_adj_expense,
--                       creation_void with currency)
--                  -> no SKU partition (intentionally aggregated)
--
-- Drops the migration 0006 catch-all `accounts_value_uk` and replaces
-- it with three conditional unique indexes plus two CHECK constraints
-- enforcing that the partition columns are populated for the kinds
-- that need them.
--
-- This migration is purely structural (no data movement). Phase 0
-- has no production data; the dev fixture (db/fixtures/small/seed.sql)
-- is updated in the same PR to populate the new columns.

DROP INDEX IF EXISTS accounts_value_uk;

-- Per-location value pools: raw materials and finished goods.
CREATE UNIQUE INDEX accounts_value_loc_uk
  ON accounts (kind, sku_id, location_id, currency)
  WHERE ledger_kind = 'value'
    AND kind IN ('inv_value_raw', 'inv_value_fg')
    AND sku_id IS NOT NULL
    AND NOT is_closed;

-- Per-routing-op value pool: WIP.
CREATE UNIQUE INDEX accounts_value_wip_uk
  ON accounts (kind, sku_id, routing_op, currency)
  WHERE ledger_kind = 'value'
    AND kind = 'inv_value_wip'
    AND sku_id IS NOT NULL
    AND NOT is_closed;

-- CHECK reinforcement: inv_value_raw / inv_value_fg with a sku_id MUST
-- have a location_id. Catches malformed inserts up-front.
ALTER TABLE accounts ADD CONSTRAINT accounts_inv_value_loc_required
  CHECK (
    NOT (kind IN ('inv_value_raw', 'inv_value_fg') AND sku_id IS NOT NULL)
    OR location_id IS NOT NULL
  );

-- CHECK reinforcement: inv_value_wip MUST have a routing_op.
ALTER TABLE accounts ADD CONSTRAINT accounts_inv_value_wip_op_required
  CHECK (kind <> 'inv_value_wip' OR routing_op IS NOT NULL);
