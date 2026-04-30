-- Revert acct-nfr: restore migration 0006's catch-all value account UK.
--
-- The down assumes no rows would violate the looser UK after partition
-- columns are dropped. Phase 0 has no production data; this is fine.

ALTER TABLE accounts DROP CONSTRAINT IF EXISTS accounts_inv_value_wip_op_required;
ALTER TABLE accounts DROP CONSTRAINT IF EXISTS accounts_inv_value_loc_required;

DROP INDEX IF EXISTS accounts_value_wip_uk;
DROP INDEX IF EXISTS accounts_value_loc_uk;

CREATE UNIQUE INDEX accounts_value_uk
  ON accounts (kind, sku_id, currency)
  WHERE ledger_kind = 'value' AND sku_id IS NOT NULL AND NOT is_closed;
