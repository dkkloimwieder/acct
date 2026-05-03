-- acct-89i — rollback of BOM2 A1 enum + SKU column extensions.
--
-- Per project convention "Phase 0/1 has no production data; down is
-- best-effort", ALTER TYPE values stay in place (PG enum value removal
-- would require recreating the type, breaking dependents). Down
-- migration removes only the additive SKU columns.

ALTER TABLE skus DROP COLUMN consumption_policy;
ALTER TABLE skus DROP COLUMN is_phantom;
ALTER TABLE skus DROP COLUMN default_lot_size;
