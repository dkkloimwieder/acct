-- Best-effort down (project convention; Phase 0/1 has no production data).

DROP FUNCTION IF EXISTS run_daily_reconciliation();
DROP FUNCTION IF EXISTS post_wo_complete(UUID, BIGINT, DATE, UUID, UUID, TEXT);
DROP FUNCTION IF EXISTS _wo_write_lot_genealogy(UUID, JSONB, NUMERIC);

DROP VIEW IF EXISTS v_lot_lineage_downstream;
DROP VIEW IF EXISTS v_lot_lineage_upstream;

DROP TRIGGER IF EXISTS trg_lot_genealogy_append_only ON lot_genealogy;
DROP TABLE IF EXISTS lot_genealogy;

-- Restoring the prior post_wo_complete (mig 0055) and prior
-- run_daily_reconciliation (mig 0058) bodies left as best-effort:
-- revert to a fresh DB and re-apply the consolidated migration train.
