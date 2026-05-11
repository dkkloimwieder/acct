-- Best-effort down (project convention; Phase 0/1 has no production data).
--
-- Restoring the prior _wo_write_lot_consumption (mig 0058),
-- _wo_emit_bom_lines (mig 0058), and post_lot_transfer (mig 0057)
-- bodies left as best-effort: revert to a fresh DB and re-apply
-- the consolidated migration train.

DROP FUNCTION IF EXISTS post_lot_transfer(UUID, UUID, JSONB, DATE, UUID, UUID, TEXT);

DROP FUNCTION IF EXISTS _wo_emit_bom_lines(
  UUID, BIGINT, INT, BIGINT, JSONB, UUID, DATE, UUID, TEXT, JSONB);

DROP FUNCTION IF EXISTS _wo_write_lot_consumption(JSONB);

DROP TRIGGER IF EXISTS trg_wo_unit_consumption_append_only ON wo_unit_consumption;
DROP TABLE IF EXISTS wo_unit_consumption;

DROP INDEX IF EXISTS lot_transfer_lines_unit_ids;
ALTER TABLE lot_transfer_lines DROP COLUMN IF EXISTS unit_ids;
