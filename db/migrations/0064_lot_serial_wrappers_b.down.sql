-- Best-effort down (project convention; Phase 0/1 has no production data).
--
-- Restoring the prior post_so_ship (mig 0053) and post_wo_complete
-- (mig 0059) bodies left as best-effort: revert to a fresh DB and
-- re-apply the consolidated migration train.

DROP FUNCTION IF EXISTS post_so_ship(UUID, JSONB, DATE, UUID, UUID, TEXT);

DROP FUNCTION IF EXISTS post_wo_complete(
  UUID, BIGINT, DATE, UUID, UUID, TEXT, JSONB);

DROP INDEX IF EXISTS so_shipment_lines_unit_ids;
ALTER TABLE so_shipment_lines DROP COLUMN IF EXISTS unit_ids;
