-- Best-effort down (project convention; Phase 0/1 has no production data).
--
-- Restoring the prior post_po_receipt (mig 0047) and
-- post_inventory_adjustment (mig 0048) bodies left as best-effort:
-- revert to a fresh DB and re-apply the consolidated migration train.

DROP FUNCTION IF EXISTS post_po_receipt(UUID, JSONB, DATE, UUID, UUID, TEXT);

DROP FUNCTION IF EXISTS post_inventory_adjustment(
  UUID, UUID, BIGINT, BIGINT, TEXT, TEXT, DATE, UUID, UUID, TEXT, JSONB);

DROP INDEX IF EXISTS po_receipt_lines_unit_ids;
ALTER TABLE po_receipt_lines DROP COLUMN IF EXISTS unit_ids;

DROP INDEX IF EXISTS inventory_adjustments_unit_ids;
ALTER TABLE inventory_adjustments DROP COLUMN IF EXISTS unit_ids;
