-- Best-effort down (project convention; Phase 0/1 has no production data).

DROP FUNCTION IF EXISTS run_daily_reconciliation();
DROP FUNCTION IF EXISTS post_op_move(UUID, INT, INT, BIGINT, DATE, UUID, UUID, TEXT);
DROP FUNCTION IF EXISTS post_wo_start(UUID, DATE, UUID, UUID, TEXT, JSONB);
DROP FUNCTION IF EXISTS _wo_emit_bom_lines(
  UUID, BIGINT, INT, BIGINT, JSONB, UUID, DATE, UUID, TEXT, JSONB
);
DROP FUNCTION IF EXISTS _wo_write_lot_consumption(JSONB);

DROP TRIGGER IF EXISTS trg_wo_lot_consumption_append_only ON wo_lot_consumption;
DROP TABLE IF EXISTS wo_lot_consumption;

-- Restoring the prior _wo_emit_bom_lines (RETURNS JSONB), prior
-- post_wo_start, post_op_move, and run_daily_reconciliation bodies
-- left as best-effort: revert to a fresh DB and re-apply the
-- consolidated migration train.
