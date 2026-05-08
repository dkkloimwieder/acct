-- Best-effort down (project convention).

-- The CREATE OR REPLACE on _post_posting_lines_apply_event reverts
-- naturally on a wipe-and-reapply; on a forward-only revert leave
-- the function in place since reverting body shape across phases is
-- a wider operation than this single migration.

DELETE FROM cost_method_strategies WHERE cost_method = 'fifo';

DROP FUNCTION IF EXISTS _fifo_write_depletions(BIGINT, UUID, UUID, SMALLINT, NUMERIC, DATE);
DROP FUNCTION IF EXISTS _compute_amount_fifo_outbound(JSONB, accounts, accounts, INT);
DROP FUNCTION IF EXISTS _fifo_walk_layers(UUID, UUID, SMALLINT, NUMERIC);
