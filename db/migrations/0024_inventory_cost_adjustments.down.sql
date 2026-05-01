-- Revert acct-14m: drop the cost-adjustment workflow table and function.
-- New enum values 'cost_adjustment' (transfer_reason) and
-- 'variance_cost_adjustment' (account_kind) stay in place per project
-- convention (PG can't cleanly drop enum values).

DROP FUNCTION IF EXISTS post_cost_adjustment(
  UUID, UUID, TEXT, TEXT, BIGINT, DATE, UUID, UUID, TEXT
);

DROP TABLE IF EXISTS inventory_cost_adjustments;
