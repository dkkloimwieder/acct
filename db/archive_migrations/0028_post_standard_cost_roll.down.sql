-- Revert acct-8rv: drop post_standard_cost_roll() and the audit table.
-- New transfer_reason ('standard_cost_roll') and account_kind
-- ('variance_std_cost_roll') stay in place per project convention
-- (PG can't cleanly drop enum values; Phase 0/1 has no production data).

DROP FUNCTION IF EXISTS post_standard_cost_roll(
  UUID, BIGINT, DATE, DATE, UUID, UUID, TEXT, BIGINT
);

DROP TABLE IF EXISTS inventory_standard_cost_rolls;
