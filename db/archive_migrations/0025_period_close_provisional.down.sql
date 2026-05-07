-- Revert acct-4mt: drop the period-close provisional side table.
-- New account_kind enum values ('variance_wac_period',
-- 'variance_wac_retroactive', 'variance_cost_adjust_retro') stay in
-- place per project convention — PG can't cleanly drop enum values
-- and Phase 0/1 has no production data depending on this asymmetry.

DROP TABLE IF EXISTS transfers_provisional;
