-- acct-b82 / Slice B.1 — schema rollback. Reverse-order DROP.
--
-- The op_move_v transfer_reason enum value is deliberately NOT
-- removed (PG enum-value removal is not idempotent and the project
-- convention is "down-migrations leave enum values in place;
-- Phase 0/1 has no production data" — same pattern as migration
-- 0020 / 0029 / 0030 down comments).

DROP TABLE IF EXISTS boms;
DROP TABLE IF EXISTS wo_routing_burdens;
DROP TABLE IF EXISTS wo_routings;
DROP TABLE IF EXISTS work_orders;
