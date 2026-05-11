-- Best-effort down (project convention; Phase 0/1 has no production data).
-- Drops the new entry point; CHECK constraint reversal + post_so_ship body
-- restoration is left as a fresh-DB-then-reapply task.
DROP FUNCTION IF EXISTS post_so_ship_psync(UUID, JSONB, DATE, UUID, UUID, TEXT);
