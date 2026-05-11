-- Best-effort down (project convention; Phase 0/1 has no production data).
--
-- Restoring the prior _post_posting_lines_apply_event (mig 0057) body
-- left as best-effort: revert to a fresh DB and re-apply the
-- consolidated migration train.

DROP FUNCTION IF EXISTS _post_posting_lines_apply_event(
  JSONB, INT, BIGINT, accounts, accounts, cost_method, BOOLEAN);
