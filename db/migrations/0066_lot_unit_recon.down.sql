-- Best-effort down (project convention; Phase 0/1 has no production data).
--
-- Restoring the prior run_daily_reconciliation (mig 0060) body left
-- as best-effort: revert to a fresh DB and re-apply the consolidated
-- migration train.

DROP FUNCTION IF EXISTS run_daily_reconciliation();
