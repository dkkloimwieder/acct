-- Best-effort down (project convention; Phase 0/1 has no production data).

DROP FUNCTION IF EXISTS run_daily_reconciliation();

-- Restoring the prior run_daily_reconciliation (mig 0059) body left
-- as best-effort: revert to a fresh DB and re-apply the consolidated
-- migration train.
