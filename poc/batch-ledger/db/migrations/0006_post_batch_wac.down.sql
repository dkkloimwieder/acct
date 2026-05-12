-- Revert post_batch to the P3 (transfer-only) version by re-applying mig 0004.
-- For PoC iteration this down is a placeholder; sqlx-cli won't auto-restore
-- the prior CREATE OR REPLACE body. Re-apply 0004 manually if needed.
DROP FUNCTION IF EXISTS post_batch(JSONB);
