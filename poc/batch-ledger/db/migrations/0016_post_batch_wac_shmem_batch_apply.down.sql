-- Reverts mig 0016: restore mig 0014's per-leg PERFORM body.
-- (Down rebuilds the function with the prior per-leg loop. For PoC scope,
-- this is best-effort; production rollback would re-CREATE OR REPLACE
-- by re-running mig 0014's body.)
DROP FUNCTION IF EXISTS post_batch_wac_shmem(JSONB);
