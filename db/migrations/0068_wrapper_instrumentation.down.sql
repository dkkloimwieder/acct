-- Best-effort down (project convention; Phase 0/1 has no production data).
--
-- Reverting the wrapper bodies to their prior CREATE OR REPLACE forms
-- (mig 0064 / 0018 / 0058) is left as best-effort: revert to a fresh DB
-- and re-apply the consolidated migration train.

DROP TABLE IF EXISTS _wrapper_section_timings;
