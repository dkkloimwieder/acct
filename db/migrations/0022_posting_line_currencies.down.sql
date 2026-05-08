-- Best-effort down: drops the table. The CREATE OR REPLACE on
-- _post_posting_lines_apply_event in the up is NOT reverted here
-- (post-shipped Phase 0/1 schema has no production data; down is for
-- ci-check revert/redeploy symmetry). After this down + re-up,
-- the function body is the consolidated 0022 form again.

DROP INDEX IF EXISTS posting_line_currencies_by_currency;
DROP TABLE IF EXISTS posting_line_currencies;
