-- 0006 never set a database comment on this column — the incorrect claim it
-- carries lives in its own file header, which is immutable. So the down
-- migration clears the comment rather than restoring a prior one; there is no
-- prior one, and re-asserting the wrong text into the schema would be worse
-- than leaving the column undocumented. Matches 0027's convention.

COMMENT ON COLUMN posting_account_map.variance_acct IS NULL;
