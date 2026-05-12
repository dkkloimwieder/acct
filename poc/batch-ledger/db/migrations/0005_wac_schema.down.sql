ALTER TABLE posting_lines DROP COLUMN IF EXISTS qty;
ALTER TABLE accounts      DROP COLUMN IF EXISTS qty;
-- account_kind enum values are not droppable cleanly; leave the values in place
-- (the down migration is best-effort for PoC iteration).
