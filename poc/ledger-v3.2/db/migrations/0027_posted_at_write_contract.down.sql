COMMENT ON COLUMN ledger_inbox.posted_at IS NULL;
COMMENT ON COLUMN trx_line.posted_at IS NULL;
DROP TRIGGER ledger_inbox_posted_at ON ledger_inbox;
DROP FUNCTION ledger_inbox_posted_at_guard();
