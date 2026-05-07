DROP FUNCTION IF EXISTS post_posting_lines(JSONB, BOOLEAN);
DROP FUNCTION IF EXISTS _post_posting_lines_apply_event(JSONB, INT, BIGINT, accounts, accounts, cost_method, BOOLEAN);
DROP FUNCTION IF EXISTS _post_posting_lines_compute_amount(JSONB, accounts, accounts, cost_method, INT);
DROP FUNCTION IF EXISTS _post_posting_lines_lock_pre_scan(JSONB, BIGINT[]);
DROP FUNCTION IF EXISTS _post_posting_lines_lookup_qty_account(accounts);
