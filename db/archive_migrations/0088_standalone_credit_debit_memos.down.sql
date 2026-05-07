-- Down: drop functions + tables.

DROP FUNCTION IF EXISTS post_vendor_debit_memo(UUID, CHAR, JSONB, DATE, UUID, UUID, TEXT, BOOLEAN);
DROP FUNCTION IF EXISTS post_customer_credit_memo(UUID, CHAR, JSONB, DATE, UUID, UUID, TEXT, BOOLEAN);

DROP TABLE IF EXISTS vendor_debit_memo_lines;
DROP TABLE IF EXISTS vendor_debit_memos;
DROP TABLE IF EXISTS customer_credit_memo_lines;
DROP TABLE IF EXISTS customer_credit_memos;
