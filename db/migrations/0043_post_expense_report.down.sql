-- Best-effort down (project convention).
-- ALTER TYPE ADD VALUE leaves 'ap_employee' and 'expense_report' on the
-- enums — Postgres has no DROP VALUE; the unused values are harmless.

DROP FUNCTION IF EXISTS post_expense_report(UUID, CHAR, JSONB, BIGINT, DATE, UUID, UUID, TEXT, TEXT);
DROP TABLE IF EXISTS expense_report_lines;
DROP TABLE IF EXISTS expense_reports;
DROP TABLE IF EXISTS employees;
