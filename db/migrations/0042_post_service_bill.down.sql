-- Best-effort down (project convention).
-- ALTER TYPE ADD VALUE leaves 'service_bill' on the type — Postgres
-- has no DROP VALUE; the unused value is harmless.

DROP FUNCTION IF EXISTS post_service_bill(UUID, CHAR, JSONB, DATE, UUID, UUID, TEXT, TEXT);
DROP TABLE IF EXISTS service_bill_lines;
DROP TABLE IF EXISTS service_bills;
