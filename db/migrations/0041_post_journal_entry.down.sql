-- Best-effort down (project convention).
-- ALTER TYPE ADD VALUE leaves 'journal_entry' on the type — Postgres
-- has no DROP VALUE; the unused value is harmless.

DROP FUNCTION IF EXISTS post_journal_entry(JSONB, DATE, UUID, UUID, TEXT);
DROP TABLE IF EXISTS journal_entry_lines;
DROP TABLE IF EXISTS journal_entries;
