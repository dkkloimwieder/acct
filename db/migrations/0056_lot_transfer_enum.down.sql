-- PG forbids dropping an enum value once it may have been used.
-- 'lot_transfer' may already appear on posting_lines.reason rows
-- after mig 0057 ships. Down is best-effort here; project-wide
-- convention (per migration-files-are-immutable-content-addressed
-- memory) is that down for ALTER TYPE ADD VALUE is a no-op.
SELECT 1;
