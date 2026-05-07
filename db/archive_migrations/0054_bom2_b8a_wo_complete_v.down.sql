-- acct-n7p — rollback note for B8 enum addition.
--
-- Project convention: ALTER TYPE ADD VALUE is best-effort on down
-- (Postgres 18 cannot remove enum values cleanly when they may be in
-- use). Phase 0/1 has no production data; a wipe is acceptable to
-- regress this migration.

-- (no-op)
