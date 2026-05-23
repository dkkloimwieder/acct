-- Rollback of 0007. Drops posting_lines_provisional.
--
-- Cannot drop the 'wac_periodic' value from the pool_method enum (Postgres
-- forbids removing enum values once added). The value stays on rollback;
-- any rows that referenced it must be migrated to another method by the
-- operator before rolling back. The PoC accepts this asymmetry — same
-- pattern as the main acct project's down-migration convention.

DROP TABLE posting_lines_provisional;
