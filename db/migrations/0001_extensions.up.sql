-- Postgres extensions.
--
-- pg_stat_statements: query-level perf observability (used by §14 perf
-- baselines).
-- pg_cron: scheduled jobs (reservation expiry; see 0011).
--
-- pg_cron is gated by the cluster-level 'cron.database_name' GUC (set
-- to 'acct' in docker-compose.yml). Its install script refuses to run
-- in any other database; this is a hard check in pg_cron 1.6 that
-- 'cron.use_background_workers' does NOT relax. Tolerate the install
-- failure here so the migration chain completes cleanly in non-cron
-- databases (e.g., the ephemeral 'acct_test' DB used by cargo test).
-- pg_cron's only consumers schedule jobs in 'acct' exclusively — they
-- will fail in their own setup if pg_cron is absent there.

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

DO $$
BEGIN
  CREATE EXTENSION IF NOT EXISTS pg_cron;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'pg_cron unavailable in % (%): %',
               current_database(), SQLSTATE, SQLERRM;
END $$;
