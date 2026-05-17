-- M5e.2 (acct-jypc): GC for completed persistent_staging rows.
--
-- The DELETE runs against this database (acct_poc_queue_v21); pg_cron's
-- metadata lives in 'acct' per postgresql.conf::cron.database_name.
-- Operator schedules from acct via cron.schedule_in_database:
--
--   SELECT cron.schedule_in_database(
--       'poc_v21_persistent_staging_gc',
--       '0 * * * *',
--       $$SELECT poc_v21_persistent_staging_gc()$$,
--       'acct_poc_queue_v21'
--   );
--
-- The migration itself does not register the cron job (no pg_cron in
-- acct_poc_queue_v21) — registration is an operational concern outside
-- migration scope. Tests invoke poc_v21_persistent_staging_gc() directly
-- with a small retention to verify the body.
--
-- Default retention pulled from poc_v21.persistent_staging_gc_retention_hours
-- (Sighup-scope GUC, default 24) so live tuning works without restart.
CREATE OR REPLACE FUNCTION poc_v21_persistent_staging_gc(
    p_retention_hours INT DEFAULT NULL
) RETURNS BIGINT
LANGUAGE plpgsql AS $$
DECLARE
    v_retention_hours INT;
    v_deleted BIGINT;
BEGIN
    v_retention_hours := COALESCE(
        p_retention_hours,
        current_setting('poc_v21.persistent_staging_gc_retention_hours')::INT
    );

    WITH deleted AS (
        DELETE FROM poc_v21_persistent_staging
         WHERE state = 'completed'
           AND enqueued_at < NOW() - (v_retention_hours || ' hours')::INTERVAL
         RETURNING request_seq
    )
    SELECT COUNT(*) INTO v_deleted FROM deleted;

    RETURN v_deleted;
END $$;
