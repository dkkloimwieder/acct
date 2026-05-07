-- Schedule the reservation expiry sweep via pg_cron.
--
-- Cadence: every 30 seconds (pg_cron 1.4+ supports sub-minute schedules
-- via the "<n> seconds" / "<n> minutes" interval syntax). Doc Part IV
-- §3.3 specifies 30s; sub-second precision is Q5 in Part VII and would
-- be revisited with LISTEN/NOTIFY rather than tightening this cadence.
--
-- The UPDATE itself is correctness-tested by T3 (acct-93b.18,
-- tests/reservation_expiry.rs). pg_cron's only role is invocation
-- frequency.
--
-- Job runs ONLY in the database named by `cron.database_name` GUC
-- (see docker-compose.yml: `cron.database_name=acct`). The DO/EXCEPTION
-- block here mirrors the 0001 extension migration: the test/CI DBs
-- (acct_test, acct_ci) don't have pg_cron and would fail on the
-- `cron.schedule` call without this guard.

DO $$
BEGIN
  PERFORM cron.schedule(
    'reservation_expiry',
    '30 seconds',
    $cron$
      UPDATE inventory_reservations
         SET status      = 'expired',
             resolved_at = clock_timestamp()
       WHERE status      = 'active'
         AND expires_at  < clock_timestamp();
    $cron$
  );
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'pg_cron unavailable in % (%): %',
               current_database(), SQLSTATE, SQLERRM;
END $$;
