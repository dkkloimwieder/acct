-- Detection gauges (recalc-c §6/§7): the two-gauge model D8's advance-on-
-- ingestion policy requires. G1 and G2 are coupled but distinct — G1 measures
-- feed-consumer health / WAL pressure, G2 measures valuation staleness — and
-- they have distinct remediation axes (feed/WAL vs recalc throughput /
-- backpressure). Conflating them is the option-A failure mode.

-- G1 — cursor / ingestion lag. Advances at ingestion speed, so it stays low
-- unless the feed consumer (dirty-set writer) stalls. Also the WAL-retention
-- signal: an abandoned slot pins WAL from restart_lsn regardless of database.
CREATE VIEW feed_lag AS
SELECT slot_name,
       active,
       confirmed_flush_lsn,
       pg_current_wal_lsn() AS current_lsn,
       pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint AS lag_bytes,
       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint AS retained_wal_bytes
FROM pg_replication_slots
WHERE slot_name = 'ledger_feed';

-- G2c — aggregate dirty-set depth + the oldest un-drained mark.
CREATE VIEW recalc_queue_depth AS
SELECT count(*)         AS dirty_pools,
       min(enqueued_at) AS oldest_enqueued_at
FROM recalc_queue;

-- G2a/G2b — per-pool settlement lag. G2a is the staleness position
-- (settled_through vs the pool's stream tip in R-1 order); G2b bounds the
-- drift magnitude (count and gross value of events behind the frontier — the
-- size of the move a forced close would emit). Detection must be per-pool: a
-- global gauge can read healthy while one hot pool lags catastrophically, and
-- max(unsettled) over pools is the close-drain floor (recalc-c §4).
CREATE VIEW recalc_pool_lag AS
SELECT p.id AS pool_id,
       ps.recalc_generation,
       ps.settled_through_posted_at,
       ps.settled_through_id,
       ps.recost_floor_posted_at,
       ps.recost_floor_id,
       q.enqueued_at AS dirty_since,
       tip.posted_at AS tip_posted_at,
       tip.id        AS tip_id,
       lag.unsettled_events,
       lag.unsettled_gross_value
FROM pool p
LEFT JOIN pool_settlement ps ON ps.pool_id = p.id
LEFT JOIN recalc_queue q ON q.pool_id = p.id
LEFT JOIN LATERAL (
    SELECT t.posted_at, t.id
    FROM trx_line t
    WHERE t.pool_id = p.id
    ORDER BY t.posted_at DESC, t.id DESC
    LIMIT 1
) tip ON true
LEFT JOIN LATERAL (
    SELECT count(*) AS unsettled_events,
           COALESCE(sum(abs(t.qty) * t.unit_cost), 0)::bigint AS unsettled_gross_value
    FROM trx_line t
    WHERE t.pool_id = p.id
      AND (ps.settled_through_posted_at IS NULL
           OR (t.posted_at, t.id)
              > (ps.settled_through_posted_at, ps.settled_through_id))
) lag ON true;
