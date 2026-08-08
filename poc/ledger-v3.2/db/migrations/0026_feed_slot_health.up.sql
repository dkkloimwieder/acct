-- Slot-loss detection and recovery (acct-1vur.2, design-v3.2 §6/§7).
--
-- (0025 is reserved for the spec-of-record COMMENT ON pass, so this lands at
-- 0026.)
--
-- `max_slot_wal_keep_size = 4GB` deliberately permits the cluster to
-- invalidate the feed slot rather than let an abandoned consumer pin WAL
-- without bound. That is the right trade, but it makes slot loss a REAL
-- operational state rather than a theoretical one — and the consumer's
-- `ensure_slot` then recreates the slot at the CURRENT WAL position, silently
-- skipping every event that was decodable in the gap. Those events never
-- lower a recost floor and never mark their pool dirty, so they are never
-- re-folded: a silent mis-valuation that defeats the whole loud-alarm
-- posture, because the alarm only fires if the event is eventually delivered.
-- The live cluster was observed with no ledger_feed slot at all while
-- poc_v3_2 held data.
--
-- This migration ships the DETECTION half: a health gauge that can actually
-- report absence, so slot loss stops being invisible.
--
-- The RECOVERY half (a reseed that rebuilds the dirty set and forces a
-- re-fold) is deliberately NOT here. Its obvious shape does not work: the
-- recalc engine treats `recost_floor_posted_at` as a BOOLEAN trigger for a
-- full opening replay (recalc.rs: `full_replay = floor_at.is_some()`, then
-- `scan_events(pool, None)`), never as a scope bound — so no choice of floor
-- can stop a re-fold from recomputing closed-period depletions, and any whose
-- cost actually moved is rejected by the 0017 settlement guard on every pass.
-- Recovery therefore needs a decision about what a close MEANS when recovery
-- later discovers the closed valuation was computed over missing events:
-- freeze it (filter the write set, closed periods final by construction) or
-- fail loud and require the out-of-scope reopen workflow (D14). That is a
-- semantic choice about closed-period finality, not an implementation
-- detail, so it is left open rather than guessed at (acct-1vur.2b).


-- ── (a) slot health ────────────────────────────────────────────────────────
--
-- 0014's `feed_lag` returns ZERO ROWS when the slot is missing, which is the
-- one case an operator most needs to see — absence is indistinguishable from
-- "query returned nothing" in every dashboard. This view always emits exactly
-- one row, so `present = false` is a value rather than an empty set.
--
-- The slot is matched on name AND database AND slot_type, matching
-- `ledger_close_period`'s feed-currency gate: slot names are cluster-global,
-- so a same-named slot in another database would otherwise read as healthy.
-- The name is the fixed literal 'ledger_feed' — the FEED_SLOT environment
-- override is removed in this change, because a renamed slot would silently
-- evade both this gauge and the close gate while the feed appeared to work.
CREATE VIEW feed_slot_health AS
SELECT 'ledger_feed'::text                       AS slot_name,
       s.slot_name IS NOT NULL                   AS present,
       COALESCE(s.active, false)                 AS active,
       s.wal_status,
       s.invalidation_reason,
       s.confirmed_flush_lsn,
       s.restart_lsn,
       CASE WHEN s.slot_name IS NULL THEN NULL
            ELSE pg_wal_lsn_diff(pg_current_wal_lsn(), s.confirmed_flush_lsn)::bigint
       END                                       AS lag_bytes,
       CASE WHEN s.slot_name IS NULL THEN NULL
            ELSE pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn)::bigint
       END                                       AS retained_wal_bytes,
       -- The single operator-facing signal. `active` is NOT part of it: the
       -- consumer uses the SQL peek/advance interface, which holds the slot
       -- only for the duration of each tick, so a healthy feed reads
       -- active = false between ticks.
       (s.slot_name IS NULL
        OR s.wal_status = 'lost'
        OR s.invalidation_reason IS NOT NULL)    AS unhealthy
  FROM (SELECT 1) AS always_one_row
  LEFT JOIN pg_replication_slots s
         ON s.slot_name = 'ledger_feed'
        AND s.slot_type = 'logical'
        AND s.database  = current_database();
