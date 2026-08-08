-- Slot-loss RECOVERY, under the decided posture (acct-1vur.2b;
-- design-v3.2 §6/§7).
--
-- 0026 shipped detection. This is recovery, and it ships with a decision
-- attached: when a re-fold discovers that a CLOSED period was valued over
-- events the feed never delivered, **correctness wins**. The engine does not
-- quietly keep the stale-but-frozen number, and it does not silently skip the
-- write. The pool enters a deliberate, documented ESCALATION state whose only
-- exit is the reopen workflow (acct-1vur.9). Two alternatives were considered
-- and declined: filtering the generation write set so closed periods are final
-- by construction (loses a known-wrong valuation silently), and gating the
-- close on a "reseed pending" flag (narrows the window, does not close it).
--
-- WHAT A FLOOR MEANS. `recost_floor_posted_at` is read by the engine as a
-- BOOLEAN: `full_replay = floor_at.is_some()`, after which the replay scans
-- the pool's whole stream. The floor's VALUE never bounds the replay or the
-- write set. So a floor here means exactly "this pool must re-fold from
-- opening" and nothing more — in particular, no choice of floor protects
-- closed periods. That protection comes from 0024's settlement guard plus the
-- escalation contract below, not from where the floor is placed. An earlier
-- draft clamped the floor to the open side of the closed frontier and
-- documented the clamp as load-bearing; it was neither, and the clamp is gone
-- rather than left as reassuring decoration.

-- ── the reseed ─────────────────────────────────────────────────────────────
--
-- Rebuilds the dirty set from durable state: enqueue the affected pools and
-- set a floor on each so the next pass is a full opening replay. Called on
-- demand and by the consumer when a slot is created over a database that
-- already holds events — a slot that was LOST, or one that never ran while
-- events accumulated (the first-startup bootstrap; same hole either way).
--
-- SCOPE is every fifo/lifo pool with physical activity — a superset of "pools
-- whose tail extends past the settled frontier". The tail test is the obvious
-- rule and it misses the case this epic exists for: an event backdated BELOW
-- the frontier does not extend the tail, and a lost slot is precisely the
-- situation where we cannot know which events went missing. Recovery is a
-- rare operator-initiated act; re-folding every strict pool is the affordable
-- choice and the only sound one. Pools whose frozen history is not
-- contradicted simply re-derive the same numbers and write nothing.
--
-- ORDERING is load-bearing and matches the feed's apply (consumer.rs): floors
-- BEFORE marks, as separate statements. The mark is what guarantees a future
-- pass, so it must be evaluated after the floor write has resolved — a mark
-- placed first can be consumed by a pass that then deletes the queue row,
-- stranding the floor with nothing to claim it. Invariant, shared with the
-- feed: a pool with a recost floor set always has a `recalc_queue` row.
CREATE FUNCTION ledger_feed_reseed() RETURNS BIGINT
LANGUAGE plpgsql AS $fn$
DECLARE
    v_count BIGINT;
BEGIN
    -- Converge with the closer on the sentinel the admission guard uses
    -- (0022), so the pool set and the closed-period picture cannot shift
    -- underneath this routine while a close is stamping.
    PERFORM pg_advisory_xact_lock_shared(32022, 0);

    CREATE TEMP TABLE _reseed_scope ON COMMIT DROP AS
    SELECT p.id AS pool_id,
           t.posted_at AS floor_at,
           t.id        AS floor_id
      FROM pool p
      JOIN LATERAL (
          SELECT tl.posted_at, tl.id
            FROM trx_line tl
           WHERE tl.pool_id = p.id
             AND tl.line_type <> 'cost_adjustment_line'
           ORDER BY tl.posted_at, tl.id
           LIMIT 1
      ) t ON true
     WHERE p.method IN ('fifo', 'lifo');

    -- Floors first. The (posted_at, id) pair is a REAL position in the R-1
    -- ordering — the pool's earliest physical event — because both guarded-min
    -- sites compare the pair lexicographically; a floor_at from one row and a
    -- floor_id from another would not order against anything.
    INSERT INTO pool_settlement (pool_id, recalc_generation,
                                 recost_floor_posted_at, recost_floor_id)
    SELECT pool_id, 0, floor_at, floor_id FROM _reseed_scope
    ON CONFLICT (pool_id) DO UPDATE
       SET recost_floor_posted_at = LEAST(pool_settlement.recost_floor_posted_at,
                                          EXCLUDED.recost_floor_posted_at),
           recost_floor_id = CASE
               WHEN pool_settlement.recost_floor_posted_at IS NULL
                 OR (EXCLUDED.recost_floor_posted_at, EXCLUDED.recost_floor_id)
                    < (pool_settlement.recost_floor_posted_at,
                       pool_settlement.recost_floor_id)
               THEN EXCLUDED.recost_floor_id
               ELSE pool_settlement.recost_floor_id END;

    -- Then marks.
    INSERT INTO recalc_queue (pool_id)
    SELECT pool_id FROM _reseed_scope
    ON CONFLICT (pool_id) DO NOTHING;

    SELECT count(*) INTO v_count FROM _reseed_scope;
    RETURN v_count;
END
$fn$;

-- ── the escalation surface ─────────────────────────────────────────────────
--
-- One row per pool whose re-fold contradicted a closed period. This is the
-- state that makes "fail loud" mean something an operator can act on rather
-- than an anonymous SQLSTATE 55000 retry loop: it names the pool, the closed
-- period, the specific depletion, and both costs, and it is DURABLE — it
-- survives worker restarts, because a restarted worker re-reads it rather
-- than rediscovering the divergence and spinning.
--
-- The engine detects the divergence BEFORE attempting the write, so nothing
-- is left half-applied: the pass records the escalation, writes no
-- generation, and the pool stops being claimed. Detection mirrors BOTH arms
-- of 0024's settlement guard — coverage by a closed period AND the monotonic
-- closed frontier (which fires in date ranges no period covers, legal since
-- 0021 permits gaps) — because a detector narrower than the guard just means
-- the guard rejects with a raw SQLSTATE instead. The guard remains the
-- backstop for any path that reaches the INSERT anyway.
--
-- The pool's `recalc_queue` row is KEPT, not deleted. Deleting it would break
-- the invariant this migration relies on ("a pool with a recost floor set
-- always has a recalc_queue row") and, worse, would make `recalc_queue_depth`
-- and `recalc_pool_lag.dirty_since` read "engine at rest" for a permanently
-- un-costed pool — the gauges going quiet at exactly the moment they should
-- shout. `claim_next` filters halted pools instead, which is what actually
-- stops the spin.
--
-- `resolved_at` is written by the reopen workflow (acct-1vur.9). Until then a
-- row here means: this pool's authoritative valuation is known to disagree
-- with a frozen period, no further costing happens on it, and repair requires
-- reopening that period.
CREATE TABLE recalc_escalation (
    id                     BIGSERIAL PRIMARY KEY,
    pool_id                BIGINT NOT NULL REFERENCES pool(id),
    closed_period_id       BIGINT REFERENCES accounting_period(id),
    depletion_trx_line_id  BIGINT NOT NULL REFERENCES trx_line(id),
    -- FALSE when the depletion had no settlement at all: it was never
    -- authoritatively costed, so `frozen_unit_cost` is its provisional
    -- observed cost rather than a finalized number. The distinction matters
    -- in the message — "was never costed" and "is frozen at" are different
    -- incidents.
    had_settlement         BOOLEAN NOT NULL,
    frozen_unit_cost       BIGINT NOT NULL,
    recomputed_unit_cost   BIGINT NOT NULL,
    qty                    BIGINT NOT NULL,
    -- Scope beyond the exemplar depletion, so the row states the pool's whole
    -- exposure rather than one line's.
    diverging_depletions   BIGINT NOT NULL,
    total_value_divergence BIGINT NOT NULL,
    detected_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at            TIMESTAMPTZ
);

-- One OPEN escalation per pool, but a pool may escalate again after the
-- reopen workflow resolves an earlier one. Keying the table on pool_id alone
-- would make the surface one-shot for all time: a post-reopen divergence
-- would halt the pool while recording nothing, which is the silent state this
-- whole mechanism exists to abolish.
CREATE UNIQUE INDEX recalc_escalation_one_open
    ON recalc_escalation (pool_id) WHERE resolved_at IS NULL;

-- The operator-facing view: everything needed to act, including what to do.
-- `closed_period_id` is NULL when the divergence is against the closed
-- FRONTIER in a date range no period covers.
CREATE VIEW recalc_escalations AS
SELECT e.id,
       e.pool_id,
       p.sku_id,
       p.location_id,
       p.method::text                       AS pool_method,
       e.closed_period_id,
       ap.start_date                        AS period_start,
       ap.end_date                          AS period_end,
       e.depletion_trx_line_id              AS exemplar_depletion_id,
       t.posted_at                          AS exemplar_posted_at,
       e.had_settlement,
       e.frozen_unit_cost                   AS exemplar_frozen_unit_cost,
       e.recomputed_unit_cost               AS exemplar_recomputed_unit_cost,
       e.qty                                AS exemplar_qty,
       e.diverging_depletions,
       e.total_value_divergence,
       e.detected_at,
       -- Dates rendered explicitly: format()'s %s applies the session
       -- DateStyle, and a DMY/MDY range is exactly what gets misread when
       -- this is pasted into an incident channel.
       format(
           'Pool %s %s: depletion %s (%s) %s, and re-folds to unit_cost %s over qty %s. '
           '%s depletion(s) diverge, total value divergence %s. '
           'Costing on this pool is HALTED. Repair requires reopening %s (acct-1vur.9).',
           e.pool_id,
           CASE WHEN e.closed_period_id IS NULL
                THEN 'diverges below the closed-period frontier'
                ELSE format('diverges from closed period %s (%s..%s)', e.closed_period_id,
                            to_char(ap.start_date, 'YYYY-MM-DD'),
                            to_char(ap.end_date, 'YYYY-MM-DD')) END,
           e.depletion_trx_line_id,
           to_char(t.posted_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SSZ'),
           CASE WHEN e.had_settlement
                THEN format('is frozen at unit_cost %s', e.frozen_unit_cost)
                ELSE format('was never authoritatively costed (provisional %s)',
                            e.frozen_unit_cost) END,
           e.recomputed_unit_cost, e.qty,
           e.diverging_depletions, e.total_value_divergence,
           COALESCE('period ' || e.closed_period_id, 'the affected closed period(s)'))
                                            AS operator_message
  FROM recalc_escalation e
  JOIN pool p                    ON p.id = e.pool_id
  LEFT JOIN accounting_period ap ON ap.id = e.closed_period_id
  JOIN trx_line t                ON t.id = e.depletion_trx_line_id
 WHERE e.resolved_at IS NULL;

-- ── the halt must be visible on the gauges operators already watch ─────────
--
-- A halted pool keeps its queue row, so it still counts in the dirty set —
-- but "dirty forever" is indistinguishable from "busy" without naming it. Both
-- registered gauges gain the halt explicitly.
CREATE OR REPLACE VIEW recalc_queue_depth AS
SELECT count(*)         AS dirty_pools,
       min(enqueued_at) AS oldest_enqueued_at,
       (SELECT count(*) FROM recalc_escalation WHERE resolved_at IS NULL)
                        AS halted_pools
FROM recalc_queue;

-- ── the halt on the per-pool gauge ─────────────────────────────────────────
--
-- `recalc_pool_lag` is the registered per-pool staleness gauge (G2a/G2b). A
-- halted pool is maximal staleness — permanently un-costed — so it must be
-- visible here, not only in a view an operator has to know to ask for.
CREATE OR REPLACE VIEW recalc_pool_lag AS
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
       lag.unsettled_gross_value,
       (SELECT e.closed_period_id FROM recalc_escalation e 
         WHERE e.pool_id = p.id AND e.resolved_at IS NULL) AS halted_by_closed_period
FROM pool p
LEFT JOIN pool_settlement ps ON ps.pool_id = p.id
LEFT JOIN recalc_queue q ON q.pool_id = p.id
LEFT JOIN LATERAL (
    SELECT t.posted_at, t.id
    FROM trx_line t
    WHERE t.pool_id = p.id
      AND t.line_type <> 'cost_adjustment_line'
    ORDER BY t.posted_at DESC, t.id DESC
    LIMIT 1
) tip ON true
LEFT JOIN LATERAL (
    SELECT count(*) AS unsettled_events,
           COALESCE(sum(abs(t.qty) * t.unit_cost), 0)::bigint AS unsettled_gross_value
    FROM trx_line t
    WHERE t.pool_id = p.id
      AND t.line_type <> 'cost_adjustment_line'
      AND (ps.settled_through_posted_at IS NULL
           OR (t.posted_at, t.id)
              > (ps.settled_through_posted_at, ps.settled_through_id))
) lag ON true;
