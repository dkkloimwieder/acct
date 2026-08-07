-- The monotonic backdate floor (acct-1vur.3, design-v3.2 §7).
--
-- 0017's guards ask only "does a CLOSED period cover this posted_at?". That
-- is coverage-shaped, and coverage has holes: dates before the first period,
-- and gaps between periods (0021 forbids overlap but deliberately permits
-- gaps). An event backdated into such a hole is admitted, the feed then
-- lowers a recost floor below a closed period's end, the engine full-replays,
-- recomputes a closed period's costs, and 0017's cost_settlement guard
-- rejects that write — every pass, forever. That is the permanent-wedge class
-- `reject_out_of_order` exists to prevent, reached through the uncovered-date
-- side door. `gate_pools` has no lower bound on its scan, so pre-period
-- activity is a supported, realistic shape rather than a corner case.
--
-- The fix reframes admission from coverage to a MONOTONIC FRONTIER: once any
-- period is closed, nothing physical may land at or before its end_date,
-- covered or not. The frontier is `max(end_date) FILTER (WHERE state =
-- 'closed')`, so it only ever moves forward.
--
-- 'closing' does NOT raise the floor. Closing means draining, not frozen
-- (close.rs) — a gate-failed close persists 'closing' precisely so the tail
-- can still be appended and drained. Interlocking a close against appends
-- that race its stamp is the (32022) fence's job, below.
--
-- Fence discipline. 0017's coverage fence works because the inserter and the
-- closer converge on the SAME advisory key: the inserter takes SHARED
-- (32022, covering_period_id), the closer takes EXCLUSIVE (32022,
-- closing_period_id). The frontier arm cannot use that key. A frontier check
-- fenced on the period that ALREADY supplies the frontier would never block
-- anything: the exclusive side is only ever held for the period being closed,
-- which is by definition not yet 'closed', and it is released at the very
-- commit that first makes it so. The two sides would never meet, leaving the
-- frontier an unsynchronized read-then-check — and for a posted_at that no
-- period covers, the coverage loop iterates zero times, so there would be no
-- fence at all.
--
-- So the frontier fences on a FIXED SENTINEL key, (32022, 0). No period may
-- use id 0. Every physical insert takes the SHARED side of it; the closer
-- takes the EXCLUSIVE side alongside its per-period fence. Now an
-- uncovered-date insert and the close that is about to raise the frontier
-- past it always converge, whatever dates are involved and even when nothing
-- is closed yet (the first close included).
--
-- LOCK ORDER: the sentinel is taken FIRST, before any per-period key, on both
-- sides. Taking it after the coverage loop would let an inserter holding
-- shared (32022, P) wait for the sentinel while the closer holding the
-- sentinel waits for exclusive (32022, P) — a deadlock. Sentinel-then-period
-- is a total order, so no cycle exists.
--
-- Cost: the sentinel briefly serializes ALL physical inserts against the
-- closer's fence window (recount + stamp), not just inserts into the closing
-- period. That is the price of interlocking uncovered dates, and it is what
-- correctness requires; uncontended (no close running) it is one shared
-- advisory lock per row.
--
-- plpgsql trigger functions are VOLATILE, so under READ COMMITTED the
-- post-lock statement takes a fresh snapshot and observes any close that
-- committed while this insert waited.
--
-- NOTE: the closer's `in_period_event_count` recount is NOT a backstop here.
-- It is scoped to fifo/lifo pools (deliberately, so non-strict activity does
-- not abort a good close), while this frontier is method-agnostic — for
-- wac/std/specific pools the trigger is the only interlock.
--
-- The engine's own `cost_adjustment_line` output stays exempt (stamped
-- posted_at = now(), excluded from replay and gauges); its valuation
-- invariant is keyed by the DEPLETION's posted_at, which the cost_settlement
-- guard covers — and that guard gains the symmetric floor here.

CREATE OR REPLACE FUNCTION period_guard_trx_line() RETURNS trigger
LANGUAGE plpgsql AS $fn$
DECLARE
    v_id        BIGINT;
    v_state     TEXT;
    v_floor_end DATE;
BEGIN
    IF NEW.line_type = 'cost_adjustment_line' THEN
        RETURN NEW;
    END IF;

    -- Sentinel fence FIRST (see header: convergence + lock order).
    PERFORM pg_advisory_xact_lock_shared(32022, 0);

    -- Coverage: no physical event may land inside a closed period.
    FOR v_id IN
        SELECT id FROM accounting_period
         WHERE NEW.posted_at >= start_date::timestamptz
           AND NEW.posted_at < (end_date + 1)::timestamptz
    LOOP
        PERFORM pg_advisory_xact_lock_shared(32022, v_id::int);
        SELECT state INTO v_state FROM accounting_period WHERE id = v_id;
        IF v_state = 'closed' THEN
            RAISE EXCEPTION 'PeriodClosed: posted_at % falls in closed accounting_period %',
                NEW.posted_at, v_id
                USING ERRCODE = '55000';
        END IF;
    END LOOP;

    -- Frontier: nothing physical at or before the latest closed period's end,
    -- whether or not any period covers that date. Read under the sentinel
    -- taken above, so a close that committed while this insert waited is
    -- visible on this statement's fresh snapshot.
    SELECT max(end_date) INTO v_floor_end
      FROM accounting_period WHERE state = 'closed';
    IF v_floor_end IS NOT NULL
       AND NEW.posted_at < (v_floor_end + 1)::timestamptz THEN
        RAISE EXCEPTION
            'PeriodClosed: posted_at % is at or before the closed-period frontier %',
            NEW.posted_at, v_floor_end
            USING ERRCODE = '55000';
    END IF;

    RETURN NEW;
END
$fn$;

-- Symmetric frontier on settlements: no settlement generation may re-cost a
-- depletion at or before the frontier. With the trx_line floor in place the
-- engine never legitimately attempts this (no such depletion can be admitted
-- after the close), so this is the fail-loud backstop for a depletion that
-- predates the frontier in an uncovered range.
CREATE OR REPLACE FUNCTION period_guard_cost_settlement() RETURNS trigger
LANGUAGE plpgsql AS $fn$
DECLARE
    v_at        TIMESTAMPTZ;
    v_period    BIGINT;
    v_floor_end DATE;
BEGIN
    PERFORM pg_advisory_xact_lock_shared(32022, 0);
    SELECT posted_at INTO v_at FROM trx_line WHERE id = NEW.depletion_trx_line_id;

    SELECT id INTO v_period FROM accounting_period
     WHERE state = 'closed'
       AND v_at >= start_date::timestamptz
       AND v_at < (end_date + 1)::timestamptz
     LIMIT 1;
    IF v_period IS NOT NULL THEN
        RAISE EXCEPTION 'PeriodClosed: settlement for depletion % (posted_at %) falls in closed accounting_period %',
            NEW.depletion_trx_line_id, v_at, v_period
            USING ERRCODE = '55000';
    END IF;

    SELECT max(end_date) INTO v_floor_end
      FROM accounting_period WHERE state = 'closed';
    IF v_floor_end IS NOT NULL AND v_at < (v_floor_end + 1)::timestamptz THEN
        RAISE EXCEPTION
            'PeriodClosed: settlement for depletion % (posted_at %) is at or before the closed-period frontier %',
            NEW.depletion_trx_line_id, v_at, v_floor_end
            USING ERRCODE = '55000';
    END IF;

    RETURN NEW;
END
$fn$;
