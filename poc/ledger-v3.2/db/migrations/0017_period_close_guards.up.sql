-- Closed-period immutability (recalc-e §4, D13): the period lock is a schema
-- invariant, not API discipline. Two BEFORE INSERT guards:
--
--   trx_line        — no physical event may land inside a closed period. This
--                     freezes the period's replay input: the strict fold is
--                     prefix-stable (a depletion's authoritative cost depends
--                     only on events at or before its R-1 position), so an
--                     unchanged physical prefix means the engine can never
--                     recompute a different authoritative cost for a closed
--                     period's depletions.
--   cost_settlement — no new settlement generation may re-cost a depletion
--                     whose posted_at falls inside a closed period (that would
--                     be a silent reopen — recalc-e §4 / D14: reopen is out of
--                     scope; this reject is where a reopen workflow would
--                     later attach). With the trx_line guard in place the
--                     engine never legitimately attempts this (frozen replay
--                     input ⇒ recomputed prefix costs are unchanged ⇒ no
--                     write); the guard is the fail-loud backstop.
--
-- Period boundary convention (used identically here, by ledger_close_period's
-- gate, and by the sweep scoping): a period covers
-- posted_at ∈ [start_date::timestamptz, (end_date + 1)::timestamptz) — DATE
-- bounds interpreted in the database timezone, end-exclusive (index-friendly).
--
-- The stamp fence: ledger_close_period takes the EXCLUSIVE side of advisory
-- key (32022, period_id) after its drain/sweep and re-checks the gate before
-- stamping 'closed'; a physical insert into a covered period takes the SHARED
-- side here and re-reads state after acquiring it. Every gate-to-stamp
-- interleaving therefore resolves: an insert that committed before the fence
-- is seen by the close's re-check; one that had not blocks on the fence until
-- the close resolves, and the fresh re-read below rejects it. plpgsql trigger
-- functions are VOLATILE, so under READ COMMITTED the post-lock SELECT takes a
-- fresh snapshot and observes a close that committed while the lock was
-- awaited.
--
-- cost_adjustment_line rows are exempt from the trx_line guard: they are the
-- engine's / close sweep's own GL output, stamped posted_at = now() and
-- excluded from replay and gauges; the valuation invariant for them is keyed
-- by the DEPLETION's posted_at, which the cost_settlement guard covers. The
-- exemption also means the close's own drain/sweep inserts never self-fence.
--
-- Advisory keyspace (database-local): (32021, period_id) = the closer-vs-closer
-- mutex, taken only by ledger_close_period; (32022, period_id) = the stamp
-- fence. The two-arg int4 form is used for pg_locks legibility; period ids
-- must fit int4 (the ::int cast fails loud otherwise).
--
-- PeriodClosed raises SQLSTATE 55000 (object_not_in_prerequisite_state) with
-- the stable 'PeriodClosed:' message prefix. The parent repo's P00xx custom
-- codes cannot be used here: pgrx's SPI boundary re-reports any SQLSTATE
-- outside its PgSqlErrorCode enum as XX000, so a custom code would be lost on
-- every error surfacing through ledger_submit_trx / the recalc engine.

CREATE FUNCTION period_guard_trx_line() RETURNS trigger LANGUAGE plpgsql AS $fn$
DECLARE
    v_id    BIGINT;
    v_state TEXT;
BEGIN
    IF NEW.line_type = 'cost_adjustment_line' THEN
        RETURN NEW;
    END IF;
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
    RETURN NEW;
END
$fn$;

CREATE TRIGGER trx_line_period_guard
    BEFORE INSERT ON trx_line
    FOR EACH ROW EXECUTE FUNCTION period_guard_trx_line();

CREATE FUNCTION period_guard_cost_settlement() RETURNS trigger LANGUAGE plpgsql AS $fn$
DECLARE
    v_at     TIMESTAMPTZ;
    v_period BIGINT;
BEGIN
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
    RETURN NEW;
END
$fn$;

CREATE TRIGGER cost_settlement_period_guard
    BEFORE INSERT ON cost_settlement
    FOR EACH ROW EXECUTE FUNCTION period_guard_cost_settlement();
