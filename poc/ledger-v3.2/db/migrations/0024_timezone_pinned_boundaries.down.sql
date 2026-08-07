-- Restore 0022's session-TimeZone-dependent casts.

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
