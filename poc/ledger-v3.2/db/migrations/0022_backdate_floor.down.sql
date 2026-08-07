-- Restore the 0017 coverage-only guard bodies.

CREATE OR REPLACE FUNCTION period_guard_trx_line() RETURNS trigger LANGUAGE plpgsql AS $fn$
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

CREATE OR REPLACE FUNCTION period_guard_cost_settlement() RETURNS trigger LANGUAGE plpgsql AS $fn$
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
