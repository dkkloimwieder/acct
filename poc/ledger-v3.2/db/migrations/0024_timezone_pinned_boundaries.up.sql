-- Session-timezone independence of period boundaries (acct-1vur.5,
-- design-v3.2 §7).
--
-- Every period-coverage predicate compared a TIMESTAMPTZ against a DATE cast
-- with `::timestamptz`, and that cast resolves in the SESSION's TimeZone.
-- 0017's header called it "the database timezone", which holds only while
-- every session inherits the cluster setting — many drivers send their own on
-- connect. A non-UTC session therefore evaluates a DIFFERENT period window
-- than the UTC writers did: with end_date 2026-07-11, a UTC session computes
-- the boundary as 2026-07-12T00:00Z while an America/New_York session
-- computes 2026-07-12T04:00Z.
--
-- That is not merely a mis-valuation: it reaches the acct-1vur.3 permanent
-- wedge through a third door. A boundary-adjacent event admitted by an
-- eastern writer (above ITS frontier) is counted in-period by the UTC closer
-- and rejected by the UTC settlement guard as below the frontier — the same
-- unswept-below-the-frontier shape, arrived at without any backdating at all.
--
-- The fix pins the conversion instead of the deployment:
--   (date_col::timestamp AT TIME ZONE 'UTC')
-- The DATE becomes a naive timestamp, which `AT TIME ZONE 'UTC'` then
-- interprets AS UTC, yielding the same absolute instant in every session.
-- The direction matters and is easy to invert: `date_col::timestamptz AT TIME
-- ZONE 'UTC'` reads the value in the session zone and then strips the zone,
-- returning a naive timestamp — wrong type and wrong instant. Verified under
-- America/New_York: the naive cast gives 2026-07-12T04:00Z, this form gives
-- 2026-07-12T00:00Z, the inverted form gives a bare 2026-07-12 04:00:00.
--
-- Six sites live here; the matching four in close.rs (`gate_pools` and the
-- in-period recount) are pinned in the same change — they are Rust string
-- literals, not schema. Storing the bounds as TIMESTAMPTZ instead was
-- rejected: a data migration plus call-site churn for no benefit this does
-- not already deliver.
--
-- Pinning the CLUSTER to timezone = 'UTC' is complementary belt-and-braces
-- and belongs with the scripted cluster prerequisites in acct-1vur.2(d); it
-- is deliberately NOT relied on here, since correctness must not depend on a
-- setting any client can override.
--
-- Choosing UTC specifically is a second, separable decision, and worth naming
-- rather than leaving implicit. The determinism argument above selects "some
-- fixed zone", not this one. But `posted_at` is caller-supplied BUSINESS
-- time, so the zone also decides which calendar day an event belongs to: for
-- a business operating in America/New_York, a movement recorded 2026-07-11
-- 22:00 local is 2026-07-12T02:00Z and therefore falls in the NEXT period
-- under UTC-day boundaries. That is the correct trade here — v3.2 has no
-- tenant or entity to hang a business calendar off, and inventing one to
-- serve a boundary cast would be the larger mistake — but a deployment with
-- a real reporting calendar would want the zone to become configuration
-- (per-entity, alongside the period definition) rather than this literal.
-- Changing it later is a one-line edit here plus the four in close.rs; the
-- shape does not have to change, only the constant.
--
-- Bodies are otherwise 0022's verbatim: the coverage loop plus the monotonic
-- closed-frontier check under the (32022, 0) sentinel.

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
         WHERE NEW.posted_at >= (start_date::timestamp AT TIME ZONE 'UTC')
           AND NEW.posted_at < ((end_date + 1)::timestamp AT TIME ZONE 'UTC')
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
       AND NEW.posted_at < ((v_floor_end + 1)::timestamp AT TIME ZONE 'UTC') THEN
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
       AND v_at >= (start_date::timestamp AT TIME ZONE 'UTC')
       AND v_at < ((end_date + 1)::timestamp AT TIME ZONE 'UTC')
     LIMIT 1;
    IF v_period IS NOT NULL THEN
        RAISE EXCEPTION 'PeriodClosed: settlement for depletion % (posted_at %) falls in closed accounting_period %',
            NEW.depletion_trx_line_id, v_at, v_period
            USING ERRCODE = '55000';
    END IF;

    SELECT max(end_date) INTO v_floor_end
      FROM accounting_period WHERE state = 'closed';
    IF v_floor_end IS NOT NULL AND v_at < ((v_floor_end + 1)::timestamp AT TIME ZONE 'UTC') THEN
        RAISE EXCEPTION
            'PeriodClosed: settlement for depletion % (posted_at %) is at or before the closed-period frontier %',
            NEW.depletion_trx_line_id, v_at, v_floor_end
            USING ERRCODE = '55000';
    END IF;

    RETURN NEW;
END
$fn$;
