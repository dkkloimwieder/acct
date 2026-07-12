-- Lazy per-pool backpressure (recalc-c §5, acct-qm7o.7).
--
-- Bounds how far the physical (qty) plane may run ahead of authoritative cost
-- on fifo/lifo pools: when a pool's unsettled-event backlog reaches the
-- configured bound, appends to THAT pool are rejected at admission (SQLSTATE
-- 53400) on both hot paths — `ledger_submit_trx` checks the touched pools
-- pre-write, and the trigger below gates raw `ledger_inbox` enqueues. The
-- staging drain applies already-admitted envelopes unchecked; admission is the
-- only gate, accepted work is never retroactively failed.
--
-- Ownership split (transitions fire exactly at the events that move the
-- metric): the FEED increments `recalc_backlog.pending_events` per delivered
-- physical fifo/lifo event and engages at the bound; the ENGINE resets the
-- counter to the exact committed tail above the new frontier at every settle
-- (wiping feed-lag skew, so the counter never drifts), applies the same bound
-- to the value it just wrote (a reset landing over the bound must not cross
-- silently), and releases at the low-water mark. Row-lock order shared by
-- every writer (feed apply, engine pass, close sweep):
-- pool_settlement → recalc_backlog → recalc_backpressure, multi-pool writers
-- ascending by pool id.

-- Tunable bounds (D10 posture: soak-output tunables, not design-time
-- constants). Single-row table; defaults calibrated ~3x the observed healthy
-- global max unsettled-event count so backpressure stays off in every passing
-- soak profile and engages within tens of seconds under a stalled engine.
CREATE TABLE recalc_backpressure_config (
    id           BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
    bound_events BIGINT NOT NULL DEFAULT 200 CHECK (bound_events > 0),
    low_water    BIGINT NOT NULL DEFAULT 20,
    CHECK (low_water >= 0 AND low_water < bound_events)
);

INSERT INTO recalc_backpressure_config DEFAULT VALUES;

-- Every engage/release/bump statement joins this row; with it missing they
-- all become silent no-ops and an engaged pool could never release. The
-- BOOLEAN PK prevents duplicates; this guard prevents emptiness.
CREATE FUNCTION recalc_backpressure_config_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'recalc_backpressure_config must keep its single row (UPDATE it instead)';
END;
$$;

CREATE TRIGGER recalc_backpressure_config_keep
    BEFORE DELETE OR TRUNCATE ON recalc_backpressure_config
    FOR EACH STATEMENT EXECUTE FUNCTION recalc_backpressure_config_guard();

-- Per-pool unsettled-event counter (G2a-shape gauge, fifo/lifo pools only).
-- Feed-incremented at mark time, engine-reset to the exact tail at settle;
-- `cost_adjustment_line` rows (the engine's own output) are never counted.
CREATE TABLE recalc_backlog (
    pool_id        BIGINT PRIMARY KEY REFERENCES pool(id),
    pending_events BIGINT NOT NULL DEFAULT 0
);

-- The throttled set. A row here rejects appends to that pool at admission;
-- the hot-path probe against this (normally empty) table's PK is both the
-- design's cheap global "backpressure active" flag and the per-pool consult —
-- one empty-index lookup in the common case.
CREATE TABLE recalc_backpressure (
    pool_id       BIGINT PRIMARY KEY REFERENCES pool(id),
    engaged_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Counter value observed at engagement (observability, not control).
    engage_events BIGINT NOT NULL
);

-- Staging-path admission gate. Enqueue is a raw heap INSERT (SPIKE-A shape),
-- so a BEFORE INSERT trigger is the admission hook. Zero-qty lines are no-ops
-- in the apply dispatch and do not count as appends here either. Malformed
-- payloads (non-array lines, non-integer pool_id/qty) fail OPEN: the gate is
-- admission control, not validation — such an envelope is admitted here and
-- classified 'failed' at drain decode, the same outcome it gets when the
-- throttled set is empty, so the error surface a producer sees never depends
-- on unrelated backpressure state.
CREATE FUNCTION ledger_inbox_backpressure_gate() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    v_pool BIGINT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM recalc_backpressure) THEN
        RETURN NEW;
    END IF;
    BEGIN
        SELECT bp.pool_id INTO v_pool
          FROM recalc_backpressure bp
         WHERE bp.pool_id IN (SELECT (l->>'pool_id')::bigint
                                FROM jsonb_array_elements(NEW.lines) AS l
                               WHERE COALESCE((l->>'qty')::bigint, 0) <> 0)
         LIMIT 1;
    EXCEPTION WHEN OTHERS THEN
        RETURN NEW;
    END;
    IF v_pool IS NOT NULL THEN
        RAISE EXCEPTION 'ledger_inbox: pool % is backpressure-throttled (unsettled backlog over bound)', v_pool
            USING ERRCODE = '53400';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER ledger_inbox_backpressure
    BEFORE INSERT ON ledger_inbox
    FOR EACH ROW EXECUTE FUNCTION ledger_inbox_backpressure_gate();
