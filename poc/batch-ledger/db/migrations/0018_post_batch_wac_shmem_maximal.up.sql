-- acct-2g9w — maximal r8xv: thin SQL wrapper over `ledger_dispatch_wac_batch`.
--
-- mig 0014's plpgsql `post_batch_wac_shmem` does ALL per-envelope work in
-- plpgsql: jsonb_set on running-avg maps, per-envelope INSERT into a temp
-- staging table, two PERFORM cross-extern calls per envelope. At batch=1000
-- × N_pools=5000 fan-out, that's ~30-40ms of plpgsql overhead per batch on
-- top of the per-cell atomic CAS work.
--
-- mig 0018 pushes the hot path into Rust via `ledger_dispatch_wac_batch`:
--
--   1. Idempotency replay filter (SQL — set-based JOIN; cheap)
--   2. Dispatcher (Rust — parse JSONB, validate, lazy shmem seed, in-batch
--      running-avg HashMap, per-leg amount + qty compute, stage_apply each
--      leg into A2 PENDING_STACK)
--   3. Set-based INSERT INTO posting_lines from dispatcher's TableIterator
--      output JOIN'd with the input map for idempotency_key / business_date
--      and `accounts` for currency
--   4. Per-envelope status return (replay vs committed) — same contract as
--      mig 0014.
--
-- Semantic differences from mig 0014:
--
--   - Replays are pre-filtered: replay envelopes do NOT contribute to the
--     in-batch running average. In mig 0014 they DO (replay-then-skip
--     pattern after pricing). Documented divergence; replays are rare and
--     mid-batch mixed replay is even rarer; the cleaner semantic is "replay
--     == no-op to ledger state, including running avg".
--   - Pool snapshot is lazy: only pools actually referenced by a non-replay
--     envelope get a shmem probe. mig 0014 unconditionally probed every WAC
--     pool the input mentions even if all envelopes referencing it turned
--     out to be replays.
--
-- Functional contract matches mig 0014: same envelope shape; same per-
-- envelope status return; same idempotency behaviour (replay returns the
-- pre-existing posting_line_id).

CREATE OR REPLACE FUNCTION post_batch_wac_shmem_maximal(p_envelopes JSONB)
RETURNS TABLE (
    envelope_idx     INT,
    status           TEXT,
    posting_line_id  BIGINT,
    error_code       TEXT,
    error_message    TEXT
)
LANGUAGE plpgsql AS $$
#variable_conflict use_column
DECLARE
    v_non_replay_envelopes JSONB;
BEGIN
    -- 1. Build the input map once, marking replays.
    CREATE TEMP TABLE IF NOT EXISTS _wac_maximal_input (
        envelope_idx     INT,
        idempotency_key  UUID,
        business_date    DATE,
        is_replay        BOOLEAN,
        replay_pl_id     BIGINT,
        new_pl_id        BIGINT,
        envelope         JSONB
    ) ON COMMIT DROP;
    TRUNCATE _wac_maximal_input;

    INSERT INTO _wac_maximal_input
        (envelope_idx, idempotency_key, business_date, is_replay, replay_pl_id, envelope)
    SELECT
        (e->>'envelope_idx')::INT,
        (e->>'idempotency_key')::UUID,
        (e->>'business_date')::DATE,
        FALSE,
        NULL,
        e
    FROM jsonb_array_elements(p_envelopes) e;

    UPDATE _wac_maximal_input s
       SET is_replay = TRUE, replay_pl_id = pl.id
      FROM posting_lines pl
     WHERE pl.idempotency_key = s.idempotency_key;

    -- 2. Hand non-replay envelopes to the Rust dispatcher. Single JSONB
    --    array; dispatcher returns priced legs via TableIterator.
    SELECT jsonb_agg(envelope ORDER BY envelope_idx)
      INTO v_non_replay_envelopes
      FROM _wac_maximal_input
     WHERE NOT is_replay;

    IF v_non_replay_envelopes IS NULL THEN
        v_non_replay_envelopes := '[]'::JSONB;
    END IF;

    -- 3. Set-based INSERT driven by the dispatcher's output. JOIN to the
    --    input map for idempotency_key / business_date and to `accounts`
    --    for currency. ORDER BY envelope_idx so posting_lines.id ordering
    --    is deterministic across the batch (no semantic dependency on it,
    --    but useful for tests).
    WITH priced AS (
        SELECT * FROM ledger_dispatch_wac_batch(v_non_replay_envelopes)
    ),
    inserted AS (
        INSERT INTO posting_lines
            (debit_account_id, credit_account_id, amount, currency,
             idempotency_key, business_date, qty)
        SELECT
            p.debit_account_id,
            p.credit_account_id,
            p.amount,
            a.currency,
            i.idempotency_key,
            i.business_date,
            p.qty
        FROM priced p
        JOIN _wac_maximal_input i ON i.envelope_idx = p.envelope_idx
        JOIN accounts a ON a.id = p.debit_account_id
        ORDER BY p.envelope_idx
        RETURNING id, idempotency_key
    )
    UPDATE _wac_maximal_input s
       SET new_pl_id = ins.id
      FROM inserted ins
     WHERE ins.idempotency_key = s.idempotency_key;

    -- 4. Per-envelope status return. Same contract as mig 0014.
    RETURN QUERY
    SELECT
        s.envelope_idx,
        CASE WHEN s.is_replay THEN 'idempotent_replay'::TEXT ELSE 'committed'::TEXT END,
        COALESCE(s.replay_pl_id, s.new_pl_id),
        NULL::TEXT,
        NULL::TEXT
    FROM _wac_maximal_input s
    ORDER BY s.envelope_idx;
END$$;
