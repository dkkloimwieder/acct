-- acct-k7c6 (P3 of acct-qdp5 PoC).
--
-- Batch entry point — accepts a JSONB array of envelopes, processes them in
-- one transaction as a batch. The fundamental shape under test:
--
--   1. PRE-LOCK STEP (separate statement; PERFORM forces strict ordering vs
--      the main CTE chain). Lock all accounts touched by the batch in
--      (id ASC) order. Two concurrent batches both order ASC → no deadlock.
--      Per-account locking is amortized across all envelopes in the batch.
--
--   2. MAIN CTE CHAIN (one statement; data-modifying CTEs are always
--      executed even when their output isn't projected).
--
--      parsed     — jsonb_array_elements unpacked.
--      replays    — envelopes whose idempotency_key already exists in
--                   posting_lines (caught BEFORE the INSERT to avoid the
--                   whole-batch rollback on UNIQUE violation).
--      to_insert  — non-replay envelopes.
--      inserted   — INSERT ... RETURNING id, idempotency_key (multi-row).
--      deltas + agg — collapse into one delta per account.
--      updated    — single UPDATE accounts FROM agg.
--
--      Final SELECT joins parsed × replays × inserted to emit per-envelope
--      status (committed | idempotent_replay).
--
-- HC6 (partial failure): whole-batch rollback. If any envelope violates the
-- amount > 0 / debit <> credit CHECKs or the UNIQUE constraint races between
-- pre-pass and INSERT (a concurrent batch inserted the same idempotency_key),
-- the entire post_batch transaction rolls back. Caller retries. This is the
-- simpler MVP semantic; per-envelope status (TigerBeetle shape) deferred to P7.
--
-- HC5 (idempotent replay): replays return the existing posting_line_id with
-- status='idempotent_replay'. Caller can distinguish from a fresh commit.
--
-- HC9 (multi-currency): currency is sourced from the debit account; the
-- batch can span currencies. No same-currency enforcement at this layer (the
-- batch can mix as long as each pair's debit/credit agree, which P3 trusts
-- the caller to ensure; HC9 in P7 will tighten this).

CREATE OR REPLACE FUNCTION post_batch(p_envelopes JSONB)
RETURNS TABLE (
    envelope_idx     INT,
    status           TEXT,
    posting_line_id  BIGINT,
    error_code       TEXT,
    error_message    TEXT
)
LANGUAGE plpgsql AS $$
BEGIN
    -- Step 1: lock accounts in deterministic order BEFORE any INSERT/UPDATE.
    PERFORM accounts.id
    FROM accounts
    WHERE accounts.id IN (
        SELECT (e->>'debit_account_id')::BIGINT  FROM jsonb_array_elements(p_envelopes) e
        UNION
        SELECT (e->>'credit_account_id')::BIGINT FROM jsonb_array_elements(p_envelopes) e
    )
    ORDER BY accounts.id
    FOR UPDATE;

    -- Step 2: parse + classify + INSERT + UPDATE + return.
    RETURN QUERY
    WITH parsed AS (
        SELECT
            (e->>'envelope_idx')::INT          AS envelope_idx,
            (e->>'debit_account_id')::BIGINT   AS debit_account_id,
            (e->>'credit_account_id')::BIGINT  AS credit_account_id,
            (e->>'amount')::BIGINT             AS amount,
            (e->>'idempotency_key')::UUID      AS idempotency_key,
            (e->>'business_date')::DATE        AS business_date
        FROM jsonb_array_elements(p_envelopes) e
    ),
    replays AS (
        SELECT p.envelope_idx, p.idempotency_key, pl.id AS posting_line_id
        FROM parsed p
        JOIN posting_lines pl ON pl.idempotency_key = p.idempotency_key
    ),
    to_insert AS (
        SELECT p.* FROM parsed p
        WHERE NOT EXISTS (SELECT 1 FROM replays r WHERE r.envelope_idx = p.envelope_idx)
    ),
    inserted AS (
        INSERT INTO posting_lines
            (debit_account_id, credit_account_id, amount, currency,
             idempotency_key, business_date)
        SELECT ti.debit_account_id, ti.credit_account_id, ti.amount, a.currency,
               ti.idempotency_key, ti.business_date
        FROM to_insert ti
        JOIN accounts a ON a.id = ti.debit_account_id
        RETURNING id, idempotency_key
    ),
    deltas AS (
        SELECT debit_account_id  AS aid,  amount AS d FROM to_insert
        UNION ALL
        SELECT credit_account_id AS aid, -amount AS d FROM to_insert
    ),
    agg AS (SELECT aid, SUM(d)::BIGINT AS d FROM deltas GROUP BY 1),
    updated AS (
        UPDATE accounts SET balance = balance + agg.d
        FROM agg WHERE accounts.id = agg.aid
        RETURNING accounts.id
    )
    SELECT
        p.envelope_idx,
        CASE WHEN r.posting_line_id IS NOT NULL THEN 'idempotent_replay'::TEXT
             ELSE 'committed'::TEXT END                  AS status,
        COALESCE(r.posting_line_id, i.id)                AS posting_line_id,
        NULL::TEXT                                        AS error_code,
        NULL::TEXT                                        AS error_message
    FROM parsed p
    LEFT JOIN replays  r ON r.envelope_idx = p.envelope_idx
    LEFT JOIN inserted i ON i.idempotency_key = p.idempotency_key
    ORDER BY p.envelope_idx;

    -- Force `updated` to be evaluated; it's data-modifying so PG executes it
    -- regardless of projection, but explicitly stating the contract here.
END$$;
