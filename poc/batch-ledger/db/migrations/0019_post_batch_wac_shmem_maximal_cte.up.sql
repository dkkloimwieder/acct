-- acct-2g9w (optimization): single-CTE variant of post_batch_wac_shmem_maximal.
--
-- mig 0018 used a TEMP TABLE pattern (CREATE TEMP TABLE ON COMMIT DROP,
-- per-batch TRUNCATE, INSERT, UPDATE for replays, final UPDATE for new ids).
-- Each batch pays ~10-30ms of temp-table maintenance.
--
-- mig 0019 collapses everything into a single SQL statement with WITH CTEs:
--   - `input`     : parse JSONB array once
--   - `existing`  : JOIN to posting_lines for replays
--   - `non_replay`: anti-JOIN of input vs existing
--   - `priced`    : ledger_dispatch_wac_batch on non-replay JSONB
--   - `inserted`  : INSERT INTO posting_lines RETURNING (id, idempotency_key)
--   - final SELECT: per-envelope status row
--
-- All in one statement = one planner pass, no temp table maintenance, no
-- per-step round-trip from the function body to the SPI layer.

CREATE OR REPLACE FUNCTION post_batch_wac_shmem_maximal(p_envelopes JSONB)
RETURNS TABLE (
    envelope_idx     INT,
    status           TEXT,
    posting_line_id  BIGINT,
    error_code       TEXT,
    error_message    TEXT
)
LANGUAGE sql AS $$
WITH input AS (
    SELECT
        (e->>'envelope_idx')::INT      AS envelope_idx,
        (e->>'idempotency_key')::UUID  AS idempotency_key,
        (e->>'business_date')::DATE    AS business_date,
        e                              AS envelope
    FROM jsonb_array_elements(p_envelopes) e
),
existing AS (
    SELECT pl.idempotency_key, pl.id AS replay_pl_id
    FROM posting_lines pl
    JOIN input i ON pl.idempotency_key = i.idempotency_key
),
non_replay_input AS (
    SELECT i.*
    FROM input i
    LEFT JOIN existing e ON e.idempotency_key = i.idempotency_key
    WHERE e.replay_pl_id IS NULL
),
priced AS (
    SELECT *
    FROM ledger_dispatch_wac_batch(
        (SELECT COALESCE(jsonb_agg(envelope ORDER BY envelope_idx), '[]'::JSONB)
         FROM non_replay_input)
    )
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
        n.idempotency_key,
        n.business_date,
        p.qty
    FROM priced p
    JOIN non_replay_input n ON n.envelope_idx = p.envelope_idx
    JOIN accounts a ON a.id = p.debit_account_id
    RETURNING id, idempotency_key
)
SELECT
    i.envelope_idx,
    CASE WHEN e.replay_pl_id IS NOT NULL THEN 'idempotent_replay'::TEXT
                                         ELSE 'committed'::TEXT END,
    COALESCE(e.replay_pl_id, ins.id),
    NULL::TEXT,
    NULL::TEXT
FROM input i
LEFT JOIN existing  e   ON e.idempotency_key = i.idempotency_key
LEFT JOIN inserted ins ON ins.idempotency_key = i.idempotency_key
ORDER BY i.envelope_idx;
$$;
