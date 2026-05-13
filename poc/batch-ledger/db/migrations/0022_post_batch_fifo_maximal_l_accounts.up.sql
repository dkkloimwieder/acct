-- acct-t59i — L-accounts variant of post_batch_fifo_maximal.
--
-- Replaces mig 0021's body. Same Rust dispatcher + single-CTE pipeline;
-- the change is WHERE serialization happens:
--
--   mig 0021: FOR UPDATE on cost_layers rows (inside the dispatcher's SPI).
--             Fan-out batches lock ~3,500 layer rows → per-batch lock
--             acquisition overwhelms set-based INSERT savings. 8-18× slower
--             than mig 0020 mutable (acct-oqje bench).
--
--   mig 0022: FOR UPDATE on accounts rows (at wrapper top). Mirrors mig 0020
--             mutable's serialization model exactly. Per-batch lock count is
--             O(#distinct accounts touched), typically O(3) for simple
--             transfers and O(pool_count + AP + COGS) for FIFO traffic.
--
-- LANGUAGE changes from sql → plpgsql so we can issue PERFORM ... FOR UPDATE
-- before the CTE chain runs. SQL inlining + FOR UPDATE inside a CTE is
-- fraught; plpgsql sidesteps it cleanly. Per-batch plpgsql overhead is
-- negligible vs the lock cost we're trading away.
--
-- Correctness: any FIFO write necessarily touches its pool account (debit on
-- fifo_receipt, credit on fifo_issue). Postgres' row-level lock on
-- accounts.id serializes concurrent writers on the same pool. The
-- dispatcher's SPI reads cost_layers via a plain SELECT — within the
-- serialized region the snapshot is internally consistent for the txn.
-- This is the same model mig 0020 mutable has used since acct-2g9w-followup.
--
-- ORDER BY accounts.id on the FOR UPDATE is load-bearing for deadlock
-- freedom: concurrent writers must acquire pool locks in the same order.
-- T5 (8-writer fan-in coupled writes) regression-nets this.

CREATE OR REPLACE FUNCTION post_batch_fifo_maximal(p_envelopes JSONB)
RETURNS TABLE (
    envelope_idx     INT,
    status           TEXT,
    posting_line_id  BIGINT,
    error_code       TEXT,
    error_message    TEXT
)
LANGUAGE plpgsql AS $$
#variable_conflict use_column
BEGIN
    -- Lock every account touched by the batch under FOR UPDATE before
    -- the dispatcher runs. UNION over both sides covers pool + AP for
    -- receipts and pool + COGS for issues. ORDER BY accounts.id enforces
    -- consistent lock-acquisition order across writers.
    PERFORM accounts.id
    FROM accounts
    WHERE accounts.id IN (
        SELECT (e->>'debit_account_id')::BIGINT  FROM jsonb_array_elements(p_envelopes) e
        UNION
        SELECT (e->>'credit_account_id')::BIGINT FROM jsonb_array_elements(p_envelopes) e
    )
    ORDER BY accounts.id
    FOR UPDATE;

    RETURN QUERY
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
    dispatched AS (
        SELECT *
        FROM ledger_dispatch_fifo_batch(
            (SELECT COALESCE(jsonb_agg(envelope ORDER BY envelope_idx), '[]'::JSONB)
             FROM non_replay_input)
        )
    ),
    legs AS (
        SELECT * FROM dispatched WHERE row_kind = 'leg'
    ),
    depls AS (
        SELECT * FROM dispatched WHERE row_kind = 'depl'
    ),
    inserted_pl AS (
        INSERT INTO posting_lines
            (debit_account_id, credit_account_id, amount, currency,
             idempotency_key, business_date, qty)
        SELECT
            l.debit_account_id,
            l.credit_account_id,
            l.amount,
            a.currency,
            n.idempotency_key,
            n.business_date,
            CASE WHEN l.kind = 'transfer' THEN NULL ELSE l.qty END
        FROM legs l
        JOIN non_replay_input n ON n.envelope_idx = l.envelope_idx
        JOIN accounts a ON a.id = l.debit_account_id
        RETURNING id, idempotency_key
    ),
    in_batch_drain AS (
        SELECT layer_sentinel, SUM(depl_qty)::BIGINT AS drain
        FROM depls
        WHERE layer_sentinel IS NOT NULL
        GROUP BY layer_sentinel
    ),
    inserted_layers AS (
        INSERT INTO cost_layers
            (pool_account_id, qty_remaining, unit_cost, receipt_date,
             receipt_posting_line_id)
        SELECT
            l.debit_account_id,
            l.qty - COALESCE(d.drain, 0),
            l.unit_cost,
            n.business_date,
            ipl.id
        FROM legs l
        JOIN non_replay_input n ON n.envelope_idx = l.envelope_idx
        JOIN inserted_pl ipl ON ipl.idempotency_key = n.idempotency_key
        LEFT JOIN in_batch_drain d ON d.layer_sentinel = l.layer_sentinel
        WHERE l.kind = 'fifo_receipt'
        RETURNING id AS real_layer_id, receipt_posting_line_id
    ),
    sentinel_map AS (
        SELECT
            l.layer_sentinel,
            il.real_layer_id
        FROM legs l
        JOIN non_replay_input n ON n.envelope_idx = l.envelope_idx
        JOIN inserted_pl ipl ON ipl.idempotency_key = n.idempotency_key
        JOIN inserted_layers il ON il.receipt_posting_line_id = ipl.id
        WHERE l.kind = 'fifo_receipt'
    ),
    resolved_depls AS (
        SELECT
            d.envelope_idx,
            COALESCE(d.layer_id, sm.real_layer_id) AS layer_id,
            d.depl_qty,
            d.depl_cost,
            ipl.id AS issue_posting_line_id
        FROM depls d
        JOIN non_replay_input n ON n.envelope_idx = d.envelope_idx
        JOIN inserted_pl ipl ON ipl.idempotency_key = n.idempotency_key
        LEFT JOIN sentinel_map sm ON sm.layer_sentinel = d.layer_sentinel
    ),
    inserted_dep AS (
        INSERT INTO cost_layer_depletions
            (layer_id, issue_posting_line_id, qty_consumed, cost_amount)
        SELECT layer_id, issue_posting_line_id, depl_qty, depl_cost
        FROM resolved_depls
        RETURNING 1
    ),
    pre_existing_drain AS (
        SELECT d.layer_id, SUM(d.depl_qty)::BIGINT AS drain
        FROM depls d
        WHERE d.layer_id IS NOT NULL
        GROUP BY d.layer_id
    ),
    updated_layers AS (
        UPDATE cost_layers
           SET qty_remaining = qty_remaining - pd.drain
          FROM pre_existing_drain pd
         WHERE cost_layers.id = pd.layer_id
        RETURNING 1
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
    LEFT JOIN inserted_pl ins ON ins.idempotency_key = i.idempotency_key
    ORDER BY i.envelope_idx;
END$$;
