-- acct-m6q3 — `post_batch_fifo_maximal`: FIFO dispatch fully in Rust.
--
-- Mirrors mig 0019's single-CTE shape for `post_batch_wac_shmem_maximal`,
-- adapted for FIFO's two output streams (legs + per-layer depletions).
-- `ledger_dispatch_fifo_batch` returns rows tagged with `row_kind`; this
-- wrapper splits them via CTEs and resolves layer_sentinel → real
-- cost_layers.id after the receipt INSERT.
--
-- Critical PG semantic constraint: an UPDATE in a WITH clause CANNOT see
-- rows that a sibling INSERT CTE just inserted. So we split layer
-- bookkeeping in two:
--
--   - In-batch new layers (those just INSERTed by inserted_layers): the
--     INSERT pre-deducts any in-batch consumption (in_batch_drain) so
--     their qty_remaining lands correct on first write.
--
--   - Pre-existing layers (loaded under FOR UPDATE by the dispatcher
--     and visible in the snapshot): UPDATE decrements them via
--     pre_existing_drain.
--
-- Pipeline:
--   input              : parse JSONB array once
--   existing           : JOIN to posting_lines for replays
--   non_replay_input   : anti-JOIN of input vs existing
--   dispatched         : ledger_dispatch_fifo_batch on non-replay JSONB
--   legs / depls       : filter dispatched by row_kind
--   inserted_pl        : INSERT INTO posting_lines RETURNING (id, idempotency_key)
--   in_batch_drain     : SUM(depl_qty) per sentinel for new layers
--   inserted_layers    : INSERT INTO cost_layers w/ qty_remaining = receipt_qty - in_batch_drain
--   sentinel_map       : (layer_sentinel → real_layer_id)
--   resolved_depls     : COALESCE(layer_id, sentinel_map.real_layer_id)
--   inserted_dep       : INSERT INTO cost_layer_depletions
--   pre_existing_drain : SUM(depl_qty) per pre-existing layer_id
--   updated_layers     : UPDATE cost_layers.qty_remaining
--   final SELECT       : per-envelope status row

CREATE OR REPLACE FUNCTION post_batch_fifo_maximal(p_envelopes JSONB)
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
$$;
