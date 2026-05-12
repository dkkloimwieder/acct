-- acct-4dg2-pac (P4 PAC variant of acct-qdp5 PoC).
--
-- Periodic Average Cost (PAC) — preset-and-backfill alternative to the
-- strict running-average WAC perpetual measured in P4.
--
-- Insight: P4's strict WAC paid O(n) plpgsql per envelope (jsonb_set on a
-- growing running-state map) costing ~50% of P3 simple's ceiling. PAC
-- snapshots the pool's average ONCE at batch start; every issue in the
-- batch prices at that constant. Pure SQL CTE chain — no FOR LOOP, no
-- jsonb_set growing pattern.
--
-- Drift vs strict WAC: an issue late in the batch should "see" prior
-- in-batch receipts' contribution to avg. PAC ignores that intra-batch
-- drift. Acct's wac_periodic semantics do the same: in-period postings
-- use a provisional cost, and a close hook posts variance per pool at
-- period close. For PoC simplicity we omit the variance posting; pool
-- balance still updates with caller-supplied receipt amounts + issue
-- amounts at snapshot avg, and a period close hook (out of PoC scope)
-- would handle reconciliation.
--
-- New envelope kinds:
--   wac_pac_receipt — like wac_receipt, no semantic change (amount =
--                     qty * unit_cost).
--   wac_pac_issue   — amount = qty * snapshot_avg (snapshot taken at
--                     batch start from accounts.balance / accounts.qty).

CREATE OR REPLACE FUNCTION post_batch(p_envelopes JSONB)
RETURNS TABLE (
    envelope_idx     INT,
    status           TEXT,
    posting_line_id  BIGINT,
    error_code       TEXT,
    error_message    TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_has_complex BOOLEAN;
BEGIN
    -- 1. Lock all touched accounts in deterministic order.
    PERFORM accounts.id
    FROM accounts
    WHERE accounts.id IN (
        SELECT (e->>'debit_account_id')::BIGINT  FROM jsonb_array_elements(p_envelopes) e
        UNION
        SELECT (e->>'credit_account_id')::BIGINT FROM jsonb_array_elements(p_envelopes) e
    )
    ORDER BY accounts.id
    FOR UPDATE;

    -- 2. Detect whether the batch contains kinds that require the plpgsql
    --    FOR LOOP (wac_perpetual, fifo_*). If so, route through the older
    --    function body (we keep that as post_batch_complex; for PoC we
    --    accept that mixing PAC + non-PAC in the same batch is unsupported
    --    and error out). Pure transfer + wac_pac_* batches use the fast path.
    SELECT EXISTS (
        SELECT 1 FROM jsonb_array_elements(p_envelopes) e
         WHERE COALESCE(e->>'kind', 'transfer') NOT IN ('transfer','wac_pac_receipt','wac_pac_issue')
    ) INTO v_has_complex;

    IF v_has_complex THEN
        RAISE EXCEPTION 'mig 0010 post_batch supports only transfer + wac_pac_* kinds. Use the 0009 body for wac_*/fifo_* (PoC-scoped limitation).';
    END IF;

    -- 2b. Validate: any wac_pac_issue with an empty pool fails the whole batch.
    PERFORM 1
    FROM jsonb_array_elements(p_envelopes) e
    JOIN accounts a ON a.id = (e->>'credit_account_id')::BIGINT
    WHERE e->>'kind' = 'wac_pac_issue'
      AND COALESCE(a.qty, 0) <= 0;
    IF FOUND THEN
        RAISE EXCEPTION 'wac_pac_issue from empty pool — whole batch rejected';
    END IF;

    -- 3. Pure-SQL CTE chain. No plpgsql FOR LOOP.
    RETURN QUERY
    WITH parsed AS (
        SELECT
            (e->>'envelope_idx')::INT          AS env_idx,
            COALESCE(e->>'kind', 'transfer')   AS kind,
            (e->>'debit_account_id')::BIGINT   AS debit_account_id,
            (e->>'credit_account_id')::BIGINT  AS credit_account_id,
            CASE WHEN e ? 'amount'    THEN (e->>'amount')::BIGINT    ELSE NULL END AS amount,
            CASE WHEN e ? 'qty'       THEN (e->>'qty')::BIGINT       ELSE NULL END AS qty,
            CASE WHEN e ? 'unit_cost' THEN (e->>'unit_cost')::BIGINT ELSE NULL END AS unit_cost,
            (e->>'idempotency_key')::UUID      AS idempotency_key,
            (e->>'business_date')::DATE        AS business_date
        FROM jsonb_array_elements(p_envelopes) e
    ),
    -- Snapshot pools touched by wac_pac_issue (taken before any INSERTs).
    pool_snapshots AS (
        SELECT a.id AS pool_id,
               CASE WHEN a.qty > 0 THEN (a.balance / a.qty) ELSE NULL END AS snapshot_avg
          FROM accounts a
         WHERE a.id IN (SELECT credit_account_id FROM parsed WHERE kind = 'wac_pac_issue')
    ),
    -- Compute final amount per envelope.
    priced AS (
        SELECT p.*,
               CASE p.kind
                 WHEN 'transfer'         THEN p.amount
                 WHEN 'wac_pac_receipt'  THEN p.qty * p.unit_cost
                 WHEN 'wac_pac_issue'    THEN
                   CASE WHEN ps.snapshot_avg IS NULL THEN NULL
                        ELSE p.qty * ps.snapshot_avg
                   END
               END AS final_amount,
               ps.snapshot_avg AS snapshot_avg_used
          FROM parsed p
          LEFT JOIN pool_snapshots ps ON ps.pool_id = p.credit_account_id
    ),
    replays AS (
        SELECT p.env_idx, p.idempotency_key, pl.id AS posting_line_id
          FROM priced p
          JOIN posting_lines pl ON pl.idempotency_key = p.idempotency_key
    ),
    to_insert AS (
        SELECT p.* FROM priced p
         WHERE NOT EXISTS (SELECT 1 FROM replays r WHERE r.env_idx = p.env_idx)
    ),
    inserted AS (
        INSERT INTO posting_lines
            (debit_account_id, credit_account_id, amount, currency,
             idempotency_key, business_date, qty)
        SELECT ti.debit_account_id, ti.credit_account_id, ti.final_amount, a.currency,
               ti.idempotency_key, ti.business_date,
               CASE WHEN ti.kind = 'transfer' THEN NULL ELSE ti.qty END
          FROM to_insert ti
          JOIN accounts a ON a.id = ti.debit_account_id
         ORDER BY ti.env_idx
        RETURNING id, idempotency_key
    ),
    bal_deltas AS (
        SELECT ti.debit_account_id  AS aid,  ti.final_amount AS d FROM to_insert ti
        UNION ALL
        SELECT ti.credit_account_id AS aid, -ti.final_amount AS d FROM to_insert ti
    ),
    qty_deltas AS (
        SELECT ti.debit_account_id  AS aid,  COALESCE(ti.qty, 0) AS d
          FROM to_insert ti WHERE ti.kind = 'wac_pac_receipt'
        UNION ALL
        SELECT ti.credit_account_id AS aid, -COALESCE(ti.qty, 0) AS d
          FROM to_insert ti WHERE ti.kind = 'wac_pac_issue'
    ),
    agg_bal AS (SELECT aid, SUM(d)::BIGINT AS d FROM bal_deltas GROUP BY aid),
    agg_qty AS (SELECT aid, SUM(d)::BIGINT AS d FROM qty_deltas GROUP BY aid),
    all_aids AS (SELECT aid FROM agg_bal UNION SELECT aid FROM agg_qty),
    combined AS (
        SELECT a.aid, COALESCE(b.d, 0) AS d_balance, COALESCE(q.d, 0) AS d_qty
          FROM all_aids a
          LEFT JOIN agg_bal b ON b.aid = a.aid
          LEFT JOIN agg_qty q ON q.aid = a.aid
    ),
    updated AS (
        UPDATE accounts
           SET balance = balance + c.d_balance,
               qty     = qty     + c.d_qty
          FROM combined c
         WHERE accounts.id = c.aid
        RETURNING accounts.id
    )
    SELECT
        p.env_idx AS envelope_idx,
        CASE WHEN r.posting_line_id IS NOT NULL THEN 'idempotent_replay'::TEXT
             ELSE 'committed'::TEXT END                              AS status,
        COALESCE(r.posting_line_id, i.id)                            AS posting_line_id,
        NULL::TEXT                                                    AS error_code,
        NULL::TEXT                                                    AS error_message
    FROM parsed p
    LEFT JOIN replays  r ON r.env_idx = p.env_idx
    LEFT JOIN inserted i ON i.idempotency_key = p.idempotency_key
    ORDER BY p.env_idx;
END$$;
