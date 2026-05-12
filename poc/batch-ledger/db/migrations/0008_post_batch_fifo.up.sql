-- acct-1hps (P5 of acct-qdp5 PoC).
--
-- Extend post_batch with FIFO kinds:
--   "fifo_receipt": debit_account_id=pool, credit_account_id=source, qty, unit_cost
--   "fifo_issue":   debit_account_id=sink, credit_account_id=pool, qty
--                   (amount computed by walking cost_layers in receipt-date order)
--
-- For PoC simplicity, FIFO uses per-envelope statements inside the plpgsql
-- FOR LOOP rather than multi-row INSERTs. This is measurably slower than the
-- transfer / wac paths but correct under all in-batch sequencing scenarios:
--   - in-batch fifo_issue sees layers INSERTed by earlier in-batch fifo_receipts.
--   - depletions reference actual cost_layers.id values directly.
--
-- Idempotency: handled by an early-return when ALL envelopes in the batch have
-- pre-existing idempotency_keys (full replay). Mixed partial-replay is handled
-- correctly because each per-envelope INSERT into posting_lines would violate
-- the UNIQUE constraint if a replay slipped through; we filter pre-loop.

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
    v_env         RECORD;
    v_kind        TEXT;
    v_pool_id     BIGINT;
    v_value       BIGINT;
    v_qty         BIGINT;
    v_amount      BIGINT;
    v_unit_cost   BIGINT;
    v_pool_key    TEXT;
    v_pool_value  JSONB := '{}'::JSONB;
    v_pool_qty    JSONB := '{}'::JSONB;
    v_layer       RECORD;
    v_remaining   BIGINT;
    v_take        BIGINT;
    v_total_cost  BIGINT;
    v_pl_id       BIGINT;
    v_layer_id    BIGINT;
    v_was_replay  BOOLEAN;
    v_plan_layer_ids BIGINT[];
    v_plan_takes     BIGINT[];
    v_plan_costs     BIGINT[];
    i             INT;
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

    -- 2. Build the per-batch staging temp.
    CREATE TEMP TABLE IF NOT EXISTS _batch_staging (
        envelope_idx       INT,
        kind               TEXT,
        debit_account_id   BIGINT,
        credit_account_id  BIGINT,
        amount             BIGINT,
        qty                BIGINT,
        currency           CHAR(3),
        idempotency_key    UUID,
        business_date      DATE,
        is_replay          BOOLEAN,
        replay_pl_id       BIGINT,
        new_pl_id          BIGINT
    ) ON COMMIT DROP;
    TRUNCATE _batch_staging;

    -- 3. Pre-load WAC pool snapshots (running map seed) — applies to wac_* kinds.
    FOR v_env IN
        SELECT DISTINCT pool_id::TEXT AS k, a.balance AS v, a.qty AS q
        FROM (
            SELECT (e->>'debit_account_id')::BIGINT  AS pool_id FROM jsonb_array_elements(p_envelopes) e WHERE e->>'kind' = 'wac_receipt'
            UNION
            SELECT (e->>'credit_account_id')::BIGINT AS pool_id FROM jsonb_array_elements(p_envelopes) e WHERE e->>'kind' = 'wac_issue'
        ) pools
        JOIN accounts a ON a.id = pools.pool_id
    LOOP
        v_pool_value := v_pool_value || jsonb_build_object(v_env.k, v_env.v);
        v_pool_qty   := v_pool_qty   || jsonb_build_object(v_env.k, v_env.q);
    END LOOP;

    -- 4. Walk envelopes in order.
    FOR v_env IN
        SELECT
            (e->>'envelope_idx')::INT          AS envelope_idx,
            COALESCE(e->>'kind', 'transfer')   AS kind,
            (e->>'debit_account_id')::BIGINT   AS debit_account_id,
            (e->>'credit_account_id')::BIGINT  AS credit_account_id,
            CASE WHEN e ? 'amount'    THEN (e->>'amount')::BIGINT    ELSE NULL END AS amount,
            CASE WHEN e ? 'qty'       THEN (e->>'qty')::BIGINT       ELSE NULL END AS qty,
            CASE WHEN e ? 'unit_cost' THEN (e->>'unit_cost')::BIGINT ELSE NULL END AS unit_cost,
            (e->>'idempotency_key')::UUID      AS idempotency_key,
            (e->>'business_date')::DATE        AS business_date
        FROM jsonb_array_elements(p_envelopes) e
        ORDER BY (e->>'envelope_idx')::INT
    LOOP
        v_kind := v_env.kind;
        v_amount := v_env.amount;
        v_qty := v_env.qty;
        v_unit_cost := v_env.unit_cost;
        v_pl_id := NULL;

        IF v_kind = 'transfer' THEN
            NULL;

        ELSIF v_kind = 'wac_receipt' THEN
            v_pool_id := v_env.debit_account_id;
            v_pool_key := v_pool_id::TEXT;
            IF v_qty IS NULL OR v_qty <= 0 OR v_unit_cost IS NULL OR v_unit_cost <= 0 THEN
                RAISE EXCEPTION 'wac_receipt envelope_idx=% needs positive qty + unit_cost', v_env.envelope_idx;
            END IF;
            v_amount := v_qty * v_unit_cost;
            v_pool_value := jsonb_set(v_pool_value, ARRAY[v_pool_key],
                to_jsonb((v_pool_value->>v_pool_key)::BIGINT + v_amount));
            v_pool_qty := jsonb_set(v_pool_qty, ARRAY[v_pool_key],
                to_jsonb((v_pool_qty->>v_pool_key)::BIGINT + v_qty));

        ELSIF v_kind = 'wac_issue' THEN
            v_pool_id := v_env.credit_account_id;
            v_pool_key := v_pool_id::TEXT;
            IF v_qty IS NULL OR v_qty <= 0 THEN
                RAISE EXCEPTION 'wac_issue envelope_idx=% needs positive qty', v_env.envelope_idx;
            END IF;
            v_value := (v_pool_value->>v_pool_key)::BIGINT;
            DECLARE
                v_running_qty BIGINT := (v_pool_qty->>v_pool_key)::BIGINT;
            BEGIN
                IF v_running_qty IS NULL OR v_running_qty <= 0 THEN
                    RAISE EXCEPTION 'wac_issue envelope_idx=% from empty pool % (running qty=%)',
                        v_env.envelope_idx, v_pool_id, v_running_qty;
                END IF;
                IF v_qty > v_running_qty THEN
                    RAISE EXCEPTION 'wac_issue envelope_idx=% qty=% exceeds running qty=%',
                        v_env.envelope_idx, v_qty, v_running_qty;
                END IF;
                v_unit_cost := v_value / v_running_qty;
                v_amount := v_unit_cost * v_qty;
                v_pool_value := jsonb_set(v_pool_value, ARRAY[v_pool_key],
                    to_jsonb(v_value - v_amount));
                v_pool_qty := jsonb_set(v_pool_qty, ARRAY[v_pool_key],
                    to_jsonb(v_running_qty - v_qty));
            END;

        ELSIF v_kind = 'fifo_receipt' THEN
            v_pool_id := v_env.debit_account_id;
            IF v_qty IS NULL OR v_qty <= 0 OR v_unit_cost IS NULL OR v_unit_cost <= 0 THEN
                RAISE EXCEPTION 'fifo_receipt envelope_idx=% needs positive qty + unit_cost', v_env.envelope_idx;
            END IF;
            v_amount := v_qty * v_unit_cost;
            v_pl_id := NULL;
            SELECT id INTO v_pl_id FROM posting_lines WHERE idempotency_key = v_env.idempotency_key;
            v_was_replay := v_pl_id IS NOT NULL;
            IF NOT v_was_replay THEN
                INSERT INTO posting_lines
                    (debit_account_id, credit_account_id, amount, currency,
                     idempotency_key, business_date, qty)
                SELECT v_env.debit_account_id, v_env.credit_account_id, v_amount, a.currency,
                       v_env.idempotency_key, v_env.business_date, v_qty
                FROM accounts a WHERE a.id = v_env.debit_account_id
                RETURNING id INTO v_pl_id;
                INSERT INTO cost_layers
                    (pool_account_id, qty_remaining, unit_cost, receipt_date, receipt_posting_line_id)
                VALUES (v_pool_id, v_qty, v_unit_cost, v_env.business_date, v_pl_id);
                UPDATE accounts SET balance = balance + v_amount, qty = qty + v_qty
                 WHERE id = v_pool_id;
                UPDATE accounts SET balance = balance - v_amount
                 WHERE id = v_env.credit_account_id;
            END IF;

        ELSIF v_kind = 'fifo_issue' THEN
            v_pool_id := v_env.credit_account_id;
            IF v_qty IS NULL OR v_qty <= 0 THEN
                RAISE EXCEPTION 'fifo_issue envelope_idx=% needs positive qty', v_env.envelope_idx;
            END IF;
            v_pl_id := NULL;
            SELECT id INTO v_pl_id FROM posting_lines WHERE idempotency_key = v_env.idempotency_key;
            v_was_replay := v_pl_id IS NOT NULL;
            IF NOT v_was_replay THEN
                -- Plan: walk layers, accumulate (layer_id, take, cost). The pool is
                -- already FOR-UPDATE-locked via the pre-lock step, so layer reads here
                -- don't need their own FOR UPDATE — no concurrent batch can mutate.
                v_remaining := v_qty;
                v_total_cost := 0;
                v_plan_layer_ids := ARRAY[]::BIGINT[];
                v_plan_takes     := ARRAY[]::BIGINT[];
                v_plan_costs     := ARRAY[]::BIGINT[];
                FOR v_layer IN
                    SELECT id, qty_remaining, unit_cost
                      FROM cost_layers
                     WHERE pool_account_id = v_pool_id AND qty_remaining > 0
                     ORDER BY receipt_date ASC, id ASC
                LOOP
                    EXIT WHEN v_remaining = 0;
                    v_take := LEAST(v_remaining, v_layer.qty_remaining);
                    v_plan_layer_ids := v_plan_layer_ids || v_layer.id;
                    v_plan_takes     := v_plan_takes     || v_take;
                    v_plan_costs     := v_plan_costs     || (v_take * v_layer.unit_cost);
                    v_total_cost := v_total_cost + (v_take * v_layer.unit_cost);
                    v_remaining := v_remaining - v_take;
                END LOOP;
                IF v_remaining > 0 THEN
                    RAISE EXCEPTION 'fifo_issue envelope_idx=% short by % units (pool % exhausted)',
                        v_env.envelope_idx, v_remaining, v_pool_id;
                END IF;
                v_amount := v_total_cost;

                -- Apply: INSERT posting_lines (with correct amount), depletions, account updates.
                INSERT INTO posting_lines
                    (debit_account_id, credit_account_id, amount, currency,
                     idempotency_key, business_date, qty)
                SELECT v_env.debit_account_id, v_env.credit_account_id, v_total_cost, a.currency,
                       v_env.idempotency_key, v_env.business_date, v_qty
                FROM accounts a WHERE a.id = v_env.debit_account_id
                RETURNING id INTO v_pl_id;

                FOR i IN 1 .. array_length(v_plan_layer_ids, 1) LOOP
                    INSERT INTO cost_layer_depletions
                        (layer_id, issue_posting_line_id, qty_consumed, cost_amount)
                    VALUES (v_plan_layer_ids[i], v_pl_id, v_plan_takes[i], v_plan_costs[i]);
                    UPDATE cost_layers SET qty_remaining = qty_remaining - v_plan_takes[i]
                     WHERE id = v_plan_layer_ids[i];
                END LOOP;

                UPDATE accounts SET balance = balance + v_total_cost WHERE id = v_env.debit_account_id;
                UPDATE accounts SET balance = balance - v_total_cost, qty = qty - v_qty
                 WHERE id = v_pool_id;
            END IF;

        ELSE
            RAISE EXCEPTION 'envelope_idx=% unknown kind %', v_env.envelope_idx, v_kind;
        END IF;

        -- Stage the envelope's outcome.
        -- For fifo_*: is_replay = v_was_replay; replay_pl_id = v_pl_id when replayed,
        --             new_pl_id = v_pl_id when freshly inserted.
        -- For transfer / wac_*: replay detection + INSERT happen later in steps 5-6.
        INSERT INTO _batch_staging
            (envelope_idx, kind, debit_account_id, credit_account_id,
             amount, qty, currency, idempotency_key, business_date,
             is_replay, replay_pl_id, new_pl_id)
        SELECT v_env.envelope_idx, v_kind, v_env.debit_account_id, v_env.credit_account_id,
               v_amount,
               CASE WHEN v_kind = 'transfer' THEN NULL ELSE v_qty END,
               a.currency,
               v_env.idempotency_key, v_env.business_date,
               CASE WHEN v_kind IN ('fifo_receipt','fifo_issue') THEN v_was_replay ELSE FALSE END,
               CASE WHEN v_kind IN ('fifo_receipt','fifo_issue') AND v_was_replay     THEN v_pl_id ELSE NULL END,
               CASE WHEN v_kind IN ('fifo_receipt','fifo_issue') AND NOT v_was_replay THEN v_pl_id ELSE NULL END
        FROM accounts a WHERE a.id = v_env.debit_account_id;
    END LOOP;

    -- 5. For transfer + wac_* kinds (which haven't been INSERTed per-envelope),
    --    do the replay-detection + multi-row INSERT + aggregated account UPDATE
    --    pattern from P3/P4.
    UPDATE _batch_staging s
       SET is_replay = TRUE, replay_pl_id = pl.id
      FROM posting_lines pl
     WHERE pl.idempotency_key = s.idempotency_key
       AND s.kind IN ('transfer','wac_receipt','wac_issue')
       AND s.replay_pl_id IS NULL;

    WITH inserted AS (
        INSERT INTO posting_lines
            (debit_account_id, credit_account_id, amount, currency,
             idempotency_key, business_date, qty)
        SELECT debit_account_id, credit_account_id, amount, currency,
               idempotency_key, business_date,
               CASE WHEN kind = 'transfer' THEN NULL ELSE qty END
        FROM _batch_staging s
        WHERE NOT s.is_replay AND s.kind IN ('transfer','wac_receipt','wac_issue')
        ORDER BY s.envelope_idx
        RETURNING id, idempotency_key
    )
    UPDATE _batch_staging s
       SET new_pl_id = i.id
      FROM inserted i
     WHERE i.idempotency_key = s.idempotency_key;

    -- Aggregate balance + qty deltas for transfer + wac kinds.
    WITH bal_deltas AS (
        SELECT s.debit_account_id  AS aid,  s.amount  AS d FROM _batch_staging s
          WHERE NOT s.is_replay AND s.kind IN ('transfer','wac_receipt','wac_issue')
        UNION ALL
        SELECT s.credit_account_id AS aid, -s.amount  AS d FROM _batch_staging s
          WHERE NOT s.is_replay AND s.kind IN ('transfer','wac_receipt','wac_issue')
    ),
    qty_deltas AS (
        SELECT s.debit_account_id  AS aid,  COALESCE(s.qty, 0) AS d
          FROM _batch_staging s WHERE NOT s.is_replay AND s.kind = 'wac_receipt'
        UNION ALL
        SELECT s.credit_account_id AS aid, -COALESCE(s.qty, 0) AS d
          FROM _batch_staging s WHERE NOT s.is_replay AND s.kind = 'wac_issue'
    ),
    agg_bal AS (SELECT aid, SUM(d)::BIGINT AS d FROM bal_deltas GROUP BY aid),
    agg_qty AS (SELECT aid, SUM(d)::BIGINT AS d FROM qty_deltas GROUP BY aid),
    all_aids AS (SELECT aid FROM agg_bal UNION SELECT aid FROM agg_qty),
    combined AS (
        SELECT a.aid, COALESCE(b.d, 0) AS d_balance, COALESCE(q.d, 0) AS d_qty
          FROM all_aids a
          LEFT JOIN agg_bal b ON b.aid = a.aid
          LEFT JOIN agg_qty q ON q.aid = a.aid
    )
    UPDATE accounts
       SET balance = balance + c.d_balance,
           qty     = qty     + c.d_qty
      FROM combined c
     WHERE accounts.id = c.aid;

    -- 6. Return per-envelope status.
    RETURN QUERY
    SELECT
        s.envelope_idx,
        CASE WHEN s.is_replay THEN 'idempotent_replay'::TEXT ELSE 'committed'::TEXT END AS status,
        COALESCE(s.replay_pl_id, s.new_pl_id) AS posting_line_id,
        NULL::TEXT AS error_code,
        NULL::TEXT AS error_message
    FROM _batch_staging s
    ORDER BY s.envelope_idx;
END$$;
