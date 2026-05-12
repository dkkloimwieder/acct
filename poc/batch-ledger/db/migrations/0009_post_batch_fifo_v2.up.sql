-- acct-1hps (P5 of acct-qdp5 PoC) — multi-row refactor.
--
-- The naive per-envelope plpgsql implementation in 0008 collapses at scale
-- (419 tps at batch=1000) because:
--   - Each fifo_receipt / fifo_issue does 3-7 SQL statements inside the FOR LOOP.
--   - At batch=1000, 700 issues × ~5 statements = 3500+ statements per batch.
--   - 20 workers each holding 20 pool locks for 30+ seconds amplifies contention.
--
-- This rewrite uses the "plan in plpgsql arrays, INSERT at end" pattern:
--
--   FOR LOOP per envelope:
--     - Maintain in-memory layer state per pool (real layers + in-batch sentinels).
--     - For receipts: append a sentinel to the pool's layer list.
--     - For issues: walk the layer list, build a depletion plan, accumulate.
--
--   After FOR LOOP, in deterministic order:
--     1. Multi-row INSERT posting_lines for ALL envelopes. RETURNING (id,
--        idempotency_key) → posting_line_map.
--     2. Multi-row INSERT cost_layers for all in-batch receipts, with
--        receipt_posting_line_id resolved via posting_line_map. RETURNING
--        (id, sentinel) → layer_id_map.
--     3. Multi-row INSERT cost_layer_depletions resolving sentinels to real
--        layer_ids via layer_id_map.
--     4. UPDATE cost_layers.qty_remaining via single UPDATE FROM VALUES
--        covering all touched layers.
--     5. UPDATE accounts (balance + qty) via single aggregated UPDATE FROM.
--
-- Expected speedup: 5-10× over 0008 by eliminating per-envelope round trips.

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
    v_pool_key    TEXT;
    v_qty         BIGINT;
    v_amount      BIGINT;
    v_unit_cost   BIGINT;

    -- WAC running maps.
    v_pool_value  JSONB := '{}'::JSONB;
    v_pool_qty    JSONB := '{}'::JSONB;
    v_value       BIGINT;
    v_running_qty BIGINT;

    -- FIFO in-memory layer state: pool_id::text -> [[sentinel_id, qty_remaining, unit_cost, receipt_date], ...]
    -- sentinel_id is positive for pre-existing layers (real cost_layers.id),
    -- negative for in-batch new layers (mapped to real ids in step 2 post-loop).
    v_layers      JSONB := '{}'::JSONB;
    v_pool_layers JSONB;
    v_layer       JSONB;
    v_layer_idx   INT;
    v_layer_qty   BIGINT;
    v_take        BIGINT;
    v_remaining   BIGINT;
    v_total_cost  BIGINT;

    -- Sentinel counter for in-batch new layers.
    v_next_sentinel INT := -1;
    v_sentinel    INT;

    -- Stage tables for the multi-row INSERT phase.
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

    -- 2. Staging temp.
    CREATE TEMP TABLE IF NOT EXISTS _batch_staging (
        envelope_idx       INT,
        kind               TEXT,
        debit_account_id   BIGINT,
        credit_account_id  BIGINT,
        amount             BIGINT,
        qty                BIGINT,
        unit_cost          BIGINT,         -- for fifo_receipt, to populate cost_layers
        business_date      DATE,
        currency           CHAR(3),
        idempotency_key    UUID,
        -- For fifo_receipt: sentinel id of the new layer.
        layer_sentinel     INT,
        -- For fifo_issue: JSONB array of [[layer_sentinel_or_real_id, qty_taken, cost_amount], ...]
        depletion_plan     JSONB,
        is_replay          BOOLEAN DEFAULT FALSE,
        replay_pl_id       BIGINT,
        new_pl_id          BIGINT
    ) ON COMMIT DROP;
    TRUNCATE _batch_staging;

    -- 3. Pre-load WAC pool snapshots (running map seed).
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

    -- 4. Pre-load FIFO layer state per pool from cost_layers.
    FOR v_env IN
        SELECT pool_id::TEXT AS k,
               jsonb_agg(jsonb_build_array(layer_id, qty_remaining, unit_cost,
                                            to_char(receipt_date, 'YYYY-MM-DD'))
                          ORDER BY receipt_date, layer_id) AS layers
        FROM (
            SELECT cl.id AS layer_id, cl.qty_remaining, cl.unit_cost, cl.receipt_date,
                   cl.pool_account_id AS pool_id
              FROM cost_layers cl
             WHERE cl.qty_remaining > 0
               AND cl.pool_account_id IN (
                    SELECT (e->>'credit_account_id')::BIGINT FROM jsonb_array_elements(p_envelopes) e
                     WHERE e->>'kind' = 'fifo_issue'
               )
        ) sub
        GROUP BY pool_id
    LOOP
        v_layers := v_layers || jsonb_build_object(v_env.k, v_env.layers);
    END LOOP;

    -- 5. Walk envelopes in order.
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
        v_qty := v_env.qty;
        v_amount := v_env.amount;
        v_unit_cost := v_env.unit_cost;
        v_sentinel := NULL;

        IF v_kind = 'transfer' THEN
            NULL;

        ELSIF v_kind = 'wac_receipt' THEN
            v_pool_id := v_env.debit_account_id;
            v_pool_key := v_pool_id::TEXT;
            IF v_qty IS NULL OR v_qty <= 0 OR v_unit_cost IS NULL OR v_unit_cost <= 0 THEN
                RAISE EXCEPTION 'wac_receipt env=% needs positive qty + unit_cost', v_env.envelope_idx;
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
                RAISE EXCEPTION 'wac_issue env=% needs positive qty', v_env.envelope_idx;
            END IF;
            v_value := (v_pool_value->>v_pool_key)::BIGINT;
            v_running_qty := (v_pool_qty->>v_pool_key)::BIGINT;
            IF v_running_qty IS NULL OR v_running_qty <= 0 THEN
                RAISE EXCEPTION 'wac_issue env=% from empty pool % (running qty=%)',
                    v_env.envelope_idx, v_pool_id, v_running_qty;
            END IF;
            IF v_qty > v_running_qty THEN
                RAISE EXCEPTION 'wac_issue env=% qty=% exceeds running qty=%',
                    v_env.envelope_idx, v_qty, v_running_qty;
            END IF;
            v_unit_cost := v_value / v_running_qty;
            v_amount := v_unit_cost * v_qty;
            v_pool_value := jsonb_set(v_pool_value, ARRAY[v_pool_key], to_jsonb(v_value - v_amount));
            v_pool_qty := jsonb_set(v_pool_qty, ARRAY[v_pool_key], to_jsonb(v_running_qty - v_qty));

        ELSIF v_kind = 'fifo_receipt' THEN
            v_pool_id := v_env.debit_account_id;
            v_pool_key := v_pool_id::TEXT;
            IF v_qty IS NULL OR v_qty <= 0 OR v_unit_cost IS NULL OR v_unit_cost <= 0 THEN
                RAISE EXCEPTION 'fifo_receipt env=% needs positive qty + unit_cost', v_env.envelope_idx;
            END IF;
            v_amount := v_qty * v_unit_cost;
            -- Assign a sentinel id for the new layer.
            v_sentinel := v_next_sentinel;
            v_next_sentinel := v_next_sentinel - 1;
            -- Append to in-memory layer list for this pool.
            v_pool_layers := COALESCE(v_layers->v_pool_key, '[]'::JSONB);
            v_pool_layers := v_pool_layers ||
                jsonb_build_array(jsonb_build_array(
                    v_sentinel, v_qty, v_unit_cost, to_char(v_env.business_date, 'YYYY-MM-DD')
                ));
            v_layers := jsonb_set(v_layers, ARRAY[v_pool_key], v_pool_layers);

        ELSIF v_kind = 'fifo_issue' THEN
            v_pool_id := v_env.credit_account_id;
            v_pool_key := v_pool_id::TEXT;
            IF v_qty IS NULL OR v_qty <= 0 THEN
                RAISE EXCEPTION 'fifo_issue env=% needs positive qty', v_env.envelope_idx;
            END IF;
            v_remaining := v_qty;
            v_total_cost := 0;
            v_pool_layers := COALESCE(v_layers->v_pool_key, '[]'::JSONB);
            -- Build depletion plan as JSONB array.
            DECLARE
                v_plan JSONB := '[]'::JSONB;
            BEGIN
                FOR v_layer_idx IN 0 .. jsonb_array_length(v_pool_layers) - 1 LOOP
                    EXIT WHEN v_remaining = 0;
                    v_layer := v_pool_layers->v_layer_idx;
                    v_layer_qty := (v_layer->>1)::BIGINT;
                    CONTINUE WHEN v_layer_qty = 0;
                    v_take := LEAST(v_remaining, v_layer_qty);
                    v_unit_cost := (v_layer->>2)::BIGINT;
                    v_plan := v_plan || jsonb_build_array(jsonb_build_array(
                        (v_layer->>0)::INT,  -- layer sentinel or real id
                        v_take,
                        v_take * v_unit_cost
                    ));
                    -- Mutate the in-memory layer state.
                    v_pool_layers := jsonb_set(v_pool_layers,
                        ARRAY[v_layer_idx::TEXT, '1'],
                        to_jsonb(v_layer_qty - v_take));
                    v_total_cost := v_total_cost + (v_take * v_unit_cost);
                    v_remaining := v_remaining - v_take;
                END LOOP;
                IF v_remaining > 0 THEN
                    RAISE EXCEPTION 'fifo_issue env=% short by % units (pool % exhausted)',
                        v_env.envelope_idx, v_remaining, v_pool_id;
                END IF;
                v_layers := jsonb_set(v_layers, ARRAY[v_pool_key], v_pool_layers);
                v_amount := v_total_cost;
                -- Stage the depletion plan.
                INSERT INTO _batch_staging
                    (envelope_idx, kind, debit_account_id, credit_account_id,
                     amount, qty, unit_cost, business_date, currency,
                     idempotency_key, depletion_plan)
                SELECT v_env.envelope_idx, v_kind, v_env.debit_account_id, v_env.credit_account_id,
                       v_amount, v_qty, NULL, v_env.business_date, a.currency,
                       v_env.idempotency_key, v_plan
                FROM accounts a WHERE a.id = v_env.debit_account_id;
                CONTINUE;  -- skip the generic staging INSERT below
            END;

        ELSE
            RAISE EXCEPTION 'env=% unknown kind %', v_env.envelope_idx, v_kind;
        END IF;

        -- Generic staging INSERT (everything except fifo_issue).
        INSERT INTO _batch_staging
            (envelope_idx, kind, debit_account_id, credit_account_id,
             amount, qty, unit_cost, business_date, currency,
             idempotency_key, layer_sentinel)
        SELECT v_env.envelope_idx, v_kind, v_env.debit_account_id, v_env.credit_account_id,
               v_amount,
               CASE WHEN v_kind = 'transfer' THEN NULL ELSE v_qty END,
               CASE WHEN v_kind = 'fifo_receipt' THEN v_unit_cost ELSE NULL END,
               v_env.business_date, a.currency,
               v_env.idempotency_key, v_sentinel
        FROM accounts a WHERE a.id = v_env.debit_account_id;
    END LOOP;

    -- 6. Replay detection across ALL kinds.
    UPDATE _batch_staging s
       SET is_replay = TRUE, replay_pl_id = pl.id
      FROM posting_lines pl
     WHERE pl.idempotency_key = s.idempotency_key;

    -- 7. Multi-row INSERT posting_lines for all non-replays.
    WITH inserted AS (
        INSERT INTO posting_lines
            (debit_account_id, credit_account_id, amount, currency,
             idempotency_key, business_date, qty)
        SELECT debit_account_id, credit_account_id, amount, currency,
               idempotency_key, business_date,
               CASE WHEN kind = 'transfer' THEN NULL ELSE qty END
          FROM _batch_staging s
         WHERE NOT s.is_replay
         ORDER BY s.envelope_idx
        RETURNING id, idempotency_key
    )
    UPDATE _batch_staging s
       SET new_pl_id = i.id
      FROM inserted i
     WHERE i.idempotency_key = s.idempotency_key;

    -- 8. Multi-row INSERT cost_layers for fifo_receipt envelopes (non-replay).
    --    The sentinel→real layer_id mapping is captured via a temp table.
    CREATE TEMP TABLE IF NOT EXISTS _sentinel_to_layer (
        sentinel INT PRIMARY KEY,
        layer_id BIGINT NOT NULL
    ) ON COMMIT DROP;
    TRUNCATE _sentinel_to_layer;

    WITH inserted AS (
        INSERT INTO cost_layers
            (pool_account_id, qty_remaining, unit_cost, receipt_date,
             receipt_posting_line_id)
        SELECT s.debit_account_id AS pool_account_id,
               s.qty               AS qty_remaining,
               s.unit_cost         AS unit_cost,
               s.business_date     AS receipt_date,
               s.new_pl_id         AS receipt_posting_line_id
          FROM _batch_staging s
         WHERE NOT s.is_replay AND s.kind = 'fifo_receipt'
         ORDER BY s.envelope_idx
        RETURNING id, receipt_posting_line_id
    )
    INSERT INTO _sentinel_to_layer (sentinel, layer_id)
    SELECT s.layer_sentinel, i.id
      FROM inserted i
      JOIN _batch_staging s ON s.new_pl_id = i.receipt_posting_line_id;

    -- 9. Multi-row INSERT cost_layer_depletions for fifo_issue envelopes.
    --    Expand the depletion plan JSONB; map sentinel→real layer_id via the temp.
    WITH expanded AS (
        SELECT s.new_pl_id     AS issue_posting_line_id,
               (slot->>0)::INT AS sentinel_or_id,
               (slot->>1)::BIGINT AS qty_consumed,
               (slot->>2)::BIGINT AS cost_amount
          FROM _batch_staging s
          CROSS JOIN LATERAL jsonb_array_elements(s.depletion_plan) AS slot
         WHERE NOT s.is_replay AND s.kind = 'fifo_issue'
    ),
    resolved AS (
        SELECT e.issue_posting_line_id,
               CASE WHEN e.sentinel_or_id < 0 THEN m.layer_id ELSE e.sentinel_or_id::BIGINT END AS layer_id,
               e.qty_consumed, e.cost_amount
          FROM expanded e
          LEFT JOIN _sentinel_to_layer m ON m.sentinel = e.sentinel_or_id
    )
    INSERT INTO cost_layer_depletions (layer_id, issue_posting_line_id, qty_consumed, cost_amount)
    SELECT layer_id, issue_posting_line_id, qty_consumed, cost_amount FROM resolved;

    -- 10. UPDATE cost_layers.qty_remaining for all touched layers. Combine
    --     both pre-existing (depleted by issues) and in-batch (might also be
    --     depleted by later issues in same batch) into one aggregated UPDATE.
    WITH all_deltas AS (
        -- Pre-existing layers depleted by in-batch fifo_issues.
        SELECT (slot->>0)::INT AS sentinel_or_id, -(slot->>1)::BIGINT AS d_qty
          FROM _batch_staging s
          CROSS JOIN LATERAL jsonb_array_elements(s.depletion_plan) AS slot
         WHERE NOT s.is_replay AND s.kind = 'fifo_issue'
    ),
    resolved AS (
        SELECT CASE WHEN d.sentinel_or_id < 0 THEN m.layer_id ELSE d.sentinel_or_id::BIGINT END AS layer_id,
               d.d_qty
          FROM all_deltas d
          LEFT JOIN _sentinel_to_layer m ON m.sentinel = d.sentinel_or_id
    ),
    agg AS (SELECT layer_id, SUM(d_qty)::BIGINT AS d FROM resolved GROUP BY layer_id)
    UPDATE cost_layers SET qty_remaining = qty_remaining + agg.d
      FROM agg WHERE cost_layers.id = agg.layer_id;

    -- 11. Aggregated account balance + qty UPDATE.
    WITH bal_deltas AS (
        SELECT s.debit_account_id  AS aid,  s.amount  AS d FROM _batch_staging s WHERE NOT s.is_replay
        UNION ALL
        SELECT s.credit_account_id AS aid, -s.amount  AS d FROM _batch_staging s WHERE NOT s.is_replay
    ),
    qty_deltas AS (
        -- WAC + FIFO receipts: pool gains qty (on debit side).
        SELECT s.debit_account_id AS aid, COALESCE(s.qty, 0) AS d
          FROM _batch_staging s WHERE NOT s.is_replay AND s.kind IN ('wac_receipt','fifo_receipt')
        UNION ALL
        -- WAC + FIFO issues: pool loses qty (on credit side).
        SELECT s.credit_account_id AS aid, -COALESCE(s.qty, 0) AS d
          FROM _batch_staging s WHERE NOT s.is_replay AND s.kind IN ('wac_issue','fifo_issue')
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

    -- 12. Return per-envelope status.
    RETURN QUERY
    SELECT
        s.envelope_idx,
        CASE WHEN s.is_replay THEN 'idempotent_replay'::TEXT ELSE 'committed'::TEXT END AS status,
        COALESCE(s.replay_pl_id, s.new_pl_id) AS posting_line_id,
        NULL::TEXT, NULL::TEXT
    FROM _batch_staging s
    ORDER BY s.envelope_idx;
END$$;
