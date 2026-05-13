-- acct-2g9w-followup — install `post_batch_fifo` as the stable mutable-baseline
-- name for FIFO benchmarking. Same body as mig 0009 (`post_batch` v2 FIFO),
-- under a separate function name so it survives subsequent CREATE OR REPLACE
-- of `post_batch` (mig 0010 PAC; current).
--
-- Coexists with post_batch_wac (mig 0015), post_batch_shmem (mig 0013), and
-- post_batch_wac_shmem_maximal (mig 0019). One body per cost method, named.

CREATE OR REPLACE FUNCTION post_batch_fifo(p_envelopes JSONB)
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

    v_pool_value  JSONB := '{}'::JSONB;
    v_pool_qty    JSONB := '{}'::JSONB;
    v_value       BIGINT;
    v_running_qty BIGINT;

    v_layers      JSONB := '{}'::JSONB;
    v_pool_layers JSONB;
    v_layer       JSONB;
    v_layer_idx   INT;
    v_layer_qty   BIGINT;
    v_take        BIGINT;
    v_remaining   BIGINT;
    v_total_cost  BIGINT;

    v_next_sentinel INT := -1;
    v_sentinel    INT;
BEGIN
    PERFORM accounts.id
    FROM accounts
    WHERE accounts.id IN (
        SELECT (e->>'debit_account_id')::BIGINT  FROM jsonb_array_elements(p_envelopes) e
        UNION
        SELECT (e->>'credit_account_id')::BIGINT FROM jsonb_array_elements(p_envelopes) e
    )
    ORDER BY accounts.id
    FOR UPDATE;

    CREATE TEMP TABLE IF NOT EXISTS _fifo_batch_staging (
        envelope_idx       INT,
        kind               TEXT,
        debit_account_id   BIGINT,
        credit_account_id  BIGINT,
        amount             BIGINT,
        qty                BIGINT,
        unit_cost          BIGINT,
        business_date      DATE,
        currency           CHAR(3),
        idempotency_key    UUID,
        layer_sentinel     INT,
        depletion_plan     JSONB,
        is_replay          BOOLEAN DEFAULT FALSE,
        replay_pl_id       BIGINT,
        new_pl_id          BIGINT
    ) ON COMMIT DROP;
    TRUNCATE _fifo_batch_staging;

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
        ELSIF v_kind = 'fifo_receipt' THEN
            v_pool_id := v_env.debit_account_id;
            v_pool_key := v_pool_id::TEXT;
            IF v_qty IS NULL OR v_qty <= 0 OR v_unit_cost IS NULL OR v_unit_cost <= 0 THEN
                RAISE EXCEPTION 'fifo_receipt env=% needs positive qty + unit_cost', v_env.envelope_idx;
            END IF;
            v_amount := v_qty * v_unit_cost;
            v_sentinel := v_next_sentinel;
            v_next_sentinel := v_next_sentinel - 1;
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
                        (v_layer->>0)::INT,
                        v_take,
                        v_take * v_unit_cost
                    ));
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
                INSERT INTO _fifo_batch_staging
                    (envelope_idx, kind, debit_account_id, credit_account_id,
                     amount, qty, unit_cost, business_date, currency,
                     idempotency_key, depletion_plan)
                SELECT v_env.envelope_idx, v_kind, v_env.debit_account_id, v_env.credit_account_id,
                       v_amount, v_qty, NULL, v_env.business_date, a.currency,
                       v_env.idempotency_key, v_plan
                FROM accounts a WHERE a.id = v_env.debit_account_id;
                CONTINUE;
            END;
        ELSE
            RAISE EXCEPTION 'post_batch_fifo: env=% unsupported kind %', v_env.envelope_idx, v_kind;
        END IF;

        INSERT INTO _fifo_batch_staging
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

    UPDATE _fifo_batch_staging s
       SET is_replay = TRUE, replay_pl_id = pl.id
      FROM posting_lines pl
     WHERE pl.idempotency_key = s.idempotency_key;

    WITH inserted AS (
        INSERT INTO posting_lines
            (debit_account_id, credit_account_id, amount, currency,
             idempotency_key, business_date, qty)
        SELECT debit_account_id, credit_account_id, amount, currency,
               idempotency_key, business_date,
               CASE WHEN kind = 'transfer' THEN NULL ELSE qty END
          FROM _fifo_batch_staging s
         WHERE NOT s.is_replay
         ORDER BY s.envelope_idx
        RETURNING id, idempotency_key
    )
    UPDATE _fifo_batch_staging s
       SET new_pl_id = i.id
      FROM inserted i
     WHERE i.idempotency_key = s.idempotency_key;

    CREATE TEMP TABLE IF NOT EXISTS _fifo_sentinel_to_layer (
        sentinel INT PRIMARY KEY,
        layer_id BIGINT NOT NULL
    ) ON COMMIT DROP;
    TRUNCATE _fifo_sentinel_to_layer;

    WITH inserted AS (
        INSERT INTO cost_layers
            (pool_account_id, qty_remaining, unit_cost, receipt_date,
             receipt_posting_line_id)
        SELECT s.debit_account_id, s.qty, s.unit_cost, s.business_date, s.new_pl_id
          FROM _fifo_batch_staging s
         WHERE NOT s.is_replay AND s.kind = 'fifo_receipt'
         ORDER BY s.envelope_idx
        RETURNING id, receipt_posting_line_id
    )
    INSERT INTO _fifo_sentinel_to_layer (sentinel, layer_id)
    SELECT s.layer_sentinel, i.id
      FROM inserted i
      JOIN _fifo_batch_staging s ON s.new_pl_id = i.receipt_posting_line_id;

    WITH expanded AS (
        SELECT s.new_pl_id AS issue_posting_line_id,
               (slot->>0)::INT AS sentinel_or_id,
               (slot->>1)::BIGINT AS qty_consumed,
               (slot->>2)::BIGINT AS cost_amount
          FROM _fifo_batch_staging s
          CROSS JOIN LATERAL jsonb_array_elements(s.depletion_plan) AS slot
         WHERE NOT s.is_replay AND s.kind = 'fifo_issue'
    ),
    resolved AS (
        SELECT e.issue_posting_line_id,
               CASE WHEN e.sentinel_or_id < 0 THEN m.layer_id ELSE e.sentinel_or_id::BIGINT END AS layer_id,
               e.qty_consumed, e.cost_amount
          FROM expanded e
          LEFT JOIN _fifo_sentinel_to_layer m ON m.sentinel = e.sentinel_or_id
    )
    INSERT INTO cost_layer_depletions (layer_id, issue_posting_line_id, qty_consumed, cost_amount)
    SELECT layer_id, issue_posting_line_id, qty_consumed, cost_amount FROM resolved;

    WITH all_deltas AS (
        SELECT (slot->>0)::INT AS sentinel_or_id, -(slot->>1)::BIGINT AS d_qty
          FROM _fifo_batch_staging s
          CROSS JOIN LATERAL jsonb_array_elements(s.depletion_plan) AS slot
         WHERE NOT s.is_replay AND s.kind = 'fifo_issue'
    ),
    resolved AS (
        SELECT CASE WHEN d.sentinel_or_id < 0 THEN m.layer_id ELSE d.sentinel_or_id::BIGINT END AS layer_id,
               d.d_qty
          FROM all_deltas d
          LEFT JOIN _fifo_sentinel_to_layer m ON m.sentinel = d.sentinel_or_id
    ),
    agg AS (SELECT layer_id, SUM(d_qty)::BIGINT AS d FROM resolved GROUP BY layer_id)
    UPDATE cost_layers SET qty_remaining = qty_remaining + agg.d
      FROM agg WHERE cost_layers.id = agg.layer_id;

    WITH bal_deltas AS (
        SELECT s.debit_account_id  AS aid,  s.amount  AS d FROM _fifo_batch_staging s WHERE NOT s.is_replay
        UNION ALL
        SELECT s.credit_account_id AS aid, -s.amount  AS d FROM _fifo_batch_staging s WHERE NOT s.is_replay
    ),
    qty_deltas AS (
        SELECT s.debit_account_id AS aid, COALESCE(s.qty, 0) AS d
          FROM _fifo_batch_staging s WHERE NOT s.is_replay AND s.kind = 'fifo_receipt'
        UNION ALL
        SELECT s.credit_account_id AS aid, -COALESCE(s.qty, 0) AS d
          FROM _fifo_batch_staging s WHERE NOT s.is_replay AND s.kind = 'fifo_issue'
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

    RETURN QUERY
    SELECT
        s.envelope_idx,
        CASE WHEN s.is_replay THEN 'idempotent_replay'::TEXT ELSE 'committed'::TEXT END AS status,
        COALESCE(s.replay_pl_id, s.new_pl_id) AS posting_line_id,
        NULL::TEXT, NULL::TEXT
    FROM _fifo_batch_staging s
    ORDER BY s.envelope_idx;
END$$;
