-- acct-n4mo M10.B4 — installs `post_batch_wac` as a standalone WAC entry point.
--
-- mig 0006 CREATE OR REPLACE'd `post_batch` with WAC perpetual semantics.
-- mig 0008/0009/0010 then overlaid it with FIFO, FIFO v2, and PAC, so the
-- WAC body is no longer reachable via the `post_batch` name. The B4 bench
-- (and any future caller that wants pure-WAC mutable semantics) needs a
-- stable named entry point.
--
-- This is mig 0006's body verbatim, except the function name. No
-- behavioral change. Re-running mig 0006 would overwrite `post_batch`
-- (now PAC); this mig installs `post_batch_wac` as a sibling.

CREATE OR REPLACE FUNCTION post_batch_wac(p_envelopes JSONB)
RETURNS TABLE (
    envelope_idx     INT,
    status           TEXT,
    posting_line_id  BIGINT,
    error_code       TEXT,
    error_message    TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_pool_value  JSONB := '{}'::JSONB;
    v_pool_qty    JSONB := '{}'::JSONB;

    v_env         RECORD;
    v_kind        TEXT;
    v_pool_id     BIGINT;
    v_value       BIGINT;
    v_qty         BIGINT;
    v_amount      BIGINT;
    v_unit_cost   BIGINT;
    v_pool_key    TEXT;
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

    CREATE TEMP TABLE IF NOT EXISTS _batch_staging_wac (
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
    TRUNCATE _batch_staging_wac;

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

        IF v_kind = 'transfer' THEN
            NULL;
        ELSIF v_kind = 'wac_receipt' THEN
            v_pool_id := v_env.debit_account_id;
            v_pool_key := v_pool_id::TEXT;
            IF v_qty IS NULL OR v_qty <= 0 THEN
                RAISE EXCEPTION 'wac_receipt envelope_idx=% missing/invalid qty', v_env.envelope_idx;
            END IF;
            IF v_unit_cost IS NULL OR v_unit_cost <= 0 THEN
                RAISE EXCEPTION 'wac_receipt envelope_idx=% missing/invalid unit_cost', v_env.envelope_idx;
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
                RAISE EXCEPTION 'wac_issue envelope_idx=% missing/invalid qty', v_env.envelope_idx;
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
        ELSE
            RAISE EXCEPTION 'envelope_idx=% unknown kind %', v_env.envelope_idx, v_kind;
        END IF;

        INSERT INTO _batch_staging_wac
            (envelope_idx, kind, debit_account_id, credit_account_id,
             amount, qty, currency, idempotency_key, business_date,
             is_replay, replay_pl_id, new_pl_id)
        SELECT
            v_env.envelope_idx, v_kind, v_env.debit_account_id, v_env.credit_account_id,
            v_amount,
            CASE WHEN v_kind = 'transfer' THEN NULL ELSE v_qty END,
            a.currency,
            v_env.idempotency_key, v_env.business_date,
            FALSE, NULL, NULL
        FROM accounts a
        WHERE a.id = v_env.debit_account_id;
    END LOOP;

    UPDATE _batch_staging_wac s
       SET is_replay = TRUE, replay_pl_id = pl.id
      FROM posting_lines pl
     WHERE pl.idempotency_key = s.idempotency_key;

    WITH inserted AS (
        INSERT INTO posting_lines
            (debit_account_id, credit_account_id, amount, currency,
             idempotency_key, business_date, qty)
        SELECT debit_account_id, credit_account_id, amount, currency,
               idempotency_key, business_date, qty
        FROM _batch_staging_wac s
        WHERE NOT s.is_replay
        ORDER BY s.envelope_idx
        RETURNING id, idempotency_key
    )
    UPDATE _batch_staging_wac s
       SET new_pl_id = i.id
      FROM inserted i
     WHERE i.idempotency_key = s.idempotency_key;

    WITH bal_deltas AS (
        SELECT s.debit_account_id  AS aid,  s.amount  AS d FROM _batch_staging_wac s WHERE NOT s.is_replay
        UNION ALL
        SELECT s.credit_account_id AS aid, -s.amount  AS d FROM _batch_staging_wac s WHERE NOT s.is_replay
    ),
    qty_deltas AS (
        SELECT s.debit_account_id  AS aid,  COALESCE(s.qty, 0) AS d
          FROM _batch_staging_wac s WHERE NOT s.is_replay AND s.kind = 'wac_receipt'
        UNION ALL
        SELECT s.credit_account_id AS aid, -COALESCE(s.qty, 0) AS d
          FROM _batch_staging_wac s WHERE NOT s.is_replay AND s.kind = 'wac_issue'
    ),
    agg_bal AS (SELECT aid, SUM(d)::BIGINT AS d FROM bal_deltas GROUP BY aid),
    agg_qty AS (SELECT aid, SUM(d)::BIGINT AS d FROM qty_deltas GROUP BY aid),
    all_aids AS (SELECT aid FROM agg_bal UNION SELECT aid FROM agg_qty),
    combined AS (
        SELECT a.aid,
               COALESCE(b.d, 0) AS d_balance,
               COALESCE(q.d, 0) AS d_qty
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
        NULL::TEXT AS error_code,
        NULL::TEXT AS error_message
    FROM _batch_staging_wac s
    ORDER BY s.envelope_idx;
END$$;
