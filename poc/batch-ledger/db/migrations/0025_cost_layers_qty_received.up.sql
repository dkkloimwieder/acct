-- acct-a3rj Phase B — `cost_layers.qty_received` + `fifo_overconsume_check`.
--
-- ## Why this column exists
--
-- A2 (the per-backend shadow approach shipped at commit dff20bd in
-- acct-b3vs) inserts `cost_layer_depletions` rows at APPLY time against
-- shadow-time slice identities. On `xact_commit`, `consume_from_head` is
-- replayed for ring-state side-effect only — its `ConsumeResult` (slices
-- + shortage) is DISCARDED at `poc/ledger-extension/src/fifo.rs:1175-1179`.
--
-- Under concurrent issues exhausting the SAME thin layer (R-MB6 in
-- `poc/batch-ledger/tests/fifo_rollback_correctness_t6.rs`), both
-- backends' shadows independently snapshot the layer at `qty_remaining=N`
-- and stage `Consume(N)`. Both apply phases insert a depletion row for
-- `{layer=L, qty=N}`. `SUM(cost_layer_depletions.qty_consumed for L) =
-- 2N > N = original receipt qty` — the over-consume gap.
--
-- Before this column, the layer's original receipt qty was not stored —
-- `qty_remaining` is decremented as depletions land (or via bgworker
-- drain). There was no fixed anchor against which to verify
-- `SUM(depletions) ≤ qty_received`.
--
-- `qty_received` is set ONCE at INSERT and never decremented. It's the
-- immutable receipt-side anchor that lets the new `fifo_overconsume_check`
-- function detect the gap structurally without requiring shmem visibility.
--
-- ## Implementation
--
-- Phase 1 (ALTER TABLE): nullable column, backfill, then NOT NULL +
-- CHECK > 0. Greenfield PoC — no production data; backfill is exact.
--
-- Phase 2 (SQL function CREATE OR REPLACE): `post_batch_fifo` (mig 0020)
-- and `post_batch_fifo_maximal` (mig 0021) updated to include
-- `qty_received` in their cost_layers INSERTs. The pgrx extension
-- INSERT sites (fifo_apply_batch_maximal in fifo.rs, fifo_apply_batch_inline
-- in lib.rs) are updated in-source and reloaded via
-- `scripts/install-into-container.sh`.
--
-- Phase 3 (`fifo_overconsume_check`): pure-SQL detection function.
-- Returns one row per layer where `SUM(depletions) > qty_received`,
-- with overshoot magnitude. Counter-grade signal — caller decides
-- whether to alert / log / repair.

-- ─── Phase 1: schema ───────────────────────────────────────────────────

ALTER TABLE cost_layers ADD COLUMN qty_received BIGINT;

UPDATE cost_layers cl
   SET qty_received = cl.qty_remaining + COALESCE(
       (SELECT SUM(cld.qty_consumed)
          FROM cost_layer_depletions cld
         WHERE cld.layer_id = cl.id),
       0
   );

ALTER TABLE cost_layers ALTER COLUMN qty_received SET NOT NULL;
ALTER TABLE cost_layers ADD CONSTRAINT cost_layers_qty_received_positive
    CHECK (qty_received > 0);

-- ─── Phase 2: SQL function updates ─────────────────────────────────────
--
-- `post_batch_fifo` (mig 0020 plpgsql; bench-baseline path used by
-- bench_fifo_fan + bench_fifo_rollback_inject). Verbatim copy of the
-- mig 0020 body with `qty_received` added to the cost_layers INSERT.
-- Sets `qty_received = s.qty` (the original receipt qty staged from
-- the envelope, before any layer mutations).

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
        -- acct-a3rj Phase B: qty_received = s.qty (the original receipt
        -- qty). qty_remaining starts equal; subsequent in-batch drain is
        -- applied later via the UPDATE cost_layers step below.
        INSERT INTO cost_layers
            (pool_account_id, qty_remaining, qty_received, unit_cost,
             receipt_date, receipt_posting_line_id)
        SELECT s.debit_account_id, s.qty, s.qty, s.unit_cost,
               s.business_date, s.new_pl_id
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

-- `post_batch_fifo_maximal` (mig 0021 sql CTE chain). Verbatim copy
-- with `qty_received = l.qty` added to the cost_layers INSERT
-- (BEFORE in-batch drain subtraction — qty_received is the original
-- receipt qty regardless of same-batch consumption).

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
    -- acct-a3rj Phase B: qty_received = l.qty (original receipt qty);
    -- qty_remaining = l.qty - in_batch_drain (post-drain).
    INSERT INTO cost_layers
        (pool_account_id, qty_remaining, qty_received, unit_cost,
         receipt_date, receipt_posting_line_id)
    SELECT
        l.debit_account_id,
        l.qty - COALESCE(d.drain, 0),
        l.qty,
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

-- ─── Phase 3: fifo_overconsume_check ───────────────────────────────────
--
-- Pure-SQL detection of the over-consume gap. Returns one row per
-- layer where `SUM(cost_layer_depletions.qty_consumed) > qty_received`.
-- This is the canonical truth — depletions are append-only and
-- qty_received is immutable, so any divergence is a real
-- over-attribution event, not a stale-state artifact.
--
-- Distinct from `fifo_arena_recon().drift`, which compares
-- shmem-live + pending against durable qty_remaining (a lag metric).
-- Recon's drift can be 0 while overconsume is non-zero (when
-- pending entries are still in flight) or non-zero while overconsume
-- is 0 (during normal pending-drain windows).
--
-- Cost: GROUP BY over (cost_layers, cost_layer_depletions). Index
-- `cost_layer_depletions_layer_idx` covers the join. Fast for PoC scale;
-- if it becomes hot at production scale, the natural next step is to
-- materialize per-layer total_consumed in shmem or a maintained column.

CREATE OR REPLACE FUNCTION fifo_overconsume_check()
RETURNS TABLE (
    layer_id        BIGINT,
    pool_account_id BIGINT,
    qty_received    BIGINT,
    total_consumed  BIGINT,
    overshoot       BIGINT
)
LANGUAGE sql STABLE AS $$
    SELECT
        cl.id,
        cl.pool_account_id,
        cl.qty_received,
        COALESCE(SUM(cld.qty_consumed), 0)::BIGINT,
        (COALESCE(SUM(cld.qty_consumed), 0) - cl.qty_received)::BIGINT
      FROM cost_layers cl
      LEFT JOIN cost_layer_depletions cld ON cld.layer_id = cl.id
     GROUP BY cl.id, cl.pool_account_id, cl.qty_received
    HAVING COALESCE(SUM(cld.qty_consumed), 0) > cl.qty_received
     ORDER BY cl.id;
$$;
