-- acct-n4mo M10.B4 — WAC apply via shmem (drop-in for post_batch_wac).
--
-- Composes mig 0006 (WAC perpetual with in-batch running-avg pricing) with
-- mig 0013's shmem apply path (ledger_apply_balance_delta). Two semantic
-- differences vs mig 0013:
--
--   1. Pool state snapshot at batch start comes from `ledger_balance_lookup`
--      (shmem) instead of `accounts.balance/qty`. Reads the coupled
--      (balance, qty) snapshot via the single 128-bit atomic load that
--      B4-prep (acct-zo4t) shipped. Pre-B4-prep this would torn-read and
--      corrupt unit_cost.
--   2. Per-leg apply at end carries BOTH amount AND qty deltas. The qty
--      leg on the pool side is LOAD-BEARING — without it, the pool's qty
--      doesn't track in shmem and the NEXT wac_issue (in a later batch)
--      computes a wrong unit cost. mig 0013's post_batch_shmem can pass
--      qty=0 because simple transfers don't track qty.
--
-- Envelope shape mirrors mig 0006:
--   "kind": "transfer" | "wac_receipt" | "wac_issue"   (default: transfer)
--   "qty": BIGINT                                       (required for wac_*)
--   "unit_cost": BIGINT                                 (required for wac_receipt)
--   "amount": BIGINT                                    (omitted for wac_*)
--
-- transfer    : amount caller-supplied; debit leg +amount, credit leg -amount; qty 0 on both.
-- wac_receipt : debit=pool. amount = qty * unit_cost. Pool gets (+amount,+qty); credit gets (-amount,0).
-- wac_issue   : credit=pool. amount = qty * running_avg. Pool gets (-amount,-qty); debit gets (+amount,0).
--
-- In-batch running map (v_pool_value, v_pool_qty) carries pool state across
-- envelopes within ONE call. Cross-batch within the same outer txn does not
-- RYW (A2 PENDING_STACK isn't applied to shmem until commit). One batch per
-- txn is the supported pattern; tests pin this.
--
-- Sign convention matches `accounts.balance` / `ledger_shmem_recon` (debit-
-- positive): drift=0 means shmem equals what mutable post_batch_wac would
-- have produced.

CREATE OR REPLACE FUNCTION post_batch_wac_shmem(p_envelopes JSONB)
RETURNS TABLE (
    envelope_idx     INT,
    status           TEXT,
    posting_line_id  BIGINT,
    error_code       TEXT,
    error_message    TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
    -- Running balance map: pool_id -> (value, qty). Seeded from shmem at
    -- batch start; updated envelope-by-envelope.
    v_pool_value  JSONB := '{}'::JSONB;
    v_pool_qty    JSONB := '{}'::JSONB;

    v_pool_rec    RECORD;
    v_env         RECORD;
    v_kind        TEXT;
    v_pool_id     BIGINT;
    v_value       BIGINT;
    v_qty         BIGINT;
    v_amount      BIGINT;
    v_unit_cost   BIGINT;
    v_pool_key    TEXT;
    leg           RECORD;
BEGIN
    -- 1. Seed pool snapshots from shmem for every WAC pool this batch touches.
    --    Coupled (balance, qty) via single AtomicU128 load per pool.
    FOR v_pool_rec IN
        SELECT DISTINCT pool_id
        FROM (
            SELECT (e->>'debit_account_id')::BIGINT  AS pool_id
              FROM jsonb_array_elements(p_envelopes) e
             WHERE e->>'kind' = 'wac_receipt'
            UNION
            SELECT (e->>'credit_account_id')::BIGINT AS pool_id
              FROM jsonb_array_elements(p_envelopes) e
             WHERE e->>'kind' = 'wac_issue'
        ) pools
    LOOP
        DECLARE
            v_b BIGINT;
            v_q BIGINT;
        BEGIN
            SELECT balance, qty INTO v_b, v_q
              FROM ledger_balance_lookup(v_pool_rec.pool_id, 1, 1::smallint, 1::smallint);
            v_pool_value := v_pool_value
                || jsonb_build_object(v_pool_rec.pool_id::TEXT, COALESCE(v_b, 0));
            v_pool_qty   := v_pool_qty
                || jsonb_build_object(v_pool_rec.pool_id::TEXT, COALESCE(v_q, 0));
        END;
    END LOOP;

    -- 2. Per-batch staging temp.
    CREATE TEMP TABLE IF NOT EXISTS _wac_shmem_batch_staging (
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
    TRUNCATE _wac_shmem_batch_staging;

    -- 3. Walk envelopes in envelope_idx order; price each via running map; stage.
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
            -- mig 0013 / P3 semantics. Caller-supplied amount.
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
                to_jsonb(COALESCE((v_pool_value->>v_pool_key)::BIGINT, 0) + v_amount));
            v_pool_qty := jsonb_set(v_pool_qty, ARRAY[v_pool_key],
                to_jsonb(COALESCE((v_pool_qty->>v_pool_key)::BIGINT, 0) + v_qty));
        ELSIF v_kind = 'wac_issue' THEN
            v_pool_id := v_env.credit_account_id;
            v_pool_key := v_pool_id::TEXT;
            IF v_qty IS NULL OR v_qty <= 0 THEN
                RAISE EXCEPTION 'wac_issue envelope_idx=% missing/invalid qty', v_env.envelope_idx;
            END IF;
            v_value := COALESCE((v_pool_value->>v_pool_key)::BIGINT, 0);
            DECLARE
                v_running_qty BIGINT := COALESCE((v_pool_qty->>v_pool_key)::BIGINT, 0);
            BEGIN
                IF v_running_qty IS NULL OR v_running_qty <= 0 THEN
                    RAISE EXCEPTION 'wac_issue envelope_idx=% from empty pool % (running qty=%)',
                        v_env.envelope_idx, v_pool_id, v_running_qty;
                END IF;
                IF v_qty > v_running_qty THEN
                    RAISE EXCEPTION 'wac_issue envelope_idx=% qty=% exceeds running qty=%',
                        v_env.envelope_idx, v_qty, v_running_qty;
                END IF;
                -- Integer division: unit_cost = value / qty (running avg). Mirrors mig 0006.
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

        INSERT INTO _wac_shmem_batch_staging
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

    -- 4. Detect replays via posting_lines.idempotency_key UNIQUE.
    UPDATE _wac_shmem_batch_staging s
       SET is_replay = TRUE, replay_pl_id = pl.id
      FROM posting_lines pl
     WHERE pl.idempotency_key = s.idempotency_key;

    -- 5. INSERT new posting_lines.
    WITH inserted AS (
        INSERT INTO posting_lines
            (debit_account_id, credit_account_id, amount, currency,
             idempotency_key, business_date, qty)
        SELECT debit_account_id, credit_account_id, amount, currency,
               idempotency_key, business_date, qty
        FROM _wac_shmem_batch_staging s
        WHERE NOT s.is_replay
        ORDER BY s.envelope_idx
        RETURNING id, idempotency_key
    )
    UPDATE _wac_shmem_batch_staging s
       SET new_pl_id = i.id
      FROM inserted i
     WHERE i.idempotency_key = s.idempotency_key;

    -- 6. Apply per-leg deltas to shmem. Carries (amount, qty) pairs per leg.
    --    NO FOR UPDATE on accounts — shmem path uses per-cell LWLock under
    --    AtomicU128 CAS. Cross-leg ordering within an envelope is irrelevant
    --    because each call stages into A2's PENDING_STACK; commit-time apply
    --    collapses them atomically.
    --
    --    transfer    : debit +amount/0,           credit -amount/0
    --    wac_receipt : debit (pool) +amount/+qty, credit         -amount/0
    --    wac_issue   : credit (pool) -amount/-qty, debit         +amount/0
    FOR leg IN
        SELECT * FROM _wac_shmem_batch_staging s WHERE NOT s.is_replay ORDER BY s.envelope_idx
    LOOP
        IF leg.kind = 'transfer' THEN
            PERFORM ledger_apply_balance_delta(
                leg.debit_account_id,  1, 1::smallint, 1::smallint,  leg.amount, 0);
            PERFORM ledger_apply_balance_delta(
                leg.credit_account_id, 1, 1::smallint, 1::smallint, -leg.amount, 0);
        ELSIF leg.kind = 'wac_receipt' THEN
            PERFORM ledger_apply_balance_delta(
                leg.debit_account_id,  1, 1::smallint, 1::smallint,  leg.amount,  leg.qty);
            PERFORM ledger_apply_balance_delta(
                leg.credit_account_id, 1, 1::smallint, 1::smallint, -leg.amount,  0);
        ELSIF leg.kind = 'wac_issue' THEN
            PERFORM ledger_apply_balance_delta(
                leg.credit_account_id, 1, 1::smallint, 1::smallint, -leg.amount, -leg.qty);
            PERFORM ledger_apply_balance_delta(
                leg.debit_account_id,  1, 1::smallint, 1::smallint,  leg.amount,  0);
        END IF;
    END LOOP;

    -- 7. Per-envelope status.
    RETURN QUERY
    SELECT
        s.envelope_idx,
        CASE WHEN s.is_replay THEN 'idempotent_replay'::TEXT ELSE 'committed'::TEXT END AS status,
        COALESCE(s.replay_pl_id, s.new_pl_id) AS posting_line_id,
        NULL::TEXT AS error_code,
        NULL::TEXT AS error_message
    FROM _wac_shmem_batch_staging s
    ORDER BY s.envelope_idx;
END$$;
