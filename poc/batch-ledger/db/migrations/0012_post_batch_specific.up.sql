-- acct-qdp5 PoC — specific/actual costing.
--
-- Each inventory unit is tracked individually with its own cost. Issues
-- specify a unit_id; cost is looked up from inventory_units.
--
-- Two function variants for the same envelope kinds:
--   post_batch_specific      — naive: UPDATE inventory_units SET status='consumed'
--   post_batch_specific_ao   — append-only: INSERT inventory_unit_events row
--
-- Bench results (batch=1000 sync_on; see bench/results-specific.md):
--   naive       — 4.4K tps (UPDATE+index contention)
--   append-only — 8.7K tps (+97%; INSERT event row instead of mutating unit)
--
-- The append-only variant is the recommended pattern for acct backport.

CREATE TABLE IF NOT EXISTS inventory_units (
    id                       BIGSERIAL PRIMARY KEY,
    pool_account_id          BIGINT NOT NULL REFERENCES accounts(id),
    serial_no                VARCHAR(64),
    unit_cost                BIGINT NOT NULL CHECK (unit_cost > 0),
    status                   TEXT NOT NULL DEFAULT 'available'
                               CHECK (status IN ('available','consumed','reserved')),
    receipt_posting_line_id  BIGINT REFERENCES posting_lines(id),
    consumed_posting_line_id BIGINT NULL REFERENCES posting_lines(id),
    received_at              TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    consumed_at              TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS inventory_units_pool_status_idx
    ON inventory_units (pool_account_id, status) WHERE status = 'available';

CREATE TABLE IF NOT EXISTS inventory_unit_events (
    id              BIGSERIAL PRIMARY KEY,
    unit_id         BIGINT NOT NULL REFERENCES inventory_units(id),
    action          TEXT NOT NULL CHECK (action IN ('receive','consume','reserve','release')),
    posting_line_id BIGINT NOT NULL REFERENCES posting_lines(id),
    event_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE OR REPLACE FUNCTION post_batch_specific(p_envelopes JSONB)
RETURNS TABLE (envelope_idx INT, status TEXT, posting_line_id BIGINT,
               error_code TEXT, error_message TEXT)
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM accounts.id FROM accounts
     WHERE accounts.id IN (
         SELECT (e->>'debit_account_id')::BIGINT  FROM jsonb_array_elements(p_envelopes) e
         UNION
         SELECT (e->>'credit_account_id')::BIGINT FROM jsonb_array_elements(p_envelopes) e
     ) ORDER BY accounts.id FOR UPDATE;

    RETURN QUERY
    WITH parsed AS (
        SELECT (e->>'envelope_idx')::INT AS env_idx,
               COALESCE(e->>'kind','transfer') AS kind,
               (e->>'debit_account_id')::BIGINT AS debit_account_id,
               (e->>'credit_account_id')::BIGINT AS credit_account_id,
               CASE WHEN e ? 'amount'    THEN (e->>'amount')::BIGINT    END AS amount,
               CASE WHEN e ? 'unit_cost' THEN (e->>'unit_cost')::BIGINT END AS unit_cost,
               CASE WHEN e ? 'unit_id'   THEN (e->>'unit_id')::BIGINT   END AS unit_id,
               CASE WHEN e ? 'serial_no' THEN (e->>'serial_no')::TEXT   END AS serial_no,
               (e->>'idempotency_key')::UUID AS idempotency_key,
               (e->>'business_date')::DATE AS business_date
          FROM jsonb_array_elements(p_envelopes) e
    ),
    priced AS (
        SELECT p.*,
               CASE p.kind
                 WHEN 'transfer'         THEN p.amount
                 WHEN 'specific_receipt' THEN p.unit_cost
                 WHEN 'specific_issue'   THEN u.unit_cost
               END AS final_amount
          FROM parsed p
          LEFT JOIN inventory_units u ON u.id = p.unit_id AND p.kind = 'specific_issue'
    ),
    replays AS (
        SELECT p.env_idx, pl.id AS posting_line_id
          FROM priced p JOIN posting_lines pl ON pl.idempotency_key = p.idempotency_key
    ),
    to_insert AS (
        SELECT pr.* FROM priced pr
         WHERE NOT EXISTS (SELECT 1 FROM replays r WHERE r.env_idx = pr.env_idx)
    ),
    inserted AS (
        INSERT INTO posting_lines
            (debit_account_id, credit_account_id, amount, currency,
             idempotency_key, business_date, qty)
        SELECT ti.debit_account_id, ti.credit_account_id, ti.final_amount, a.currency,
               ti.idempotency_key, ti.business_date,
               CASE WHEN ti.kind IN ('specific_receipt','specific_issue') THEN 1 ELSE NULL END
          FROM to_insert ti JOIN accounts a ON a.id = ti.debit_account_id
         ORDER BY ti.env_idx
        RETURNING id, idempotency_key
    ),
    new_units AS (
        INSERT INTO inventory_units (pool_account_id, serial_no, unit_cost, receipt_posting_line_id)
        SELECT ti.debit_account_id, ti.serial_no, ti.unit_cost, i.id
          FROM to_insert ti JOIN inserted i ON i.idempotency_key = ti.idempotency_key
         WHERE ti.kind = 'specific_receipt'
        RETURNING id
    ),
    consumed_units AS (
        UPDATE inventory_units u
           SET status = 'consumed', consumed_at = clock_timestamp(),
               consumed_posting_line_id = i.id
          FROM to_insert ti JOIN inserted i ON i.idempotency_key = ti.idempotency_key
         WHERE ti.kind = 'specific_issue' AND u.id = ti.unit_id
        RETURNING u.id
    ),
    bal_deltas AS (
        SELECT ti.debit_account_id AS aid, ti.final_amount AS d FROM to_insert ti
        UNION ALL
        SELECT ti.credit_account_id AS aid, -ti.final_amount AS d FROM to_insert ti
    ),
    qty_deltas AS (
        SELECT ti.debit_account_id AS aid, 1 AS d FROM to_insert ti WHERE ti.kind = 'specific_receipt'
        UNION ALL
        SELECT ti.credit_account_id AS aid, -1 AS d FROM to_insert ti WHERE ti.kind = 'specific_issue'
    ),
    agg AS (
        SELECT aid, SUM(d_b)::BIGINT AS d_balance, SUM(d_q)::BIGINT AS d_qty
        FROM (SELECT aid, d AS d_b, 0 AS d_q FROM bal_deltas
              UNION ALL
              SELECT aid, 0 AS d_b, d AS d_q FROM qty_deltas) x GROUP BY aid
    ),
    updated AS (
        UPDATE accounts SET balance = balance + agg.d_balance, qty = qty + agg.d_qty
          FROM agg WHERE accounts.id = agg.aid RETURNING accounts.id
    )
    SELECT p.env_idx,
           CASE WHEN r.posting_line_id IS NOT NULL THEN 'idempotent_replay'::TEXT ELSE 'committed'::TEXT END,
           COALESCE(r.posting_line_id, i.id), NULL::TEXT, NULL::TEXT
      FROM parsed p
      LEFT JOIN replays  r ON r.env_idx = p.env_idx
      LEFT JOIN inserted i ON i.idempotency_key = p.idempotency_key
      ORDER BY p.env_idx;
END$$;

-- Append-only variant: replaces UPDATE inventory_units with INSERT into
-- inventory_unit_events. ~2× throughput on the consumption side.
CREATE OR REPLACE FUNCTION post_batch_specific_ao(p_envelopes JSONB)
RETURNS TABLE (envelope_idx INT, status TEXT, posting_line_id BIGINT,
               error_code TEXT, error_message TEXT)
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM accounts.id FROM accounts
     WHERE accounts.id IN (
         SELECT (e->>'debit_account_id')::BIGINT  FROM jsonb_array_elements(p_envelopes) e
         UNION
         SELECT (e->>'credit_account_id')::BIGINT FROM jsonb_array_elements(p_envelopes) e
     ) ORDER BY accounts.id FOR UPDATE;

    RETURN QUERY
    WITH parsed AS (
        SELECT (e->>'envelope_idx')::INT AS env_idx,
               COALESCE(e->>'kind','transfer') AS kind,
               (e->>'debit_account_id')::BIGINT AS debit_account_id,
               (e->>'credit_account_id')::BIGINT AS credit_account_id,
               CASE WHEN e ? 'amount'    THEN (e->>'amount')::BIGINT    END AS amount,
               CASE WHEN e ? 'unit_cost' THEN (e->>'unit_cost')::BIGINT END AS unit_cost,
               CASE WHEN e ? 'unit_id'   THEN (e->>'unit_id')::BIGINT   END AS unit_id,
               (e->>'idempotency_key')::UUID AS idempotency_key,
               (e->>'business_date')::DATE AS business_date
          FROM jsonb_array_elements(p_envelopes) e
    ),
    priced AS (
        SELECT p.*,
               CASE p.kind
                 WHEN 'transfer'         THEN p.amount
                 WHEN 'specific_receipt' THEN p.unit_cost
                 WHEN 'specific_issue'   THEN u.unit_cost
               END AS final_amount
          FROM parsed p
          LEFT JOIN inventory_units u ON u.id = p.unit_id AND p.kind = 'specific_issue'
    ),
    replays AS (
        SELECT p.env_idx, pl.id AS posting_line_id
          FROM priced p JOIN posting_lines pl ON pl.idempotency_key = p.idempotency_key
    ),
    to_insert AS (
        SELECT pr.* FROM priced pr
         WHERE NOT EXISTS (SELECT 1 FROM replays r WHERE r.env_idx = pr.env_idx)
    ),
    inserted AS (
        INSERT INTO posting_lines
            (debit_account_id, credit_account_id, amount, currency,
             idempotency_key, business_date, qty)
        SELECT ti.debit_account_id, ti.credit_account_id, ti.final_amount, a.currency,
               ti.idempotency_key, ti.business_date,
               CASE WHEN ti.kind IN ('specific_receipt','specific_issue') THEN 1 ELSE NULL END
          FROM to_insert ti JOIN accounts a ON a.id = ti.debit_account_id
         ORDER BY ti.env_idx
        RETURNING id, idempotency_key
    ),
    new_units AS (
        INSERT INTO inventory_units (pool_account_id, unit_cost, receipt_posting_line_id)
        SELECT ti.debit_account_id, ti.unit_cost, i.id
          FROM to_insert ti JOIN inserted i ON i.idempotency_key = ti.idempotency_key
         WHERE ti.kind = 'specific_receipt'
        RETURNING id
    ),
    consume_events AS (
        INSERT INTO inventory_unit_events (unit_id, action, posting_line_id)
        SELECT ti.unit_id, 'consume', i.id
          FROM to_insert ti JOIN inserted i ON i.idempotency_key = ti.idempotency_key
         WHERE ti.kind = 'specific_issue'
        RETURNING id
    ),
    bal_deltas AS (
        SELECT ti.debit_account_id AS aid, ti.final_amount AS d FROM to_insert ti
        UNION ALL
        SELECT ti.credit_account_id AS aid, -ti.final_amount AS d FROM to_insert ti
    ),
    qty_deltas AS (
        SELECT ti.debit_account_id AS aid, 1 AS d FROM to_insert ti WHERE ti.kind = 'specific_receipt'
        UNION ALL
        SELECT ti.credit_account_id AS aid, -1 AS d FROM to_insert ti WHERE ti.kind = 'specific_issue'
    ),
    agg AS (
        SELECT aid, SUM(d_b)::BIGINT AS d_balance, SUM(d_q)::BIGINT AS d_qty
        FROM (SELECT aid, d AS d_b, 0 AS d_q FROM bal_deltas
              UNION ALL
              SELECT aid, 0 AS d_b, d AS d_q FROM qty_deltas) x GROUP BY aid
    ),
    updated AS (
        UPDATE accounts SET balance = balance + agg.d_balance, qty = qty + agg.d_qty
          FROM agg WHERE accounts.id = agg.aid RETURNING accounts.id
    )
    SELECT p.env_idx,
           CASE WHEN r.posting_line_id IS NOT NULL THEN 'idempotent_replay'::TEXT ELSE 'committed'::TEXT END,
           COALESCE(r.posting_line_id, i.id), NULL::TEXT, NULL::TEXT
      FROM parsed p
      LEFT JOIN replays  r ON r.env_idx = p.env_idx
      LEFT JOIN inserted i ON i.idempotency_key = p.idempotency_key
      ORDER BY p.env_idx;
END$$;
