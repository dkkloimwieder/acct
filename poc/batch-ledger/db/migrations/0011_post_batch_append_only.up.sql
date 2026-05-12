-- acct-qdp5 PoC — append-only / TigerBeetle-aligned hot path.
--
-- post_batch_append_only is the simplest possible hot path: INSERT into
-- posting_lines, return per-envelope status. NO `UPDATE accounts SET balance`,
-- NO `FOR UPDATE` pre-lock. Balance is derived: `SELECT SUM(amount) FROM
-- posting_lines WHERE debit_account_id = X OR credit_account_id = X` — or
-- via a periodically-refreshed projection table (out of scope here).
--
-- This is the equivalent of TigerBeetle's "no row-level state during commit"
-- shape in pure Postgres. The PoC measurement confirms it doubles throughput
-- over the P3 simple shape (UPDATE accounts) at the same workload.
--
-- Kept as a SEPARATELY-NAMED function so it coexists with post_batch (which
-- handles the cost-method-aware path). Caller routes per their needs.

CREATE OR REPLACE FUNCTION post_batch_append_only(p_envelopes JSONB)
RETURNS TABLE (
    envelope_idx     INT,
    status           TEXT,
    posting_line_id  BIGINT,
    error_code       TEXT,
    error_message    TEXT
)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    WITH parsed AS (
        SELECT (e->>'envelope_idx')::INT AS env_idx,
               (e->>'debit_account_id')::BIGINT AS debit_account_id,
               (e->>'credit_account_id')::BIGINT AS credit_account_id,
               (e->>'amount')::BIGINT AS amount,
               (e->>'idempotency_key')::UUID AS idempotency_key,
               (e->>'business_date')::DATE AS business_date
          FROM jsonb_array_elements(p_envelopes) e
    ),
    replays AS (
        SELECT p.env_idx, p.idempotency_key, pl.id AS posting_line_id
          FROM parsed p JOIN posting_lines pl ON pl.idempotency_key = p.idempotency_key
    ),
    to_insert AS (
        SELECT p.* FROM parsed p
         WHERE NOT EXISTS (SELECT 1 FROM replays r WHERE r.env_idx = p.env_idx)
    ),
    inserted AS (
        INSERT INTO posting_lines
            (debit_account_id, credit_account_id, amount, currency,
             idempotency_key, business_date)
        SELECT ti.debit_account_id, ti.credit_account_id, ti.amount, a.currency,
               ti.idempotency_key, ti.business_date
          FROM to_insert ti JOIN accounts a ON a.id = ti.debit_account_id
         ORDER BY ti.env_idx
        RETURNING id, idempotency_key
    )
    SELECT p.env_idx,
           CASE WHEN r.posting_line_id IS NOT NULL THEN 'idempotent_replay'::TEXT
                ELSE 'committed'::TEXT END,
           COALESCE(r.posting_line_id, i.id), NULL::TEXT, NULL::TEXT
      FROM parsed p
      LEFT JOIN replays  r ON r.env_idx = p.env_idx
      LEFT JOIN inserted i ON i.idempotency_key = p.idempotency_key
      ORDER BY p.env_idx;
END$$;
