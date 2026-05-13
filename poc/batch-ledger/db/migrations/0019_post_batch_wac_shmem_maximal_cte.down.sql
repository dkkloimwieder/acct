-- acct-2g9w — restore mig 0018's plpgsql body (CREATE OR REPLACE restores).
CREATE OR REPLACE FUNCTION post_batch_wac_shmem_maximal(p_envelopes JSONB)
RETURNS TABLE (
    envelope_idx     INT,
    status           TEXT,
    posting_line_id  BIGINT,
    error_code       TEXT,
    error_message    TEXT
)
LANGUAGE plpgsql AS $$
#variable_conflict use_column
DECLARE
    v_non_replay_envelopes JSONB;
BEGIN
    CREATE TEMP TABLE IF NOT EXISTS _wac_maximal_input (
        envelope_idx     INT,
        idempotency_key  UUID,
        business_date    DATE,
        is_replay        BOOLEAN,
        replay_pl_id     BIGINT,
        new_pl_id        BIGINT,
        envelope         JSONB
    ) ON COMMIT DROP;
    TRUNCATE _wac_maximal_input;

    INSERT INTO _wac_maximal_input
        (envelope_idx, idempotency_key, business_date, is_replay, replay_pl_id, envelope)
    SELECT
        (e->>'envelope_idx')::INT,
        (e->>'idempotency_key')::UUID,
        (e->>'business_date')::DATE,
        FALSE,
        NULL,
        e
    FROM jsonb_array_elements(p_envelopes) e;

    UPDATE _wac_maximal_input s
       SET is_replay = TRUE, replay_pl_id = pl.id
      FROM posting_lines pl
     WHERE pl.idempotency_key = s.idempotency_key;

    SELECT jsonb_agg(envelope ORDER BY envelope_idx)
      INTO v_non_replay_envelopes
      FROM _wac_maximal_input
     WHERE NOT is_replay;

    IF v_non_replay_envelopes IS NULL THEN
        v_non_replay_envelopes := '[]'::JSONB;
    END IF;

    WITH priced AS (
        SELECT * FROM ledger_dispatch_wac_batch(v_non_replay_envelopes)
    ),
    inserted AS (
        INSERT INTO posting_lines
            (debit_account_id, credit_account_id, amount, currency,
             idempotency_key, business_date, qty)
        SELECT
            p.debit_account_id,
            p.credit_account_id,
            p.amount,
            a.currency,
            i.idempotency_key,
            i.business_date,
            p.qty
        FROM priced p
        JOIN _wac_maximal_input i ON i.envelope_idx = p.envelope_idx
        JOIN accounts a ON a.id = p.debit_account_id
        ORDER BY p.envelope_idx
        RETURNING id, idempotency_key
    )
    UPDATE _wac_maximal_input s
       SET new_pl_id = ins.id
      FROM inserted ins
     WHERE ins.idempotency_key = s.idempotency_key;

    RETURN QUERY
    SELECT
        s.envelope_idx,
        CASE WHEN s.is_replay THEN 'idempotent_replay'::TEXT ELSE 'committed'::TEXT END,
        COALESCE(s.replay_pl_id, s.new_pl_id),
        NULL::TEXT,
        NULL::TEXT
    FROM _wac_maximal_input s
    ORDER BY s.envelope_idx;
END$$;
