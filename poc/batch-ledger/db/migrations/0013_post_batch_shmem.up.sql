-- acct-sw4i M8 — integrate ledger_extension into the PoC apply path.
--
-- post_batch_shmem replaces post_batch's `UPDATE accounts SET balance`
-- step with `ledger_apply_balance_delta()` calls into the shmem hash
-- table. Schema mapping (PoC has no multi-dim accounts; the extension's
-- key is (account_id, period_id, currency_id, ledger_kind)):
--
--   account_id   = accounts.id (BIGINT, unchanged)
--   period_id    = 1            (PoC is single-period)
--   currency_id  = 1            (PoC is single-currency for the bench)
--   ledger_kind  = 1            (PoC is single-ledger)
--
-- Sign convention follows post_batch / accounts.balance: the debit leg
-- adds +amount, the credit leg adds -amount, regardless of account
-- kind. ledger_shmem_recon() (M7) computes ledger truth with the same
-- convention; drift=0 after a batch means shmem matches what
-- accounts.balance WOULD be if post_batch had run.
--
-- Differences from post_batch (acct-k7c6):
--   1. NO FOR UPDATE pre-lock on accounts. The shmem path takes its
--      own per-cell LWLock (acquired inside ledger_apply_balance_delta);
--      accounts isn't even read.
--   2. NO UPDATE accounts. accounts.balance is irrelevant — the live
--      balance lives in shmem now, queryable via balance(account_id,
--      period_id, currency_id, ledger_kind).
--   3. The function loops through to_insert and PERFORMs ledger_apply_
--      balance_delta per leg AFTER the INSERT batch. This pulls the
--      apply outside the CTE chain because pg_extern functions can't
--      run inside data-modifying CTEs.

CREATE OR REPLACE FUNCTION post_batch_shmem(p_envelopes JSONB)
RETURNS TABLE (
    envelope_idx     INT,
    status           TEXT,
    posting_line_id  BIGINT,
    error_code       TEXT,
    error_message    TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
    leg          RECORD;
BEGIN
    -- INSERT posting_lines via the same CTE chain as post_batch_append_only.
    -- Captures (env_idx, debit_account_id, credit_account_id, amount,
    -- posting_line_id, status) into a TEMP TABLE so the apply pass can
    -- iterate without touching the CTE again.
    CREATE TEMP TABLE _shmem_batch_results ON COMMIT DROP AS
    WITH parsed AS (
        SELECT (e->>'envelope_idx')::INT       AS env_idx,
               (e->>'debit_account_id')::BIGINT  AS debit_account_id,
               (e->>'credit_account_id')::BIGINT AS credit_account_id,
               (e->>'amount')::BIGINT          AS amount,
               (e->>'idempotency_key')::UUID    AS idempotency_key,
               (e->>'business_date')::DATE      AS business_date
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
        RETURNING id, idempotency_key, debit_account_id, credit_account_id, amount
    )
    SELECT p.env_idx,
           CASE WHEN r.posting_line_id IS NOT NULL THEN 'idempotent_replay'::TEXT
                ELSE 'committed'::TEXT END                          AS status,
           COALESCE(r.posting_line_id, i.id)                         AS posting_line_id,
           p.debit_account_id, p.credit_account_id, p.amount,
           (r.posting_line_id IS NULL)                               AS is_fresh
      FROM parsed p
      LEFT JOIN replays  r ON r.env_idx = p.env_idx
      LEFT JOIN inserted i ON i.idempotency_key = p.idempotency_key;

    -- Apply per-leg balance deltas via the shmem extension. Skip
    -- idempotent replays — their posting_lines row already existed,
    -- so a prior run already applied the shmem delta (if not, that's
    -- a pre-extension data scenario and recon will surface it).
    FOR leg IN SELECT * FROM _shmem_batch_results WHERE is_fresh ORDER BY env_idx
    LOOP
        PERFORM ledger_apply_balance_delta(
            leg.debit_account_id,  1, 1::smallint, 1::smallint,  leg.amount,  0);
        PERFORM ledger_apply_balance_delta(
            leg.credit_account_id, 1, 1::smallint, 1::smallint, -leg.amount,  0);
    END LOOP;

    RETURN QUERY
    SELECT r.env_idx, r.status, r.posting_line_id,
           NULL::TEXT, NULL::TEXT
      FROM _shmem_batch_results r
     ORDER BY r.env_idx;

    DROP TABLE _shmem_batch_results;
END$$;
