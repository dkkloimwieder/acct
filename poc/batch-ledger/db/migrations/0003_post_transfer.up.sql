-- acct-zdrm (P2 of acct-qdp5 PoC).
--
-- pgledger-equivalent single-transfer entry point. One row INSERT into
-- posting_lines + two row UPDATEs on accounts. No batching, no smarts.
-- Sign convention: balance += amount on the debit side, balance -= amount
-- on the credit side. This is NOT proper double-entry semantics (debit-normal
-- vs credit-normal handling differs); it's a measurement proxy that gives us
-- the same lock surface and WAL footprint as a correct implementation.
-- Correctness is P3+'s concern; P2 is throughput calibration.

CREATE OR REPLACE FUNCTION post_transfer(
    p_debit_account_id  BIGINT,
    p_credit_account_id BIGINT,
    p_amount            BIGINT,
    p_idempotency_key   UUID
) RETURNS BIGINT
LANGUAGE plpgsql AS $$
DECLARE
    v_pl_id    BIGINT;
    v_currency CHAR(3);
BEGIN
    SELECT currency INTO v_currency FROM accounts WHERE id = p_debit_account_id;
    IF v_currency IS NULL THEN
        RAISE EXCEPTION 'debit account % not found', p_debit_account_id;
    END IF;

    INSERT INTO posting_lines (debit_account_id, credit_account_id, amount,
                               currency, idempotency_key, business_date)
    VALUES (p_debit_account_id, p_credit_account_id, p_amount,
            v_currency, p_idempotency_key, CURRENT_DATE)
    RETURNING id INTO v_pl_id;

    UPDATE accounts SET balance = balance + p_amount WHERE id = p_debit_account_id;
    UPDATE accounts SET balance = balance - p_amount WHERE id = p_credit_account_id;

    RETURN v_pl_id;
END$$;
