-- Down: restore mig 0081's post_ar_payment + mig 0082's post_ap_payment
-- (cross-currency settlement support reverted).

DROP FUNCTION IF EXISTS post_ar_payment(
  UUID, CHAR, BIGINT, DATE, UUID, UUID, TEXT, CHAR, BIGINT
);

CREATE OR REPLACE FUNCTION post_ar_payment(
  p_customer_id     UUID,
  p_currency        CHAR(3),
  p_amount          BIGINT,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id    UUID;
  v_doc_id         UUID;
  v_customer_check UUID;
  v_cash_acct      BIGINT;
  v_cust_ar        BIGINT;
  v_batch          JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM ar_payments
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  IF p_amount IS NULL OR p_amount <= 0 THEN
    RAISE EXCEPTION 'ar_payment_invalid: amount must be > 0 (got %)', p_amount
      USING ERRCODE = 'P0039';
  END IF;

  SELECT id INTO v_customer_check FROM customers WHERE id = p_customer_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'ar_payment_invalid: customer % not found', p_customer_id
      USING ERRCODE = 'P0039';
  END IF;

  SELECT id INTO v_cash_acct FROM accounts
   WHERE kind='cash' AND ledger_kind='value'
     AND currency=p_currency AND NOT is_closed;
  IF v_cash_acct IS NULL THEN
    RAISE EXCEPTION 'no open cash account for ccy=%', p_currency
      USING ERRCODE = 'P0010';
  END IF;

  SELECT id INTO v_cust_ar FROM accounts
   WHERE kind='ar' AND counterparty_id=p_customer_id
     AND currency=p_currency AND NOT is_closed;
  IF v_cust_ar IS NULL THEN
    RAISE EXCEPTION 'no open ar account for customer=% ccy=%',
                    p_customer_id, p_currency USING ERRCODE = 'P0010';
  END IF;

  INSERT INTO ar_payments (
    customer_id, currency, amount, business_date, posted_by,
    idempotency_key, notes
  ) VALUES (
    p_customer_id, p_currency, p_amount, p_business_date, p_posted_by,
    p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM ar_payments
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  v_batch := jsonb_build_array(jsonb_build_object(
    'reason',            'ar_payment',
    'document_kind',     'ar_payment',
    'document_id',       v_doc_id,
    'debit_account_id',  v_cash_acct,
    'credit_account_id', v_cust_ar,
    'amount',            p_amount,
    'business_date',     p_business_date,
    'idempotency_key',   gen_random_uuid(),
    'counterparty_id',   p_customer_id,
    'posted_by',         p_posted_by
  ));

  PERFORM post_transfers(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;

DROP FUNCTION IF EXISTS post_ap_payment(
  UUID, CHAR, BIGINT, DATE, UUID, UUID, TEXT, CHAR, BIGINT
);

CREATE OR REPLACE FUNCTION post_ap_payment(
  p_vendor_id       UUID,
  p_currency        CHAR(3),
  p_amount          BIGINT,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id   UUID;
  v_doc_id        UUID;
  v_vendor_check  UUID;
  v_cash_acct     BIGINT;
  v_vend_ap       BIGINT;
  v_batch         JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM ap_payments
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  IF p_amount IS NULL OR p_amount <= 0 THEN
    RAISE EXCEPTION 'ap_payment_invalid: amount must be > 0 (got %)', p_amount
      USING ERRCODE = 'P0042';
  END IF;

  SELECT id INTO v_vendor_check FROM vendors WHERE id = p_vendor_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'ap_payment_invalid: vendor % not found', p_vendor_id
      USING ERRCODE = 'P0042';
  END IF;

  SELECT id INTO v_cash_acct FROM accounts
   WHERE kind='cash' AND ledger_kind='value'
     AND currency=p_currency AND NOT is_closed;
  IF v_cash_acct IS NULL THEN
    RAISE EXCEPTION 'no open cash account for ccy=%', p_currency
      USING ERRCODE = 'P0010';
  END IF;

  SELECT id INTO v_vend_ap FROM accounts
   WHERE kind='ap' AND counterparty_id=p_vendor_id
     AND currency=p_currency AND NOT is_closed;
  IF v_vend_ap IS NULL THEN
    RAISE EXCEPTION 'no open ap account for vendor=% ccy=%',
                    p_vendor_id, p_currency USING ERRCODE = 'P0010';
  END IF;

  INSERT INTO ap_payments (
    vendor_id, currency, amount, business_date, posted_by,
    idempotency_key, notes
  ) VALUES (
    p_vendor_id, p_currency, p_amount, p_business_date, p_posted_by,
    p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM ap_payments
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  v_batch := jsonb_build_array(jsonb_build_object(
    'reason',            'ap_payment',
    'document_kind',     'ap_payment',
    'document_id',       v_doc_id,
    'debit_account_id',  v_vend_ap,
    'credit_account_id', v_cash_acct,
    'amount',            p_amount,
    'business_date',     p_business_date,
    'idempotency_key',   gen_random_uuid(),
    'counterparty_id',   p_vendor_id,
    'posted_by',         p_posted_by
  ));

  PERFORM post_transfers(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;
