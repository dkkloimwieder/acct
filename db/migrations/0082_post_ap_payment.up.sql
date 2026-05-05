-- acct-bvh — post_ap_payment cash-out workflow.
--
-- Symmetric companion to post_ar_payment (Slice C, mig 0081). Where AR
-- payments are cash DR / ar CR, AP payments are ap DR / cash CR. Same
-- shape: header-only document, single ledger event, fast-path replay
-- + ON CONFLICT idempotency.
--
-- Slice A explicitly deferred this; Slice C's post_ar_payment surfaced
-- the asymmetry. No enum extensions are needed — `ap_payment` is in
-- the transfer_reason enum since mig 0002, and `ap` / `cash` account
-- kinds exist from the seed schema.
--
-- New error code: P0042 (ap_payment_invalid).

-- ============================================================
-- ap_payments
-- ============================================================
--
-- Single-event document: ap DR / cash CR. No detail table — partial
-- payments accumulate against the ap balance, mirroring how
-- ar_payments handles partial collection. Phase 2 follow-up is the
-- allocate-against-bill workflow (drains specific vendor_bills vs.
-- running ap balance).

CREATE TABLE ap_payments (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  vendor_id       UUID NOT NULL REFERENCES vendors(id),
  currency        CHAR(3) NOT NULL,
  amount          BIGINT NOT NULL CHECK (amount > 0),
  business_date   DATE NOT NULL,
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID NOT NULL UNIQUE,
  notes           TEXT
);

CREATE INDEX ap_payments_vendor    ON ap_payments (vendor_id);
CREATE INDEX ap_payments_posted_at ON ap_payments (posted_at);

COMMENT ON TABLE ap_payments IS
  'AP payment event. One row per post_ap_payment call. Single event: '
  'ap(vendor, currency) DR / cash(currency) CR. Mirror of ar_payments.';

-- ============================================================
-- post_ap_payment
-- ============================================================

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

COMMENT ON FUNCTION post_ap_payment(UUID, CHAR, BIGINT, DATE, UUID, UUID, TEXT) IS
  'AP payment. Single event: ap(vendor, currency) DR / cash(currency) '
  'CR. Symmetric to post_ar_payment. No three-way match; partial '
  'payments accumulate against ap balance. Phase 2 follow-up: '
  'allocate-against-bill workflow that drains specific vendor_bills.';
