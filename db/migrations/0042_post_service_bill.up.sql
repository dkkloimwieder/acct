-- ============================================================
-- acct-wb75.4.2 — Phase F2: post_service_bill (vendor service
-- invoice without PO; no GRNI staging).
--
-- Per research/posting-lines-convergence-plan.md §4.F.F2.
--
-- Vendor service invoice (utilities, consulting, freight,
-- subscriptions) is structurally distinct from PO-matched goods
-- bills: there is no PO line to three-way-match against, no goods
-- receipt that already accrued ap_unsettled, no inventory leg.
-- Posts directly to vendor ap.
--
-- Deliberately separate from post_ap_bill's existing 'service'
-- line kind: post_ap_bill is the goods-bill matcher (its 'service'
-- kind is a convenience for mixed bills with PO lines). A vendor
-- service invoice that arrives standalone goes through this
-- wrapper with its own document type, idempotency key space,
-- invoice_number column, and posting_line_reason.
--
-- Posts (per line):
--   expense_account DR / vendor_ap CR  for amount
--   tax_account     DR / vendor_ap CR  for tax_amount  (if > 0)
--
-- All legs share document_id and document_line_id so the bill is
-- traceable from any leg back to service_bill_lines.
--
-- Constraints (enforced before any INSERT):
--   * vendor exists; vendor_ap account open in p_currency
--   * non-empty array of lines
--   * each line amount > 0
--   * expense_account: value-ledger, currency=p_currency,
--     sku_id IS NULL (SKU-bearing accounts go through inventory
--     wrappers, not service bills), not closed
--   * if tax_amount > 0: tax_account_id required, same account
--     constraints as expense_account
--   * expense / tax / vendor_ap pairwise distinct per posting
--   * tax_amount >= 0
--
-- Non-goals (deliberate):
--   * no posting_line_inventory write (no qty leg, no SKU leg)
--   * no inventory_movements write ('service_bill' maps to NULL
--     in _inventory_movement_event_type via ELSE branch, so the
--     D-block in apply_event skips per-row)
--   * no posting_lines_provisional flagging (reason isn't in the
--     cost-event list at apply_event line 278)
--   * no FX (cross-currency settlement deferred to acct-3xcg /
--     acct-3dz2; payment vs invoice currency handled by
--     post_ap_payment, not here)
--
-- Error code: P0045 'service_bill_invalid'.
-- ============================================================

ALTER TYPE posting_line_reason ADD VALUE IF NOT EXISTS 'service_bill';

CREATE TABLE service_bills (
  id              UUID    NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  vendor_id       UUID    NOT NULL REFERENCES vendors(id),
  currency        CHAR(3) NOT NULL,
  business_date   DATE    NOT NULL,
  posted_by       UUID    NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID    NOT NULL UNIQUE,
  invoice_number  TEXT,
  memo            TEXT
);

CREATE INDEX service_bills_vendor_id    ON service_bills (vendor_id);
CREATE INDEX service_bills_posted_at    ON service_bills (posted_at);
CREATE INDEX service_bills_business_date ON service_bills (business_date);

CREATE TABLE service_bill_lines (
  id                 UUID   NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  service_bill_id    UUID   NOT NULL REFERENCES service_bills(id),
  line_no            INT    NOT NULL CHECK (line_no >= 1),
  expense_account_id BIGINT NOT NULL REFERENCES accounts(id),
  amount             BIGINT NOT NULL CHECK (amount > 0),
  tax_account_id     BIGINT REFERENCES accounts(id),
  tax_amount         BIGINT NOT NULL DEFAULT 0 CHECK (tax_amount >= 0),
  description        TEXT,
  UNIQUE (service_bill_id, line_no),
  CHECK (
    (tax_amount = 0 AND tax_account_id IS NULL) OR
    (tax_amount > 0 AND tax_account_id IS NOT NULL)
  )
);

CREATE INDEX service_bill_lines_bill_id ON service_bill_lines (service_bill_id);

CREATE OR REPLACE FUNCTION post_service_bill(
  p_vendor_id       UUID,
  p_currency        CHAR(3),
  p_lines           JSONB,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_invoice_number  TEXT DEFAULT NULL,
  p_memo            TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id UUID;
  v_doc_id      UUID;
  v_vendor_chk  UUID;
  v_ven_ap      BIGINT;
  v_n           INT;
  v_idx         INT;
  v_line        JSONB;
  v_exp_id      BIGINT;
  v_amount      BIGINT;
  v_tax_id      BIGINT;
  v_tax_amount  BIGINT;
  v_desc        TEXT;
  v_exp_acct    accounts%ROWTYPE;
  v_tax_acct    accounts%ROWTYPE;
  v_line_id     UUID;
  v_batch       JSONB := '[]'::JSONB;
BEGIN
  -- Idempotent replay (fast path; the ON CONFLICT below catches the
  -- race between this SELECT and the header INSERT).
  SELECT id INTO v_existing_id FROM service_bills WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN
    RETURN v_existing_id;
  END IF;

  IF p_currency IS NULL OR length(p_currency) <> 3 THEN
    RAISE EXCEPTION 'service_bill_invalid: p_currency required (3-char)'
      USING ERRCODE = 'P0045';
  END IF;

  SELECT id INTO v_vendor_chk FROM vendors WHERE id = p_vendor_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'service_bill_invalid: vendor % not found', p_vendor_id
      USING ERRCODE = 'P0045';
  END IF;

  IF p_lines IS NULL OR jsonb_typeof(p_lines) <> 'array' THEN
    RAISE EXCEPTION 'service_bill_invalid: p_lines must be a JSONB array'
      USING ERRCODE = 'P0045';
  END IF;
  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'service_bill_invalid: p_lines must be non-empty'
      USING ERRCODE = 'P0045';
  END IF;

  -- Vendor ap account in the bill currency. Per the GRNI design
  -- service bills bypass ap_unsettled and post directly to ap.
  SELECT id INTO v_ven_ap FROM accounts
   WHERE kind='ap' AND counterparty_id=p_vendor_id
     AND currency=p_currency AND NOT is_closed;
  IF v_ven_ap IS NULL THEN
    RAISE EXCEPTION 'service_bill_invalid: no open ap account for vendor=% ccy=%',
      p_vendor_id, p_currency USING ERRCODE = 'P0045';
  END IF;

  -- Validate every line up front. We don't INSERT or call
  -- post_posting_lines until the whole batch is well-formed.
  FOR v_idx IN 1..v_n LOOP
    v_line       := p_lines -> (v_idx - 1);
    v_exp_id     := (v_line->>'expense_account_id')::BIGINT;
    v_amount     := (v_line->>'amount')::BIGINT;
    v_tax_id     := (v_line->>'tax_account_id')::BIGINT;
    v_tax_amount := COALESCE((v_line->>'tax_amount')::BIGINT, 0);

    IF v_exp_id IS NULL OR v_amount IS NULL THEN
      RAISE EXCEPTION
        'service_bill_invalid: line %: expense_account_id and amount are required',
        v_idx USING ERRCODE = 'P0045';
    END IF;
    IF v_amount <= 0 THEN
      RAISE EXCEPTION 'service_bill_invalid: line %: amount must be > 0 (got %)',
        v_idx, v_amount USING ERRCODE = 'P0045';
    END IF;
    IF v_tax_amount < 0 THEN
      RAISE EXCEPTION 'service_bill_invalid: line %: tax_amount must be >= 0 (got %)',
        v_idx, v_tax_amount USING ERRCODE = 'P0045';
    END IF;
    IF (v_tax_amount > 0) <> (v_tax_id IS NOT NULL) THEN
      RAISE EXCEPTION
        'service_bill_invalid: line %: tax_amount > 0 requires tax_account_id (and vice versa)',
        v_idx USING ERRCODE = 'P0045';
    END IF;

    SELECT * INTO v_exp_acct FROM accounts WHERE id = v_exp_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'service_bill_invalid: line %: expense_account_id % not found',
        v_idx, v_exp_id USING ERRCODE = 'P0045';
    END IF;
    IF v_exp_acct.is_closed THEN
      RAISE EXCEPTION 'service_bill_invalid: line %: expense account % is closed',
        v_idx, v_exp_id USING ERRCODE = 'P0045';
    END IF;
    IF v_exp_acct.ledger_kind <> 'value' THEN
      RAISE EXCEPTION
        'service_bill_invalid: line %: expense account must be value-ledger (got %)',
        v_idx, v_exp_acct.ledger_kind USING ERRCODE = 'P0045';
    END IF;
    IF v_exp_acct.sku_id IS NOT NULL THEN
      RAISE EXCEPTION
        'service_bill_invalid: line %: expense account is SKU-bearing; use inventory wrappers',
        v_idx USING ERRCODE = 'P0045';
    END IF;
    IF v_exp_acct.currency IS DISTINCT FROM p_currency THEN
      RAISE EXCEPTION
        'service_bill_invalid: line %: expense account currency=% but bill currency=%',
        v_idx, v_exp_acct.currency, p_currency USING ERRCODE = 'P0045';
    END IF;
    IF v_exp_id = v_ven_ap THEN
      RAISE EXCEPTION
        'service_bill_invalid: line %: expense account equals vendor ap account',
        v_idx USING ERRCODE = 'P0045';
    END IF;

    IF v_tax_amount > 0 THEN
      SELECT * INTO v_tax_acct FROM accounts WHERE id = v_tax_id;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'service_bill_invalid: line %: tax_account_id % not found',
          v_idx, v_tax_id USING ERRCODE = 'P0045';
      END IF;
      IF v_tax_acct.is_closed THEN
        RAISE EXCEPTION 'service_bill_invalid: line %: tax account % is closed',
          v_idx, v_tax_id USING ERRCODE = 'P0045';
      END IF;
      IF v_tax_acct.ledger_kind <> 'value' THEN
        RAISE EXCEPTION
          'service_bill_invalid: line %: tax account must be value-ledger (got %)',
          v_idx, v_tax_acct.ledger_kind USING ERRCODE = 'P0045';
      END IF;
      IF v_tax_acct.sku_id IS NOT NULL THEN
        RAISE EXCEPTION
          'service_bill_invalid: line %: tax account is SKU-bearing',
          v_idx USING ERRCODE = 'P0045';
      END IF;
      IF v_tax_acct.currency IS DISTINCT FROM p_currency THEN
        RAISE EXCEPTION
          'service_bill_invalid: line %: tax account currency=% but bill currency=%',
          v_idx, v_tax_acct.currency, p_currency USING ERRCODE = 'P0045';
      END IF;
      IF v_tax_id = v_ven_ap THEN
        RAISE EXCEPTION
          'service_bill_invalid: line %: tax account equals vendor ap account',
          v_idx USING ERRCODE = 'P0045';
      END IF;
      IF v_tax_id = v_exp_id THEN
        RAISE EXCEPTION
          'service_bill_invalid: line %: tax account equals expense account',
          v_idx USING ERRCODE = 'P0045';
      END IF;
    END IF;
  END LOOP;

  -- INSERT header. ON CONFLICT handles the idempotent-replay race
  -- where two callers try to claim the same idempotency_key
  -- between our SELECT above and this INSERT.
  INSERT INTO service_bills (
    vendor_id, currency, business_date, posted_by,
    idempotency_key, invoice_number, memo
  ) VALUES (
    p_vendor_id, p_currency, p_business_date, p_posted_by,
    p_idempotency_key, p_invoice_number, p_memo
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM service_bills WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  -- INSERT lines + build the post_posting_lines batch.
  FOR v_idx IN 1..v_n LOOP
    v_line       := p_lines -> (v_idx - 1);
    v_exp_id     := (v_line->>'expense_account_id')::BIGINT;
    v_amount     := (v_line->>'amount')::BIGINT;
    v_tax_id     := (v_line->>'tax_account_id')::BIGINT;
    v_tax_amount := COALESCE((v_line->>'tax_amount')::BIGINT, 0);
    v_desc       := v_line->>'description';

    INSERT INTO service_bill_lines (
      service_bill_id, line_no, expense_account_id, amount,
      tax_account_id, tax_amount, description
    ) VALUES (
      v_doc_id, v_idx, v_exp_id, v_amount,
      v_tax_id, v_tax_amount, v_desc
    ) RETURNING id INTO v_line_id;

    -- expense DR / vendor_ap CR for amount
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'service_bill',
      'document_kind',     'service_bill',
      'document_id',       v_doc_id,
      'document_line_id',  v_line_id,
      'debit_account_id',  v_exp_id,
      'credit_account_id', v_ven_ap,
      'amount',            v_amount,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   p_vendor_id,
      'posted_by',         p_posted_by
    ));

    -- tax DR / vendor_ap CR for tax_amount (if any)
    IF v_tax_amount > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'service_bill',
        'document_kind',     'service_bill',
        'document_id',       v_doc_id,
        'document_line_id',  v_line_id,
        'debit_account_id',  v_tax_id,
        'credit_account_id', v_ven_ap,
        'amount',            v_tax_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_vendor_id,
        'posted_by',         p_posted_by
      ));
    END IF;
  END LOOP;

  PERFORM post_posting_lines(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;
