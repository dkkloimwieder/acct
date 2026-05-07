-- acct-3yno / acct-7t4.5 — disposal_match line kind on post_ap_bill.
--
-- Closes the AP-side reconciliation loop on the by-products epic:
-- the disposal vendor sends a bill for actual waste pickup, and we
-- match it against the WO-time accrual posted by mig 0099.
--
-- Schema:
--   * vendor_bill_lines.kind extended to include 'disposal_match'
--   * disposal_wo_event_id (UUID, FK wo_events) — identifies the
--     originating WO completion event
--   * by_product_no (INT) — composite key into wo_by_products
--     (wo_id resolved via the wo_event)
--   * CHECK constraint extended to encode per-kind required fields
--
-- Function (post_ap_bill):
--   New 'disposal_match' branch mirrors po_match's three-way match
--   shape against the WO-side accrual:
--
--   1. Resolve wo_event → wo_id; look up wo_by_products(wo_id, no).
--   2. Validate treatment='disposal_cost', vendor matches, currency
--      matches.
--   3. Cumulative billed ≤ wo_by_products.actual_qty (mirrors
--      po_match's qty availability check, no returns concept).
--   4. unit_cost vs |unit_value| within vendor.unit_cost_tolerance_pct.
--   5. Base leg: accrued_disposal_liability(vendor) DR / ap(vendor) CR
--      at the original accrual price (qty × |unit_value|).
--   6. Diff leg: variance_match_tolerance routed sign-aware against
--      ap (mirrors mig 0090's po_match tolerance pattern).
--
--   Symmetric with po_match end-to-end. Reuses tolerance machinery
--   from mig 0090; reuses three-way mismatch error code P0024.
--   Mixed bills with po_match + service + disposal_match in one call
--   are supported — each line is independent.

-- ============================================================
-- Schema extension.
-- ============================================================

ALTER TABLE vendor_bill_lines
  ADD COLUMN disposal_wo_event_id UUID REFERENCES wo_events(id),
  ADD COLUMN by_product_no        INT;

ALTER TABLE vendor_bill_lines DROP CONSTRAINT vendor_bill_lines_check;
ALTER TABLE vendor_bill_lines DROP CONSTRAINT vendor_bill_lines_kind_check;

ALTER TABLE vendor_bill_lines
  ADD CONSTRAINT vendor_bill_lines_kind_check
  CHECK (kind IN ('po_match', 'service', 'disposal_match'));

ALTER TABLE vendor_bill_lines
  ADD CONSTRAINT vendor_bill_lines_check
  CHECK (
    (kind = 'po_match'
     AND po_line_id IS NOT NULL
     AND expense_account_id IS NULL
     AND qty IS NOT NULL AND qty > 0
     AND unit_cost IS NOT NULL AND unit_cost >= 0
     AND disposal_wo_event_id IS NULL
     AND by_product_no IS NULL)
    OR
    (kind = 'service'
     AND po_line_id IS NULL
     AND expense_account_id IS NOT NULL
     AND qty IS NULL
     AND unit_cost IS NULL
     AND disposal_wo_event_id IS NULL
     AND by_product_no IS NULL)
    OR
    (kind = 'disposal_match'
     AND po_line_id IS NULL
     AND expense_account_id IS NULL
     AND qty IS NOT NULL AND qty > 0
     AND unit_cost IS NOT NULL AND unit_cost >= 0
     AND disposal_wo_event_id IS NOT NULL
     AND by_product_no IS NOT NULL)
  );

CREATE INDEX vendor_bill_lines_disposal_event
  ON vendor_bill_lines (disposal_wo_event_id, by_product_no)
  WHERE disposal_wo_event_id IS NOT NULL;

COMMENT ON COLUMN vendor_bill_lines.disposal_wo_event_id IS
  'For kind=''disposal_match'' only: the wo_event (typically a '
  'wo_complete event) that triggered the by-product accrual being '
  'drained. Combined with by_product_no, identifies the wo_by_products '
  'row whose accrual this bill line is matching. NULL for po_match / '
  'service. acct-7t4.5.';

COMMENT ON COLUMN vendor_bill_lines.by_product_no IS
  'For kind=''disposal_match'' only: the by_product_no on the '
  'referenced wo_by_products row. Composite key with disposal_wo_'
  'event_id (which resolves to wo_id). acct-7t4.5.';

-- ============================================================
-- post_ap_bill — add disposal_match branch.
-- ============================================================

CREATE OR REPLACE FUNCTION post_ap_bill(
  p_vendor_id       UUID,
  p_currency        CHAR(3),
  p_lines           JSONB,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id      UUID;
  v_doc_id           UUID;
  v_vendor_check     UUID;
  v_tolerance_pct    NUMERIC(5,2);
  v_n                INT;
  v_idx              INT;
  v_line             JSONB;
  v_kind             TEXT;
  v_po_line_id       UUID;
  v_qty              BIGINT;
  v_unit_cost        BIGINT;
  v_amount           BIGINT;
  v_expense_acct     BIGINT;
  v_pl               RECORD;
  v_total_received   BIGINT;
  v_total_billed     BIGINT;
  v_returns_to_us    BIGINT;
  v_avail            BIGINT;
  v_ven_unsettled    BIGINT;
  v_ven_ap           BIGINT;
  v_match_tol_acct   BIGINT;
  v_exp_acct         accounts%ROWTYPE;
  v_bill_line_id     UUID;
  v_diff_total       BIGINT;
  v_diff_pct         NUMERIC(10,4);
  v_amount_at_po     BIGINT;
  v_batch            JSONB := '[]'::JSONB;
  -- acct-7t4.5 disposal_match
  v_disp_event_id    UUID;
  v_by_product_no    INT;
  v_disp_wo_id       UUID;
  v_wo_currency      CHAR(3);
  v_bp               wo_by_products%ROWTYPE;
  v_accrued_unit     BIGINT;
  v_disp_liability   BIGINT;
  v_amount_at_accrual BIGINT;
BEGIN
  SELECT id INTO v_existing_id FROM vendor_bills
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT id, unit_cost_tolerance_pct INTO v_vendor_check, v_tolerance_pct
    FROM vendors WHERE id = p_vendor_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'ap_bill_invalid_line: vendor % not found', p_vendor_id
      USING ERRCODE = 'P0025';
  END IF;

  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'ap_bill_invalid_line: empty bill for vendor %', p_vendor_id
      USING ERRCODE = 'P0025';
  END IF;

  SELECT id INTO v_ven_ap FROM accounts
   WHERE kind='ap' AND counterparty_id=p_vendor_id
     AND currency=p_currency AND NOT is_closed;
  IF v_ven_ap IS NULL THEN
    RAISE EXCEPTION 'no open ap account for vendor=% ccy=%',
                    p_vendor_id, p_currency USING ERRCODE = 'P0010';
  END IF;

  INSERT INTO vendor_bills (
    vendor_id, currency, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_vendor_id, p_currency, p_business_date, p_posted_by,
    p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM vendor_bills WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  FOR v_idx IN 1..v_n LOOP
    v_line   := p_lines -> (v_idx - 1);
    v_kind   := v_line->>'kind';
    v_amount := (v_line->>'amount')::BIGINT;

    IF v_kind = 'po_match' THEN
      v_po_line_id := (v_line->>'po_line_id')::UUID;
      v_qty        := (v_line->>'qty')::BIGINT;
      v_unit_cost  := (v_line->>'unit_cost')::BIGINT;

      SELECT pl.po_id, pl.unit_cost, pl.currency, po.vendor_id
        INTO v_pl
        FROM purchase_order_lines pl
        JOIN purchase_orders po ON po.id = pl.po_id
       WHERE pl.id = v_po_line_id
         FOR UPDATE OF pl;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'ap_bill_invalid_line: line % po_line % not found',
                        v_idx, v_po_line_id USING ERRCODE = 'P0025';
      END IF;
      IF v_pl.vendor_id IS DISTINCT FROM p_vendor_id THEN
        RAISE EXCEPTION
          'ap_bill_invalid_line: line % po_line % belongs to vendor % '
          'but bill is for vendor %',
          v_idx, v_po_line_id, v_pl.vendor_id, p_vendor_id
          USING ERRCODE = 'P0025';
      END IF;
      IF v_pl.currency <> p_currency THEN
        RAISE EXCEPTION
          'ap_bill_invalid_line: line % po_line currency=% but bill currency=%',
          v_idx, v_pl.currency, p_currency USING ERRCODE = 'P0025';
      END IF;

      IF v_unit_cost <> v_pl.unit_cost THEN
        IF v_tolerance_pct = 0 THEN
          RAISE EXCEPTION
            'ap_bill_three_way_mismatch: line % unit_cost % does not match '
            'po_line.unit_cost %',
            v_idx, v_unit_cost, v_pl.unit_cost
            USING ERRCODE = 'P0024';
        END IF;
        IF v_pl.unit_cost = 0 THEN
          RAISE EXCEPTION
            'ap_bill_three_way_mismatch: line % po_line.unit_cost is 0 '
            'but bill unit_cost is % (zero-baseline; out of tolerance '
            'by definition, vendor tolerance %%%)',
            v_idx, v_unit_cost, v_tolerance_pct
            USING ERRCODE = 'P0024';
        END IF;
        v_diff_pct := ABS(v_unit_cost - v_pl.unit_cost) * 100.0 / v_pl.unit_cost;
        IF v_diff_pct > v_tolerance_pct THEN
          RAISE EXCEPTION
            'ap_bill_three_way_mismatch: line % unit_cost % differs from '
            'po_line.unit_cost % by %%% (vendor tolerance %%%)',
            v_idx, v_unit_cost, v_pl.unit_cost, v_diff_pct, v_tolerance_pct
            USING ERRCODE = 'P0024';
        END IF;
      END IF;
      IF v_amount <> v_qty * v_unit_cost THEN
        RAISE EXCEPTION
          'ap_bill_three_way_mismatch: line % amount % <> qty % × unit_cost %',
          v_idx, v_amount, v_qty, v_unit_cost
          USING ERRCODE = 'P0024';
      END IF;

      SELECT COALESCE(SUM(qty_received), 0) INTO v_total_received
        FROM po_receipt_lines WHERE po_line_id = v_po_line_id;
      SELECT COALESCE(SUM(qty), 0) INTO v_total_billed
        FROM vendor_bill_lines
       WHERE po_line_id = v_po_line_id AND kind = 'po_match';
      SELECT COALESCE(SUM(prl.qty_to_ap_unsettled), 0) INTO v_returns_to_us
        FROM po_return_lines prl
        JOIN po_receipt_lines rcl ON rcl.id = prl.recv_line_id
       WHERE rcl.po_line_id = v_po_line_id;
      v_avail := v_total_received - v_total_billed - v_returns_to_us;
      IF v_qty > v_avail THEN
        RAISE EXCEPTION
          'ap_bill_three_way_mismatch: line % qty % exceeds received-not-'
          'billed-not-returned remainder % for po_line % (received=%, '
          'already billed=%, prior returns to ap_unsettled=%)',
          v_idx, v_qty, v_avail, v_po_line_id, v_total_received,
          v_total_billed, v_returns_to_us
          USING ERRCODE = 'P0024';
      END IF;

      SELECT id INTO v_ven_unsettled FROM accounts
       WHERE kind='ap_unsettled' AND counterparty_id=p_vendor_id
         AND currency=p_currency AND NOT is_closed;
      IF v_ven_unsettled IS NULL THEN
        RAISE EXCEPTION 'no open ap_unsettled account for vendor=% ccy=%',
                        p_vendor_id, p_currency USING ERRCODE = 'P0010';
      END IF;

      INSERT INTO vendor_bill_lines (
        bill_id, line_no, kind, po_line_id, qty, unit_cost, amount
      ) VALUES (
        v_doc_id, v_idx, 'po_match', v_po_line_id, v_qty, v_unit_cost, v_amount
      ) RETURNING id INTO v_bill_line_id;

      v_amount_at_po := v_qty * v_pl.unit_cost;
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ap_bill',
        'document_kind',     'vendor_bill',
        'document_id',       v_doc_id,
        'document_line_id',  v_bill_line_id,
        'debit_account_id',  v_ven_unsettled,
        'credit_account_id', v_ven_ap,
        'amount',            v_amount_at_po,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_vendor_id,
        'posted_by',         p_posted_by
      ));

      v_diff_total := v_amount - v_amount_at_po;
      IF v_diff_total <> 0 THEN
        SELECT id INTO v_match_tol_acct FROM accounts
         WHERE kind='variance_match_tolerance' AND ledger_kind='value'
           AND currency=p_currency AND NOT is_closed;
        IF v_match_tol_acct IS NULL THEN
          RAISE EXCEPTION 'no open variance_match_tolerance account for ccy=%',
                          p_currency USING ERRCODE = 'P0010';
        END IF;

        IF v_diff_total > 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'ap_bill',
            'document_kind',     'vendor_bill',
            'document_id',       v_doc_id,
            'document_line_id',  v_bill_line_id,
            'debit_account_id',  v_match_tol_acct,
            'credit_account_id', v_ven_ap,
            'amount',            v_diff_total,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_vendor_id,
            'posted_by',         p_posted_by
          ));
        ELSE
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'ap_bill',
            'document_kind',     'vendor_bill',
            'document_id',       v_doc_id,
            'document_line_id',  v_bill_line_id,
            'debit_account_id',  v_ven_ap,
            'credit_account_id', v_match_tol_acct,
            'amount',            -v_diff_total,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_vendor_id,
            'posted_by',         p_posted_by
          ));
        END IF;
      END IF;

    ELSIF v_kind = 'service' THEN
      v_expense_acct := (v_line->>'expense_account_id')::BIGINT;

      SELECT * INTO v_exp_acct FROM accounts WHERE id = v_expense_acct;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'ap_bill_invalid_line: line % expense_account_id % not found',
                        v_idx, v_expense_acct USING ERRCODE = 'P0025';
      END IF;
      IF v_exp_acct.is_closed THEN
        RAISE EXCEPTION 'ap_bill_invalid_line: line % expense account % is closed',
                        v_idx, v_expense_acct USING ERRCODE = 'P0025';
      END IF;
      IF v_exp_acct.ledger_kind <> 'value' THEN
        RAISE EXCEPTION 'ap_bill_invalid_line: line % expense account % is %, expected value',
                        v_idx, v_expense_acct, v_exp_acct.ledger_kind
          USING ERRCODE = 'P0025';
      END IF;
      IF v_exp_acct.currency <> p_currency THEN
        RAISE EXCEPTION 'ap_bill_invalid_line: line % expense account ccy=% but bill ccy=%',
                        v_idx, v_exp_acct.currency, p_currency
          USING ERRCODE = 'P0025';
      END IF;

      INSERT INTO vendor_bill_lines (
        bill_id, line_no, kind, expense_account_id, amount
      ) VALUES (
        v_doc_id, v_idx, 'service', v_expense_acct, v_amount
      ) RETURNING id INTO v_bill_line_id;

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ap_bill',
        'document_kind',     'vendor_bill',
        'document_id',       v_doc_id,
        'document_line_id',  v_bill_line_id,
        'debit_account_id',  v_expense_acct,
        'credit_account_id', v_ven_ap,
        'amount',            v_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_vendor_id,
        'posted_by',         p_posted_by
      ));

    ELSIF v_kind = 'disposal_match' THEN
      v_disp_event_id := (v_line->>'disposal_wo_event_id')::UUID;
      v_by_product_no := (v_line->>'by_product_no')::INT;
      v_qty           := (v_line->>'qty')::BIGINT;
      v_unit_cost     := (v_line->>'unit_cost')::BIGINT;

      -- Resolve the wo_event → wo_id, and validate currency matches
      -- the originating WO. Bill-side currency is canonical for the
      -- accrual lookup; per-vendor accrued_disposal_liability is
      -- partitioned by currency, so a bill in EUR can't drain a USD
      -- accrual even if the same vendor was involved.
      SELECT we.wo_id, wo.currency
        INTO v_disp_wo_id, v_wo_currency
        FROM wo_events we
        JOIN work_orders wo ON wo.id = we.wo_id
       WHERE we.id = v_disp_event_id;
      IF NOT FOUND THEN
        RAISE EXCEPTION
          'ap_bill_invalid_line: line % disposal_wo_event_id % not found',
          v_idx, v_disp_event_id USING ERRCODE = 'P0025';
      END IF;
      IF v_wo_currency <> p_currency THEN
        RAISE EXCEPTION
          'ap_bill_invalid_line: line % wo currency=% but bill currency=%',
          v_idx, v_wo_currency, p_currency USING ERRCODE = 'P0025';
      END IF;

      -- Lock the wo_by_products row to serialize concurrent matching
      -- against the same accrual.
      SELECT * INTO v_bp FROM wo_by_products
       WHERE wo_id = v_disp_wo_id AND by_product_no = v_by_product_no
         FOR UPDATE;
      IF NOT FOUND THEN
        RAISE EXCEPTION
          'ap_bill_invalid_line: line % wo_by_products(wo=%,no=%) not found',
          v_idx, v_disp_wo_id, v_by_product_no USING ERRCODE = 'P0025';
      END IF;
      IF v_bp.treatment <> 'disposal_cost' THEN
        RAISE EXCEPTION
          'ap_bill_invalid_line: line % wo_by_products row treatment=% '
          '(only disposal_cost rows accept disposal_match bills)',
          v_idx, v_bp.treatment USING ERRCODE = 'P0025';
      END IF;
      IF v_bp.disposal_vendor_id IS DISTINCT FROM p_vendor_id THEN
        RAISE EXCEPTION
          'ap_bill_invalid_line: line % wo_by_products vendor=% but bill '
          'vendor=%',
          v_idx, v_bp.disposal_vendor_id, p_vendor_id
          USING ERRCODE = 'P0025';
      END IF;

      v_accrued_unit := ABS(v_bp.unit_value);

      -- Tolerance-aware unit_cost match (mirror po_match shape).
      IF v_unit_cost <> v_accrued_unit THEN
        IF v_tolerance_pct = 0 THEN
          RAISE EXCEPTION
            'ap_bill_three_way_mismatch: line % unit_cost % does not match '
            'accrued unit_value %',
            v_idx, v_unit_cost, v_accrued_unit
            USING ERRCODE = 'P0024';
        END IF;
        IF v_accrued_unit = 0 THEN
          RAISE EXCEPTION
            'ap_bill_three_way_mismatch: line % accrued unit_value is 0 '
            'but bill unit_cost is % (zero-baseline; out of tolerance '
            'by definition, vendor tolerance %%%)',
            v_idx, v_unit_cost, v_tolerance_pct
            USING ERRCODE = 'P0024';
        END IF;
        v_diff_pct := ABS(v_unit_cost - v_accrued_unit) * 100.0 / v_accrued_unit;
        IF v_diff_pct > v_tolerance_pct THEN
          RAISE EXCEPTION
            'ap_bill_three_way_mismatch: line % unit_cost % differs from '
            'accrued unit_value % by %%% (vendor tolerance %%%)',
            v_idx, v_unit_cost, v_accrued_unit, v_diff_pct, v_tolerance_pct
            USING ERRCODE = 'P0024';
        END IF;
      END IF;
      IF v_amount <> v_qty * v_unit_cost THEN
        RAISE EXCEPTION
          'ap_bill_three_way_mismatch: line % amount % <> qty % × unit_cost %',
          v_idx, v_amount, v_qty, v_unit_cost
          USING ERRCODE = 'P0024';
      END IF;

      -- Cumulative qty match against accrued actual_qty.
      SELECT COALESCE(SUM(qty), 0) INTO v_total_billed
        FROM vendor_bill_lines
       WHERE kind = 'disposal_match'
         AND disposal_wo_event_id = v_disp_event_id
         AND by_product_no = v_by_product_no;
      v_avail := v_bp.actual_qty - v_total_billed;
      IF v_qty > v_avail THEN
        RAISE EXCEPTION
          'ap_bill_three_way_mismatch: line % qty % exceeds accrued-not-'
          'billed remainder % for wo_by_products(wo=%,no=%) (accrued=%, '
          'already billed=%)',
          v_idx, v_qty, v_avail, v_disp_wo_id, v_by_product_no,
          v_bp.actual_qty, v_total_billed
          USING ERRCODE = 'P0024';
      END IF;

      -- Resolve the per-vendor accrual liability account.
      SELECT id INTO v_disp_liability FROM accounts
       WHERE kind = 'accrued_disposal_liability'
         AND counterparty_id = p_vendor_id
         AND currency = p_currency
         AND NOT is_closed;
      IF v_disp_liability IS NULL THEN
        RAISE EXCEPTION
          'no open accrued_disposal_liability account for vendor=% ccy=%',
          p_vendor_id, p_currency USING ERRCODE = 'P0010';
      END IF;

      INSERT INTO vendor_bill_lines (
        bill_id, line_no, kind,
        disposal_wo_event_id, by_product_no,
        qty, unit_cost, amount
      ) VALUES (
        v_doc_id, v_idx, 'disposal_match',
        v_disp_event_id, v_by_product_no,
        v_qty, v_unit_cost, v_amount
      ) RETURNING id INTO v_bill_line_id;

      -- Base leg: drain liability at accrued unit price.
      v_amount_at_accrual := v_qty * v_accrued_unit;
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ap_bill',
        'document_kind',     'vendor_bill',
        'document_id',       v_doc_id,
        'document_line_id',  v_bill_line_id,
        'debit_account_id',  v_disp_liability,
        'credit_account_id', v_ven_ap,
        'amount',            v_amount_at_accrual,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_vendor_id,
        'posted_by',         p_posted_by
      ));

      -- Diff leg (variance_match_tolerance, sign-aware against ap).
      v_diff_total := v_amount - v_amount_at_accrual;
      IF v_diff_total <> 0 THEN
        SELECT id INTO v_match_tol_acct FROM accounts
         WHERE kind='variance_match_tolerance' AND ledger_kind='value'
           AND currency=p_currency AND NOT is_closed;
        IF v_match_tol_acct IS NULL THEN
          RAISE EXCEPTION 'no open variance_match_tolerance account for ccy=%',
                          p_currency USING ERRCODE = 'P0010';
        END IF;

        IF v_diff_total > 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'ap_bill',
            'document_kind',     'vendor_bill',
            'document_id',       v_doc_id,
            'document_line_id',  v_bill_line_id,
            'debit_account_id',  v_match_tol_acct,
            'credit_account_id', v_ven_ap,
            'amount',            v_diff_total,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_vendor_id,
            'posted_by',         p_posted_by
          ));
        ELSE
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'ap_bill',
            'document_kind',     'vendor_bill',
            'document_id',       v_doc_id,
            'document_line_id',  v_bill_line_id,
            'debit_account_id',  v_ven_ap,
            'credit_account_id', v_match_tol_acct,
            'amount',            -v_diff_total,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_vendor_id,
            'posted_by',         p_posted_by
          ));
        END IF;
      END IF;

    ELSE
      RAISE EXCEPTION 'ap_bill_invalid_line: line % unknown kind %',
                      v_idx, v_kind USING ERRCODE = 'P0025';
    END IF;
  END LOOP;

  PERFORM post_transfers(v_batch, FALSE);
  RETURN v_doc_id;
END;
$$;

COMMENT ON FUNCTION post_ap_bill(UUID, CHAR, JSONB, DATE, UUID, UUID, TEXT) IS
  'Vendor bill posting. Three line kinds: po_match (clears po_receipt-'
  'staged ap_unsettled → ap), service (direct expense → ap), '
  'disposal_match (drains by-product accrued_disposal_liability → ap, '
  'acct-7t4.5). All three honor vendor.unit_cost_tolerance_pct and '
  'absorb within-tolerance deltas to variance_match_tolerance. Out-of-'
  'tolerance raises P0024.';
