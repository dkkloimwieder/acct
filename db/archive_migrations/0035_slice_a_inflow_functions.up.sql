-- acct-7mg / Slice A.1 Part B (functions) — post_po_receipt and
-- post_ap_bill.
--
-- post_po_receipt: receipt of goods against an open PO. For each
-- line: posts qty (stock_available DR / supplier_pool CR), value
-- (inv_value_raw DR / ap_unsettled CR), and for standard SKUs with
-- PPV a third event through variance_ppv. WAC SKUs (perpetual /
-- periodic / retroactive) post at po_unit_cost; the pool re-averages
-- naturally.
--
-- post_ap_bill: vendor bill, two line modes:
--   * po_match: ap_unsettled DR / ap CR. Validates strict three-way
--     match — qty ≤ received-not-yet-billed remainder for this
--     po_line; unit_cost equals po_line.unit_cost; line.amount =
--     qty × unit_cost. Multiple bill lines in the same batch
--     targeting the same po_line are processed in order; later lines
--     see earlier inserts via READ COMMITTED.
--   * service: caller-supplied expense_account_id DR / ap CR. No
--     PO reference; covers utilities, professional services, etc.
--     Caller-supplied taxonomy (acct-063 follow-up).
--
-- Mixed bills (po_match + service in one document) supported.
--
-- Error codes (all new in Slice A):
--   P0022 — po_receipt_invalid: PO missing, supplier missing on PO,
--           po_line missing, po_line not part of PO.
--   P0023 — po_line_overreceived: cumulative qty_received would
--           exceed qty_ordered. Strict for Phase 1; tolerance is
--           Phase 2.
--   P0024 — ap_bill_three_way_mismatch: qty over received-not-billed,
--           unit_cost diverges from po_line, or amount diverges from
--           qty × unit_cost.
--   P0025 — ap_bill_invalid_line: service line missing
--           expense_account_id, expense account is closed / wrong
--           ledger / wrong currency, po_match line wrong supplier,
--           etc. (CHECK at table layer catches the table-level
--           violations; this code surfaces semantic violations.)
--
-- Existing P0010 used for "no open <kind> account configured for
-- this (supplier|sku|currency)" — same pattern as inventory_adjust.

-- ============================================================
-- post_po_receipt
-- ============================================================

CREATE OR REPLACE FUNCTION post_po_receipt(
  p_po_id           UUID,
  p_lines           JSONB,
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
  v_supplier_id   UUID;
  v_n             INT;
  v_idx           INT;
  v_line          JSONB;
  v_po_line_id    UUID;
  v_qty_received  BIGINT;
  v_pl            RECORD;
  v_already_recv  BIGINT;
  v_cost_method   cost_method;
  v_std_cost      BIGINT;
  v_qty_acct      BIGINT;
  v_val_acct      BIGINT;
  v_sup_qty       BIGINT;
  v_sup_val       BIGINT;
  v_var_acct      BIGINT;
  v_val_unit      BIGINT;
  v_val_amount    BIGINT;
  v_ppv_amount    BIGINT;
  v_recv_line_id  UUID;
  v_batch         JSONB := '[]'::JSONB;
BEGIN
  -- Fast-path replay.
  SELECT id INTO v_existing_id FROM po_receipts
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  -- PO + supplier.
  SELECT supplier_id INTO v_supplier_id FROM purchase_orders WHERE id = p_po_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'po_receipt_invalid: PO % not found', p_po_id
      USING ERRCODE = 'P0022';
  END IF;
  IF v_supplier_id IS NULL THEN
    RAISE EXCEPTION 'po_receipt_invalid: PO % has no supplier_id', p_po_id
      USING ERRCODE = 'P0022';
  END IF;

  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'po_receipt_invalid: empty lines for PO %', p_po_id
      USING ERRCODE = 'P0022';
  END IF;

  -- Reserve doc id by inserting receipt header.
  INSERT INTO po_receipts (po_id, business_date, posted_by, idempotency_key, notes)
  VALUES (p_po_id, p_business_date, p_posted_by, p_idempotency_key, p_notes)
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM po_receipts WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  FOR v_idx IN 1..v_n LOOP
    v_line         := p_lines -> (v_idx - 1);
    v_po_line_id   := (v_line->>'po_line_id')::UUID;
    v_qty_received := (v_line->>'qty_received')::BIGINT;

    IF v_qty_received IS NULL OR v_qty_received <= 0 THEN
      RAISE EXCEPTION 'po_receipt_invalid: line % qty_received must be > 0',
                      v_idx USING ERRCODE = 'P0022';
    END IF;

    -- PO line + ownership check.
    SELECT po_id, sku_id, location_id, qty_ordered, unit_cost, currency
      INTO v_pl
      FROM purchase_order_lines WHERE id = v_po_line_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'po_receipt_invalid: po_line % not found', v_po_line_id
        USING ERRCODE = 'P0022';
    END IF;
    IF v_pl.po_id <> p_po_id THEN
      RAISE EXCEPTION 'po_receipt_invalid: po_line % belongs to PO % not %',
                      v_po_line_id, v_pl.po_id, p_po_id
        USING ERRCODE = 'P0022';
    END IF;

    -- Strict over-receipt gate.
    SELECT COALESCE(SUM(qty_received), 0) INTO v_already_recv
      FROM po_receipt_lines WHERE po_line_id = v_po_line_id;
    IF v_already_recv + v_qty_received > v_pl.qty_ordered THEN
      RAISE EXCEPTION
        'po_line_overreceived: po_line %: ordered=%, already received=%, '
        'this receipt=%; cumulative would exceed qty_ordered',
        v_po_line_id, v_pl.qty_ordered, v_already_recv, v_qty_received
        USING ERRCODE = 'P0023';
    END IF;

    -- Cost method on SKU drives value-leg + PPV decisions.
    SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_pl.sku_id;

    IF v_cost_method IN ('fifo', 'lot') THEN
      RAISE EXCEPTION
        'cost_method_not_implemented: % for po_receipt (sku=%); see acct-8gg',
        v_cost_method, v_pl.sku_id USING ERRCODE = 'P0006';
    END IF;

    -- Resolve accounts.
    SELECT id INTO v_qty_acct FROM accounts
     WHERE kind='stock_available' AND sku_id=v_pl.sku_id
       AND location_id=v_pl.location_id AND NOT is_closed;
    IF v_qty_acct IS NULL THEN
      RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                      v_pl.sku_id, v_pl.location_id USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_val_acct FROM accounts
     WHERE kind='inv_value_raw' AND sku_id=v_pl.sku_id
       AND location_id=v_pl.location_id
       AND currency=v_pl.currency AND NOT is_closed;
    IF v_val_acct IS NULL THEN
      RAISE EXCEPTION 'no open inv_value_raw account for sku=% loc=% ccy=%',
                      v_pl.sku_id, v_pl.location_id, v_pl.currency
        USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_sup_qty FROM accounts
     WHERE kind='supplier_pool' AND counterparty_id=v_supplier_id
       AND ledger_kind='qty' AND NOT is_closed;
    IF v_sup_qty IS NULL THEN
      RAISE EXCEPTION 'no open supplier_pool(qty) account for supplier=%',
                      v_supplier_id USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_sup_val FROM accounts
     WHERE kind='ap_unsettled' AND counterparty_id=v_supplier_id
       AND currency=v_pl.currency AND NOT is_closed;
    IF v_sup_val IS NULL THEN
      RAISE EXCEPTION 'no open ap_unsettled account for supplier=% ccy=%',
                      v_supplier_id, v_pl.currency USING ERRCODE = 'P0010';
    END IF;

    -- Value computation. Standard SKU values inventory at standard cost
    -- and routes (po-std)*qty through variance_ppv. WAC SKUs value at
    -- po_unit_cost; pool re-averages.
    IF v_cost_method = 'standard' THEN
      v_std_cost   := resolve_standard_cost_at(v_pl.sku_id, p_business_date);
      v_val_unit   := v_std_cost;
      v_val_amount := v_qty_received * v_std_cost;
      v_ppv_amount := v_qty_received * (v_pl.unit_cost - v_std_cost);
    ELSE
      v_val_unit   := v_pl.unit_cost;
      v_val_amount := v_qty_received * v_pl.unit_cost;
      v_ppv_amount := 0;
    END IF;

    IF v_ppv_amount <> 0 THEN
      SELECT id INTO v_var_acct FROM accounts
       WHERE kind='variance_ppv' AND ledger_kind='value'
         AND currency=v_pl.currency AND NOT is_closed;
      IF v_var_acct IS NULL THEN
        RAISE EXCEPTION 'no open variance_ppv account for ccy=%',
                        v_pl.currency USING ERRCODE = 'P0010';
      END IF;
    END IF;

    -- INSERT receipt-line first so events can carry document_line_id.
    INSERT INTO po_receipt_lines (receipt_id, po_line_id, qty_received)
    VALUES (v_doc_id, v_po_line_id, v_qty_received)
    RETURNING id INTO v_recv_line_id;

    -- Event 1: qty leg.
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'po_receipt',
      'document_kind',     'po_receipt',
      'document_id',       v_doc_id,
      'document_line_id',  v_recv_line_id,
      'debit_account_id',  v_qty_acct,
      'credit_account_id', v_sup_qty,
      'amount',            v_qty_received,
      'qty',               v_qty_received,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   v_supplier_id,
      'posted_by',         p_posted_by
    ));

    -- Event 2: value leg.
    IF v_val_amount > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'po_receipt',
        'document_kind',     'po_receipt',
        'document_id',       v_doc_id,
        'document_line_id',  v_recv_line_id,
        'debit_account_id',  v_val_acct,
        'credit_account_id', v_sup_val,
        'amount',            v_val_amount,
        'qty',               v_qty_received,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   v_supplier_id,
        'posted_by',         p_posted_by
      ));
    END IF;

    -- Event 3: PPV (only standard SKU with non-zero variance).
    IF v_ppv_amount > 0 THEN
      -- Unfavorable: po > std. variance_ppv DR / ap_unsettled CR.
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ppv',
        'document_kind',     'po_receipt',
        'document_id',       v_doc_id,
        'document_line_id',  v_recv_line_id,
        'debit_account_id',  v_var_acct,
        'credit_account_id', v_sup_val,
        'amount',            v_ppv_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   v_supplier_id,
        'posted_by',         p_posted_by
      ));
    ELSIF v_ppv_amount < 0 THEN
      -- Favorable: po < std. ap_unsettled DR / variance_ppv CR.
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ppv',
        'document_kind',     'po_receipt',
        'document_id',       v_doc_id,
        'document_line_id',  v_recv_line_id,
        'debit_account_id',  v_sup_val,
        'credit_account_id', v_var_acct,
        'amount',            -v_ppv_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   v_supplier_id,
        'posted_by',         p_posted_by
      ));
    END IF;
  END LOOP;

  PERFORM post_transfers(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;

COMMENT ON FUNCTION post_po_receipt(UUID, JSONB, DATE, UUID, UUID, TEXT) IS
  'Receipt of goods against a PO. Per line: posts qty (stock_available '
  'DR / supplier_pool CR), value (inv_value_raw DR / ap_unsettled CR), '
  'and PPV for standard SKUs with std≠po. WAC SKUs post at po_unit_cost. '
  'Strict over-receipt rejection (P0023). GRNI semantics: ap_unsettled '
  'is credited, not ap (cleared by post_ap_bill).';

-- ============================================================
-- post_ap_bill
-- ============================================================

CREATE OR REPLACE FUNCTION post_ap_bill(
  p_supplier_id     UUID,
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
  v_existing_id    UUID;
  v_doc_id         UUID;
  v_supplier_check UUID;
  v_n              INT;
  v_idx            INT;
  v_line           JSONB;
  v_kind           TEXT;
  v_po_line_id     UUID;
  v_qty            BIGINT;
  v_unit_cost      BIGINT;
  v_amount         BIGINT;
  v_expense_acct   BIGINT;
  v_pl             RECORD;
  v_pl_po_supplier UUID;
  v_total_received BIGINT;
  v_total_billed   BIGINT;
  v_avail          BIGINT;
  v_sup_unsettled  BIGINT;
  v_sup_ap         BIGINT;
  v_exp_acct       accounts%ROWTYPE;
  v_bill_line_id   UUID;
  v_batch          JSONB := '[]'::JSONB;
BEGIN
  -- Fast-path replay.
  SELECT id INTO v_existing_id FROM vendor_bills
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  -- Supplier exists.
  SELECT id INTO v_supplier_check FROM suppliers WHERE id = p_supplier_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'ap_bill_invalid_line: supplier % not found', p_supplier_id
      USING ERRCODE = 'P0025';
  END IF;

  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'ap_bill_invalid_line: empty bill for supplier %', p_supplier_id
      USING ERRCODE = 'P0025';
  END IF;

  -- Resolve supplier ap (one bill = one supplier = one ap account).
  SELECT id INTO v_sup_ap FROM accounts
   WHERE kind='ap' AND counterparty_id=p_supplier_id
     AND currency=p_currency AND NOT is_closed;
  IF v_sup_ap IS NULL THEN
    RAISE EXCEPTION 'no open ap account for supplier=% ccy=%',
                    p_supplier_id, p_currency USING ERRCODE = 'P0010';
  END IF;

  -- Reserve doc id.
  INSERT INTO vendor_bills (
    supplier_id, currency, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_supplier_id, p_currency, p_business_date, p_posted_by,
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

      -- Look up po_line + verify PO supplier matches bill supplier.
      SELECT pl.po_id, pl.unit_cost, pl.currency, po.supplier_id
        INTO v_pl
        FROM purchase_order_lines pl
        JOIN purchase_orders po ON po.id = pl.po_id
       WHERE pl.id = v_po_line_id;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'ap_bill_invalid_line: line % po_line % not found',
                        v_idx, v_po_line_id USING ERRCODE = 'P0025';
      END IF;
      IF v_pl.supplier_id IS DISTINCT FROM p_supplier_id THEN
        RAISE EXCEPTION
          'ap_bill_invalid_line: line % po_line % belongs to supplier % '
          'but bill is for supplier %',
          v_idx, v_po_line_id, v_pl.supplier_id, p_supplier_id
          USING ERRCODE = 'P0025';
      END IF;
      IF v_pl.currency <> p_currency THEN
        RAISE EXCEPTION
          'ap_bill_invalid_line: line % po_line currency=% but bill currency=%',
          v_idx, v_pl.currency, p_currency USING ERRCODE = 'P0025';
      END IF;

      -- Three-way match: strict equality on unit_cost.
      IF v_unit_cost <> v_pl.unit_cost THEN
        RAISE EXCEPTION
          'ap_bill_three_way_mismatch: line % unit_cost % does not match '
          'po_line.unit_cost %',
          v_idx, v_unit_cost, v_pl.unit_cost
          USING ERRCODE = 'P0024';
      END IF;
      -- Three-way match: strict equality on amount = qty × unit_cost.
      IF v_amount <> v_qty * v_unit_cost THEN
        RAISE EXCEPTION
          'ap_bill_three_way_mismatch: line % amount % <> qty % × unit_cost %',
          v_idx, v_amount, v_qty, v_unit_cost
          USING ERRCODE = 'P0024';
      END IF;

      -- Three-way match: cumulative billed ≤ cumulative received per
      -- po_line. Counts vendor_bill_lines from prior batches AND
      -- prior iterations of this batch (visible via READ COMMITTED
      -- after our INSERTs below).
      SELECT COALESCE(SUM(qty_received), 0) INTO v_total_received
        FROM po_receipt_lines WHERE po_line_id = v_po_line_id;
      SELECT COALESCE(SUM(qty), 0) INTO v_total_billed
        FROM vendor_bill_lines
       WHERE po_line_id = v_po_line_id AND kind = 'po_match';
      v_avail := v_total_received - v_total_billed;
      IF v_qty > v_avail THEN
        RAISE EXCEPTION
          'ap_bill_three_way_mismatch: line % qty % exceeds received-not-'
          'billed remainder % for po_line % (received=%, already billed=%)',
          v_idx, v_qty, v_avail, v_po_line_id, v_total_received, v_total_billed
          USING ERRCODE = 'P0024';
      END IF;

      -- ap_unsettled side.
      SELECT id INTO v_sup_unsettled FROM accounts
       WHERE kind='ap_unsettled' AND counterparty_id=p_supplier_id
         AND currency=p_currency AND NOT is_closed;
      IF v_sup_unsettled IS NULL THEN
        RAISE EXCEPTION 'no open ap_unsettled account for supplier=% ccy=%',
                        p_supplier_id, p_currency USING ERRCODE = 'P0010';
      END IF;

      INSERT INTO vendor_bill_lines (
        bill_id, line_no, kind, po_line_id, qty, unit_cost, amount
      ) VALUES (
        v_doc_id, v_idx, 'po_match', v_po_line_id, v_qty, v_unit_cost, v_amount
      ) RETURNING id INTO v_bill_line_id;

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ap_bill',
        'document_kind',     'vendor_bill',
        'document_id',       v_doc_id,
        'document_line_id',  v_bill_line_id,
        'debit_account_id',  v_sup_unsettled,
        'credit_account_id', v_sup_ap,
        'amount',            v_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_supplier_id,
        'posted_by',         p_posted_by
      ));

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
        'credit_account_id', v_sup_ap,
        'amount',            v_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_supplier_id,
        'posted_by',         p_posted_by
      ));

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
  'Vendor bill. po_match lines clear ap_unsettled → ap with strict '
  'three-way match (qty within received-not-billed remainder, '
  'unit_cost matches po_line, amount matches qty × unit_cost). '
  'service lines post caller-supplied expense_account → ap. Mixed '
  'po_match + service in one bill is supported.';
