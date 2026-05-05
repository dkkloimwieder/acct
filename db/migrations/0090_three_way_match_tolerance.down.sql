-- Down: drop tolerance columns + restore strict-match function bodies.
-- account_kind value left in place per project convention.

ALTER TABLE customers DROP COLUMN IF EXISTS unit_price_tolerance_pct;
ALTER TABLE vendors   DROP COLUMN IF EXISTS unit_cost_tolerance_pct;

-- Restore mig 0086 post_ap_bill (strict match).
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
  v_existing_id    UUID;
  v_doc_id         UUID;
  v_vendor_check   UUID;
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
  v_total_received BIGINT;
  v_total_billed   BIGINT;
  v_returns_to_us  BIGINT;
  v_avail          BIGINT;
  v_ven_unsettled  BIGINT;
  v_ven_ap         BIGINT;
  v_exp_acct       accounts%ROWTYPE;
  v_bill_line_id   UUID;
  v_batch          JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM vendor_bills
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;
  SELECT id INTO v_vendor_check FROM vendors WHERE id = p_vendor_id;
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
       WHERE pl.id = v_po_line_id FOR UPDATE OF pl;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'ap_bill_invalid_line: line % po_line % not found',
                        v_idx, v_po_line_id USING ERRCODE = 'P0025';
      END IF;
      IF v_pl.vendor_id IS DISTINCT FROM p_vendor_id THEN
        RAISE EXCEPTION 'ap_bill_invalid_line: line % po_line % belongs to vendor % but bill is for vendor %',
                        v_idx, v_po_line_id, v_pl.vendor_id, p_vendor_id USING ERRCODE = 'P0025';
      END IF;
      IF v_pl.currency <> p_currency THEN
        RAISE EXCEPTION 'ap_bill_invalid_line: line % po_line currency=% but bill currency=%',
                        v_idx, v_pl.currency, p_currency USING ERRCODE = 'P0025';
      END IF;
      IF v_unit_cost <> v_pl.unit_cost THEN
        RAISE EXCEPTION 'ap_bill_three_way_mismatch: line % unit_cost % does not match po_line.unit_cost %',
                        v_idx, v_unit_cost, v_pl.unit_cost USING ERRCODE = 'P0024';
      END IF;
      IF v_amount <> v_qty * v_unit_cost THEN
        RAISE EXCEPTION 'ap_bill_three_way_mismatch: line % amount % <> qty % × unit_cost %',
                        v_idx, v_amount, v_qty, v_unit_cost USING ERRCODE = 'P0024';
      END IF;
      SELECT COALESCE(SUM(qty_received), 0) INTO v_total_received
        FROM po_receipt_lines WHERE po_line_id = v_po_line_id;
      SELECT COALESCE(SUM(qty), 0) INTO v_total_billed
        FROM vendor_bill_lines WHERE po_line_id = v_po_line_id AND kind = 'po_match';
      SELECT COALESCE(SUM(prl.qty_to_ap_unsettled), 0) INTO v_returns_to_us
        FROM po_return_lines prl
        JOIN po_receipt_lines rcl ON rcl.id = prl.recv_line_id
       WHERE rcl.po_line_id = v_po_line_id;
      v_avail := v_total_received - v_total_billed - v_returns_to_us;
      IF v_qty > v_avail THEN
        RAISE EXCEPTION 'ap_bill_three_way_mismatch: line % qty % exceeds received-not-billed-not-returned remainder % for po_line % (received=%, already billed=%, prior returns to ap_unsettled=%)',
                        v_idx, v_qty, v_avail, v_po_line_id, v_total_received, v_total_billed, v_returns_to_us
                        USING ERRCODE = 'P0024';
      END IF;
      SELECT id INTO v_ven_unsettled FROM accounts
       WHERE kind='ap_unsettled' AND counterparty_id=p_vendor_id
         AND currency=p_currency AND NOT is_closed;
      IF v_ven_unsettled IS NULL THEN
        RAISE EXCEPTION 'no open ap_unsettled account for vendor=% ccy=%',
                        p_vendor_id, p_currency USING ERRCODE = 'P0010';
      END IF;
      INSERT INTO vendor_bill_lines (bill_id, line_no, kind, po_line_id, qty, unit_cost, amount)
      VALUES (v_doc_id, v_idx, 'po_match', v_po_line_id, v_qty, v_unit_cost, v_amount)
      RETURNING id INTO v_bill_line_id;
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason','ap_bill','document_kind','vendor_bill',
        'document_id',v_doc_id,'document_line_id',v_bill_line_id,
        'debit_account_id',v_ven_unsettled,'credit_account_id',v_ven_ap,
        'amount',v_amount,'business_date',p_business_date,
        'idempotency_key',gen_random_uuid(),
        'counterparty_id',p_vendor_id,'posted_by',p_posted_by
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
                        v_idx, v_expense_acct, v_exp_acct.ledger_kind USING ERRCODE = 'P0025';
      END IF;
      IF v_exp_acct.currency <> p_currency THEN
        RAISE EXCEPTION 'ap_bill_invalid_line: line % expense account ccy=% but bill ccy=%',
                        v_idx, v_exp_acct.currency, p_currency USING ERRCODE = 'P0025';
      END IF;
      INSERT INTO vendor_bill_lines (bill_id, line_no, kind, expense_account_id, amount)
      VALUES (v_doc_id, v_idx, 'service', v_expense_acct, v_amount)
      RETURNING id INTO v_bill_line_id;
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason','ap_bill','document_kind','vendor_bill',
        'document_id',v_doc_id,'document_line_id',v_bill_line_id,
        'debit_account_id',v_expense_acct,'credit_account_id',v_ven_ap,
        'amount',v_amount,'business_date',p_business_date,
        'idempotency_key',gen_random_uuid(),
        'counterparty_id',p_vendor_id,'posted_by',p_posted_by
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

-- Restore mig 0086 post_customer_invoice (strict match).
CREATE OR REPLACE FUNCTION post_customer_invoice(
  p_customer_id     UUID,
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
  v_existing_id     UUID;
  v_doc_id          UUID;
  v_customer_check  UUID;
  v_n               INT;
  v_idx             INT;
  v_line            JSONB;
  v_kind            TEXT;
  v_so_line_id      UUID;
  v_qty             BIGINT;
  v_unit_price      BIGINT;
  v_amount          BIGINT;
  v_tax_amount      BIGINT;
  v_revenue_acct_id BIGINT;
  v_sl              RECORD;
  v_total_shipped   BIGINT;
  v_total_invoiced  BIGINT;
  v_returns_to_us   BIGINT;
  v_avail           BIGINT;
  v_cust_unsettled  BIGINT;
  v_cust_ar         BIGINT;
  v_cust_tax        BIGINT;
  v_rev_acct        accounts%ROWTYPE;
  v_inv_line_id     UUID;
  v_batch           JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM customer_invoices
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;
  SELECT id INTO v_customer_check FROM customers WHERE id = p_customer_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'customer_invoice_invalid_line: customer % not found',
                    p_customer_id USING ERRCODE = 'P0041';
  END IF;
  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'customer_invoice_invalid_line: empty invoice for customer %',
                    p_customer_id USING ERRCODE = 'P0041';
  END IF;
  SELECT id INTO v_cust_ar FROM accounts
   WHERE kind='ar' AND counterparty_id=p_customer_id
     AND currency=p_currency AND NOT is_closed;
  IF v_cust_ar IS NULL THEN
    RAISE EXCEPTION 'no open ar account for customer=% ccy=%',
                    p_customer_id, p_currency USING ERRCODE = 'P0010';
  END IF;
  INSERT INTO customer_invoices (customer_id, currency, business_date, posted_by, idempotency_key, notes)
  VALUES (p_customer_id, p_currency, p_business_date, p_posted_by, p_idempotency_key, p_notes)
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;
  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM customer_invoices WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;
  FOR v_idx IN 1..v_n LOOP
    v_line       := p_lines -> (v_idx - 1);
    v_kind       := v_line->>'kind';
    v_amount     := (v_line->>'amount')::BIGINT;
    v_tax_amount := COALESCE((v_line->>'tax_amount')::BIGINT, 0);
    IF v_kind = 'so_match' THEN
      v_so_line_id := (v_line->>'so_line_id')::UUID;
      v_qty        := (v_line->>'qty')::BIGINT;
      v_unit_price := (v_line->>'unit_price')::BIGINT;
      SELECT sl.so_id, sl.unit_price, sl.currency, so.customer_id
        INTO v_sl
        FROM sales_order_lines sl
        JOIN sales_orders so ON so.id = sl.so_id
       WHERE sl.id = v_so_line_id FOR UPDATE OF sl;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'customer_invoice_invalid_line: line % so_line % not found',
                        v_idx, v_so_line_id USING ERRCODE = 'P0041';
      END IF;
      IF v_sl.customer_id IS DISTINCT FROM p_customer_id THEN
        RAISE EXCEPTION 'customer_invoice_invalid_line: line % so_line % belongs to customer % but invoice is for customer %',
                        v_idx, v_so_line_id, v_sl.customer_id, p_customer_id USING ERRCODE = 'P0041';
      END IF;
      IF v_sl.currency <> p_currency THEN
        RAISE EXCEPTION 'customer_invoice_invalid_line: line % so_line currency=% but invoice currency=%',
                        v_idx, v_sl.currency, p_currency USING ERRCODE = 'P0041';
      END IF;
      IF v_unit_price <> v_sl.unit_price THEN
        RAISE EXCEPTION 'customer_invoice_three_way_mismatch: line % unit_price % does not match so_line.unit_price %',
                        v_idx, v_unit_price, v_sl.unit_price USING ERRCODE = 'P0040';
      END IF;
      IF v_amount <> v_qty * v_unit_price THEN
        RAISE EXCEPTION 'customer_invoice_three_way_mismatch: line % amount % <> qty % × unit_price %',
                        v_idx, v_amount, v_qty, v_unit_price USING ERRCODE = 'P0040';
      END IF;
      SELECT COALESCE(SUM(qty_shipped), 0) INTO v_total_shipped
        FROM so_shipment_lines WHERE so_line_id = v_so_line_id;
      SELECT COALESCE(SUM(qty), 0) INTO v_total_invoiced
        FROM customer_invoice_lines WHERE so_line_id = v_so_line_id AND kind = 'so_match';
      SELECT COALESCE(SUM(crl.qty_to_ar_unsettled), 0) INTO v_returns_to_us
        FROM customer_return_lines crl
        JOIN so_shipment_lines     ssl ON ssl.id = crl.ship_line_id
       WHERE ssl.so_line_id = v_so_line_id;
      v_avail := v_total_shipped - v_total_invoiced - v_returns_to_us;
      IF v_qty > v_avail THEN
        RAISE EXCEPTION 'customer_invoice_three_way_mismatch: line % qty % exceeds shipped-not-invoiced-not-returned remainder % for so_line % (shipped=%, already invoiced=%, prior returns to ar_unsettled=%)',
                        v_idx, v_qty, v_avail, v_so_line_id, v_total_shipped, v_total_invoiced, v_returns_to_us
                        USING ERRCODE = 'P0040';
      END IF;
      SELECT id INTO v_cust_unsettled FROM accounts
       WHERE kind='ar_unsettled' AND counterparty_id=p_customer_id
         AND currency=p_currency AND NOT is_closed;
      IF v_cust_unsettled IS NULL THEN
        RAISE EXCEPTION 'no open ar_unsettled account for customer=% ccy=%',
                        p_customer_id, p_currency USING ERRCODE = 'P0010';
      END IF;
      INSERT INTO customer_invoice_lines (invoice_id, line_no, kind, so_line_id, qty, unit_price, amount, tax_amount)
      VALUES (v_doc_id, v_idx, 'so_match', v_so_line_id, v_qty, v_unit_price, v_amount, v_tax_amount)
      RETURNING id INTO v_inv_line_id;
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason','ar_invoice','document_kind','customer_invoice',
        'document_id',v_doc_id,'document_line_id',v_inv_line_id,
        'debit_account_id',v_cust_ar,'credit_account_id',v_cust_unsettled,
        'amount',v_amount,'business_date',p_business_date,
        'idempotency_key',gen_random_uuid(),
        'counterparty_id',p_customer_id,'posted_by',p_posted_by
      ));
    ELSIF v_kind = 'service' THEN
      v_revenue_acct_id := (v_line->>'revenue_account_id')::BIGINT;
      SELECT * INTO v_rev_acct FROM accounts WHERE id = v_revenue_acct_id;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'customer_invoice_invalid_line: line % revenue_account_id % not found',
                        v_idx, v_revenue_acct_id USING ERRCODE = 'P0041';
      END IF;
      IF v_rev_acct.is_closed THEN
        RAISE EXCEPTION 'customer_invoice_invalid_line: line % revenue account % is closed',
                        v_idx, v_revenue_acct_id USING ERRCODE = 'P0041';
      END IF;
      IF v_rev_acct.ledger_kind <> 'value' THEN
        RAISE EXCEPTION 'customer_invoice_invalid_line: line % revenue account % is %, expected value',
                        v_idx, v_revenue_acct_id, v_rev_acct.ledger_kind USING ERRCODE = 'P0041';
      END IF;
      IF v_rev_acct.currency <> p_currency THEN
        RAISE EXCEPTION 'customer_invoice_invalid_line: line % revenue account ccy=% but invoice ccy=%',
                        v_idx, v_rev_acct.currency, p_currency USING ERRCODE = 'P0041';
      END IF;
      INSERT INTO customer_invoice_lines (invoice_id, line_no, kind, revenue_account_id, amount, tax_amount)
      VALUES (v_doc_id, v_idx, 'service', v_revenue_acct_id, v_amount, v_tax_amount)
      RETURNING id INTO v_inv_line_id;
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason','ar_invoice','document_kind','customer_invoice',
        'document_id',v_doc_id,'document_line_id',v_inv_line_id,
        'debit_account_id',v_cust_ar,'credit_account_id',v_revenue_acct_id,
        'amount',v_amount,'business_date',p_business_date,
        'idempotency_key',gen_random_uuid(),
        'counterparty_id',p_customer_id,'posted_by',p_posted_by
      ));
      IF v_tax_amount > 0 THEN
        SELECT id INTO v_cust_tax FROM accounts
         WHERE kind='sales_tax_payable' AND ledger_kind='value'
           AND currency=p_currency AND NOT is_closed;
        IF v_cust_tax IS NULL THEN
          RAISE EXCEPTION 'no open sales_tax_payable account for ccy=%',
                          p_currency USING ERRCODE = 'P0010';
        END IF;
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason','ar_invoice','document_kind','customer_invoice',
          'document_id',v_doc_id,'document_line_id',v_inv_line_id,
          'debit_account_id',v_cust_ar,'credit_account_id',v_cust_tax,
          'amount',v_tax_amount,'business_date',p_business_date,
          'idempotency_key',gen_random_uuid(),
          'counterparty_id',p_customer_id,'posted_by',p_posted_by
        ));
      END IF;
    ELSE
      RAISE EXCEPTION 'customer_invoice_invalid_line: line % unknown kind %',
                      v_idx, v_kind USING ERRCODE = 'P0041';
    END IF;
  END LOOP;
  PERFORM post_transfers(v_batch, FALSE);
  RETURN v_doc_id;
END;
$$;
