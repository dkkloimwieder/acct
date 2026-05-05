-- Down: revert post_po_return / post_customer_return to mig 0084/0085 shape;
-- restore post_ap_bill / post_customer_invoice to mig 0076/0081 shape;
-- drop split-tracking columns and check constraints.

DROP FUNCTION IF EXISTS post_customer_return(UUID, JSONB, DATE, UUID, UUID, TEXT, BOOLEAN);
DROP FUNCTION IF EXISTS post_po_return(UUID, JSONB, DATE, UUID, UUID, TEXT, BOOLEAN);

ALTER TABLE customer_return_lines DROP CONSTRAINT IF EXISTS customer_return_lines_split_check;
ALTER TABLE customer_return_lines DROP COLUMN IF EXISTS qty_to_ar;
ALTER TABLE customer_return_lines DROP COLUMN IF EXISTS qty_to_ar_unsettled;

ALTER TABLE po_return_lines DROP CONSTRAINT IF EXISTS po_return_lines_split_check;
ALTER TABLE po_return_lines DROP COLUMN IF EXISTS qty_to_ap;
ALTER TABLE po_return_lines DROP COLUMN IF EXISTS qty_to_ap_unsettled;

-- Restore mig 0085 post_po_return.
CREATE OR REPLACE FUNCTION post_po_return(
  p_vendor_id       UUID,
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
  v_n                INT;
  v_idx              INT;
  v_line             JSONB;
  v_recv_line_id     UUID;
  v_qty_returned     BIGINT;
  v_pl               RECORD;
  v_already_returned BIGINT;
  v_remaining        BIGINT;
  v_cost_method      cost_method;
  v_inv_unit         BIGINT;
  v_inv_amount       BIGINT;
  v_ap_amount        BIGINT;
  v_ppv_amount       BIGINT;
  v_qty_acct         BIGINT;
  v_val_acct         BIGINT;
  v_ven_qty          BIGINT;
  v_ven_ap           BIGINT;
  v_var_acct         BIGINT;
  v_return_line_id   UUID;
  v_batch            JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM po_returns
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT id INTO v_vendor_check FROM vendors WHERE id = p_vendor_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'po_return_invalid: vendor % not found', p_vendor_id
      USING ERRCODE = 'P0046';
  END IF;

  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'po_return_invalid: empty lines for vendor %',
                    p_vendor_id USING ERRCODE = 'P0046';
  END IF;

  INSERT INTO po_returns (vendor_id, business_date, posted_by, idempotency_key, notes)
  VALUES (p_vendor_id, p_business_date, p_posted_by, p_idempotency_key, p_notes)
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM po_returns WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  FOR v_idx IN 1..v_n LOOP
    v_line         := p_lines -> (v_idx - 1);
    v_recv_line_id := (v_line->>'recv_line_id')::UUID;
    v_qty_returned := (v_line->>'qty_returned')::BIGINT;

    IF v_qty_returned IS NULL OR v_qty_returned <= 0 THEN
      RAISE EXCEPTION 'po_return_invalid: line % qty_returned must be > 0',
                      v_idx USING ERRCODE = 'P0046';
    END IF;

    SELECT
      prl.id           AS recv_line_id,
      prl.qty_received AS qty_received,
      pol.sku_id       AS sku_id,
      pol.location_id  AS location_id,
      pol.unit_cost    AS unit_cost,
      pol.currency     AS currency,
      po.vendor_id     AS vendor_id
    INTO v_pl
    FROM po_receipt_lines prl
    JOIN purchase_order_lines pol ON pol.id = prl.po_line_id
    JOIN po_receipts          pr  ON pr.id  = prl.receipt_id
    JOIN purchase_orders      po  ON po.id  = pr.po_id
    WHERE prl.id = v_recv_line_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'po_return_invalid: recv_line % not found',
                      v_recv_line_id USING ERRCODE = 'P0046';
    END IF;
    IF v_pl.vendor_id <> p_vendor_id THEN
      RAISE EXCEPTION
        'po_return_invalid: recv_line % belongs to vendor %, not %',
        v_recv_line_id, v_pl.vendor_id, p_vendor_id
        USING ERRCODE = 'P0046';
    END IF;

    SELECT COALESCE(SUM(prl.qty_returned), 0)
      INTO v_already_returned
      FROM po_return_lines prl
      JOIN po_returns      pr ON pr.id = prl.return_id
     WHERE prl.recv_line_id = v_recv_line_id
       AND pr.id <> v_doc_id;

    v_remaining := v_pl.qty_received - v_already_returned;
    IF v_qty_returned > v_remaining THEN
      RAISE EXCEPTION
        'po_return_overreturned: recv_line % received=% already_returned=% requested=% remaining=%',
        v_recv_line_id, v_pl.qty_received, v_already_returned, v_qty_returned, v_remaining
        USING ERRCODE = 'P0047';
    END IF;

    SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_pl.sku_id;
    IF v_cost_method = 'standard' THEN
      v_inv_unit := resolve_standard_cost_at(v_pl.sku_id, p_business_date);
    ELSIF v_cost_method IN ('wac_perpetual', 'wac_periodic', 'wac_retroactive') THEN
      v_inv_unit := v_pl.unit_cost;
    ELSE
      RAISE EXCEPTION 'cost_method_not_implemented: % for po_return',
                      v_cost_method USING ERRCODE = 'P0006';
    END IF;

    v_inv_amount := v_qty_returned * v_inv_unit;
    v_ap_amount  := v_qty_returned * v_pl.unit_cost;
    v_ppv_amount := v_ap_amount - v_inv_amount;

    SELECT id INTO v_qty_acct FROM accounts
     WHERE kind='stock_available' AND sku_id=v_pl.sku_id
       AND location_id=v_pl.location_id AND NOT is_closed;
    IF v_qty_acct IS NULL THEN
      RAISE EXCEPTION 'no open stock_available for sku=% loc=%',
                      v_pl.sku_id, v_pl.location_id USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_val_acct FROM accounts
     WHERE kind='inv_value_raw' AND sku_id=v_pl.sku_id
       AND location_id=v_pl.location_id AND currency=v_pl.currency
       AND NOT is_closed;
    IF v_val_acct IS NULL THEN
      RAISE EXCEPTION 'no open inv_value_raw for sku=% loc=% ccy=%',
                      v_pl.sku_id, v_pl.location_id, v_pl.currency
        USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_ven_qty FROM accounts
     WHERE kind='vendor_pool' AND counterparty_id=p_vendor_id
       AND NOT is_closed;
    IF v_ven_qty IS NULL THEN
      SELECT id INTO v_ven_qty FROM accounts
       WHERE kind='vendor_pool' AND counterparty_id IS NULL
         AND NOT is_closed;
    END IF;
    IF v_ven_qty IS NULL THEN
      RAISE EXCEPTION 'no open vendor_pool for vendor=%',
                      p_vendor_id USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_ven_ap FROM accounts
     WHERE kind='ap' AND counterparty_id=p_vendor_id
       AND currency=v_pl.currency AND NOT is_closed;
    IF v_ven_ap IS NULL THEN
      RAISE EXCEPTION 'no open ap for vendor=% ccy=%',
                      p_vendor_id, v_pl.currency USING ERRCODE = 'P0010';
    END IF;

    IF v_ppv_amount <> 0 THEN
      SELECT id INTO v_var_acct FROM accounts
       WHERE kind='variance_ppv' AND currency=v_pl.currency AND NOT is_closed;
      IF v_var_acct IS NULL THEN
        RAISE EXCEPTION 'no open variance_ppv for ccy=%',
                        v_pl.currency USING ERRCODE = 'P0010';
      END IF;
    END IF;

    INSERT INTO po_return_lines (
      return_id, line_no, recv_line_id, qty_returned, unit_cost
    ) VALUES (
      v_doc_id, v_idx, v_recv_line_id, v_qty_returned, v_pl.unit_cost
    )
    RETURNING id INTO v_return_line_id;

    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'po_return_to_vendor',
      'document_kind',     'po_return',
      'document_id',       v_doc_id,
      'document_line_id',  v_return_line_id,
      'debit_account_id',  v_ven_qty,
      'credit_account_id', v_qty_acct,
      'amount',            v_qty_returned,
      'qty',               v_qty_returned,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   p_vendor_id,
      'posted_by',         p_posted_by
    ));

    IF v_ppv_amount > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ppv',
        'document_kind',     'po_return',
        'document_id',       v_doc_id,
        'document_line_id',  v_return_line_id,
        'debit_account_id',  v_ven_ap,
        'credit_account_id', v_var_acct,
        'amount',            v_ppv_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_vendor_id,
        'posted_by',         p_posted_by
      ));
    ELSIF v_ppv_amount < 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ppv',
        'document_kind',     'po_return',
        'document_id',       v_doc_id,
        'document_line_id',  v_return_line_id,
        'debit_account_id',  v_var_acct,
        'credit_account_id', v_ven_ap,
        'amount',            -v_ppv_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_vendor_id,
        'posted_by',         p_posted_by
      ));
    END IF;

    IF v_inv_amount > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'po_return_to_vendor',
        'document_kind',     'po_return',
        'document_id',       v_doc_id,
        'document_line_id',  v_return_line_id,
        'debit_account_id',  v_ven_ap,
        'credit_account_id', v_val_acct,
        'amount',            v_inv_amount,
        'qty',               v_qty_returned,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_vendor_id,
        'posted_by',         p_posted_by
      ));
    END IF;
  END LOOP;

  PERFORM post_transfers(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;

-- Restore mig 0084 post_customer_return.
CREATE OR REPLACE FUNCTION post_customer_return(
  p_customer_id     UUID,
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
  v_customer_check   UUID;
  v_n                INT;
  v_idx              INT;
  v_line             JSONB;
  v_ship_line_id     UUID;
  v_qty_returned     BIGINT;
  v_disposition      return_disposition;
  v_sl               RECORD;
  v_already_returned BIGINT;
  v_remaining        BIGINT;
  v_tax_pro          BIGINT;
  v_qty_dr_acct      BIGINT;
  v_val_dr_acct      BIGINT;
  v_cust_qty         BIGINT;
  v_cust_unsettled   BIGINT;
  v_cogs_acct        BIGINT;
  v_revenue_acct     BIGINT;
  v_tax_acct         BIGINT;
  v_cust_ar          BIGINT;
  v_var_scrap        BIGINT;
  v_return_line_id   UUID;
  v_batch            JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM customer_returns
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT id INTO v_customer_check FROM customers WHERE id = p_customer_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'customer_return_invalid: customer % not found', p_customer_id
      USING ERRCODE = 'P0044';
  END IF;

  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'customer_return_invalid: empty lines for customer %',
                    p_customer_id USING ERRCODE = 'P0044';
  END IF;

  INSERT INTO customer_returns (customer_id, business_date, posted_by, idempotency_key, notes)
  VALUES (p_customer_id, p_business_date, p_posted_by, p_idempotency_key, p_notes)
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM customer_returns WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  FOR v_idx IN 1..v_n LOOP
    v_line         := p_lines -> (v_idx - 1);
    v_ship_line_id := (v_line->>'ship_line_id')::UUID;
    v_qty_returned := (v_line->>'qty_returned')::BIGINT;
    v_disposition  := (v_line->>'disposition')::return_disposition;

    IF v_qty_returned IS NULL OR v_qty_returned <= 0 THEN
      RAISE EXCEPTION 'customer_return_invalid: line % qty_returned must be > 0',
                      v_idx USING ERRCODE = 'P0044';
    END IF;

    SELECT
      ssl.id            AS ship_line_id,
      ssl.qty_shipped   AS qty_shipped,
      ssl.unit_cost     AS unit_cost,
      ssl.unit_price    AS unit_price,
      ssl.tax_amount    AS tax_amount,
      sl.sku_id         AS sku_id,
      sl.ship_location_id AS ship_location_id,
      sl.currency       AS currency,
      so.customer_id    AS customer_id
    INTO v_sl
    FROM so_shipment_lines ssl
    JOIN sales_order_lines sl ON sl.id = ssl.so_line_id
    JOIN so_shipments      ss ON ss.id = ssl.shipment_id
    JOIN sales_orders      so ON so.id = ss.so_id
    WHERE ssl.id = v_ship_line_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'customer_return_invalid: ship_line % not found',
                      v_ship_line_id USING ERRCODE = 'P0044';
    END IF;
    IF v_sl.customer_id <> p_customer_id THEN
      RAISE EXCEPTION
        'customer_return_invalid: ship_line % belongs to customer %, not %',
        v_ship_line_id, v_sl.customer_id, p_customer_id
        USING ERRCODE = 'P0044';
    END IF;

    SELECT COALESCE(SUM(crl.qty_returned), 0)
      INTO v_already_returned
      FROM customer_return_lines crl
      JOIN customer_returns cr ON cr.id = crl.return_id
     WHERE crl.ship_line_id = v_ship_line_id
       AND cr.id <> v_doc_id;

    v_remaining := v_sl.qty_shipped - v_already_returned;
    IF v_qty_returned > v_remaining THEN
      RAISE EXCEPTION
        'customer_return_overreturned: ship_line % shipped=% already_returned=% requested=% remaining=%',
        v_ship_line_id, v_sl.qty_shipped, v_already_returned, v_qty_returned, v_remaining
        USING ERRCODE = 'P0045';
    END IF;

    IF v_sl.tax_amount > 0 AND v_sl.qty_shipped > 0 THEN
      v_tax_pro := (v_sl.tax_amount * v_qty_returned) / v_sl.qty_shipped;
    ELSE
      v_tax_pro := 0;
    END IF;

    IF v_disposition = 'restock' THEN
      SELECT id INTO v_qty_dr_acct FROM accounts
       WHERE kind='stock_available' AND sku_id=v_sl.sku_id
         AND location_id=v_sl.ship_location_id AND NOT is_closed;
    ELSIF v_disposition = 'scrap' THEN
      SELECT id INTO v_qty_dr_acct FROM accounts
       WHERE kind='stock_scrap' AND sku_id=v_sl.sku_id AND NOT is_closed;
    ELSIF v_disposition = 'repair' THEN
      SELECT id INTO v_qty_dr_acct FROM accounts
       WHERE kind='stock_quarantine' AND sku_id=v_sl.sku_id
         AND location_id=v_sl.ship_location_id AND NOT is_closed;
    END IF;

    IF v_disposition IN ('restock', 'repair') THEN
      SELECT id INTO v_val_dr_acct FROM accounts
       WHERE kind='inv_value_fg' AND sku_id=v_sl.sku_id
         AND location_id=v_sl.ship_location_id AND currency=v_sl.currency
         AND NOT is_closed;
    ELSE
      SELECT id INTO v_var_scrap FROM accounts
       WHERE kind='variance_scrap' AND currency=v_sl.currency AND NOT is_closed;
      v_val_dr_acct := v_var_scrap;
    END IF;

    SELECT id INTO v_cust_qty FROM accounts
     WHERE kind='customer_pool' AND counterparty_id=p_customer_id
       AND NOT is_closed;
    IF v_cust_qty IS NULL THEN
      SELECT id INTO v_cust_qty FROM accounts
       WHERE kind='customer_pool' AND counterparty_id IS NULL
         AND NOT is_closed;
    END IF;

    SELECT id INTO v_cogs_acct FROM accounts
     WHERE kind='cogs' AND currency=v_sl.currency AND NOT is_closed;
    SELECT id INTO v_revenue_acct FROM accounts
     WHERE kind='revenue' AND currency=v_sl.currency AND NOT is_closed;
    SELECT id INTO v_cust_ar FROM accounts
     WHERE kind='ar' AND counterparty_id=p_customer_id
       AND currency=v_sl.currency AND NOT is_closed;

    INSERT INTO customer_return_lines (
      return_id, line_no, ship_line_id, qty_returned, disposition,
      unit_cost, unit_price, tax_amount
    ) VALUES (
      v_doc_id, v_idx, v_ship_line_id, v_qty_returned, v_disposition,
      v_sl.unit_cost, v_sl.unit_price, v_tax_pro
    )
    RETURNING id INTO v_return_line_id;

    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'customer_return',
      'document_kind',     'customer_return',
      'document_id',       v_doc_id,
      'document_line_id',  v_return_line_id,
      'debit_account_id',  v_qty_dr_acct,
      'credit_account_id', v_cust_qty,
      'amount',            v_qty_returned,
      'qty',               v_qty_returned,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   p_customer_id,
      'posted_by',         p_posted_by
    ));

    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'customer_return',
      'document_kind',     'customer_return',
      'document_id',       v_doc_id,
      'document_line_id',  v_return_line_id,
      'debit_account_id',  v_val_dr_acct,
      'credit_account_id', v_cogs_acct,
      'amount',            v_qty_returned * v_sl.unit_cost,
      'qty',               v_qty_returned,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   p_customer_id,
      'posted_by',         p_posted_by
    ));

    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'customer_return',
      'document_kind',     'customer_return',
      'document_id',       v_doc_id,
      'document_line_id',  v_return_line_id,
      'debit_account_id',  v_revenue_acct,
      'credit_account_id', v_cust_ar,
      'amount',            v_qty_returned * v_sl.unit_price,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   p_customer_id,
      'posted_by',         p_posted_by
    ));

    IF v_tax_pro > 0 THEN
      SELECT id INTO v_tax_acct FROM accounts
       WHERE kind='sales_tax_payable' AND currency=v_sl.currency
         AND NOT is_closed;
      SELECT id INTO v_cust_unsettled FROM accounts
       WHERE kind='ar_unsettled' AND counterparty_id=p_customer_id
         AND currency=v_sl.currency AND NOT is_closed;
      IF v_cust_unsettled IS NULL THEN
        SELECT id INTO v_cust_unsettled FROM accounts
         WHERE kind='ar_unsettled' AND counterparty_id IS NULL
           AND currency=v_sl.currency AND NOT is_closed;
      END IF;
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'customer_return',
        'document_kind',     'customer_return',
        'document_id',       v_doc_id,
        'document_line_id',  v_return_line_id,
        'debit_account_id',  v_tax_acct,
        'credit_account_id', v_cust_unsettled,
        'amount',            v_tax_pro,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_customer_id,
        'posted_by',         p_posted_by
      ));
    END IF;
  END LOOP;

  PERFORM post_transfers(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;

-- Restore mig 0076 post_ap_bill (without the returns_to_us subtraction).
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
        RAISE EXCEPTION
          'ap_bill_three_way_mismatch: line % unit_cost % does not match '
          'po_line.unit_cost %',
          v_idx, v_unit_cost, v_pl.unit_cost
          USING ERRCODE = 'P0024';
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
      v_avail := v_total_received - v_total_billed;
      IF v_qty > v_avail THEN
        RAISE EXCEPTION
          'ap_bill_three_way_mismatch: line % qty % exceeds received-not-'
          'billed remainder % for po_line % (received=%, already billed=%)',
          v_idx, v_qty, v_avail, v_po_line_id, v_total_received, v_total_billed
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

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ap_bill',
        'document_kind',     'vendor_bill',
        'document_id',       v_doc_id,
        'document_line_id',  v_bill_line_id,
        'debit_account_id',  v_ven_unsettled,
        'credit_account_id', v_ven_ap,
        'amount',            v_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_vendor_id,
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
        'credit_account_id', v_ven_ap,
        'amount',            v_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_vendor_id,
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

-- Restore mig 0081 post_customer_invoice (without the returns_to_us subtraction).
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

  INSERT INTO customer_invoices (
    customer_id, currency, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_customer_id, p_currency, p_business_date, p_posted_by,
    p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM customer_invoices
     WHERE idempotency_key = p_idempotency_key;
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
       WHERE sl.id = v_so_line_id;
      IF NOT FOUND THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % so_line % not found',
          v_idx, v_so_line_id USING ERRCODE = 'P0041';
      END IF;
      IF v_sl.customer_id IS DISTINCT FROM p_customer_id THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % so_line % belongs to '
          'customer % but invoice is for customer %',
          v_idx, v_so_line_id, v_sl.customer_id, p_customer_id
          USING ERRCODE = 'P0041';
      END IF;
      IF v_sl.currency <> p_currency THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % so_line currency=% but invoice currency=%',
          v_idx, v_sl.currency, p_currency USING ERRCODE = 'P0041';
      END IF;

      IF v_unit_price <> v_sl.unit_price THEN
        RAISE EXCEPTION
          'customer_invoice_three_way_mismatch: line % unit_price % does '
          'not match so_line.unit_price %',
          v_idx, v_unit_price, v_sl.unit_price USING ERRCODE = 'P0040';
      END IF;
      IF v_amount <> v_qty * v_unit_price THEN
        RAISE EXCEPTION
          'customer_invoice_three_way_mismatch: line % amount % <> qty % × unit_price %',
          v_idx, v_amount, v_qty, v_unit_price USING ERRCODE = 'P0040';
      END IF;

      SELECT COALESCE(SUM(qty_shipped), 0) INTO v_total_shipped
        FROM so_shipment_lines WHERE so_line_id = v_so_line_id;
      SELECT COALESCE(SUM(qty), 0) INTO v_total_invoiced
        FROM customer_invoice_lines
       WHERE so_line_id = v_so_line_id AND kind = 'so_match';
      v_avail := v_total_shipped - v_total_invoiced;
      IF v_qty > v_avail THEN
        RAISE EXCEPTION
          'customer_invoice_three_way_mismatch: line % qty % exceeds '
          'shipped-not-invoiced remainder % for so_line % (shipped=%, '
          'already invoiced=%)',
          v_idx, v_qty, v_avail, v_so_line_id, v_total_shipped, v_total_invoiced
          USING ERRCODE = 'P0040';
      END IF;

      SELECT id INTO v_cust_unsettled FROM accounts
       WHERE kind='ar_unsettled' AND counterparty_id=p_customer_id
         AND currency=p_currency AND NOT is_closed;
      IF v_cust_unsettled IS NULL THEN
        RAISE EXCEPTION 'no open ar_unsettled account for customer=% ccy=%',
                        p_customer_id, p_currency USING ERRCODE = 'P0010';
      END IF;

      INSERT INTO customer_invoice_lines (
        invoice_id, line_no, kind, so_line_id, qty, unit_price, amount, tax_amount
      ) VALUES (
        v_doc_id, v_idx, 'so_match', v_so_line_id, v_qty, v_unit_price,
        v_amount, v_tax_amount
      ) RETURNING id INTO v_inv_line_id;

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ar_invoice',
        'document_kind',     'customer_invoice',
        'document_id',       v_doc_id,
        'document_line_id',  v_inv_line_id,
        'debit_account_id',  v_cust_ar,
        'credit_account_id', v_cust_unsettled,
        'amount',            v_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_customer_id,
        'posted_by',         p_posted_by
      ));

    ELSIF v_kind = 'service' THEN
      v_revenue_acct_id := (v_line->>'revenue_account_id')::BIGINT;

      SELECT * INTO v_rev_acct FROM accounts WHERE id = v_revenue_acct_id;
      IF NOT FOUND THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % revenue_account_id % not found',
          v_idx, v_revenue_acct_id USING ERRCODE = 'P0041';
      END IF;
      IF v_rev_acct.is_closed THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % revenue account % is closed',
          v_idx, v_revenue_acct_id USING ERRCODE = 'P0041';
      END IF;
      IF v_rev_acct.ledger_kind <> 'value' THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % revenue account % is %, expected value',
          v_idx, v_revenue_acct_id, v_rev_acct.ledger_kind
          USING ERRCODE = 'P0041';
      END IF;
      IF v_rev_acct.currency <> p_currency THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % revenue account ccy=% but invoice ccy=%',
          v_idx, v_rev_acct.currency, p_currency USING ERRCODE = 'P0041';
      END IF;

      INSERT INTO customer_invoice_lines (
        invoice_id, line_no, kind, revenue_account_id, amount, tax_amount
      ) VALUES (
        v_doc_id, v_idx, 'service', v_revenue_acct_id, v_amount, v_tax_amount
      ) RETURNING id INTO v_inv_line_id;

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ar_invoice',
        'document_kind',     'customer_invoice',
        'document_id',       v_doc_id,
        'document_line_id',  v_inv_line_id,
        'debit_account_id',  v_cust_ar,
        'credit_account_id', v_revenue_acct_id,
        'amount',            v_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_customer_id,
        'posted_by',         p_posted_by
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
          'reason',            'ar_invoice',
          'document_kind',     'customer_invoice',
          'document_id',       v_doc_id,
          'document_line_id',  v_inv_line_id,
          'debit_account_id',  v_cust_ar,
          'credit_account_id', v_cust_tax,
          'amount',            v_tax_amount,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'counterparty_id',   p_customer_id,
          'posted_by',         p_posted_by
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
