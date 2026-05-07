-- Down: revert post_po_return body to mig 0087 shape (uses skus.cost_method
-- snapshot but not the prior-period adjustment routing). Leave the
-- account_kind value in place per project convention (ALTER TYPE DROP
-- VALUE is unsupported in PG; the value remains in the type but is no
-- longer referenced).

CREATE OR REPLACE FUNCTION post_po_return(
  p_vendor_id              UUID,
  p_lines                  JSONB,
  p_business_date          DATE,
  p_posted_by              UUID,
  p_idempotency_key        UUID,
  p_notes                  TEXT DEFAULT NULL,
  p_override_closed_period BOOLEAN DEFAULT FALSE
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id        UUID;
  v_doc_id             UUID;
  v_vendor_check       UUID;
  v_n                  INT;
  v_idx                INT;
  v_line               JSONB;
  v_recv_line_id       UUID;
  v_qty_returned       BIGINT;
  v_pl                 RECORD;
  v_po_line_id         UUID;
  v_total_recv         BIGINT;
  v_total_billed       BIGINT;
  v_prior_to_unsettled BIGINT;
  v_prior_to_ap        BIGINT;
  v_unsettled_rem      BIGINT;
  v_ap_rem             BIGINT;
  v_qty_to_unsettled   BIGINT;
  v_qty_to_ap          BIGINT;
  v_cost_method        cost_method;
  v_inv_unit           BIGINT;
  v_qty_acct           BIGINT;
  v_val_acct           BIGINT;
  v_ven_qty            BIGINT;
  v_ven_unsettled      BIGINT;
  v_ven_ap             BIGINT;
  v_var_acct           BIGINT;
  v_return_line_id     UUID;
  v_batch              JSONB := '[]'::JSONB;
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
      prl.id                     AS recv_line_id,
      prl.po_line_id             AS po_line_id,
      prl.cost_method_at_receipt AS cost_method_snap,
      pol.sku_id                 AS sku_id,
      pol.location_id            AS location_id,
      pol.unit_cost              AS unit_cost,
      pol.currency               AS currency,
      po.vendor_id               AS vendor_id
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
      RAISE EXCEPTION 'po_return_invalid: recv_line % belongs to vendor %, not %',
                      v_recv_line_id, v_pl.vendor_id, p_vendor_id
        USING ERRCODE = 'P0046';
    END IF;

    v_po_line_id := v_pl.po_line_id;
    PERFORM 1 FROM purchase_order_lines WHERE id = v_po_line_id FOR UPDATE;

    SELECT COALESCE(SUM(qty_received), 0) INTO v_total_recv
      FROM po_receipt_lines WHERE po_line_id = v_po_line_id;
    SELECT COALESCE(SUM(qty), 0) INTO v_total_billed
      FROM vendor_bill_lines WHERE po_line_id = v_po_line_id AND kind = 'po_match';
    SELECT COALESCE(SUM(prl.qty_to_ap_unsettled), 0),
           COALESCE(SUM(prl.qty_to_ap), 0)
      INTO v_prior_to_unsettled, v_prior_to_ap
      FROM po_return_lines prl
      JOIN po_receipt_lines rcl ON rcl.id = prl.recv_line_id
      JOIN po_returns       pr  ON pr.id  = prl.return_id
     WHERE rcl.po_line_id = v_po_line_id AND pr.id <> v_doc_id;

    v_unsettled_rem := v_total_recv - v_total_billed - v_prior_to_unsettled;
    v_ap_rem        := v_total_billed - v_prior_to_ap;
    v_qty_to_unsettled := LEAST(v_qty_returned, GREATEST(v_unsettled_rem, 0));
    v_qty_to_ap        := v_qty_returned - v_qty_to_unsettled;

    IF v_qty_to_ap > v_ap_rem THEN
      RAISE EXCEPTION 'po_return_overreturned: po_line % cumulative would exceed billed-not-returned + unsettled-not-returned (recv=%, billed=%, prior_to_unsettled=%, prior_to_ap=%, requested=%)',
        v_po_line_id, v_total_recv, v_total_billed, v_prior_to_unsettled, v_prior_to_ap, v_qty_returned
        USING ERRCODE = 'P0047';
    END IF;

    v_cost_method := v_pl.cost_method_snap;
    IF v_cost_method = 'standard' THEN
      v_inv_unit := resolve_standard_cost_at(v_pl.sku_id, p_business_date);
    ELSIF v_cost_method IN ('wac_perpetual', 'wac_periodic', 'wac_retroactive') THEN
      v_inv_unit := v_pl.unit_cost;
    ELSE
      RAISE EXCEPTION 'cost_method_not_implemented: % for po_return',
                      v_cost_method USING ERRCODE = 'P0006';
    END IF;

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
                      v_pl.sku_id, v_pl.location_id, v_pl.currency USING ERRCODE = 'P0010';
    END IF;
    SELECT id INTO v_ven_qty FROM accounts
     WHERE kind='vendor_pool' AND counterparty_id=p_vendor_id AND NOT is_closed;
    IF v_ven_qty IS NULL THEN
      SELECT id INTO v_ven_qty FROM accounts
       WHERE kind='vendor_pool' AND counterparty_id IS NULL AND NOT is_closed;
    END IF;
    IF v_ven_qty IS NULL THEN
      RAISE EXCEPTION 'no open vendor_pool for vendor=%',
                      p_vendor_id USING ERRCODE = 'P0010';
    END IF;
    IF v_qty_to_unsettled > 0 THEN
      SELECT id INTO v_ven_unsettled FROM accounts
       WHERE kind='ap_unsettled' AND counterparty_id=p_vendor_id
         AND currency=v_pl.currency AND NOT is_closed;
      IF v_ven_unsettled IS NULL THEN
        RAISE EXCEPTION 'no open ap_unsettled for vendor=% ccy=%',
                        p_vendor_id, v_pl.currency USING ERRCODE = 'P0010';
      END IF;
    END IF;
    IF v_qty_to_ap > 0 THEN
      SELECT id INTO v_ven_ap FROM accounts
       WHERE kind='ap' AND counterparty_id=p_vendor_id
         AND currency=v_pl.currency AND NOT is_closed;
      IF v_ven_ap IS NULL THEN
        RAISE EXCEPTION 'no open ap for vendor=% ccy=%',
                        p_vendor_id, v_pl.currency USING ERRCODE = 'P0010';
      END IF;
    END IF;
    IF v_cost_method = 'standard' AND v_pl.unit_cost <> v_inv_unit THEN
      SELECT id INTO v_var_acct FROM accounts
       WHERE kind='variance_ppv' AND currency=v_pl.currency AND NOT is_closed;
      IF v_var_acct IS NULL THEN
        RAISE EXCEPTION 'no open variance_ppv for ccy=%',
                        v_pl.currency USING ERRCODE = 'P0010';
      END IF;
    END IF;

    INSERT INTO po_return_lines (
      return_id, line_no, recv_line_id, qty_returned, unit_cost,
      qty_to_ap_unsettled, qty_to_ap
    ) VALUES (
      v_doc_id, v_idx, v_recv_line_id, v_qty_returned, v_pl.unit_cost,
      v_qty_to_unsettled, v_qty_to_ap
    ) RETURNING id INTO v_return_line_id;

    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason','po_return_to_vendor','document_kind','po_return',
      'document_id',v_doc_id,'document_line_id',v_return_line_id,
      'debit_account_id',v_ven_qty,'credit_account_id',v_qty_acct,
      'amount',v_qty_returned,'qty',v_qty_returned,
      'business_date',p_business_date,'idempotency_key',gen_random_uuid(),
      'counterparty_id',p_vendor_id,'posted_by',p_posted_by
    ));

    IF v_qty_to_unsettled > 0 THEN
      DECLARE
        v_inv_amt_us BIGINT := v_qty_to_unsettled * v_inv_unit;
        v_ppv_amt_us BIGINT := v_qty_to_unsettled * v_pl.unit_cost
                              - v_qty_to_unsettled * v_inv_unit;
      BEGIN
        IF v_ppv_amt_us > 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason','ppv','document_kind','po_return',
            'document_id',v_doc_id,'document_line_id',v_return_line_id,
            'debit_account_id',v_ven_unsettled,'credit_account_id',v_var_acct,
            'amount',v_ppv_amt_us,'business_date',p_business_date,
            'idempotency_key',gen_random_uuid(),
            'counterparty_id',p_vendor_id,'posted_by',p_posted_by
          ));
        ELSIF v_ppv_amt_us < 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason','ppv','document_kind','po_return',
            'document_id',v_doc_id,'document_line_id',v_return_line_id,
            'debit_account_id',v_var_acct,'credit_account_id',v_ven_unsettled,
            'amount',-v_ppv_amt_us,'business_date',p_business_date,
            'idempotency_key',gen_random_uuid(),
            'counterparty_id',p_vendor_id,'posted_by',p_posted_by
          ));
        END IF;
        IF v_inv_amt_us > 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason','po_return_to_vendor','document_kind','po_return',
            'document_id',v_doc_id,'document_line_id',v_return_line_id,
            'debit_account_id',v_ven_unsettled,'credit_account_id',v_val_acct,
            'amount',v_inv_amt_us,'qty',v_qty_to_unsettled,
            'business_date',p_business_date,'idempotency_key',gen_random_uuid(),
            'counterparty_id',p_vendor_id,'posted_by',p_posted_by
          ));
        END IF;
      END;
    END IF;

    IF v_qty_to_ap > 0 THEN
      DECLARE
        v_inv_amt_ap BIGINT := v_qty_to_ap * v_inv_unit;
        v_ppv_amt_ap BIGINT := v_qty_to_ap * v_pl.unit_cost
                              - v_qty_to_ap * v_inv_unit;
      BEGIN
        IF v_ppv_amt_ap > 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason','ppv','document_kind','po_return',
            'document_id',v_doc_id,'document_line_id',v_return_line_id,
            'debit_account_id',v_ven_ap,'credit_account_id',v_var_acct,
            'amount',v_ppv_amt_ap,'business_date',p_business_date,
            'idempotency_key',gen_random_uuid(),
            'counterparty_id',p_vendor_id,'posted_by',p_posted_by
          ));
        ELSIF v_ppv_amt_ap < 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason','ppv','document_kind','po_return',
            'document_id',v_doc_id,'document_line_id',v_return_line_id,
            'debit_account_id',v_var_acct,'credit_account_id',v_ven_ap,
            'amount',-v_ppv_amt_ap,'business_date',p_business_date,
            'idempotency_key',gen_random_uuid(),
            'counterparty_id',p_vendor_id,'posted_by',p_posted_by
          ));
        END IF;
        IF v_inv_amt_ap > 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason','po_return_to_vendor','document_kind','po_return',
            'document_id',v_doc_id,'document_line_id',v_return_line_id,
            'debit_account_id',v_ven_ap,'credit_account_id',v_val_acct,
            'amount',v_inv_amt_ap,'qty',v_qty_to_ap,
            'business_date',p_business_date,'idempotency_key',gen_random_uuid(),
            'counterparty_id',p_vendor_id,'posted_by',p_posted_by
          ));
        END IF;
      END;
    END IF;
  END LOOP;

  PERFORM post_transfers(v_batch, p_override_closed_period);
  RETURN v_doc_id;
END;
$$;
