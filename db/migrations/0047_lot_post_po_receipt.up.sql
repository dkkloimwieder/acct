-- ============================================================
-- Phase E2 follow-up L1 (acct-q9ef, sub-issue of acct-uze).
--
-- Wrapper integration for lot tracking — first of four (mirrors
-- FIFO W1-W4 pattern from acct-xxrs / acct-clrd).
--
-- post_po_receipt accepts per-line lot metadata via p_lines JSONB
-- extension keys: lot_code (REQUIRED for lot_fifo SKUs),
-- expiration_date / manufacture_date / supplier_lot_number /
-- quality_status / attributes (all optional). The wrapper forwards
-- these keys into the value-leg event JSON; apply_event's E2 block
-- (mig 0046) creates the inventory_lots row and stamps lot_id on
-- posting_line_inventory + inventory_movements.
--
-- po_receipt_lines.lot_id BIGINT NULL added as audit / snapshot
-- column; stamped post-PERFORM by joining posting_lines →
-- inventory_lots on receipt_posting_line_id.
--
-- Validation: lot_fifo SKU + missing lot_code raises P0006 at the
-- wrapper for a clearer error message than apply_event's
-- event-index reference. Non-lot SKU + lot_code passes through
-- harmlessly (lot_id stays NULL on po_receipt_lines).
--
-- 'lot' (legacy enum value, distinct from 'lot_fifo') still raises
-- P0006 — its branch is preserved.
-- ============================================================

ALTER TABLE po_receipt_lines ADD COLUMN lot_id BIGINT;

CREATE INDEX po_receipt_lines_lot
  ON po_receipt_lines (lot_id) WHERE lot_id IS NOT NULL;

COMMENT ON COLUMN po_receipt_lines.lot_id IS
  'Audit pointer to the inventory_lots row created by this receipt '
  'line for lot_fifo SKUs. NULL for non-lot SKUs. Stamped after '
  'post_posting_lines returns by joining posting_lines.document_line_id '
  '→ inventory_lots.receipt_posting_line_id.';

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
  v_vendor_id     UUID;
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
  v_ven_qty       BIGINT;
  v_ven_val       BIGINT;
  v_var_acct      BIGINT;
  v_val_unit      BIGINT;
  v_val_amount    BIGINT;
  v_ppv_amount    BIGINT;
  v_recv_line_id  UUID;
  v_batch         JSONB := '[]'::JSONB;
  v_lot_code      TEXT;
  v_value_event   JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM po_receipts
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT vendor_id INTO v_vendor_id FROM purchase_orders WHERE id = p_po_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'po_receipt_invalid: PO % not found', p_po_id
      USING ERRCODE = 'P0022';
  END IF;
  IF v_vendor_id IS NULL THEN
    RAISE EXCEPTION 'po_receipt_invalid: PO % has no vendor_id', p_po_id
      USING ERRCODE = 'P0022';
  END IF;

  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'po_receipt_invalid: empty lines for PO %', p_po_id
      USING ERRCODE = 'P0022';
  END IF;

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

    SELECT po_id, sku_id, location_id, qty_ordered, unit_cost, currency
      INTO v_pl
      FROM purchase_order_lines WHERE id = v_po_line_id FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'po_receipt_invalid: po_line % not found', v_po_line_id
        USING ERRCODE = 'P0022';
    END IF;
    IF v_pl.po_id <> p_po_id THEN
      RAISE EXCEPTION 'po_receipt_invalid: po_line % belongs to PO % not %',
                      v_po_line_id, v_pl.po_id, p_po_id USING ERRCODE = 'P0022';
    END IF;

    SELECT COALESCE(SUM(qty_received), 0) INTO v_already_recv
      FROM po_receipt_lines WHERE po_line_id = v_po_line_id;
    IF v_already_recv + v_qty_received > v_pl.qty_ordered THEN
      RAISE EXCEPTION
        'po_line_overreceived: po_line %: ordered=%, already received=%, '
        'this receipt=%; cumulative would exceed qty_ordered',
        v_po_line_id, v_pl.qty_ordered, v_already_recv, v_qty_received
        USING ERRCODE = 'P0023';
    END IF;

    SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_pl.sku_id;
    IF v_cost_method = 'lot' THEN
      RAISE EXCEPTION
        'cost_method_not_implemented: % for po_receipt (sku=%); see acct-8gg',
        v_cost_method, v_pl.sku_id USING ERRCODE = 'P0006';
    END IF;

    -- Wrapper-level validation: lot_fifo SKUs require lot_code per line.
    -- apply_event would also raise P0006 from _lot_create_from_event with
    -- an event-index reference; this surfaces line-index instead.
    v_lot_code := v_line->>'lot_code';
    IF v_cost_method = 'lot_fifo'
       AND (v_lot_code IS NULL OR length(v_lot_code) = 0) THEN
      RAISE EXCEPTION
        'po_receipt_invalid: line % requires lot_code for lot_fifo sku=%',
        v_idx, v_pl.sku_id USING ERRCODE = 'P0022';
    END IF;

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

    SELECT id INTO v_ven_qty FROM accounts
     WHERE kind='vendor_pool' AND counterparty_id=v_vendor_id
       AND ledger_kind='qty' AND NOT is_closed;
    IF v_ven_qty IS NULL THEN
      RAISE EXCEPTION 'no open vendor_pool(qty) account for vendor=%',
                      v_vendor_id USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_ven_val FROM accounts
     WHERE kind='ap_unsettled' AND counterparty_id=v_vendor_id
       AND currency=v_pl.currency AND NOT is_closed;
    IF v_ven_val IS NULL THEN
      RAISE EXCEPTION 'no open ap_unsettled account for vendor=% ccy=%',
                      v_vendor_id, v_pl.currency USING ERRCODE = 'P0010';
    END IF;

    IF v_cost_method = 'standard' THEN
      v_std_cost   := _resolve_standard_cost_at(v_pl.sku_id, p_business_date);
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

    INSERT INTO po_receipt_lines (
      receipt_id, po_line_id, qty_received, cost_method_at_receipt
    ) VALUES (v_doc_id, v_po_line_id, v_qty_received, v_cost_method)
    RETURNING id INTO v_recv_line_id;

    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason','po_receipt','document_kind','po_receipt',
      'document_id',v_doc_id,'document_line_id',v_recv_line_id,
      'debit_account_id',v_qty_acct,'credit_account_id',v_ven_qty,
      'amount',v_qty_received,'qty',v_qty_received,
      'business_date',p_business_date,'idempotency_key',gen_random_uuid(),
      'counterparty_id',v_vendor_id,'posted_by',p_posted_by
    ));

    IF v_val_amount > 0 THEN
      v_value_event := jsonb_build_object(
        'reason','po_receipt','document_kind','po_receipt',
        'document_id',v_doc_id,'document_line_id',v_recv_line_id,
        'debit_account_id',v_val_acct,'credit_account_id',v_ven_val,
        'amount',v_val_amount,'qty',v_qty_received,
        'business_date',p_business_date,'idempotency_key',gen_random_uuid(),
        'counterparty_id',v_vendor_id,'posted_by',p_posted_by
      );

      -- Forward lot metadata for lot_fifo SKUs. Apply_event's E2 block
      -- reads lot_code (REQUIRED) plus optional manufacture_date /
      -- expiration_date / supplier_lot_number / quality_status /
      -- attributes from the event JSON top-level keys.
      IF v_cost_method = 'lot_fifo' THEN
        v_value_event := v_value_event || jsonb_build_object(
          'lot_code',            v_line->>'lot_code',
          'manufacture_date',    v_line->>'manufacture_date',
          'expiration_date',     v_line->>'expiration_date',
          'supplier_lot_number', v_line->>'supplier_lot_number',
          'quality_status',      v_line->>'quality_status',
          'attributes',          v_line->'attributes'
        );
      END IF;

      v_batch := v_batch || jsonb_build_array(v_value_event);
    END IF;

    IF v_ppv_amount > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason','ppv','document_kind','po_receipt',
        'document_id',v_doc_id,'document_line_id',v_recv_line_id,
        'debit_account_id',v_var_acct,'credit_account_id',v_ven_val,
        'amount',v_ppv_amount,'business_date',p_business_date,
        'idempotency_key',gen_random_uuid(),
        'counterparty_id',v_vendor_id,'posted_by',p_posted_by
      ));
    ELSIF v_ppv_amount < 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason','ppv','document_kind','po_receipt',
        'document_id',v_doc_id,'document_line_id',v_recv_line_id,
        'debit_account_id',v_ven_val,'credit_account_id',v_var_acct,
        'amount',-v_ppv_amount,'business_date',p_business_date,
        'idempotency_key',gen_random_uuid(),
        'counterparty_id',v_vendor_id,'posted_by',p_posted_by
      ));
    END IF;
  END LOOP;

  PERFORM post_posting_lines(v_batch, FALSE);

  -- Stamp po_receipt_lines.lot_id from the inventory_lots row that
  -- apply_event's E2 block created against the value-leg posting.
  -- For non-lot SKUs the JOIN matches zero rows and is a no-op.
  UPDATE po_receipt_lines prl
     SET lot_id = il.lot_id
    FROM posting_lines pl
    JOIN inventory_lots il ON il.receipt_posting_line_id = pl.id
   WHERE prl.receipt_id = v_doc_id
     AND pl.document_line_id = prl.id
     AND pl.document_kind = 'po_receipt'
     AND pl.reason = 'po_receipt';

  RETURN v_doc_id;
END;
$$;
