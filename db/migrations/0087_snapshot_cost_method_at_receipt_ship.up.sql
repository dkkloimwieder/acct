-- acct-6d8 — snapshot cost_method on po_receipt_lines and so_shipment_lines
-- so post_po_return / post_customer_return dispatch on the cost_method
-- that was active at the original event time, not the SKU's current
-- cost_method.
--
-- Why: SKU cost_method changes (rare but possible during conversion
-- projects) made the existing dispatch on `skus.cost_method` produce
-- inverted PPV math on returns:
--
--   * Standard → wac SKU change: original receipt posted PPV; return
--     dispatched on wac, skipped PPV reversal → orphan variance.
--
--   * Wac → standard SKU change: original receipt had no PPV; return
--     dispatched on standard, fabricated PPV out of nothing.
--
-- Fix: persist cost_method on po_receipt_lines.cost_method_at_receipt
-- and so_shipment_lines.cost_method_at_ship at INSERT time. Returns
-- read the snapshot; SKU cost_method changes after-the-fact don't
-- affect prior receipts/shipments.
--
-- Note: this only fixes per-line cost_method snapshotting. Standard-
-- cost VALUE drift between receipt and return is independently bounded
-- by `resolve_standard_cost_at(business_date)` walking the append-only
-- standard_costs table — but only when cost_method = standard at both
-- points. For SKUs that flipped methods, even that helper would return
-- a misleading value if used. The snapshot avoids the latter trap.

-- ============================================================
-- 1. Add columns + backfill from current skus.cost_method.
-- ============================================================

ALTER TABLE po_receipt_lines
  ADD COLUMN cost_method_at_receipt cost_method NOT NULL DEFAULT 'standard';

UPDATE po_receipt_lines prl
   SET cost_method_at_receipt = s.cost_method
  FROM purchase_order_lines pol
  JOIN skus s ON s.id = pol.sku_id
 WHERE prl.po_line_id = pol.id;

COMMENT ON COLUMN po_receipt_lines.cost_method_at_receipt IS
  'SKU cost_method captured at receipt-post time. post_po_return '
  'dispatches on this snapshot to keep PPV math consistent with the '
  'original receipt even if the SKU later changes cost_method.';

ALTER TABLE so_shipment_lines
  ADD COLUMN cost_method_at_ship cost_method NOT NULL DEFAULT 'standard';

UPDATE so_shipment_lines ssl
   SET cost_method_at_ship = s.cost_method
  FROM sales_order_lines sl
  JOIN skus s ON s.id = sl.sku_id
 WHERE ssl.so_line_id = sl.id;

COMMENT ON COLUMN so_shipment_lines.cost_method_at_ship IS
  'SKU cost_method captured at ship-post time. Symmetry with '
  'po_receipt_lines.cost_method_at_receipt; post_customer_return '
  'currently does not dispatch on it (cogs reversal uses snapshotted '
  'unit_cost), but reserved for future cost_method-aware reversal '
  'logic and parallel structure with the AP side.';

-- ============================================================
-- 2. post_po_receipt — capture cost_method on INSERT.
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
                      v_po_line_id, v_pl.po_id, p_po_id
        USING ERRCODE = 'P0022';
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

    IF v_cost_method IN ('fifo', 'lot') THEN
      RAISE EXCEPTION
        'cost_method_not_implemented: % for po_receipt (sku=%); see acct-8gg',
        v_cost_method, v_pl.sku_id USING ERRCODE = 'P0006';
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

    INSERT INTO po_receipt_lines (
      receipt_id, po_line_id, qty_received, cost_method_at_receipt
    )
    VALUES (v_doc_id, v_po_line_id, v_qty_received, v_cost_method)
    RETURNING id INTO v_recv_line_id;

    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'po_receipt',
      'document_kind',     'po_receipt',
      'document_id',       v_doc_id,
      'document_line_id',  v_recv_line_id,
      'debit_account_id',  v_qty_acct,
      'credit_account_id', v_ven_qty,
      'amount',            v_qty_received,
      'qty',               v_qty_received,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   v_vendor_id,
      'posted_by',         p_posted_by
    ));

    IF v_val_amount > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'po_receipt',
        'document_kind',     'po_receipt',
        'document_id',       v_doc_id,
        'document_line_id',  v_recv_line_id,
        'debit_account_id',  v_val_acct,
        'credit_account_id', v_ven_val,
        'amount',            v_val_amount,
        'qty',               v_qty_received,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   v_vendor_id,
        'posted_by',         p_posted_by
      ));
    END IF;

    IF v_ppv_amount > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ppv',
        'document_kind',     'po_receipt',
        'document_id',       v_doc_id,
        'document_line_id',  v_recv_line_id,
        'debit_account_id',  v_var_acct,
        'credit_account_id', v_ven_val,
        'amount',            v_ppv_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   v_vendor_id,
        'posted_by',         p_posted_by
      ));
    ELSIF v_ppv_amount < 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ppv',
        'document_kind',     'po_receipt',
        'document_id',       v_doc_id,
        'document_line_id',  v_recv_line_id,
        'debit_account_id',  v_ven_val,
        'credit_account_id', v_var_acct,
        'amount',            -v_ppv_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   v_vendor_id,
        'posted_by',         p_posted_by
      ));
    END IF;
  END LOOP;

  PERFORM post_transfers(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;

-- ============================================================
-- 3. post_so_ship — capture cost_method on INSERT.
-- ============================================================

CREATE OR REPLACE FUNCTION post_so_ship(
  p_so_id           UUID,
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
  v_customer_id    UUID;
  v_n              INT;
  v_idx            INT;
  v_line           JSONB;
  v_so_line_id     UUID;
  v_qty_shipped    BIGINT;
  v_unit_price     BIGINT;
  v_tax_amount     BIGINT;
  v_sl             RECORD;
  v_already_ship   BIGINT;
  v_cost_method    cost_method;
  v_unit_cost      BIGINT;
  v_qty_acct       BIGINT;
  v_val_acct       BIGINT;
  v_cust_qty       BIGINT;
  v_cust_unsettled BIGINT;
  v_revenue_acct   BIGINT;
  v_cogs_acct      BIGINT;
  v_tax_acct       BIGINT;
  v_qty_balance    BIGINT;
  v_value_balance  BIGINT;
  v_ship_line_id   UUID;
  v_batch          JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM so_shipments
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT customer_id INTO v_customer_id FROM sales_orders WHERE id = p_so_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'so_ship_invalid: SO % not found', p_so_id
      USING ERRCODE = 'P0037';
  END IF;
  IF v_customer_id IS NULL THEN
    RAISE EXCEPTION 'so_ship_invalid: SO % has no customer_id', p_so_id
      USING ERRCODE = 'P0037';
  END IF;

  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'so_ship_invalid: empty lines for SO %', p_so_id
      USING ERRCODE = 'P0037';
  END IF;

  INSERT INTO so_shipments (so_id, business_date, posted_by, idempotency_key, notes)
  VALUES (p_so_id, p_business_date, p_posted_by, p_idempotency_key, p_notes)
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM so_shipments WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  FOR v_idx IN 1..v_n LOOP
    v_line        := p_lines -> (v_idx - 1);
    v_so_line_id  := (v_line->>'so_line_id')::UUID;
    v_qty_shipped := (v_line->>'qty_shipped')::BIGINT;

    IF v_qty_shipped IS NULL OR v_qty_shipped <= 0 THEN
      RAISE EXCEPTION 'so_ship_invalid: line % qty_shipped must be > 0',
                      v_idx USING ERRCODE = 'P0037';
    END IF;

    SELECT so_id, sku_id, ship_location_id, qty_ordered, unit_price,
           currency, tax_amount
      INTO v_sl
      FROM sales_order_lines WHERE id = v_so_line_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'so_ship_invalid: so_line % not found', v_so_line_id
        USING ERRCODE = 'P0037';
    END IF;
    IF v_sl.so_id <> p_so_id THEN
      RAISE EXCEPTION 'so_ship_invalid: so_line % belongs to SO % not %',
                      v_so_line_id, v_sl.so_id, p_so_id
        USING ERRCODE = 'P0037';
    END IF;

    SELECT COALESCE(SUM(qty_shipped), 0) INTO v_already_ship
      FROM so_shipment_lines WHERE so_line_id = v_so_line_id;
    IF v_already_ship + v_qty_shipped > v_sl.qty_ordered THEN
      RAISE EXCEPTION
        'so_line_overshipped: so_line %: ordered=%, already shipped=%, '
        'this shipment=%; cumulative would exceed qty_ordered',
        v_so_line_id, v_sl.qty_ordered, v_already_ship, v_qty_shipped
        USING ERRCODE = 'P0038';
    END IF;

    v_unit_price := COALESCE((v_line->>'unit_price')::BIGINT, v_sl.unit_price);
    v_tax_amount := COALESCE((v_line->>'tax_amount')::BIGINT, v_sl.tax_amount);

    SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_sl.sku_id;

    IF v_cost_method IN ('fifo', 'lot') THEN
      RAISE EXCEPTION
        'cost_method_not_implemented: % for so_ship (sku=%); see acct-8gg',
        v_cost_method, v_sl.sku_id USING ERRCODE = 'P0006';
    END IF;

    SELECT id INTO v_qty_acct FROM accounts
     WHERE kind='stock_available' AND sku_id=v_sl.sku_id
       AND location_id=v_sl.ship_location_id AND NOT is_closed;
    IF v_qty_acct IS NULL THEN
      RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                      v_sl.sku_id, v_sl.ship_location_id USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_val_acct FROM accounts
     WHERE kind='inv_value_fg' AND sku_id=v_sl.sku_id
       AND location_id=v_sl.ship_location_id
       AND currency=v_sl.currency AND NOT is_closed;
    IF v_val_acct IS NULL THEN
      RAISE EXCEPTION 'no open inv_value_fg account for sku=% loc=% ccy=%',
                      v_sl.sku_id, v_sl.ship_location_id, v_sl.currency
        USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_cust_qty FROM accounts
     WHERE kind='customer_pool' AND counterparty_id=v_customer_id
       AND ledger_kind='qty' AND NOT is_closed;
    IF v_cust_qty IS NULL THEN
      RAISE EXCEPTION 'no open customer_pool(qty) account for customer=%',
                      v_customer_id USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_cust_unsettled FROM accounts
     WHERE kind='ar_unsettled' AND counterparty_id=v_customer_id
       AND currency=v_sl.currency AND NOT is_closed;
    IF v_cust_unsettled IS NULL THEN
      RAISE EXCEPTION 'no open ar_unsettled account for customer=% ccy=%',
                      v_customer_id, v_sl.currency USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_revenue_acct FROM accounts
     WHERE kind='revenue' AND ledger_kind='value'
       AND currency=v_sl.currency AND NOT is_closed;
    IF v_revenue_acct IS NULL THEN
      RAISE EXCEPTION 'no open revenue account for ccy=%',
                      v_sl.currency USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_cogs_acct FROM accounts
     WHERE kind='cogs' AND ledger_kind='value'
       AND currency=v_sl.currency AND NOT is_closed;
    IF v_cogs_acct IS NULL THEN
      RAISE EXCEPTION 'no open cogs account for ccy=%',
                      v_sl.currency USING ERRCODE = 'P0010';
    END IF;

    IF v_cost_method = 'standard' THEN
      v_unit_cost := resolve_standard_cost_at(v_sl.sku_id, p_business_date);
    ELSE
      SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = v_val_acct THEN  t.qty
                               WHEN t.credit_account_id = v_val_acct THEN -t.qty END), 0)
        INTO v_qty_balance
        FROM transfers t
       WHERE v_val_acct IN (t.debit_account_id, t.credit_account_id)
         AND t.qty IS NOT NULL;

      IF v_qty_balance <= 0 THEN
        RAISE EXCEPTION
          'wac so_ship qty balance is %, cannot price (sku=%, loc=%, ccy=%)',
          v_qty_balance, v_sl.sku_id, v_sl.ship_location_id, v_sl.currency
          USING ERRCODE = 'P0006';
      END IF;
      SELECT debits_total - credits_total INTO v_value_balance
        FROM accounts WHERE id = v_val_acct;
      IF v_value_balance < 0 THEN v_value_balance := 0; END IF;
      v_unit_cost := v_value_balance / v_qty_balance;
    END IF;

    IF v_tax_amount > 0 THEN
      SELECT id INTO v_tax_acct FROM accounts
       WHERE kind='sales_tax_payable' AND ledger_kind='value'
         AND currency=v_sl.currency AND NOT is_closed;
      IF v_tax_acct IS NULL THEN
        RAISE EXCEPTION 'no open sales_tax_payable account for ccy=%',
                        v_sl.currency USING ERRCODE = 'P0010';
      END IF;
    END IF;

    INSERT INTO so_shipment_lines (
      shipment_id, so_line_id, qty_shipped, unit_cost, unit_price, tax_amount,
      cost_method_at_ship
    ) VALUES (
      v_doc_id, v_so_line_id, v_qty_shipped, v_unit_cost, v_unit_price, v_tax_amount,
      v_cost_method
    ) RETURNING id INTO v_ship_line_id;

    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'so_ship',
      'document_kind',     'so_shipment',
      'document_id',       v_doc_id,
      'document_line_id',  v_ship_line_id,
      'debit_account_id',  v_cust_qty,
      'credit_account_id', v_qty_acct,
      'amount',            v_qty_shipped,
      'qty',               v_qty_shipped,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   v_customer_id,
      'posted_by',         p_posted_by
    ));

    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'so_ship',
      'document_kind',     'so_shipment',
      'document_id',       v_doc_id,
      'document_line_id',  v_ship_line_id,
      'debit_account_id',  v_cogs_acct,
      'credit_account_id', v_val_acct,
      'amount',            v_qty_shipped * v_unit_cost,
      'qty',               v_qty_shipped,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   v_customer_id,
      'posted_by',         p_posted_by
    ));

    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'so_ship',
      'document_kind',     'so_shipment',
      'document_id',       v_doc_id,
      'document_line_id',  v_ship_line_id,
      'debit_account_id',  v_cust_unsettled,
      'credit_account_id', v_revenue_acct,
      'amount',            v_qty_shipped * v_unit_price,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   v_customer_id,
      'posted_by',         p_posted_by
    ));

    IF v_tax_amount > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'so_ship',
        'document_kind',     'so_shipment',
        'document_id',       v_doc_id,
        'document_line_id',  v_ship_line_id,
        'debit_account_id',  v_cust_unsettled,
        'credit_account_id', v_tax_acct,
        'amount',            v_tax_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   v_customer_id,
        'posted_by',         p_posted_by
      ));
    END IF;
  END LOOP;

  UPDATE inventory_reservations
     SET status = 'shipped',
         resolved_at = clock_timestamp()
   WHERE so_id = p_so_id
     AND status IN ('active', 'allocated');

  PERFORM post_transfers(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;

-- ============================================================
-- 4. post_po_return — read cost_method_at_receipt instead of skus.cost_method.
-- ============================================================

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

    -- Pull both po-line metadata AND the cost_method snapshot from the
    -- original receipt line.
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
      RAISE EXCEPTION
        'po_return_invalid: recv_line % belongs to vendor %, not %',
        v_recv_line_id, v_pl.vendor_id, p_vendor_id
        USING ERRCODE = 'P0046';
    END IF;

    v_po_line_id := v_pl.po_line_id;

    PERFORM 1 FROM purchase_order_lines WHERE id = v_po_line_id FOR UPDATE;

    SELECT COALESCE(SUM(qty_received), 0)
      INTO v_total_recv
      FROM po_receipt_lines
     WHERE po_line_id = v_po_line_id;

    SELECT COALESCE(SUM(qty), 0)
      INTO v_total_billed
      FROM vendor_bill_lines
     WHERE po_line_id = v_po_line_id AND kind = 'po_match';

    SELECT
      COALESCE(SUM(prl.qty_to_ap_unsettled), 0),
      COALESCE(SUM(prl.qty_to_ap), 0)
      INTO v_prior_to_unsettled, v_prior_to_ap
      FROM po_return_lines prl
      JOIN po_receipt_lines rcl ON rcl.id = prl.recv_line_id
      JOIN po_returns       pr  ON pr.id  = prl.return_id
     WHERE rcl.po_line_id = v_po_line_id
       AND pr.id <> v_doc_id;

    v_unsettled_rem := v_total_recv - v_total_billed - v_prior_to_unsettled;
    v_ap_rem        := v_total_billed - v_prior_to_ap;

    v_qty_to_unsettled := LEAST(v_qty_returned, GREATEST(v_unsettled_rem, 0));
    v_qty_to_ap        := v_qty_returned - v_qty_to_unsettled;

    IF v_qty_to_ap > v_ap_rem THEN
      RAISE EXCEPTION
        'po_return_overreturned: po_line % cumulative would exceed billed-not-returned + unsettled-not-returned (recv=%, billed=%, prior_to_unsettled=%, prior_to_ap=%, requested=%)',
        v_po_line_id, v_total_recv, v_total_billed,
        v_prior_to_unsettled, v_prior_to_ap, v_qty_returned
        USING ERRCODE = 'P0047';
    END IF;

    -- Use the snapshotted cost_method, NOT the SKU's current cost_method.
    -- acct-6d8: keeps PPV math consistent with the original receipt
    -- when the SKU's cost_method has changed since.
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

    IF v_qty_to_unsettled > 0 THEN
      DECLARE
        v_inv_amt_us BIGINT := v_qty_to_unsettled * v_inv_unit;
        v_ppv_amt_us BIGINT := v_qty_to_unsettled * v_pl.unit_cost
                              - v_qty_to_unsettled * v_inv_unit;
      BEGIN
        IF v_ppv_amt_us > 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'ppv',
            'document_kind',     'po_return',
            'document_id',       v_doc_id,
            'document_line_id',  v_return_line_id,
            'debit_account_id',  v_ven_unsettled,
            'credit_account_id', v_var_acct,
            'amount',            v_ppv_amt_us,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_vendor_id,
            'posted_by',         p_posted_by
          ));
        ELSIF v_ppv_amt_us < 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'ppv',
            'document_kind',     'po_return',
            'document_id',       v_doc_id,
            'document_line_id',  v_return_line_id,
            'debit_account_id',  v_var_acct,
            'credit_account_id', v_ven_unsettled,
            'amount',            -v_ppv_amt_us,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_vendor_id,
            'posted_by',         p_posted_by
          ));
        END IF;

        IF v_inv_amt_us > 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'po_return_to_vendor',
            'document_kind',     'po_return',
            'document_id',       v_doc_id,
            'document_line_id',  v_return_line_id,
            'debit_account_id',  v_ven_unsettled,
            'credit_account_id', v_val_acct,
            'amount',            v_inv_amt_us,
            'qty',               v_qty_to_unsettled,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_vendor_id,
            'posted_by',         p_posted_by
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
            'reason',            'ppv',
            'document_kind',     'po_return',
            'document_id',       v_doc_id,
            'document_line_id',  v_return_line_id,
            'debit_account_id',  v_ven_ap,
            'credit_account_id', v_var_acct,
            'amount',            v_ppv_amt_ap,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_vendor_id,
            'posted_by',         p_posted_by
          ));
        ELSIF v_ppv_amt_ap < 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'ppv',
            'document_kind',     'po_return',
            'document_id',       v_doc_id,
            'document_line_id',  v_return_line_id,
            'debit_account_id',  v_var_acct,
            'credit_account_id', v_ven_ap,
            'amount',            -v_ppv_amt_ap,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_vendor_id,
            'posted_by',         p_posted_by
          ));
        END IF;

        IF v_inv_amt_ap > 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'po_return_to_vendor',
            'document_kind',     'po_return',
            'document_id',       v_doc_id,
            'document_line_id',  v_return_line_id,
            'debit_account_id',  v_ven_ap,
            'credit_account_id', v_val_acct,
            'amount',            v_inv_amt_ap,
            'qty',               v_qty_to_ap,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_vendor_id,
            'posted_by',         p_posted_by
          ));
        END IF;
      END;
    END IF;
  END LOOP;

  PERFORM post_transfers(v_batch, p_override_closed_period);

  RETURN v_doc_id;
END;
$$;
