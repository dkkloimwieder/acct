-- sxl2.3 (acct-sxl2.3): wrappers A — post_po_receipt +
-- post_inventory_adjustment extended for tracked_by='lot_and_serial'.
--
-- WHY: sxl2.2 (mig 0062) added the apply_event E2.5 block that
-- creates inventory_units + emits type=1 receipt events when event
-- JSON carries 'unit_serials' / 'external_serials'. This mig threads
-- those keys through the two inflow wrappers (po_receipt is the
-- primary inbound path; inventory_adjustment is the secondary +qty /
-- -qty path).
--
-- DESIGN:
--   post_po_receipt: each line of p_lines may carry 'unit_serials'
--     and 'external_serials' arrays. Wrapper validates length matches
--     qty_received (cleaner error than apply_event's event-index
--     reference); forwards into v_value_event JSON. Non-
--     lot_and_serial SKUs reject unit_serials with P0006 (catch caller
--     bugs early).
--
--   post_inventory_adjustment: extends p_lot_metadata with three new
--     optional keys:
--       'unit_serials':     TEXT[] for +qty (system labels);
--                           or for -qty (XOR with 'unit_ids') to
--                           resolve unit_ids from serials.
--       'external_serials': TEXT[] for +qty only (supplier labels).
--       'unit_ids':         BIGINT[] for -qty (caller pre-resolved).
--
--     +qty path: forwards 'unit_serials' / 'external_serials' into
--       value-leg event JSON for apply_event's E2.5 block. Auto-
--       generation in apply_event handles the absence case.
--
--     -qty path on tracked_by='lot_and_serial' SKU:
--       1. Resolve unit_ids (from 'unit_ids' directly, or from
--          'unit_serials' via lookup in active inventory_units).
--       2. Validate count = |p_qty_delta|.
--       3. Validate all units: status active, product matches,
--          location matches, SAME lot_id + lot_receipt_date.
--       4. Pin v_specific_lot_id from the units (overrides
--          p_lot_metadata->'lot_id' if both supplied —
--          unit-resolution is more specific).
--       5. Issue-side flow continues normally via _lot_write_issues.
--       6. Post-PERFORM: UPDATE units status='consumed', emit
--          inventory_unit_events type=2 (issue), stamp
--          inventory_adjustments.unit_ids audit column.
--
-- AUDIT COLUMNS:
--   po_receipt_lines.unit_ids BIGINT[] — stamped post-PERFORM with
--     unit_ids created by apply_event's E2.5 block for this line.
--   inventory_adjustments.unit_ids BIGINT[] — stamped post-PERFORM
--     with the consumed units (-qty) or created units (+qty).
--
-- VERBATIM-COPY DISCIPLINE: post_po_receipt body copied byte-for-byte
-- from mig 0047 (acct-q9ef); post_inventory_adjustment from mig 0048
-- (acct-b0j1). post_inventory_adjustment keeps its 11-arg signature
-- (CREATE OR REPLACE works because no param renames or additions).

-- ---------- 1. Audit columns ----------

ALTER TABLE po_receipt_lines ADD COLUMN unit_ids BIGINT[];

CREATE INDEX po_receipt_lines_unit_ids
  ON po_receipt_lines USING GIN (unit_ids)
  WHERE unit_ids IS NOT NULL;

COMMENT ON COLUMN po_receipt_lines.unit_ids IS
  'Audit pointer: inventory_units rows created by apply_event''s '
  'E2.5 block for this receipt line. NULL for non-lot_and_serial '
  'SKUs. Stamped post-PERFORM.';

ALTER TABLE inventory_adjustments ADD COLUMN unit_ids BIGINT[];

CREATE INDEX inventory_adjustments_unit_ids
  ON inventory_adjustments USING GIN (unit_ids)
  WHERE unit_ids IS NOT NULL;

COMMENT ON COLUMN inventory_adjustments.unit_ids IS
  'Audit pointer: inventory_units created (+qty) or consumed (-qty) '
  'by this adjustment for tracked_by=''lot_and_serial'' SKUs. NULL '
  'for non-lot_and_serial SKUs. Stamped post-PERFORM.';

-- ---------- 2. post_po_receipt ----------

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
  v_tracked_by    inventory_tracking;
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
  v_unit_serials  JSONB;
  v_external_serials JSONB;
  v_us_len        INT;
  v_es_len        INT;
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

    SELECT cost_method, tracked_by
      INTO v_cost_method, v_tracked_by
      FROM skus WHERE id = v_pl.sku_id;
    IF v_cost_method = 'lot' THEN
      RAISE EXCEPTION
        'cost_method_not_implemented: % for po_receipt (sku=%); see acct-8gg',
        v_cost_method, v_pl.sku_id USING ERRCODE = 'P0006';
    END IF;

    -- Wrapper-level validation: lot_fifo SKUs require lot_code per line.
    v_lot_code := v_line->>'lot_code';
    IF v_cost_method = 'lot_fifo'
       AND (v_lot_code IS NULL OR length(v_lot_code) = 0) THEN
      RAISE EXCEPTION
        'po_receipt_invalid: line % requires lot_code for lot_fifo sku=%',
        v_idx, v_pl.sku_id USING ERRCODE = 'P0022';
    END IF;

    -- sxl2.3: validate unit_serials / external_serials length when
    -- supplied. Reject when supplied to non-lot_and_serial SKU (catch
    -- caller bugs early).
    v_unit_serials     := v_line->'unit_serials';
    v_external_serials := v_line->'external_serials';

    IF v_unit_serials IS NOT NULL OR v_external_serials IS NOT NULL THEN
      IF v_tracked_by <> 'lot_and_serial' THEN
        RAISE EXCEPTION
          'po_receipt_invalid: line % carries unit_serials/external_serials '
          'but sku=% is tracked_by=% (only ''lot_and_serial'' accepts unit '
          'metadata)',
          v_idx, v_pl.sku_id, v_tracked_by USING ERRCODE = 'P0006';
      END IF;
    END IF;

    IF v_unit_serials IS NOT NULL THEN
      v_us_len := jsonb_array_length(v_unit_serials);
      IF v_us_len <> v_qty_received THEN
        RAISE EXCEPTION
          'po_receipt_invalid: line % unit_serials length % does not match '
          'qty_received %',
          v_idx, v_us_len, v_qty_received USING ERRCODE = 'P0006';
      END IF;
    END IF;

    IF v_external_serials IS NOT NULL THEN
      v_es_len := jsonb_array_length(v_external_serials);
      IF v_es_len <> v_qty_received THEN
        RAISE EXCEPTION
          'po_receipt_invalid: line % external_serials length % does not '
          'match qty_received %',
          v_idx, v_es_len, v_qty_received USING ERRCODE = 'P0006';
      END IF;
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

      IF v_cost_method = 'lot_fifo' THEN
        v_value_event := v_value_event || jsonb_build_object(
          'lot_code',            v_line->>'lot_code',
          'manufacture_date',    v_line->>'manufacture_date',
          'expiration_date',     v_line->>'expiration_date',
          'supplier_lot_number', v_line->>'supplier_lot_number',
          'quality_status',      v_line->>'quality_status',
          'attributes',          v_line->'attributes'
        );

        -- sxl2.3: forward serial arrays to apply_event's E2.5 block.
        -- The block reads 'unit_serials' / 'external_serials' top-level
        -- keys; only fires when v_tracked_by='lot_and_serial'.
        IF v_unit_serials IS NOT NULL THEN
          v_value_event := v_value_event || jsonb_build_object(
            'unit_serials', v_unit_serials
          );
        END IF;
        IF v_external_serials IS NOT NULL THEN
          v_value_event := v_value_event || jsonb_build_object(
            'external_serials', v_external_serials
          );
        END IF;
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
  UPDATE po_receipt_lines prl
     SET lot_id = il.lot_id
    FROM posting_lines pl
    JOIN inventory_lots il ON il.receipt_posting_line_id = pl.id
   WHERE prl.receipt_id = v_doc_id
     AND pl.document_line_id = prl.id
     AND pl.document_kind = 'po_receipt'
     AND pl.reason = 'po_receipt';

  -- sxl2.3: stamp po_receipt_lines.unit_ids from inventory_units
  -- created by apply_event's E2.5 block. Aggregates the unit_ids
  -- created against this receipt line's value-leg posting.
  UPDATE po_receipt_lines prl
     SET unit_ids = aggs.unit_ids
    FROM (
      SELECT pl.document_line_id AS prl_id,
             array_agg(iu.unit_id ORDER BY iu.unit_id) AS unit_ids
        FROM posting_lines pl
        JOIN inventory_units iu ON iu.receipt_posting_line_id = pl.id
       WHERE pl.document_kind = 'po_receipt'
         AND pl.reason = 'po_receipt'
       GROUP BY pl.document_line_id
    ) aggs
   WHERE prl.receipt_id = v_doc_id
     AND prl.id = aggs.prl_id;

  RETURN v_doc_id;
END;
$$;

-- ---------- 3. post_inventory_adjustment ----------

CREATE OR REPLACE FUNCTION post_inventory_adjustment(
  p_sku_id          UUID,
  p_location_id     UUID,
  p_qty_delta       BIGINT,
  p_unit_cost       BIGINT,
  p_currency        TEXT,
  p_inventory_class TEXT,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL,
  p_lot_metadata    JSONB DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id      UUID;
  v_doc_id           UUID;
  v_cost_method      cost_method;
  v_tracked_by       inventory_tracking;
  v_qty_acct         BIGINT;
  v_val_acct         BIGINT;
  v_void_qty         BIGINT;
  v_void_val         BIGINT;
  v_value_kind       TEXT;
  v_lock_first       BIGINT;
  v_lock_second      BIGINT;
  v_qty_balance      BIGINT;
  v_val_balance      BIGINT;
  v_effective_uc     BIGINT;
  v_qty_amount       BIGINT;
  v_val_amount       BIGINT;
  v_qty_debit        BIGINT;
  v_qty_credit       BIGINT;
  v_val_debit        BIGINT;
  v_val_credit       BIGINT;
  v_batch            JSONB;
  v_value_event      JSONB;
  v_qty_event        JSONB;
  v_needs_provisional_method TEXT := NULL;
  v_value_posting_line_id BIGINT;
  v_period_id        BIGINT;
  v_lot_code         TEXT;
  v_specific_lot_id  BIGINT;
  v_unit_serials     JSONB;
  v_external_serials JSONB;
  v_unit_ids_json    JSONB;
  v_unit_ids         BIGINT[];
  v_resolved_unit_ids BIGINT[];
  v_unit_count       INT;
  v_unit_serials_arr TEXT[];
  v_unit_check_lot   BIGINT;
  v_unit_check_date  DATE;
BEGIN
  SELECT id INTO v_existing_id FROM inventory_adjustments WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT cost_method, tracked_by INTO v_cost_method, v_tracked_by
    FROM skus WHERE id = p_sku_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'sku % not found', p_sku_id USING ERRCODE = 'P0010'; END IF;

  IF v_cost_method IN ('wac_periodic', 'wac_retroactive') AND p_inventory_class = 'wip' THEN
    RAISE EXCEPTION
      '% adjustment on inv_value_wip class not supported in Phase 1 '
      '(see acct-p7v Phase 2 Epic J: wac across WIP pools); sku=%',
      v_cost_method, p_sku_id USING ERRCODE = 'P0006';
  END IF;

  -- sxl2.3: pre-parse unit-related keys; reject on non-lot_and_serial
  -- SKU when supplied (catch caller bugs early).
  IF p_lot_metadata IS NOT NULL THEN
    v_unit_serials     := p_lot_metadata->'unit_serials';
    v_external_serials := p_lot_metadata->'external_serials';
    v_unit_ids_json    := p_lot_metadata->'unit_ids';

    IF (v_unit_serials IS NOT NULL OR v_external_serials IS NOT NULL
        OR v_unit_ids_json IS NOT NULL)
       AND v_tracked_by <> 'lot_and_serial' THEN
      RAISE EXCEPTION
        'inventory_adjustment_invalid: unit_serials/external_serials/'
        'unit_ids supplied but sku=% is tracked_by=% (only '
        '''lot_and_serial'' accepts unit metadata)',
        p_sku_id, v_tracked_by USING ERRCODE = 'P0006';
    END IF;

    -- XOR on issue path: cannot supply both unit_ids and unit_serials.
    IF p_qty_delta < 0
       AND v_unit_ids_json IS NOT NULL
       AND v_unit_serials IS NOT NULL THEN
      RAISE EXCEPTION
        'inventory_adjustment_invalid: cannot supply both unit_ids and '
        'unit_serials for -qty adjustment (XOR); sku=%',
        p_sku_id USING ERRCODE = 'P0006';
    END IF;
  END IF;

  SELECT id INTO v_qty_acct FROM accounts
   WHERE kind = 'stock_available' AND sku_id = p_sku_id AND location_id = p_location_id AND NOT is_closed;
  IF v_qty_acct IS NULL THEN
    RAISE EXCEPTION 'no open stock_available account for sku=% loc=%', p_sku_id, p_location_id USING ERRCODE = 'P0010';
  END IF;

  v_value_kind := 'inv_value_' || p_inventory_class;
  EXECUTE format('SELECT id FROM accounts WHERE kind = %L AND sku_id = $1 AND location_id = $2 AND currency = $3 AND NOT is_closed', v_value_kind)
    INTO v_val_acct USING p_sku_id, p_location_id, p_currency;
  IF v_val_acct IS NULL THEN
    RAISE EXCEPTION 'no open % account for sku=% loc=% ccy=%', v_value_kind, p_sku_id, p_location_id, p_currency USING ERRCODE = 'P0010';
  END IF;

  SELECT id INTO v_void_qty FROM accounts WHERE kind = 'creation_void' AND ledger_kind = 'qty' AND NOT is_closed;
  IF v_void_qty IS NULL THEN RAISE EXCEPTION 'no creation_void(qty) account configured' USING ERRCODE = 'P0010'; END IF;

  SELECT id INTO v_void_val FROM accounts WHERE kind = 'inv_adj_expense' AND ledger_kind = 'value' AND currency = p_currency AND NOT is_closed;
  IF v_void_val IS NULL THEN RAISE EXCEPTION 'no inv_adj_expense(value, ccy=%) account configured', p_currency USING ERRCODE = 'P0010'; END IF;

  CASE v_cost_method
  WHEN 'standard' THEN
    IF p_unit_cost IS NOT NULL THEN
      RAISE EXCEPTION 'standard SKU % has a fixed standard cost; do not pass p_unit_cost (got %)', p_sku_id, p_unit_cost USING ERRCODE = 'P0011';
    END IF;
    v_effective_uc := _resolve_standard_cost_at(p_sku_id, p_business_date);

  WHEN 'wac_perpetual' THEN
    v_lock_first := LEAST(v_qty_acct, v_val_acct);
    v_lock_second := GREATEST(v_qty_acct, v_val_acct);
    PERFORM 1 FROM accounts WHERE id = v_lock_first FOR UPDATE;
    PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;
    SELECT COALESCE(SUM(CASE WHEN t.debit_account_id = v_val_acct THEN t.qty WHEN t.credit_account_id = v_val_acct THEN -t.qty END), 0)
      INTO v_qty_balance FROM posting_lines t
     WHERE v_val_acct IN (t.debit_account_id, t.credit_account_id) AND t.qty IS NOT NULL;
    SELECT debits_total - credits_total INTO v_val_balance FROM accounts WHERE id = v_val_acct;
    IF p_qty_delta > 0 THEN
      IF p_unit_cost IS NULL THEN
        IF v_qty_balance <= 0 THEN
          RAISE EXCEPTION 'wac_perpetual SKU % at sku=% loc=% has empty pool (qty_balance=%); caller must pass p_unit_cost on first adjustment-in to seed', p_sku_id, p_sku_id, p_location_id, v_qty_balance USING ERRCODE = 'P0011';
        END IF;
        v_effective_uc := v_val_balance / v_qty_balance;
      ELSE
        v_effective_uc := p_unit_cost;
      END IF;
    ELSE
      IF p_unit_cost IS NOT NULL THEN
        RAISE EXCEPTION 'wac_perpetual depletion does not accept asserted unit_cost (got % on sku=% loc=%); use lot cost_method (acct-8gg) for asserted-cost-per-transaction needs', p_unit_cost, p_sku_id, p_location_id USING ERRCODE = 'P0011';
      END IF;
      IF v_qty_balance <= 0 THEN
        RAISE EXCEPTION 'wac_perpetual SKU % at sku=% loc=% has empty pool; cannot deplete', p_sku_id, p_sku_id, p_location_id USING ERRCODE = 'P0010';
      END IF;
      v_effective_uc := v_val_balance / v_qty_balance;
    END IF;

  WHEN 'wac_periodic', 'wac_retroactive' THEN
    v_lock_first := LEAST(v_qty_acct, v_val_acct);
    v_lock_second := GREATEST(v_qty_acct, v_val_acct);
    PERFORM 1 FROM accounts WHERE id = v_lock_first FOR UPDATE;
    PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;
    SELECT COALESCE(SUM(CASE WHEN t.debit_account_id = v_val_acct THEN t.qty WHEN t.credit_account_id = v_val_acct THEN -t.qty END), 0)
      INTO v_qty_balance FROM posting_lines t
     WHERE v_val_acct IN (t.debit_account_id, t.credit_account_id) AND t.qty IS NOT NULL;
    SELECT debits_total - credits_total INTO v_val_balance FROM accounts WHERE id = v_val_acct;
    IF p_qty_delta > 0 THEN
      IF p_unit_cost IS NULL THEN
        IF v_qty_balance <= 0 THEN
          RAISE EXCEPTION '% SKU % at sku=% loc=% has empty pool (qty_balance=%); caller must pass p_unit_cost on first adjustment-in to seed',
                          v_cost_method, p_sku_id, p_sku_id, p_location_id, v_qty_balance USING ERRCODE = 'P0011';
        END IF;
        v_effective_uc := v_val_balance / v_qty_balance;
      ELSE
        v_effective_uc := p_unit_cost;
      END IF;
    ELSE
      IF p_unit_cost IS NOT NULL THEN
        RAISE EXCEPTION '% depletion does not accept asserted unit_cost (got % on sku=% loc=%); use lot cost_method (acct-8gg) for asserted-cost-per-transaction needs',
                        v_cost_method, p_unit_cost, p_sku_id, p_location_id USING ERRCODE = 'P0011';
      END IF;
      IF v_qty_balance <= 0 THEN
        RAISE EXCEPTION '% SKU % at sku=% loc=% has empty pool; cannot deplete', v_cost_method, p_sku_id, p_sku_id, p_location_id USING ERRCODE = 'P0010';
      END IF;
      v_effective_uc := v_val_balance / v_qty_balance;
      v_needs_provisional_method := v_cost_method::TEXT;
    END IF;

  WHEN 'fifo' THEN
    IF p_inventory_class <> 'raw' THEN
      RAISE EXCEPTION
        'fifo adjustment on inv_value_% class not supported in MVP '
        '(see acct-xxrs W4 for FG-FIFO via post_so_ship); sku=%',
        p_inventory_class, p_sku_id USING ERRCODE = 'P0006';
    END IF;
    v_lock_first := LEAST(v_qty_acct, v_val_acct);
    v_lock_second := GREATEST(v_qty_acct, v_val_acct);
    PERFORM 1 FROM accounts WHERE id = v_lock_first FOR UPDATE;
    PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;
    IF p_qty_delta > 0 THEN
      IF p_unit_cost IS NULL THEN
        RAISE EXCEPTION
          'fifo adjustment-in requires p_unit_cost (sku=% loc=%); '
          'each layer carries its own asserted cost',
          p_sku_id, p_location_id USING ERRCODE = 'P0011';
      END IF;
      v_effective_uc := p_unit_cost;
    ELSE
      IF p_unit_cost IS NOT NULL THEN
        RAISE EXCEPTION
          'fifo depletion does not accept asserted unit_cost (got % '
          'on sku=% loc=%); FIFO walks layers',
          p_unit_cost, p_sku_id, p_location_id USING ERRCODE = 'P0011';
      END IF;
      SELECT COALESCE(SUM(cost_amount), 0)::BIGINT
        INTO v_val_amount
        FROM _fifo_walk_layers(p_sku_id, p_location_id, 1::SMALLINT,
                               abs(p_qty_delta)::NUMERIC);
      v_effective_uc := v_val_amount / abs(p_qty_delta);
    END IF;

  WHEN 'lot_fifo' THEN
    IF p_inventory_class <> 'raw' THEN
      RAISE EXCEPTION
        'lot_fifo adjustment on inv_value_% class not supported in MVP '
        '(see L4 for FG-lot via post_so_ship); sku=%',
        p_inventory_class, p_sku_id USING ERRCODE = 'P0006';
    END IF;
    v_lock_first := LEAST(v_qty_acct, v_val_acct);
    v_lock_second := GREATEST(v_qty_acct, v_val_acct);
    PERFORM 1 FROM accounts WHERE id = v_lock_first FOR UPDATE;
    PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;

    IF p_qty_delta > 0 THEN
      IF p_unit_cost IS NULL THEN
        RAISE EXCEPTION
          'lot_fifo adjustment-in requires p_unit_cost (sku=% loc=%); '
          'each lot carries its own asserted cost',
          p_sku_id, p_location_id USING ERRCODE = 'P0011';
      END IF;
      v_lot_code := p_lot_metadata->>'lot_code';
      IF v_lot_code IS NULL OR length(v_lot_code) = 0 THEN
        RAISE EXCEPTION
          'lot_fifo adjustment-in requires lot_code in p_lot_metadata '
          '(sku=% loc=%)',
          p_sku_id, p_location_id USING ERRCODE = 'P0022';
      END IF;
      v_effective_uc := p_unit_cost;

      -- sxl2.3: validate +qty unit/external serial lengths (when
      -- supplied) against |p_qty_delta|. Reject mismatches early;
      -- forwarded into event JSON for apply_event's E2.5 block.
      IF v_unit_serials IS NOT NULL THEN
        IF jsonb_array_length(v_unit_serials) <> abs(p_qty_delta) THEN
          RAISE EXCEPTION
            'inventory_adjustment_invalid: unit_serials length % does not '
            'match |qty_delta| %',
            jsonb_array_length(v_unit_serials), abs(p_qty_delta)
            USING ERRCODE = 'P0006';
        END IF;
      END IF;
      IF v_external_serials IS NOT NULL THEN
        IF jsonb_array_length(v_external_serials) <> abs(p_qty_delta) THEN
          RAISE EXCEPTION
            'inventory_adjustment_invalid: external_serials length % does '
            'not match |qty_delta| %',
            jsonb_array_length(v_external_serials), abs(p_qty_delta)
            USING ERRCODE = 'P0006';
        END IF;
      END IF;

    ELSE
      -- -qty path.
      IF p_unit_cost IS NOT NULL THEN
        RAISE EXCEPTION
          'lot_fifo depletion does not accept asserted unit_cost '
          '(got % on sku=% loc=%); cost is taken from the named lot',
          p_unit_cost, p_sku_id, p_location_id USING ERRCODE = 'P0011';
      END IF;

      -- sxl2.3: tracked_by='lot_and_serial' -qty requires explicit
      -- unit identification (Q3: pre-scanned units at point of
      -- consumption). Resolve unit_ids from either p_lot_metadata->
      -- 'unit_ids' (BIGINT array) or ->'unit_serials' (TEXT array).
      IF v_tracked_by = 'lot_and_serial' THEN
        IF v_unit_ids_json IS NOT NULL THEN
          SELECT array_agg((x)::BIGINT ORDER BY ord)
            INTO v_unit_ids
            FROM jsonb_array_elements_text(v_unit_ids_json)
                 WITH ORDINALITY AS t(x, ord);
        ELSIF v_unit_serials IS NOT NULL THEN
          SELECT array_agg(s ORDER BY ord)
            INTO v_unit_serials_arr
            FROM jsonb_array_elements_text(v_unit_serials)
                 WITH ORDINALITY AS t(s, ord);
        ELSE
          RAISE EXCEPTION
            'inventory_adjustment_invalid: tracked_by=''lot_and_serial'' '
            '-qty requires p_lot_metadata->''unit_ids'' or '
            '->''unit_serials'' to identify the consumed units; sku=%',
            p_sku_id USING ERRCODE = 'P0006';
        END IF;

        v_unit_count := abs(p_qty_delta)::INT;
        IF v_unit_ids IS NOT NULL THEN
          IF COALESCE(array_length(v_unit_ids, 1), 0) <> v_unit_count THEN
            RAISE EXCEPTION
              'inventory_adjustment_invalid: unit_ids length % does not '
              'match |qty_delta| %',
              COALESCE(array_length(v_unit_ids, 1), 0), v_unit_count
              USING ERRCODE = 'P0006';
          END IF;
          v_resolved_unit_ids := v_unit_ids;
        ELSE
          -- Resolve serials -> ids (look up active units by product +
          -- serial_no). Active = status IN (available, reserved,
          -- allocated, on_hold, returned).
          IF COALESCE(array_length(v_unit_serials_arr, 1), 0) <> v_unit_count THEN
            RAISE EXCEPTION
              'inventory_adjustment_invalid: unit_serials length % does '
              'not match |qty_delta| %',
              COALESCE(array_length(v_unit_serials_arr, 1), 0), v_unit_count
              USING ERRCODE = 'P0006';
          END IF;
          SELECT array_agg(unit_id ORDER BY arr.ord)
            INTO v_resolved_unit_ids
            FROM unnest(v_unit_serials_arr) WITH ORDINALITY AS arr(s, ord)
            JOIN inventory_units iu
              ON iu.product_id = p_sku_id
             AND iu.serial_no = arr.s
             AND iu.status IN ('available', 'reserved', 'allocated',
                               'on_hold', 'returned');
          IF COALESCE(array_length(v_resolved_unit_ids, 1), 0)
             <> v_unit_count THEN
            RAISE EXCEPTION
              'inventory_adjustment_invalid: one or more unit_serials '
              'did not resolve to an active unit for sku=% (resolved %/%)',
              p_sku_id,
              COALESCE(array_length(v_resolved_unit_ids, 1), 0),
              v_unit_count
              USING ERRCODE = 'P0006';
          END IF;
        END IF;

        -- Lock the units FOR UPDATE; validate state (active, same
        -- product, same location, same lot).
        PERFORM 1 FROM inventory_units
         WHERE unit_id = ANY(v_resolved_unit_ids)
         ORDER BY unit_id
           FOR UPDATE;

        SELECT MIN(lot_id), MAX(lot_id),
               MIN(lot_receipt_date), MAX(lot_receipt_date),
               COUNT(*)
          INTO v_unit_check_lot, v_specific_lot_id,
               v_unit_check_date, v_unit_check_date,
               v_unit_count
          FROM inventory_units
         WHERE unit_id = ANY(v_resolved_unit_ids)
           AND product_id = p_sku_id
           AND current_location_id = p_location_id
           AND status IN ('available', 'reserved', 'allocated',
                          'on_hold', 'returned');

        IF v_unit_count <> COALESCE(array_length(v_resolved_unit_ids, 1), 0) THEN
          RAISE EXCEPTION
            'inventory_adjustment_invalid: one or more unit_ids are not '
            'active / not at sku=% / not at loc=% (matched %/%)',
            p_sku_id, p_location_id, v_unit_count,
            COALESCE(array_length(v_resolved_unit_ids, 1), 0)
            USING ERRCODE = 'P0006';
        END IF;
        IF v_unit_check_lot <> v_specific_lot_id THEN
          RAISE EXCEPTION
            'inventory_adjustment_invalid: unit_ids span multiple lots '
            '(% to %); one adjustment must consume from a single lot',
            v_unit_check_lot, v_specific_lot_id USING ERRCODE = 'P0006';
        END IF;
      ELSE
        -- Non-lot_and_serial: existing lot_fifo behaviour.
        -- Operator must supply explicit lot_id via p_lot_metadata.
        v_specific_lot_id := (p_lot_metadata->>'lot_id')::BIGINT;
        IF v_specific_lot_id IS NULL THEN
          RAISE EXCEPTION
            'lot_fifo adjustment-out requires explicit lot_id in '
            'p_lot_metadata (sku=% loc=%); FIFO default not provided '
            'for adjustments — operator must specify the lot',
            p_sku_id, p_location_id USING ERRCODE = 'P0022';
        END IF;
      END IF;

      SELECT COALESCE(SUM(cost_amount), 0)::BIGINT
        INTO v_val_amount
        FROM _lot_walk_layers(p_sku_id, p_location_id, 1::SMALLINT,
                              abs(p_qty_delta)::NUMERIC, v_specific_lot_id);
      v_effective_uc := v_val_amount / abs(p_qty_delta);
    END IF;

  WHEN 'lot' THEN
    RAISE EXCEPTION 'cost_method_not_implemented: % (sku=%); see acct-uze',
      v_cost_method, p_sku_id USING ERRCODE = 'P0006';

  ELSE
    RAISE EXCEPTION 'unknown cost_method % for sku=%', v_cost_method, p_sku_id USING ERRCODE = 'P0011';
  END CASE;

  v_qty_amount := abs(p_qty_delta);
  IF v_val_amount IS NULL THEN
    v_val_amount := v_qty_amount * v_effective_uc;
  END IF;

  IF p_qty_delta > 0 THEN
    v_qty_debit := v_qty_acct; v_qty_credit := v_void_qty;
    v_val_debit := v_val_acct; v_val_credit := v_void_val;
  ELSE
    v_qty_debit := v_void_qty; v_qty_credit := v_qty_acct;
    v_val_debit := v_void_val; v_val_credit := v_val_acct;
  END IF;

  INSERT INTO inventory_adjustments (
    sku_id, location_id, qty_delta, unit_cost, currency,
    inventory_class, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_sku_id, p_location_id, p_qty_delta, v_effective_uc, p_currency,
    p_inventory_class, p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM inventory_adjustments WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  v_qty_event := jsonb_build_object(
    'reason','inventory_adjustment','document_kind','inventory_adjustment_doc',
    'document_id',v_doc_id,
    'debit_account_id',v_qty_debit,'credit_account_id',v_qty_credit,
    'amount',v_qty_amount,'qty',v_qty_amount,
    'business_date',p_business_date,'idempotency_key',gen_random_uuid(),
    'posted_by',p_posted_by
  );

  IF v_val_amount > 0 THEN
    v_value_event := jsonb_build_object(
      'reason','inventory_adjustment','document_kind','inventory_adjustment_doc',
      'document_id',v_doc_id,
      'debit_account_id',v_val_debit,'credit_account_id',v_val_credit,
      'amount',v_val_amount,'qty',v_qty_amount,
      'business_date',p_business_date,'idempotency_key',gen_random_uuid(),
      'posted_by',p_posted_by
    );

    IF v_cost_method = 'lot_fifo' THEN
      IF p_qty_delta > 0 THEN
        v_value_event := v_value_event || jsonb_build_object(
          'lot_code',            p_lot_metadata->>'lot_code',
          'manufacture_date',    p_lot_metadata->>'manufacture_date',
          'expiration_date',     p_lot_metadata->>'expiration_date',
          'supplier_lot_number', p_lot_metadata->>'supplier_lot_number',
          'quality_status',      p_lot_metadata->>'quality_status',
          'attributes',          p_lot_metadata->'attributes'
        );

        -- sxl2.3: forward unit serial arrays to apply_event's E2.5 block.
        IF v_unit_serials IS NOT NULL THEN
          v_value_event := v_value_event || jsonb_build_object(
            'unit_serials', v_unit_serials
          );
        END IF;
        IF v_external_serials IS NOT NULL THEN
          v_value_event := v_value_event || jsonb_build_object(
            'external_serials', v_external_serials
          );
        END IF;
      ELSE
        v_value_event := v_value_event || jsonb_build_object(
          'lot_id', v_specific_lot_id
        );
      END IF;
    END IF;

    v_batch := jsonb_build_array(v_qty_event, v_value_event);
  ELSE
    v_batch := jsonb_build_array(v_qty_event);
  END IF;

  PERFORM post_posting_lines(v_batch, FALSE);

  -- Stamp inventory_adjustments.lot_id post-PERFORM.
  IF v_cost_method = 'lot_fifo' THEN
    IF p_qty_delta > 0 THEN
      UPDATE inventory_adjustments ia
         SET lot_id = il.lot_id
        FROM posting_lines pl
        JOIN inventory_lots il ON il.receipt_posting_line_id = pl.id
       WHERE ia.id = v_doc_id
         AND pl.document_id = v_doc_id
         AND pl.reason = 'inventory_adjustment';
    ELSE
      UPDATE inventory_adjustments
         SET lot_id = v_specific_lot_id
       WHERE id = v_doc_id;
    END IF;
  END IF;

  -- sxl2.3: per-unit lifecycle for tracked_by='lot_and_serial'.
  IF v_tracked_by = 'lot_and_serial' THEN
    SELECT id INTO v_value_posting_line_id
      FROM posting_lines
     WHERE document_id = v_doc_id
       AND reason = 'inventory_adjustment'
       AND (debit_account_id = v_val_acct OR credit_account_id = v_val_acct);

    IF p_qty_delta > 0 THEN
      -- +qty: units already created by apply_event's E2.5 block; stamp
      -- the audit column by aggregating the created units.
      UPDATE inventory_adjustments
         SET unit_ids = aggs.unit_ids
        FROM (
          SELECT array_agg(unit_id ORDER BY unit_id) AS unit_ids
            FROM inventory_units
           WHERE receipt_posting_line_id = v_value_posting_line_id
        ) aggs
       WHERE id = v_doc_id;
    ELSE
      -- -qty: flip the resolved units to 'consumed' and emit a
      -- per-unit type=2 (issue) event tied to the value-leg posting.
      UPDATE inventory_units
         SET status = 'consumed',
             updated_at = clock_timestamp()
       WHERE unit_id = ANY(v_resolved_unit_ids);

      INSERT INTO inventory_unit_events (
        unit_id, event_date, event_type,
        posting_line_id, new_status, location_id_from
      )
      SELECT unit_id, p_business_date, 2,
             v_value_posting_line_id, 'consumed', p_location_id
        FROM unnest(v_resolved_unit_ids) AS unit_id;

      UPDATE inventory_adjustments
         SET unit_ids = v_resolved_unit_ids
       WHERE id = v_doc_id;
    END IF;
  END IF;

  IF v_needs_provisional_method IS NOT NULL THEN
    SELECT id INTO v_value_posting_line_id FROM posting_lines WHERE document_id = v_doc_id AND reason = 'inventory_adjustment' AND credit_account_id = v_val_acct;
    SELECT id INTO v_period_id FROM periods WHERE opens_at <= p_business_date AND closes_at >= p_business_date;
    INSERT INTO posting_lines_provisional (posting_line_id, period_id, cost_method, qty)
    VALUES (v_value_posting_line_id, v_period_id, v_needs_provisional_method::cost_method, v_qty_amount);
  END IF;

  RETURN v_doc_id;
END;
$$;
