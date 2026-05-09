-- ============================================================
-- Phase E2 follow-up L2 (acct-b0j1, sub-issue of acct-uze).
--
-- Wrapper integration for lot tracking — second of four (mirrors
-- FIFO W2 from acct-8skl, mig 0035).
--
-- Adds a 'lot_fifo' WHEN branch to post_inventory_adjustment:
--
--   +qty (adjustment-in): caller MUST supply p_unit_cost (each lot
--     carries its own asserted cost) AND p_lot_metadata->>'lot_code'.
--     Optional metadata (manufacture_date / expiration_date /
--     supplier_lot_number / quality_status / attributes) flows into
--     the value-leg event JSON; apply_event's E2 block creates the
--     inventory_lots row.
--
--   -qty (adjustment-out): caller MUST NOT supply p_unit_cost AND
--     MUST supply p_lot_metadata->>'lot_id' (explicit lot pin —
--     FIFO default deliberately disallowed for adjustments per L2
--     design call: operator-driven adjustments require explicit lot
--     intent; FIFO/FEFO walks are reserved for production paths
--     rm_issue_to_wo and post_so_ship). Walks the named lot under
--     FOR UPDATE; emits value-leg with the walked total as the
--     caller-supplied amount. Reason 'inventory_adjustment' is not
--     in the dispatcher's cost-event list so the amount stands.
--     apply_event's _lot_write_issues re-walks the lot under the
--     same locks (same txn) and writes inventory_lot_events 'issue'
--     rows whose qty_delta sums match per-row by construction.
--
-- v_effective_uc on adjustment-out is the weighted average
-- (walked_total / abs(qty_delta)). It's a BIGINT audit field on
-- inventory_adjustments.unit_cost; per-event truth lives in
-- inventory_lot_events.
--
-- inventory_adjustments.lot_id BIGINT NULL audit column added;
-- stamped post-PERFORM (inflow: lookup created lot via
-- inventory_lots.receipt_posting_line_id; outflow: stamp the
-- caller-supplied v_specific_lot_id).
--
-- MVP scope restriction: lot_fifo adjustment supports
-- inventory_class='raw' only. FG / WIP raise P0006 (deferred to L4).
--
-- Signature change: adds p_lot_metadata JSONB DEFAULT NULL at the
-- end. Requires DROP FUNCTION + CREATE (PG forbids parameter changes
-- via CREATE OR REPLACE). Existing positional-bound callers (the
-- t1 binaries) skip the new param via DEFAULT NULL — non-breaking.
--
-- 'lot' (legacy enum value) still raises P0006.
-- ============================================================

ALTER TABLE inventory_adjustments ADD COLUMN lot_id BIGINT;

CREATE INDEX inventory_adjustments_lot
  ON inventory_adjustments (lot_id) WHERE lot_id IS NOT NULL;

COMMENT ON COLUMN inventory_adjustments.lot_id IS
  'Audit pointer: lot_id of the inventory_lots row created (+qty) or '
  'depleted (-qty) by this adjustment. NULL for non-lot SKUs. Stamped '
  'after post_posting_lines returns.';

DROP FUNCTION IF EXISTS post_inventory_adjustment(
  UUID, UUID, BIGINT, BIGINT, TEXT, TEXT, DATE, UUID, UUID, TEXT
);

CREATE FUNCTION post_inventory_adjustment(
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
BEGIN
  SELECT id INTO v_existing_id FROM inventory_adjustments WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT cost_method INTO v_cost_method FROM skus WHERE id = p_sku_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'sku % not found', p_sku_id USING ERRCODE = 'P0010'; END IF;

  IF v_cost_method IN ('wac_periodic', 'wac_retroactive') AND p_inventory_class = 'wip' THEN
    RAISE EXCEPTION
      '% adjustment on inv_value_wip class not supported in Phase 1 '
      '(see acct-p7v Phase 2 Epic J: wac across WIP pools); sku=%',
      v_cost_method, p_sku_id USING ERRCODE = 'P0006';
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
    -- FIFO at MVP supports inv_value_raw layers only.
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
    -- L2: lot_fifo adjustment supports inv_value_raw only at MVP.
    -- FG / WIP deferred to L4 (post_so_ship FG-lot end-to-end).
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
      -- Adjustment-in: caller MUST supply p_unit_cost AND
      -- p_lot_metadata->>'lot_code'. Optional metadata flows through.
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
    ELSE
      -- Adjustment-out: caller MUST supply p_lot_metadata->>'lot_id'
      -- (explicit pin — no FIFO default for adjustments per L2 design;
      -- operator-driven and lot intent must be explicit).
      IF p_unit_cost IS NOT NULL THEN
        RAISE EXCEPTION
          'lot_fifo depletion does not accept asserted unit_cost '
          '(got % on sku=% loc=%); cost is taken from the named lot',
          p_unit_cost, p_sku_id, p_location_id USING ERRCODE = 'P0011';
      END IF;
      v_specific_lot_id := (p_lot_metadata->>'lot_id')::BIGINT;
      IF v_specific_lot_id IS NULL THEN
        RAISE EXCEPTION
          'lot_fifo adjustment-out requires explicit lot_id in '
          'p_lot_metadata (sku=% loc=%); FIFO default not provided '
          'for adjustments — operator must specify the lot',
          p_sku_id, p_location_id USING ERRCODE = 'P0022';
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
  -- FIFO -qty / lot_fifo -qty branches set v_val_amount from the
  -- layer/lot walk. All other branches leave it NULL through CASE;
  -- recompute from v_qty_amount × v_effective_uc here.
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

    -- Forward lot metadata for lot_fifo. Apply_event's E2 block reads
    -- lot_code (REQUIRED for inflow) + optional metadata, OR lot_id
    -- (REQUIRED for outflow), from event JSON top-level keys.
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
      -- Inflow: look up the lot apply_event just created.
      UPDATE inventory_adjustments ia
         SET lot_id = il.lot_id
        FROM posting_lines pl
        JOIN inventory_lots il ON il.receipt_posting_line_id = pl.id
       WHERE ia.id = v_doc_id
         AND pl.document_id = v_doc_id
         AND pl.reason = 'inventory_adjustment';
    ELSE
      -- Outflow: stamp the operator-supplied specific lot id.
      UPDATE inventory_adjustments
         SET lot_id = v_specific_lot_id
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
