-- Down for L2: drop the 11-arg post_inventory_adjustment, restore
-- mig 0035's 10-arg signature and body verbatim, drop the
-- inventory_adjustments.lot_id audit column.

DROP FUNCTION IF EXISTS post_inventory_adjustment(
  UUID, UUID, BIGINT, BIGINT, TEXT, TEXT, DATE, UUID, UUID, TEXT, JSONB
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
  p_notes           TEXT DEFAULT NULL
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
  v_needs_provisional_method TEXT := NULL;
  v_value_posting_line_id BIGINT;
  v_period_id        BIGINT;
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

  IF v_val_amount > 0 THEN
    v_batch := jsonb_build_array(
      jsonb_build_object('reason','inventory_adjustment','document_kind','inventory_adjustment_doc','document_id',v_doc_id,'debit_account_id',v_qty_debit,'credit_account_id',v_qty_credit,'amount',v_qty_amount,'qty',v_qty_amount,'business_date',p_business_date,'idempotency_key',gen_random_uuid(),'posted_by',p_posted_by),
      jsonb_build_object('reason','inventory_adjustment','document_kind','inventory_adjustment_doc','document_id',v_doc_id,'debit_account_id',v_val_debit,'credit_account_id',v_val_credit,'amount',v_val_amount,'qty',v_qty_amount,'business_date',p_business_date,'idempotency_key',gen_random_uuid(),'posted_by',p_posted_by)
    );
  ELSE
    v_batch := jsonb_build_array(
      jsonb_build_object('reason','inventory_adjustment','document_kind','inventory_adjustment_doc','document_id',v_doc_id,'debit_account_id',v_qty_debit,'credit_account_id',v_qty_credit,'amount',v_qty_amount,'qty',v_qty_amount,'business_date',p_business_date,'idempotency_key',gen_random_uuid(),'posted_by',p_posted_by)
    );
  END IF;

  PERFORM post_posting_lines(v_batch, FALSE);

  IF v_needs_provisional_method IS NOT NULL THEN
    SELECT id INTO v_value_posting_line_id FROM posting_lines WHERE document_id = v_doc_id AND reason = 'inventory_adjustment' AND credit_account_id = v_val_acct;
    SELECT id INTO v_period_id FROM periods WHERE opens_at <= p_business_date AND closes_at >= p_business_date;
    INSERT INTO posting_lines_provisional (posting_line_id, period_id, cost_method, qty)
    VALUES (v_value_posting_line_id, v_period_id, v_needs_provisional_method::cost_method, v_qty_amount);
  END IF;

  RETURN v_doc_id;
END;
$$;

DROP INDEX IF EXISTS inventory_adjustments_lot;
ALTER TABLE inventory_adjustments DROP COLUMN IF EXISTS lot_id;
