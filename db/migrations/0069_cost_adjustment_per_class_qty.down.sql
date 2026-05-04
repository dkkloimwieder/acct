-- acct-fii down — restore mig 0024 body of post_cost_adjustment with
-- the cross-class stock_available qty divisor.

CREATE OR REPLACE FUNCTION post_cost_adjustment(
  p_sku_id           UUID,
  p_location_id      UUID,
  p_currency         TEXT,
  p_inventory_class  TEXT,
  p_target_unit_cost BIGINT,
  p_business_date    DATE,
  p_posted_by        UUID,
  p_idempotency_key  UUID,
  p_notes            TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id  UUID;
  v_doc_id       UUID;
  v_cost_method  cost_method;
  v_qty_acct     BIGINT;
  v_val_acct     BIGINT;
  v_var_acct     BIGINT;
  v_value_kind   TEXT;
  v_lock_first   BIGINT;
  v_lock_second  BIGINT;
  v_pool_qty     BIGINT;
  v_pool_value   BIGINT;
  v_prior_unit   BIGINT;
  v_new_value    BIGINT;
  v_delta        BIGINT;
  v_amount       BIGINT;
  v_debit        BIGINT;
  v_credit       BIGINT;
  v_batch        JSONB;
BEGIN
  SELECT id INTO v_existing_id
    FROM inventory_cost_adjustments
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN
    RETURN v_existing_id;
  END IF;

  IF p_target_unit_cost < 0 THEN
    RAISE EXCEPTION 'p_target_unit_cost must be >= 0 (got %)', p_target_unit_cost
      USING ERRCODE = '23514';
  END IF;

  SELECT cost_method INTO v_cost_method FROM skus WHERE id = p_sku_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'sku % not found', p_sku_id USING ERRCODE = 'P0010';
  END IF;

  CASE v_cost_method
  WHEN 'standard' THEN
    RAISE EXCEPTION
      'cost_adjustment not applicable to standard SKU % — to change a '
      'standard SKU''s cost, update skus.standard_cost via a separate '
      'workflow (not yet implemented)',
      p_sku_id USING ERRCODE = 'P0011';
  WHEN 'wac_perpetual' THEN
    NULL;
  WHEN 'wac_periodic' THEN
    RAISE EXCEPTION
      'cost_method_not_implemented: cost_adjustment on wac_periodic '
      'requires period-close machinery (acct-s6n + acct-qfj); sku=%',
      p_sku_id USING ERRCODE = 'P0006';
  WHEN 'wac_retroactive' THEN
    RAISE EXCEPTION
      'cost_method_not_implemented: cost_adjustment on wac_retroactive '
      'requires period-close machinery (acct-s6n + acct-9tw); sku=%',
      p_sku_id USING ERRCODE = 'P0006';
  WHEN 'fifo', 'lot' THEN
    RAISE EXCEPTION
      'cost_method_not_implemented: cost_adjustment on % SKU; see acct-8gg; sku=%',
      v_cost_method, p_sku_id USING ERRCODE = 'P0006';
  ELSE
    RAISE EXCEPTION 'unknown cost_method % for sku=%', v_cost_method, p_sku_id
      USING ERRCODE = 'P0011';
  END CASE;

  SELECT id INTO v_qty_acct
    FROM accounts
   WHERE kind        = 'stock_available'
     AND sku_id      = p_sku_id
     AND location_id = p_location_id
     AND NOT is_closed;
  IF v_qty_acct IS NULL THEN
    RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                    p_sku_id, p_location_id
      USING ERRCODE = 'P0010';
  END IF;

  v_value_kind := 'inv_value_' || p_inventory_class;
  EXECUTE format(
    'SELECT id FROM accounts
      WHERE kind = %L AND sku_id = $1 AND location_id = $2
        AND currency = $3 AND NOT is_closed',
    v_value_kind
  )
  INTO v_val_acct
  USING p_sku_id, p_location_id, p_currency;
  IF v_val_acct IS NULL THEN
    RAISE EXCEPTION 'no open % account for sku=% loc=% ccy=%',
                    v_value_kind, p_sku_id, p_location_id, p_currency
      USING ERRCODE = 'P0010';
  END IF;

  SELECT id INTO v_var_acct
    FROM accounts
   WHERE kind = 'variance_cost_adjustment' AND ledger_kind = 'value'
     AND currency = p_currency AND NOT is_closed;
  IF v_var_acct IS NULL THEN
    RAISE EXCEPTION 'no variance_cost_adjustment(value, ccy=%) account configured',
                    p_currency
      USING ERRCODE = 'P0010';
  END IF;

  v_lock_first  := LEAST(v_qty_acct, v_val_acct);
  v_lock_second := GREATEST(v_qty_acct, v_val_acct);
  PERFORM 1 FROM accounts WHERE id = v_lock_first  FOR UPDATE;
  PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;

  SELECT debits_total - credits_total INTO v_pool_qty
    FROM accounts WHERE id = v_qty_acct;
  SELECT debits_total - credits_total INTO v_pool_value
    FROM accounts WHERE id = v_val_acct;

  IF v_pool_qty <= 0 THEN
    RAISE EXCEPTION
      'cost_adjustment requires non-empty pool; sku=% loc=% has qty=%',
      p_sku_id, p_location_id, v_pool_qty
      USING ERRCODE = 'P0010';
  END IF;

  v_prior_unit := v_pool_value / v_pool_qty;
  v_new_value  := p_target_unit_cost * v_pool_qty;
  v_delta      := v_new_value - v_pool_value;

  INSERT INTO inventory_cost_adjustments (
    sku_id, location_id, currency, inventory_class,
    prior_unit_cost, target_unit_cost, delta_value, pool_qty,
    business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_sku_id, p_location_id, p_currency, p_inventory_class,
    v_prior_unit, p_target_unit_cost, v_delta, v_pool_qty,
    p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id
      FROM inventory_cost_adjustments
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  IF v_delta = 0 THEN
    RETURN v_doc_id;
  END IF;

  IF v_delta > 0 THEN
    v_debit  := v_val_acct;
    v_credit := v_var_acct;
    v_amount := v_delta;
  ELSE
    v_debit  := v_var_acct;
    v_credit := v_val_acct;
    v_amount := -v_delta;
  END IF;

  v_batch := jsonb_build_array(
    jsonb_build_object(
      'reason',            'cost_adjustment',
      'document_kind',     'inventory_cost_adjustment',
      'document_id',       v_doc_id,
      'debit_account_id',  v_debit,
      'credit_account_id', v_credit,
      'amount',            v_amount,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'posted_by',         p_posted_by
    )
  );

  PERFORM post_transfers(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;
