-- acct-fii — post_cost_adjustment reads class-specific qty divisor.
--
-- BUG. mig 0024 reads v_pool_qty from stock_available(sku, location),
-- which is NOT class-segmented (one row per (sku, loc) regardless of
-- inventory_class). When a SKU has both raw and fg pools at the same
-- location — a common ERP layout (contract manufacturers, small
-- shops, anywhere a SKU can be both purchased input and sold output)
-- — stock_available qty is the COMBINED qty across classes. The
-- function then computes:
--   v_new_value = target_unit_cost × v_pool_qty (combined)
--   v_delta     = v_new_value - v_pool_value (class-specific)
-- The class-specific value pool ends up over- or under-adjusted by
-- the OTHER class's qty share.
--
-- CONCRETE FAILURE.
--   SKU X at loc L: 100 raw @ $10 ($1000 inv_value_raw),
--                   50  fg  @ $20 ($1000 inv_value_fg).
--   stock_available(X, L) = 150 (combined).
--   post_cost_adjustment(X, L, USD, 'raw', target=$12):
--     v_pool_qty   = 150 (BUG — should be 100, raw class only)
--     v_pool_value = $1000 (raw class, correct)
--     v_new_value  = 150 × $12 = $1800
--     v_delta      = $1800 - $1000 = $800
--   Posts: dr inv_value_raw / cr variance_cost_adjustment $800.
--   Result: inv_value_raw = $1800 over raw_qty 100 → avg $18, NOT $12.
--   The function over-shoots by exactly the OTHER class's qty share
--   times the target unit cost.
--
-- FIX. Replace the stock_available qty read with the per-class qty
-- SUM signed by direction on the value pool — same pattern acct-1vr /
-- mig 0030 established for WAC math, used in tier 1/2 of acct-rgb's
-- _wo_emit_bom_lines wac_perpetual / wac_periodic branches:
--
--   SELECT COALESCE(SUM(
--     CASE WHEN t.debit_account_id  = v_val_acct THEN  t.qty
--          WHEN t.credit_account_id = v_val_acct THEN -t.qty END
--   ), 0) INTO v_pool_qty
--     FROM transfers t
--    WHERE v_val_acct IN (t.debit_account_id, t.credit_account_id)
--      AND t.qty IS NOT NULL;
--
-- This sums signed qty on transfers TOUCHING the class-specific value
-- account: PO receipts (debit value pool) add qty, issues (credit
-- value pool) subtract qty. Class-isolated.
--
-- LOCK ORDER. v_val_acct is already locked FOR UPDATE before the
-- read. Holding the val_acct lock under exclusive mode is sufficient
-- to ensure the per-class qty SUM is consistent across concurrent
-- posters (every transfer that affects this divisor must touch
-- v_val_acct, which the lock serializes). The stock_available
-- account remains FOR UPDATE-locked too — not for the divisor (no
-- longer used) but to maintain the broader lock-order invariant
-- with concurrent post_inventory_adjustment, which holds both.
--
-- WHY pool_qty CAN BE NULL/<=0 NOW. The signed SUM on a fresh class
-- (no PO receipts yet, but qty might exist via cross-class moves —
-- shouldn't happen in practice but defensively) returns 0 with
-- COALESCE. The existing pool_qty <= 0 → P0010 check catches it.
--
-- AUDIT IMPLICATIONS. inventory_cost_adjustments.pool_qty stores the
-- divisor at adjustment time. Pre-fix this was the cross-class qty;
-- post-fix it's the per-class qty. Historical rows are not migrated
-- (Phase 0/1 has no production data). The column meaning changes
-- subtly but the column itself remains. Audit queries that compare
-- pool_qty against a sum of inflows on the same class will now match
-- correctly.

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
  -- Fast-path replay check.
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

  -- Lock qty + value accounts in ascending id order. Same lock pattern
  -- as post_inventory_adjustment so the two functions interleave safely.
  -- stock_available is locked even though no qty leg is posted and the
  -- divisor no longer reads from it — held to maintain lock-order
  -- invariant with concurrent inventory_adjustment.
  v_lock_first  := LEAST(v_qty_acct, v_val_acct);
  v_lock_second := GREATEST(v_qty_acct, v_val_acct);
  PERFORM 1 FROM accounts WHERE id = v_lock_first  FOR UPDATE;
  PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;

  -- Per-class qty divisor (acct-fii / acct-1vr pattern). SUM signed
  -- by direction on the class-specific value pool: PO receipts (debit
  -- value) add qty, issues (credit value) subtract. Class-isolated;
  -- works correctly when raw + fg share a location.
  SELECT COALESCE(SUM(
    CASE
      WHEN t.debit_account_id  = v_val_acct THEN  t.qty
      WHEN t.credit_account_id = v_val_acct THEN -t.qty
    END
  ), 0) INTO v_pool_qty
    FROM transfers t
   WHERE v_val_acct IN (t.debit_account_id, t.credit_account_id)
     AND t.qty IS NOT NULL;

  SELECT debits_total - credits_total INTO v_pool_value
    FROM accounts WHERE id = v_val_acct;

  IF v_pool_qty <= 0 THEN
    RAISE EXCEPTION
      'cost_adjustment requires non-empty class pool; sku=% loc=% class=% '
      'has per-class qty=%',
      p_sku_id, p_location_id, p_inventory_class, v_pool_qty
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

COMMENT ON FUNCTION post_cost_adjustment(
  UUID, UUID, TEXT, TEXT, BIGINT, DATE, UUID, UUID, TEXT
) IS
  'cost_adjustment workflow (Phase 1 Epic D, mig 0024). acct-fii (mig '
  '0069): qty divisor reads per-class signed SUM on the value-account '
  'transfer log instead of stock_available.balance. Class-isolated when '
  'raw + fg share a location; previously over-adjusted by the other '
  'class''s qty contribution.';
