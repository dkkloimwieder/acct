-- Down: revert post_standard_cost_roll to mig 0071's body (8-arg
-- signature, WIP gate trips unconditionally, no WIP revaluation).
-- Drop the 9-arg overload first since CREATE OR REPLACE can't change
-- the parameter list. account_kind enum value
-- 'variance_wip_revaluation' is left in place per project convention
-- (ALTER TYPE DROP VALUE not supported).

DROP FUNCTION IF EXISTS post_standard_cost_roll(
  UUID, BIGINT, DATE, DATE, UUID, UUID, TEXT, BIGINT, BOOLEAN
);

CREATE OR REPLACE FUNCTION post_standard_cost_roll(
  p_sku_id            UUID,
  p_new_cost          BIGINT,
  p_effective_at      DATE,
  p_business_date     DATE,
  p_posted_by         UUID,
  p_idempotency_key   UUID,
  p_notes             TEXT   DEFAULT NULL,
  p_expected_old_cost BIGINT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id      UUID;
  v_doc_id           UUID;
  v_cost_method      cost_method;
  v_max_effective    DATE;
  v_prior            BIGINT;
  v_wip_count        BIGINT;
  v_var_acct         BIGINT;
  v_pool_record      RECORD;
  v_pool_qty         BIGINT;
  v_total_qty        BIGINT := 0;
  v_total_delta      BIGINT := 0;
  v_delta            BIGINT;
  v_amount           BIGINT;
  v_debit            BIGINT;
  v_credit           BIGINT;
  v_lock_ids         BIGINT[];
  v_lock_id          BIGINT;
  v_batch            JSONB := '[]'::JSONB;
  v_future_dated     BOOLEAN;
BEGIN
  SELECT id INTO v_existing_id
    FROM inventory_standard_cost_rolls
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN
    RETURN v_existing_id;
  END IF;

  IF p_new_cost < 0 THEN
    RAISE EXCEPTION 'p_new_cost must be >= 0 (got %)', p_new_cost
      USING ERRCODE = '23514';
  END IF;

  SELECT cost_method INTO v_cost_method
    FROM skus WHERE id = p_sku_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'sku % not found', p_sku_id USING ERRCODE = 'P0010';
  END IF;

  CASE v_cost_method
  WHEN 'standard' THEN
    NULL;
  WHEN 'wac_perpetual' THEN
    RAISE EXCEPTION
      'standard_cost_roll not applicable to wac_perpetual SKU % — use '
      'post_cost_adjustment for WAC pools (Epic D / acct-14m)',
      p_sku_id USING ERRCODE = 'P0011';
  WHEN 'wac_periodic' THEN
    RAISE EXCEPTION
      'standard_cost_roll not applicable to wac_periodic SKU % — use '
      'post_cost_adjustment for WAC pools (Epic D / acct-14m)',
      p_sku_id USING ERRCODE = 'P0011';
  WHEN 'wac_retroactive' THEN
    RAISE EXCEPTION
      'standard_cost_roll not applicable to wac_retroactive SKU % — use '
      'post_cost_adjustment for WAC pools (Epic D / acct-14m)',
      p_sku_id USING ERRCODE = 'P0011';
  WHEN 'fifo', 'lot' THEN
    RAISE EXCEPTION
      'cost_method_not_implemented: standard_cost_roll on % SKU %; see acct-8gg',
      v_cost_method, p_sku_id USING ERRCODE = 'P0006';
  ELSE
    RAISE EXCEPTION 'unknown cost_method % for sku=%', v_cost_method, p_sku_id
      USING ERRCODE = 'P0011';
  END CASE;

  SELECT MAX(effective_at) INTO v_max_effective
    FROM standard_costs WHERE sku_id = p_sku_id;

  IF v_max_effective IS NOT NULL AND p_effective_at <= v_max_effective THEN
    RAISE EXCEPTION
      'retroactive_std_cost_roll_blocked: sku=% has standard_costs row at '
      'effective_at=%; p_effective_at=% must be strictly greater. '
      'Retroactive standard cost corrections are not supported in Phase 1.',
      p_sku_id, v_max_effective, p_effective_at
      USING ERRCODE = 'P0019';
  END IF;

  BEGIN
    v_prior := resolve_standard_cost_at(p_sku_id, p_business_date);
  EXCEPTION WHEN SQLSTATE 'P0018' THEN
    v_prior := NULL;
  END;

  IF p_expected_old_cost IS DISTINCT FROM v_prior THEN
    RAISE EXCEPTION
      'optimistic_concurrency_violation: caller expected prior=%, actual prior=%',
      p_expected_old_cost, v_prior
      USING ERRCODE = 'P0017';
  END IF;

  SELECT COUNT(*) INTO v_wip_count
    FROM accounts
   WHERE kind = 'inv_value_wip'
     AND sku_id = p_sku_id
     AND NOT is_closed
     AND (debits_total - credits_total) > 0;

  IF v_wip_count > 0 THEN
    RAISE EXCEPTION
      'wip_present_blocks_std_cost_roll: sku=% has % open inv_value_wip pool(s) '
      'with non-zero balance. Phase 1 blocks rolls when WIP exists; the '
      'WIP material revaluation companion workflow is tracked as Epic G '
      '(acct-bru). Close out WIP via wo_complete + scrap or wait for '
      'production to drain before rolling.',
      p_sku_id, v_wip_count
      USING ERRCODE = 'P0006';
  END IF;

  v_future_dated := (p_effective_at > p_business_date);

  INSERT INTO standard_costs (
    sku_id, cost, effective_at, posted_by, idempotency_key, notes
  ) VALUES (
    p_sku_id, p_new_cost, p_effective_at, p_posted_by,
    gen_random_uuid(), p_notes
  );

  IF NOT v_future_dated AND v_prior IS NOT NULL AND v_prior <> p_new_cost THEN
    SELECT array_agg(id ORDER BY id) INTO v_lock_ids
      FROM accounts
     WHERE kind IN ('inv_value_raw', 'inv_value_fg')
       AND sku_id = p_sku_id
       AND NOT is_closed;

    IF v_lock_ids IS NOT NULL THEN
      FOREACH v_lock_id IN ARRAY v_lock_ids LOOP
        PERFORM 1 FROM accounts WHERE id = v_lock_id FOR UPDATE;
      END LOOP;
    END IF;

    FOR v_pool_record IN
      SELECT v.id          AS val_acct,
             v.currency    AS currency,
             v.location_id AS location_id
        FROM accounts v
       WHERE v.kind IN ('inv_value_raw', 'inv_value_fg')
         AND v.sku_id = p_sku_id
         AND NOT v.is_closed
       ORDER BY v.id
    LOOP
      SELECT COALESCE(SUM(
        CASE
          WHEN t.debit_account_id  = v_pool_record.val_acct THEN  t.qty
          WHEN t.credit_account_id = v_pool_record.val_acct THEN -t.qty
        END
      ), 0) INTO v_pool_qty
        FROM transfers t
       WHERE v_pool_record.val_acct IN (t.debit_account_id, t.credit_account_id)
         AND t.qty IS NOT NULL;

      IF v_pool_qty IS NULL OR v_pool_qty = 0 THEN
        CONTINUE;
      END IF;

      v_delta := v_pool_qty * (p_new_cost - v_prior);
      IF v_delta = 0 THEN
        CONTINUE;
      END IF;

      v_total_qty   := v_total_qty + v_pool_qty;
      v_total_delta := v_total_delta + v_delta;

      SELECT id INTO v_var_acct
        FROM accounts
       WHERE kind = 'variance_std_cost_roll'
         AND ledger_kind = 'value'
         AND currency = v_pool_record.currency
         AND NOT is_closed;
      IF v_var_acct IS NULL THEN
        RAISE EXCEPTION
          'no variance_std_cost_roll(value, ccy=%) account configured',
          v_pool_record.currency
          USING ERRCODE = 'P0010';
      END IF;

      IF v_delta > 0 THEN
        v_debit  := v_pool_record.val_acct;
        v_credit := v_var_acct;
        v_amount := v_delta;
      ELSE
        v_debit  := v_var_acct;
        v_credit := v_pool_record.val_acct;
        v_amount := -v_delta;
      END IF;

      v_batch := v_batch || jsonb_build_array(
        jsonb_build_object(
          'reason',            'standard_cost_roll',
          'document_kind',     'inventory_standard_cost_roll',
          'document_id',       NULL,
          'debit_account_id',  v_debit,
          'credit_account_id', v_credit,
          'amount',            v_amount,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        )
      );
    END LOOP;
  END IF;

  INSERT INTO inventory_standard_cost_rolls (
    sku_id, prior_standard_cost, target_standard_cost, effective_at,
    total_delta_value, pool_qty, business_date, posted_by,
    idempotency_key, notes
  ) VALUES (
    p_sku_id, v_prior, p_new_cost, p_effective_at,
    v_total_delta, v_total_qty, p_business_date, p_posted_by,
    p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id
      FROM inventory_standard_cost_rolls
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  IF jsonb_array_length(v_batch) > 0 THEN
    SELECT jsonb_agg(jsonb_set(ev, '{document_id}', to_jsonb(v_doc_id::TEXT)))
      INTO v_batch
      FROM jsonb_array_elements(v_batch) ev;
    PERFORM post_transfers(v_batch, FALSE);
  END IF;

  RETURN v_doc_id;
END;
$$;
