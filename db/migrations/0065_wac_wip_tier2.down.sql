-- acct-smn down — best-effort revert to tier 1 (mig 0064) bodies.

-- Restore the original CHECK on transfers_provisional. This is a best-
-- effort revert: any rows finalized by tier 2's internal-chain path
-- (variance_amount != 0 with variance_transfer_id NULL) will violate
-- the restored CHECK if present at down-migration time. Acceptable for
-- a forward-only project; documented per project convention.

ALTER TABLE transfers_provisional
  DROP CONSTRAINT IF EXISTS transfers_provisional_check;

ALTER TABLE transfers_provisional
  ADD CONSTRAINT transfers_provisional_check CHECK (
    CASE
      WHEN finalized_at IS NULL THEN
        variance_amount IS NULL AND variance_transfer_id IS NULL
      WHEN variance_amount = 0 THEN
        variance_transfer_id IS NULL
      ELSE
        variance_transfer_id IS NOT NULL
    END
  );

-- post_wo_start: restore the multi-op gate for wac_periodic.

CREATE OR REPLACE FUNCTION post_wo_start(
  p_wo_id           UUID,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id     UUID;
  v_event_id        UUID;
  v_wo              work_orders%ROWTYPE;
  v_first_op        INT;
  v_op_count        INT;
  v_cost_method     cost_method;
  v_qty_acct_wip    BIGINT;
  v_void_qty        BIGINT;
  v_val_acct_wip    BIGINT;
  v_bom             bom_headers%ROWTYPE;
  v_bad_op          INT;
  v_alloc_sum       NUMERIC;
  v_batch           JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  SELECT * INTO v_wo FROM work_orders WHERE id = p_wo_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: WO % not found', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;

  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF v_wo.status <> 'draft' THEN
    RAISE EXCEPTION 'wo_invalid: WO % status=% not draft (already started)',
                    p_wo_id, v_wo.status USING ERRCODE = 'P0026';
  END IF;

  SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_wo.parent_sku_id;
  IF v_cost_method NOT IN ('standard', 'wac_perpetual', 'wac_periodic') THEN
    RAISE EXCEPTION
      'wo_invalid: parent_sku % has cost_method=%; ''wac_retroactive'' '
      'parents are not supported on WO. wac_retroactive on WIP requires '
      'the chronological-replay refactor (acct-rso, tier 3 of acct-8in).',
      v_wo.parent_sku_id, v_cost_method USING ERRCODE = 'P0026';
  END IF;

  SELECT MIN(routing_op), COUNT(*) INTO v_first_op, v_op_count
    FROM wo_routings WHERE wo_id = p_wo_id;
  IF v_op_count = 0 THEN
    RAISE EXCEPTION 'wo_invalid: WO % has no routing operations', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;

  IF v_cost_method = 'wac_periodic' AND v_op_count > 1 THEN
    RAISE EXCEPTION
      'wo_invalid: parent_sku % has cost_method=% with %-op routing; '
      'multi-op routing on wac_periodic parents requires the topological '
      'per-pool close hook recompute (acct-smn, tier 2 of acct-8in). '
      'Use single-op routing for tier 1 (acct-bol).',
      v_wo.parent_sku_id, v_cost_method, v_op_count
      USING ERRCODE = 'P0026';
  END IF;

  SELECT id INTO v_qty_acct_wip FROM accounts
   WHERE kind='stock_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=v_first_op AND NOT is_closed;
  IF v_qty_acct_wip IS NULL THEN
    RAISE EXCEPTION 'no open stock_wip account for sku=% op=%',
                    v_wo.parent_sku_id, v_first_op USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_void_qty FROM accounts
   WHERE kind='creation_void' AND ledger_kind='qty' AND NOT is_closed;
  IF v_void_qty IS NULL THEN
    RAISE EXCEPTION 'no creation_void(qty) account configured'
      USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_val_acct_wip FROM accounts
   WHERE kind='inv_value_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=v_first_op AND currency=v_wo.currency
     AND NOT is_closed;
  IF v_val_acct_wip IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_wip account for sku=% op=% ccy=%',
                    v_wo.parent_sku_id, v_first_op, v_wo.currency
      USING ERRCODE = 'P0010';
  END IF;

  v_bom := _wo_resolve_bom_for(p_wo_id, p_business_date);

  SELECT exp.applies_at_op INTO v_bad_op
    FROM _wo_explode_bom(v_bom.id, p_business_date) exp
   WHERE NOT EXISTS (
     SELECT 1 FROM wo_routings wr
      WHERE wr.wo_id = p_wo_id AND wr.routing_op = exp.applies_at_op
   )
   LIMIT 1;
  IF v_bad_op IS NOT NULL THEN
    RAISE EXCEPTION
      'wo_start_op_mismatch: bom_lines reference applies_at_op=% '
      'which is not in wo_routings(wo=%)',
      v_bad_op, p_wo_id USING ERRCODE = 'P0028';
  END IF;

  PERFORM 1 FROM wo_outputs WHERE wo_id = p_wo_id LIMIT 1;
  IF NOT FOUND THEN
    INSERT INTO wo_outputs (
      wo_id, output_no, output_sku_id, fg_location_id, qty,
      allocation_method, allocation_pct
    ) VALUES (
      p_wo_id, 1, v_wo.parent_sku_id, v_wo.fg_location_id, v_wo.qty_target,
      'primary', 100
    );
  ELSE
    SELECT COALESCE(SUM(allocation_pct), 0)
      INTO v_alloc_sum
      FROM wo_outputs WHERE wo_id = p_wo_id;
    IF v_alloc_sum <> 100 THEN
      RAISE EXCEPTION
        'output_allocation_invalid: wo_outputs(wo=%) allocation_pct sums to % (expected 100)',
        p_wo_id, v_alloc_sum USING ERRCODE = 'P0033';
    END IF;
  END IF;

  INSERT INTO wo_events (
    wo_id, event_kind, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_wo_id, 'start', p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_event_id;
  IF v_event_id IS NULL THEN RETURN p_wo_id; END IF;

  v_batch := v_batch || jsonb_build_array(jsonb_build_object(
    'reason',            'wo_start',
    'document_kind',     'wo_event',
    'document_id',       v_event_id,
    'debit_account_id',  v_qty_acct_wip,
    'credit_account_id', v_void_qty,
    'amount',            v_wo.qty_target,
    'qty',               v_wo.qty_target,
    'business_date',     p_business_date,
    'idempotency_key',   gen_random_uuid(),
    'posted_by',         p_posted_by
  ));

  v_batch := v_batch || _wo_emit_bom_lines(
    p_wo_id, v_bom.id, v_first_op, v_wo.qty_target,
    jsonb_build_object('fire_at', 'wo_start'),
    v_event_id, p_business_date, p_posted_by
  );

  v_batch := v_batch || _wo_emit_bom_lines(
    p_wo_id, v_bom.id, v_first_op, v_wo.qty_target,
    jsonb_build_object('fire_at', 'op_arrival', 'applies_at_op', v_first_op),
    v_event_id, p_business_date, p_posted_by
  );

  PERFORM post_transfers(v_batch, FALSE);
  UPDATE work_orders SET status = 'released' WHERE id = p_wo_id;
  RETURN p_wo_id;
END;
$$;

-- post_op_move: drop wac_periodic from the wac branch (revert to 0063).

CREATE OR REPLACE FUNCTION post_op_move(
  p_wo_id           UUID,
  p_from_op         INT,
  p_to_op           INT,
  p_qty             BIGINT,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id      UUID;
  v_event_id         UUID;
  v_wo               work_orders%ROWTYPE;
  v_from_count       INT;
  v_to_count         INT;
  v_qty_from         BIGINT;
  v_qty_to           BIGINT;
  v_val_from         BIGINT;
  v_val_to           BIGINT;
  v_value_amount     BIGINT;
  v_batch            JSONB := '[]'::JSONB;
  v_bom              bom_headers%ROWTYPE;
  v_first_op         INT;
  v_default_lot_size BIGINT;
  v_per_unit_cum     BIGINT;
  v_per_lot_cum      BIGINT;
  v_first_arrival    BOOLEAN;
  v_cost_method      cost_method;
  v_pool_value       BIGINT;
  v_pool_qty         BIGINT;
  v_unit             BIGINT;
BEGIN
  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF p_qty IS NULL OR p_qty <= 0 THEN
    RAISE EXCEPTION 'wo_invalid: op_move qty must be > 0 (got %)', p_qty
      USING ERRCODE = 'P0026';
  END IF;
  IF p_from_op = p_to_op THEN
    RAISE EXCEPTION 'routing_op_invalid: from_op (%) = to_op (%)',
                    p_from_op, p_to_op USING ERRCODE = 'P0028';
  END IF;

  SELECT * INTO v_wo FROM work_orders WHERE id = p_wo_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: WO % not found', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;
  IF v_wo.status <> 'released' THEN
    RAISE EXCEPTION 'wo_invalid: WO % status=% not released',
                    p_wo_id, v_wo.status USING ERRCODE = 'P0026';
  END IF;

  SELECT COUNT(*) INTO v_from_count FROM wo_routings
   WHERE wo_id = p_wo_id AND routing_op = p_from_op;
  IF v_from_count = 0 THEN
    RAISE EXCEPTION 'routing_op_invalid: from_op % not in WO % routing',
                    p_from_op, p_wo_id USING ERRCODE = 'P0028';
  END IF;
  SELECT COUNT(*) INTO v_to_count FROM wo_routings
   WHERE wo_id = p_wo_id AND routing_op = p_to_op;
  IF v_to_count = 0 THEN
    RAISE EXCEPTION 'routing_op_invalid: to_op % not in WO % routing',
                    p_to_op, p_wo_id USING ERRCODE = 'P0028';
  END IF;

  SELECT id INTO v_qty_from FROM accounts
   WHERE kind='stock_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_from_op AND NOT is_closed;
  IF v_qty_from IS NULL THEN
    RAISE EXCEPTION 'no open stock_wip account for sku=% op=%',
                    v_wo.parent_sku_id, p_from_op USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_qty_to FROM accounts
   WHERE kind='stock_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_to_op AND NOT is_closed;
  IF v_qty_to IS NULL THEN
    RAISE EXCEPTION 'no open stock_wip account for sku=% op=%',
                    v_wo.parent_sku_id, p_to_op USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_val_from FROM accounts
   WHERE kind='inv_value_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_from_op AND currency=v_wo.currency AND NOT is_closed;
  IF v_val_from IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_wip account for sku=% op=% ccy=%',
                    v_wo.parent_sku_id, p_from_op, v_wo.currency
      USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_val_to FROM accounts
   WHERE kind='inv_value_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_to_op AND currency=v_wo.currency AND NOT is_closed;
  IF v_val_to IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_wip account for sku=% op=% ccy=%',
                    v_wo.parent_sku_id, p_to_op, v_wo.currency
      USING ERRCODE = 'P0010';
  END IF;

  v_bom := _wo_resolve_bom_for(p_wo_id, p_business_date);
  SELECT cost_method, default_lot_size INTO v_cost_method, v_default_lot_size
    FROM skus WHERE id = v_wo.parent_sku_id;
  SELECT MIN(routing_op) INTO v_first_op
    FROM wo_routings WHERE wo_id = p_wo_id;

  IF v_cost_method = 'standard' THEN
    SELECT COALESCE(SUM(
      CASE
        WHEN exp.kind = 'item' THEN
          (exp.qty_per_parent
            * resolve_standard_cost_at(exp.component_sku_id, p_business_date))
        WHEN exp.kind = 'service' AND exp.basis = 'per_unit' THEN exp.std_amount
        ELSE 0
      END
    ), 0) INTO v_per_unit_cum
      FROM _wo_explode_bom(v_bom.id, p_business_date) exp
     WHERE exp.basis = 'per_unit'
       AND exp.applies_at_op <= p_from_op;

    SELECT COALESCE(SUM(exp.std_amount), 0) / v_default_lot_size
      INTO v_per_lot_cum
      FROM _wo_explode_bom(v_bom.id, p_business_date) exp
     WHERE exp.basis = 'per_lot'
       AND (
         exp.fire_at = 'wo_start'
         OR (exp.fire_at = 'op_arrival' AND exp.applies_at_op <= p_from_op)
       );

    v_value_amount := p_qty * (v_per_unit_cum + v_per_lot_cum);

  ELSIF v_cost_method = 'wac_perpetual' THEN
    PERFORM 1 FROM accounts WHERE id = v_val_from FOR UPDATE;
    SELECT (debits_total - credits_total) INTO v_pool_value
      FROM accounts WHERE id = v_val_from;
    SELECT (debits_total - credits_total) INTO v_pool_qty
      FROM accounts WHERE id = v_qty_from;

    IF v_pool_qty IS NULL OR v_pool_qty <= 0 THEN
      v_value_amount := 0;
    ELSE
      v_unit := GREATEST(COALESCE(v_pool_value, 0), 0) / v_pool_qty;
      v_value_amount := p_qty * v_unit;
    END IF;

  ELSE
    RAISE EXCEPTION
      'wo_invalid: parent_sku % has cost_method=% which post_op_move '
      'does not handle (deferred to acct-8in)',
      v_wo.parent_sku_id, v_cost_method USING ERRCODE = 'P0026';
  END IF;

  v_first_arrival := NOT EXISTS (
    SELECT 1 FROM wo_events
     WHERE wo_id = p_wo_id
       AND (
         (event_kind = 'op_move' AND routing_op_to = p_to_op)
         OR (event_kind = 'start' AND p_to_op = v_first_op)
       )
  );

  INSERT INTO wo_events (
    wo_id, event_kind, routing_op_from, routing_op_to, qty,
    business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_wo_id, 'op_move', p_from_op, p_to_op, p_qty,
    p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_event_id;
  IF v_event_id IS NULL THEN RETURN p_wo_id; END IF;

  v_batch := v_batch || jsonb_build_array(jsonb_build_object(
    'reason',            'op_move',
    'document_kind',     'wo_event',
    'document_id',       v_event_id,
    'debit_account_id',  v_qty_to,
    'credit_account_id', v_qty_from,
    'amount',            p_qty,
    'qty',               p_qty,
    'business_date',     p_business_date,
    'idempotency_key',   gen_random_uuid(),
    'posted_by',         p_posted_by
  ));

  IF v_value_amount > 0 THEN
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'op_move_v',
      'document_kind',     'wo_event',
      'document_id',       v_event_id,
      'debit_account_id',  v_val_to,
      'credit_account_id', v_val_from,
      'amount',            v_value_amount,
      'qty',               p_qty,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'posted_by',         p_posted_by
    ));
  END IF;

  IF v_first_arrival THEN
    v_batch := v_batch || _wo_emit_bom_lines(
      p_wo_id, v_bom.id, p_to_op, p_qty,
      jsonb_build_object('fire_at', 'op_arrival', 'applies_at_op', p_to_op),
      v_event_id, p_business_date, p_posted_by
    );
  ELSE
    v_batch := v_batch || _wo_emit_bom_lines(
      p_wo_id, v_bom.id, p_to_op, p_qty,
      jsonb_build_object('fire_at',        'op_arrival',
                         'applies_at_op',  p_to_op,
                         'basis',          'per_unit',
                         'kind',           'service'),
      v_event_id, p_business_date, p_posted_by
    );
  END IF;

  PERFORM post_transfers(v_batch, FALSE);
  RETURN p_wo_id;
END;
$$;

-- wac_periodic_close_hook: revert to mig 0064 body (per-row, per-pool;
-- no topological walk).

CREATE OR REPLACE FUNCTION wac_periodic_close_hook(
  p_period_id         BIGINT,
  p_force_provisional BOOLEAN DEFAULT FALSE
) RETURNS BIGINT LANGUAGE plpgsql AS $$
DECLARE
  v_period_opens   DATE;
  v_period_closes  DATE;
  v_period_code    TEXT;
  v_count          BIGINT := 0;
  v_row            RECORD;
  v_orig           RECORD;
  v_pool_acct      accounts%ROWTYPE;
  v_pool_value_in  BIGINT;
  v_pool_qty_in    BIGINT;
  v_final_avg      BIGINT;
  v_provisional    BIGINT;
  v_variance       BIGINT;
  v_var_acct       BIGINT;
  v_batch          JSONB;
  v_var_xfer_id    BIGINT;
  v_orig_debit     BIGINT;
  v_orig_credit    BIGINT;
  v_event_a        JSONB;
  v_event_b        JSONB;
BEGIN
  SELECT opens_at, closes_at, code
    INTO v_period_opens, v_period_closes, v_period_code
    FROM periods WHERE id = p_period_id;
  IF NOT FOUND THEN RETURN 0; END IF;

  FOR v_row IN
    SELECT * FROM transfers_provisional
     WHERE period_id = p_period_id AND cost_method = 'wac_periodic'
       AND finalized_at IS NULL
     ORDER BY transfer_id FOR UPDATE
  LOOP
    SELECT * INTO v_orig FROM transfers WHERE id = v_row.transfer_id;
    SELECT * INTO v_pool_acct FROM accounts WHERE id = v_orig.credit_account_id;

    SELECT COALESCE(SUM(amount), 0) INTO v_pool_value_in
      FROM transfers WHERE debit_account_id = v_pool_acct.id
        AND business_date BETWEEN v_period_opens AND v_period_closes;

    v_pool_qty_in := _wac_close_pool_qty_in(
      v_pool_acct, v_period_opens, v_period_closes
    );
    IF v_pool_qty_in IS NULL THEN
      RAISE EXCEPTION
        'wac_periodic_close: cannot resolve qty account for value pool %',
        v_pool_acct.id USING ERRCODE = 'P0010';
    END IF;

    IF v_pool_qty_in = 0 THEN
      IF p_force_provisional THEN CONTINUE; END IF;
      RAISE EXCEPTION
        'wac_periodic_close_no_receipts: period % (id=%) has provisional '
        'depletions on pool kind=% sku=% loc=% ccy=% but zero receipts in '
        'period; post receipts and retry the close, or close with '
        'p_force_provisional=TRUE.',
        v_period_code, p_period_id, v_pool_acct.kind, v_pool_acct.sku_id,
        v_pool_acct.location_id, v_pool_acct.currency
        USING ERRCODE = 'P0020';
    END IF;

    v_final_avg := v_pool_value_in / v_pool_qty_in;
    v_provisional := v_orig.amount / v_row.qty;
    v_variance := (v_final_avg - v_provisional) * v_row.qty;

    IF v_variance = 0 THEN
      UPDATE transfers_provisional SET finalized_at = clock_timestamp(),
        variance_amount = 0, variance_transfer_id = NULL
        WHERE transfer_id = v_row.transfer_id;
      v_count := v_count + 1; CONTINUE;
    END IF;

    SELECT id INTO v_var_acct FROM accounts WHERE kind = 'variance_wac_period'
      AND ledger_kind = 'value' AND currency = v_pool_acct.currency AND NOT is_closed;
    IF v_var_acct IS NULL THEN
      RAISE EXCEPTION 'wac_periodic_close: no variance_wac_period(value, ccy=%) account configured',
        v_pool_acct.currency USING ERRCODE = 'P0010';
    END IF;

    v_orig_debit  := v_orig.debit_account_id;
    v_orig_credit := v_orig.credit_account_id;

    IF v_pool_acct.kind = 'inv_value_wip' THEN
      IF v_variance > 0 THEN
        v_event_a := jsonb_build_object(
          'reason', 'cost_restate', 'document_kind', 'wac_periodic_close',
          'document_id', gen_random_uuid(),
          'debit_account_id', v_orig_debit, 'credit_account_id', v_var_acct,
          'amount', v_variance, 'business_date', v_period_closes,
          'idempotency_key', gen_random_uuid(),
          'posted_by', '00000000-0000-0000-0000-000000000000');
      ELSE
        v_event_a := jsonb_build_object(
          'reason', 'cost_restate', 'document_kind', 'wac_periodic_close',
          'document_id', gen_random_uuid(),
          'debit_account_id', v_var_acct, 'credit_account_id', v_orig_debit,
          'amount', -v_variance, 'business_date', v_period_closes,
          'idempotency_key', gen_random_uuid(),
          'posted_by', '00000000-0000-0000-0000-000000000000');
      END IF;
      v_batch := jsonb_build_array(v_event_a);
    ELSE
      IF v_variance > 0 THEN
        v_event_a := jsonb_build_object(
          'reason', 'cost_restate', 'document_kind', 'wac_periodic_close',
          'document_id', gen_random_uuid(),
          'debit_account_id', v_orig_debit, 'credit_account_id', v_var_acct,
          'amount', v_variance, 'business_date', v_period_closes,
          'idempotency_key', gen_random_uuid(),
          'posted_by', '00000000-0000-0000-0000-000000000000');
        v_event_b := jsonb_build_object(
          'reason', 'cost_restate', 'document_kind', 'wac_periodic_close',
          'document_id', gen_random_uuid(),
          'debit_account_id', v_var_acct, 'credit_account_id', v_orig_credit,
          'amount', v_variance, 'business_date', v_period_closes,
          'idempotency_key', gen_random_uuid(),
          'posted_by', '00000000-0000-0000-0000-000000000000');
      ELSE
        v_event_a := jsonb_build_object(
          'reason', 'cost_restate', 'document_kind', 'wac_periodic_close',
          'document_id', gen_random_uuid(),
          'debit_account_id', v_var_acct, 'credit_account_id', v_orig_debit,
          'amount', -v_variance, 'business_date', v_period_closes,
          'idempotency_key', gen_random_uuid(),
          'posted_by', '00000000-0000-0000-0000-000000000000');
        v_event_b := jsonb_build_object(
          'reason', 'cost_restate', 'document_kind', 'wac_periodic_close',
          'document_id', gen_random_uuid(),
          'debit_account_id', v_orig_credit, 'credit_account_id', v_var_acct,
          'amount', -v_variance, 'business_date', v_period_closes,
          'idempotency_key', gen_random_uuid(),
          'posted_by', '00000000-0000-0000-0000-000000000000');
      END IF;
      v_batch := jsonb_build_array(v_event_a, v_event_b);
    END IF;

    PERFORM post_transfers(v_batch, TRUE);
    SELECT id INTO v_var_xfer_id FROM transfers WHERE idempotency_key = (v_event_a->>'idempotency_key')::UUID;
    UPDATE transfers_provisional SET finalized_at = clock_timestamp(),
      variance_amount = v_variance, variance_transfer_id = v_var_xfer_id
      WHERE transfer_id = v_row.transfer_id;
    v_count := v_count + 1;
  END LOOP;

  RETURN v_count;
END;
$$;
