-- acct-rso down — restore mig 0067 _wo_emit_bom_lines, mig 0065
-- post_wo_start / post_op_move, and mig 0031 wac_retroactive_close_hook.

-- ============================================================
-- post_wo_start: restore mig 0065 body (wac_retroactive raises P0026).
-- ============================================================

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

-- ============================================================
-- post_op_move: restore mig 0065 body (no wac_retroactive in wac branch).
-- ============================================================

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

  ELSIF v_cost_method IN ('wac_perpetual', 'wac_periodic') THEN
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
      'does not handle (deferred to acct-rso, tier 3)',
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

-- ============================================================
-- _wo_emit_bom_lines: restore mig 0067 body (wac_retroactive raises P0026).
-- ============================================================

CREATE OR REPLACE FUNCTION _wo_emit_bom_lines(
  p_wo_id          UUID,
  p_bom_id         BIGINT,
  p_routing_op     INT,
  p_qty            BIGINT,
  p_filter         JSONB,
  p_event_id       UUID,
  p_business_date  DATE,
  p_posted_by      UUID
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
  v_wo                   work_orders%ROWTYPE;
  v_val_acct_wip         BIGINT;
  v_batch                JSONB := '[]'::JSONB;
  v_line                 RECORD;
  v_filter_kind          TEXT;
  v_filter_basis         TEXT;
  v_filter_fire_at       TEXT;
  v_filter_applies_at_op INT;
  v_adj_qty              BIGINT;
  v_value                BIGINT;
  v_amount               BIGINT;
  v_reason               transfer_reason;
  v_comp_consumed        BIGINT;
  v_comp_qty_acct        BIGINT;
  v_comp_val_acct        BIGINT;
  v_applied_kind         account_kind;
  v_applied_acct         BIGINT;
  v_comp_std_cost        BIGINT;
  v_comp_cost_method     cost_method;
  v_parent_cost_method   cost_method;
  v_pool_qty             BIGINT;
  v_pool_value           BIGINT;
  v_unit                 BIGINT;
BEGIN
  IF p_qty IS NULL OR p_qty <= 0 THEN
    RAISE EXCEPTION 'wo_invalid: _wo_emit_bom_lines requires positive p_qty (got %)', p_qty
      USING ERRCODE = 'P0026';
  END IF;
  IF p_bom_id IS NULL THEN
    RAISE EXCEPTION 'wo_invalid: _wo_emit_bom_lines requires non-NULL p_bom_id'
      USING ERRCODE = 'P0026';
  END IF;

  SELECT * INTO v_wo FROM work_orders WHERE id = p_wo_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: WO % not found', p_wo_id USING ERRCODE = 'P0026';
  END IF;

  SELECT cost_method INTO v_parent_cost_method
    FROM skus WHERE id = v_wo.parent_sku_id;

  SELECT id INTO v_val_acct_wip FROM accounts
   WHERE kind='inv_value_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_routing_op AND currency=v_wo.currency
     AND NOT is_closed;
  IF v_val_acct_wip IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_wip account for sku=% op=% ccy=%',
                    v_wo.parent_sku_id, p_routing_op, v_wo.currency
      USING ERRCODE = 'P0010';
  END IF;

  v_filter_kind          := p_filter->>'kind';
  v_filter_basis         := p_filter->>'basis';
  v_filter_fire_at       := p_filter->>'fire_at';
  v_filter_applies_at_op := NULLIF(p_filter->>'applies_at_op', '')::INT;

  FOR v_line IN
    SELECT exp.*
      FROM _wo_explode_bom(p_bom_id, p_business_date) exp
     WHERE (v_filter_kind          IS NULL OR exp.kind          = v_filter_kind)
       AND (v_filter_basis         IS NULL OR exp.basis         = v_filter_basis)
       AND (v_filter_fire_at       IS NULL OR exp.fire_at       = v_filter_fire_at)
       AND (v_filter_applies_at_op IS NULL OR exp.applies_at_op = v_filter_applies_at_op)
     ORDER BY exp.source_bom_id, exp.source_line_no, exp.depth
  LOOP
    IF v_line.kind = 'item' THEN
      v_adj_qty := p_qty * v_line.qty_per_parent;

      SELECT id INTO v_comp_consumed FROM accounts
       WHERE kind='stock_consumed' AND sku_id=v_line.component_sku_id
         AND ledger_kind='qty' AND NOT is_closed;
      IF v_comp_consumed IS NULL THEN
        RAISE EXCEPTION 'no open stock_consumed account for sku=%',
                        v_line.component_sku_id USING ERRCODE = 'P0010';
      END IF;

      SELECT id INTO v_comp_qty_acct FROM accounts
       WHERE kind='stock_available' AND sku_id=v_line.component_sku_id
         AND location_id=v_line.component_loc_id AND NOT is_closed;
      IF v_comp_qty_acct IS NULL THEN
        RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                        v_line.component_sku_id, v_line.component_loc_id
          USING ERRCODE = 'P0010';
      END IF;

      SELECT id INTO v_comp_val_acct FROM accounts
       WHERE kind='inv_value_raw' AND sku_id=v_line.component_sku_id
         AND location_id=v_line.component_loc_id AND currency=v_wo.currency
         AND NOT is_closed;
      IF v_comp_val_acct IS NULL THEN
        RAISE EXCEPTION 'no open inv_value_raw account for sku=% loc=% ccy=%',
                        v_line.component_sku_id, v_line.component_loc_id, v_wo.currency
          USING ERRCODE = 'P0010';
      END IF;

      SELECT cost_method INTO v_comp_cost_method
        FROM skus WHERE id = v_line.component_sku_id;

      CASE v_comp_cost_method
        WHEN 'standard' THEN
          v_comp_std_cost := resolve_standard_cost_at(
            v_line.component_sku_id, p_business_date
          );
          v_value := v_adj_qty * v_comp_std_cost;

        WHEN 'wac_perpetual' THEN
          PERFORM 1 FROM accounts WHERE id = v_comp_val_acct FOR UPDATE;
          SELECT COALESCE(SUM(
            CASE
              WHEN t.debit_account_id  = v_comp_val_acct THEN  t.qty
              WHEN t.credit_account_id = v_comp_val_acct THEN -t.qty
            END
          ), 0)
            INTO v_pool_qty
            FROM transfers t
           WHERE v_comp_val_acct IN (t.debit_account_id, t.credit_account_id)
             AND t.qty IS NOT NULL;

          IF v_pool_qty <= 0 THEN
            RAISE EXCEPTION
              'rm_issue_empty_pool: wac_perpetual component % at sku=% loc=% '
              'has empty inv_value_raw pool (per-class qty=%); cannot issue '
              '% units to WO %',
              v_line.component_sku_id, v_line.component_sku_id,
              v_line.component_loc_id, v_pool_qty, v_adj_qty, p_wo_id
              USING ERRCODE = 'P0010';
          END IF;

          SELECT (debits_total - credits_total) INTO v_pool_value
            FROM accounts WHERE id = v_comp_val_acct;
          v_unit  := GREATEST(COALESCE(v_pool_value, 0), 0) / v_pool_qty;
          v_value := v_adj_qty * v_unit;

        WHEN 'wac_periodic' THEN
          IF v_parent_cost_method <> 'wac_periodic' THEN
            RAISE EXCEPTION
              'rm_issue_mixed_cost_method: wac_periodic component % at '
              'sku=% requires wac_periodic parent (parent % has '
              'cost_method=%). Mixed parent/component cost methods '
              'deferred to acct-7eo.',
              v_line.component_sku_id, v_line.component_sku_id,
              v_wo.parent_sku_id, v_parent_cost_method
              USING ERRCODE = 'P0026';
          END IF;

          PERFORM 1 FROM accounts WHERE id = v_comp_val_acct FOR UPDATE;
          SELECT COALESCE(SUM(
            CASE
              WHEN t.debit_account_id  = v_comp_val_acct THEN  t.qty
              WHEN t.credit_account_id = v_comp_val_acct THEN -t.qty
            END
          ), 0)
            INTO v_pool_qty
            FROM transfers t
           WHERE v_comp_val_acct IN (t.debit_account_id, t.credit_account_id)
             AND t.qty IS NOT NULL;

          IF v_pool_qty <= 0 THEN
            RAISE EXCEPTION
              'rm_issue_empty_pool: wac_periodic component % at sku=% loc=% '
              'has empty inv_value_raw pool (per-class qty=%); cannot issue '
              '% units to WO %',
              v_line.component_sku_id, v_line.component_sku_id,
              v_line.component_loc_id, v_pool_qty, v_adj_qty, p_wo_id
              USING ERRCODE = 'P0010';
          END IF;

          SELECT (debits_total - credits_total) INTO v_pool_value
            FROM accounts WHERE id = v_comp_val_acct;
          v_unit  := GREATEST(COALESCE(v_pool_value, 0), 0) / v_pool_qty;
          v_value := v_adj_qty * v_unit;

        WHEN 'wac_retroactive' THEN
          RAISE EXCEPTION
            'rm_issue_to_wo from wac_retroactive component % deferred to '
            'acct-rso (tier 3 of acct-rgb / acct-8in): per-event '
            'chronological replay across consumption events',
            v_line.component_sku_id USING ERRCODE = 'P0026';

        WHEN 'fifo', 'lot' THEN
          RAISE EXCEPTION
            'cost_method_not_implemented: % for component % (acct-8gg)',
            v_comp_cost_method, v_line.component_sku_id
            USING ERRCODE = 'P0006';

        ELSE
          RAISE EXCEPTION
            'unknown cost_method % for component %',
            v_comp_cost_method, v_line.component_sku_id
            USING ERRCODE = 'P0011';
      END CASE;

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'rm_issue_to_wo',
        'document_kind',     'wo_event',
        'document_id',       p_event_id,
        'debit_account_id',  v_comp_consumed,
        'credit_account_id', v_comp_qty_acct,
        'amount',            v_adj_qty,
        'qty',               v_adj_qty,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      ));

      IF v_value > 0 THEN
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'rm_issue_to_wo',
          'document_kind',     'wo_event',
          'document_id',       p_event_id,
          'debit_account_id',  v_val_acct_wip,
          'credit_account_id', v_comp_val_acct,
          'amount',            v_value,
          'qty',               v_adj_qty,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        ));
      END IF;

    ELSIF v_line.kind IN ('service', 'charge') THEN
      IF v_line.basis = 'per_unit' THEN
        v_amount := p_qty * v_line.std_amount;
      ELSE
        v_amount := v_line.std_amount;
      END IF;
      IF v_amount <= 0 THEN
        CONTINUE;
      END IF;

      v_reason := _wo_apply_reason_for(v_line.absorption_class_id, v_line.basis);

      SELECT applied_account_kind INTO v_applied_kind FROM absorption_classes
       WHERE id = v_line.absorption_class_id;
      IF v_applied_kind IS NULL THEN
        RAISE EXCEPTION 'wo_invalid: absorption_class id=% not found',
                        v_line.absorption_class_id USING ERRCODE = 'P0026';
      END IF;

      SELECT id INTO v_applied_acct FROM accounts
       WHERE kind = v_applied_kind AND ledger_kind='value'
         AND currency = v_wo.currency AND NOT is_closed
       LIMIT 1;
      IF v_applied_acct IS NULL THEN
        RAISE EXCEPTION 'no open % account for ccy=%',
                        v_applied_kind, v_wo.currency
          USING ERRCODE = 'P0010';
      END IF;

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            v_reason,
        'document_kind',     'wo_event',
        'document_id',       p_event_id,
        'debit_account_id',  v_val_acct_wip,
        'credit_account_id', v_applied_acct,
        'amount',            v_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      ));
    END IF;
  END LOOP;

  RETURN v_batch;
END;
$$;

-- ============================================================
-- wac_retroactive_close_hook: restore mig 0031 body (single-pool replay,
-- no WIP support, no topology, no cache, no internal-chain branch).
-- ============================================================

CREATE OR REPLACE FUNCTION wac_retroactive_close_hook(
  p_period_id         BIGINT,
  p_force_provisional BOOLEAN DEFAULT FALSE
) RETURNS BIGINT LANGUAGE plpgsql AS $$
DECLARE
  v_period_opens   DATE;
  v_period_closes  DATE;
  v_period_code    TEXT;
  v_count          BIGINT := 0;
  v_pool           RECORD;
  v_event          RECORD;
  v_pool_value     BIGINT;
  v_pool_qty       BIGINT;
  v_recomputed_avg BIGINT;
  v_recomputed_amt BIGINT;
  v_variance       BIGINT;
  v_var_acct       BIGINT;
  v_batch          JSONB;
  v_var_xfer_id    BIGINT;
  v_event_a        JSONB;
  v_event_b        JSONB;
  v_orig_amount    BIGINT;
BEGIN
  SELECT opens_at, closes_at, code INTO v_period_opens, v_period_closes, v_period_code
    FROM periods WHERE id = p_period_id;
  IF NOT FOUND THEN RETURN 0; END IF;

  FOR v_pool IN
    SELECT DISTINCT a.id AS pool_id, a.kind, a.sku_id, a.location_id, a.currency
      FROM transfers_provisional tp
      JOIN transfers t ON t.id = tp.transfer_id
      JOIN accounts a ON a.id = t.credit_account_id
     WHERE tp.period_id = p_period_id
       AND tp.cost_method = 'wac_retroactive'
       AND tp.finalized_at IS NULL
     ORDER BY a.id
  LOOP
    SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = v_pool.pool_id THEN  t.amount
                             WHEN t.credit_account_id = v_pool.pool_id THEN -t.amount END), 0)
      INTO v_pool_value
      FROM transfers t
     WHERE v_pool.pool_id IN (t.debit_account_id, t.credit_account_id)
       AND t.business_date < v_period_opens;
    SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = v_pool.pool_id THEN  t.qty
                             WHEN t.credit_account_id = v_pool.pool_id THEN -t.qty END), 0)
      INTO v_pool_qty
      FROM transfers t
     WHERE v_pool.pool_id IN (t.debit_account_id, t.credit_account_id)
       AND t.business_date < v_period_opens
       AND t.qty IS NOT NULL;

    FOR v_event IN
      SELECT t.id, t.amount, t.qty, t.debit_account_id, t.credit_account_id,
             t.business_date, t.posted_at,
             tp.transfer_id IS NOT NULL AS is_prov
        FROM transfers t
        LEFT JOIN transfers_provisional tp
          ON tp.transfer_id = t.id
         AND tp.cost_method = 'wac_retroactive'
         AND tp.finalized_at IS NULL
       WHERE v_pool.pool_id IN (t.debit_account_id, t.credit_account_id)
         AND t.business_date BETWEEN v_period_opens AND v_period_closes
       ORDER BY t.business_date, t.posted_at, t.id
    LOOP
      IF v_event.debit_account_id = v_pool.pool_id THEN
        v_pool_value := v_pool_value + v_event.amount;
        IF v_event.qty IS NOT NULL THEN
          v_pool_qty := v_pool_qty + v_event.qty;
        END IF;
      ELSE
        IF v_event.qty IS NULL THEN
          v_pool_value := v_pool_value - v_event.amount;
          CONTINUE;
        END IF;

        IF v_pool_qty <= 0 THEN
          IF p_force_provisional AND v_event.is_prov THEN
            CONTINUE;
          END IF;
          RAISE EXCEPTION
            'wac_retroactive_replay_pool_empty: period % (id=%) pool kind=% sku=% '
            'loc=% ccy=%: running qty went non-positive at depletion of transfer %; '
            'this indicates the perpetual chain has an inconsistency (more depletions '
            'than receipts of valid age). Pass p_force_provisional=TRUE to skip this row.',
            v_period_code, p_period_id, v_pool.kind, v_pool.sku_id, v_pool.location_id,
            v_pool.currency, v_event.id
            USING ERRCODE = 'P0006';
        END IF;

        v_recomputed_avg := v_pool_value / v_pool_qty;
        v_recomputed_amt := v_event.qty * v_recomputed_avg;
        v_orig_amount    := v_event.amount;

        IF v_event.is_prov THEN
          v_variance := v_recomputed_amt - v_orig_amount;
          IF v_variance = 0 THEN
            UPDATE transfers_provisional
               SET finalized_at = clock_timestamp(),
                   variance_amount = 0,
                   variance_transfer_id = NULL
             WHERE transfer_id = v_event.id;
          ELSE
            SELECT id INTO v_var_acct FROM accounts
             WHERE kind = 'variance_wac_retroactive' AND ledger_kind = 'value'
               AND currency = v_pool.currency AND NOT is_closed;
            IF v_var_acct IS NULL THEN
              RAISE EXCEPTION
                'wac_retroactive_close: no variance_wac_retroactive(value, ccy=%) account configured',
                v_pool.currency USING ERRCODE = 'P0010';
            END IF;

            IF v_variance > 0 THEN
              v_event_a := jsonb_build_object('reason','cost_restate','document_kind','wac_retroactive_close','document_id',gen_random_uuid(),'debit_account_id',v_event.debit_account_id,'credit_account_id',v_var_acct,'amount',v_variance,'business_date',v_period_closes,'idempotency_key',gen_random_uuid(),'posted_by','00000000-0000-0000-0000-000000000000');
              v_event_b := jsonb_build_object('reason','cost_restate','document_kind','wac_retroactive_close','document_id',gen_random_uuid(),'debit_account_id',v_var_acct,'credit_account_id',v_pool.pool_id,'amount',v_variance,'business_date',v_period_closes,'idempotency_key',gen_random_uuid(),'posted_by','00000000-0000-0000-0000-000000000000');
            ELSE
              v_event_a := jsonb_build_object('reason','cost_restate','document_kind','wac_retroactive_close','document_id',gen_random_uuid(),'debit_account_id',v_var_acct,'credit_account_id',v_event.debit_account_id,'amount',-v_variance,'business_date',v_period_closes,'idempotency_key',gen_random_uuid(),'posted_by','00000000-0000-0000-0000-000000000000');
              v_event_b := jsonb_build_object('reason','cost_restate','document_kind','wac_retroactive_close','document_id',gen_random_uuid(),'debit_account_id',v_pool.pool_id,'credit_account_id',v_var_acct,'amount',-v_variance,'business_date',v_period_closes,'idempotency_key',gen_random_uuid(),'posted_by','00000000-0000-0000-0000-000000000000');
            END IF;

            v_batch := jsonb_build_array(v_event_a, v_event_b);
            PERFORM post_transfers(v_batch, FALSE);
            SELECT id INTO v_var_xfer_id FROM transfers WHERE idempotency_key = (v_event_b->>'idempotency_key')::UUID;

            UPDATE transfers_provisional
               SET finalized_at = clock_timestamp(),
                   variance_amount = v_variance,
                   variance_transfer_id = v_var_xfer_id
             WHERE transfer_id = v_event.id;
          END IF;

          v_count := v_count + 1;
          v_pool_value := v_pool_value - v_recomputed_amt;
          v_pool_qty   := v_pool_qty - v_event.qty;
        ELSE
          v_pool_value := v_pool_value - v_orig_amount;
          v_pool_qty   := v_pool_qty - v_event.qty;
        END IF;
      END IF;
    END LOOP;
  END LOOP;

  RETURN v_count;
END;
$$;
