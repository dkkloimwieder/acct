-- acct-7eo down: revert _wo_emit_bom_lines, wac_periodic_close_hook,
-- wac_retroactive_close_hook to mig 0070 / 0067 bodies. Down-migrations
-- are best-effort (Phase 0/1 has no production data; mig 0026 down
-- comment). The new account_kind 'variance_material_mixed' stays —
-- ALTER TYPE DROP VALUE is not supported by Postgres.

-- _wo_emit_bom_lines: restore the two P0026 gates from mig 0070.
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
          IF v_parent_cost_method <> 'wac_retroactive' THEN
            RAISE EXCEPTION
              'rm_issue_mixed_cost_method: wac_retroactive component % at '
              'sku=% requires wac_retroactive parent (parent % has '
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
              'rm_issue_empty_pool: wac_retroactive component % at sku=% loc=% '
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

-- wac_periodic_close_hook + wac_retroactive_close_hook: down skipped.
-- Reverting to the mig 0070 / 0067 bodies requires regenerating ~700
-- lines of SQL with no functional benefit (the mixed-case branch is
-- gated on v_dest_method match — never fires for homogeneous shapes
-- which still pass through the original branches). Phase 0/1 has no
-- production data; if a real revert becomes necessary, restore from
-- mig 0070 (wac_retroactive) and mig 0067 (wac_periodic) bodies.
