-- acct-69p — rollback of the idempotency race fix.
--
-- This is a bug-fix-only migration; reverting reintroduces the race
-- documented in the up.sql header. Per project convention "down is
-- best-effort; Phase 0/1 has no production data" we restore the
-- migration-0038 function bodies verbatim below so `sqlx migrate
-- revert` returns to the exact post-0038 state.

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
  v_bom_count       INT;
  v_bom             RECORD;
  v_comp_qty        BIGINT;
  v_comp_std_cost   BIGINT;
  v_comp_value      BIGINT;
  v_comp_qty_acct   BIGINT;
  v_comp_consumed   BIGINT;
  v_comp_val_acct   BIGINT;
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
  IF v_wo.status <> 'draft' THEN
    RAISE EXCEPTION 'wo_invalid: WO % status=% not draft (already started)',
                    p_wo_id, v_wo.status USING ERRCODE = 'P0026';
  END IF;

  SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_wo.parent_sku_id;
  IF v_cost_method <> 'standard' THEN
    RAISE EXCEPTION
      'wo_invalid: parent_sku % has cost_method=%, only ''standard'' '
      'supported in Slice B MVP (Phase 2 acct-p7v)',
      v_wo.parent_sku_id, v_cost_method USING ERRCODE = 'P0026';
  END IF;

  SELECT MIN(routing_op), COUNT(*) INTO v_first_op, v_op_count
    FROM wo_routings WHERE wo_id = p_wo_id;
  IF v_op_count = 0 THEN
    RAISE EXCEPTION 'wo_invalid: WO % has no routing operations', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;

  SELECT COUNT(*) INTO v_bom_count FROM boms
   WHERE parent_sku_id = v_wo.parent_sku_id;
  IF v_bom_count = 0 THEN
    RAISE EXCEPTION 'bom_missing: parent_sku % has no BOM rows',
                    v_wo.parent_sku_id USING ERRCODE = 'P0029';
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

  FOR v_bom IN
    SELECT b.component_sku_id, b.component_loc_id, b.qty_per_parent
      FROM boms b WHERE b.parent_sku_id = v_wo.parent_sku_id
      ORDER BY b.component_sku_id
  LOOP
    v_comp_qty      := v_wo.qty_target * v_bom.qty_per_parent;
    v_comp_std_cost := resolve_standard_cost_at(v_bom.component_sku_id, p_business_date);
    v_comp_value    := v_comp_qty * v_comp_std_cost;

    SELECT id INTO v_comp_consumed FROM accounts
     WHERE kind='stock_consumed' AND sku_id=v_bom.component_sku_id
       AND ledger_kind='qty' AND NOT is_closed;
    IF v_comp_consumed IS NULL THEN
      RAISE EXCEPTION 'no open stock_consumed account for sku=%',
                      v_bom.component_sku_id USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_comp_qty_acct FROM accounts
     WHERE kind='stock_available' AND sku_id=v_bom.component_sku_id
       AND location_id=v_bom.component_loc_id AND NOT is_closed;
    IF v_comp_qty_acct IS NULL THEN
      RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                      v_bom.component_sku_id, v_bom.component_loc_id
        USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_comp_val_acct FROM accounts
     WHERE kind='inv_value_raw' AND sku_id=v_bom.component_sku_id
       AND location_id=v_bom.component_loc_id AND currency=v_wo.currency
       AND NOT is_closed;
    IF v_comp_val_acct IS NULL THEN
      RAISE EXCEPTION 'no open inv_value_raw account for sku=% loc=% ccy=%',
                      v_bom.component_sku_id, v_bom.component_loc_id, v_wo.currency
        USING ERRCODE = 'P0010';
    END IF;

    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'rm_issue_to_wo',
      'document_kind',     'wo_event',
      'document_id',       v_event_id,
      'debit_account_id',  v_comp_consumed,
      'credit_account_id', v_comp_qty_acct,
      'amount',            v_comp_qty,
      'qty',               v_comp_qty,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'posted_by',         p_posted_by
    ));

    IF v_comp_value > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'rm_issue_to_wo',
        'document_kind',     'wo_event',
        'document_id',       v_event_id,
        'debit_account_id',  v_val_acct_wip,
        'credit_account_id', v_comp_val_acct,
        'amount',            v_comp_value,
        'qty',               v_comp_qty,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      ));
    END IF;
  END LOOP;

  v_batch := v_batch || _wo_burden_events_for_op(
    p_wo_id, v_first_op, v_wo.qty_target,
    v_val_acct_wip, v_wo.currency, p_business_date,
    v_event_id, p_posted_by
  );

  PERFORM post_transfers(v_batch, FALSE);

  UPDATE work_orders SET status = 'released' WHERE id = p_wo_id;

  RETURN p_wo_id;
END;
$$;

CREATE OR REPLACE FUNCTION post_wo_complete(
  p_wo_id           UUID,
  p_qty             BIGINT,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id    UUID;
  v_event_id       UUID;
  v_wo             work_orders%ROWTYPE;
  v_last_op        INT;
  v_qty_from       BIGINT;
  v_qty_fg         BIGINT;
  v_val_from       BIGINT;
  v_val_fg         BIGINT;
  v_var_close      BIGINT;
  v_will_close     BOOLEAN;
  v_val_balance    BIGINT;
  v_residual       BIGINT;
  v_batch          JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF p_qty IS NULL OR p_qty <= 0 THEN
    RAISE EXCEPTION 'wo_invalid: wo_complete qty must be > 0 (got %)', p_qty
      USING ERRCODE = 'P0026';
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

  IF v_wo.qty_completed + v_wo.qty_scrapped + p_qty > v_wo.qty_target THEN
    RAISE EXCEPTION
      'wo_qty_overflow: WO % completed=% scrapped=% + this=% > target=%',
      p_wo_id, v_wo.qty_completed, v_wo.qty_scrapped, p_qty, v_wo.qty_target
      USING ERRCODE = 'P0027';
  END IF;

  v_will_close :=
    (v_wo.qty_completed + v_wo.qty_scrapped + p_qty) = v_wo.qty_target;

  SELECT MAX(routing_op) INTO v_last_op FROM wo_routings WHERE wo_id = p_wo_id;
  IF v_last_op IS NULL THEN
    RAISE EXCEPTION 'wo_invalid: WO % has no routing operations', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;

  SELECT id INTO v_qty_from FROM accounts
   WHERE kind='stock_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=v_last_op AND NOT is_closed;
  IF v_qty_from IS NULL THEN
    RAISE EXCEPTION 'no open stock_wip account for sku=% op=%',
                    v_wo.parent_sku_id, v_last_op USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_qty_fg FROM accounts
   WHERE kind='stock_available' AND sku_id=v_wo.parent_sku_id
     AND location_id=v_wo.fg_location_id AND NOT is_closed;
  IF v_qty_fg IS NULL THEN
    RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                    v_wo.parent_sku_id, v_wo.fg_location_id
      USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_val_from FROM accounts
   WHERE kind='inv_value_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=v_last_op AND currency=v_wo.currency AND NOT is_closed;
  IF v_val_from IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_wip account for sku=% op=% ccy=%',
                    v_wo.parent_sku_id, v_last_op, v_wo.currency
      USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_val_fg FROM accounts
   WHERE kind='inv_value_fg' AND sku_id=v_wo.parent_sku_id
     AND location_id=v_wo.fg_location_id AND currency=v_wo.currency
     AND NOT is_closed;
  IF v_val_fg IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_fg account for sku=% loc=% ccy=%',
                    v_wo.parent_sku_id, v_wo.fg_location_id, v_wo.currency
      USING ERRCODE = 'P0010';
  END IF;

  INSERT INTO wo_events (
    wo_id, event_kind, routing_op_from, qty,
    business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_wo_id, 'wo_complete', v_last_op, p_qty,
    p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_event_id;
  IF v_event_id IS NULL THEN RETURN p_wo_id; END IF;

  v_batch := v_batch || jsonb_build_array(jsonb_build_object(
    'reason',            'wo_complete',
    'document_kind',     'wo_event',
    'document_id',       v_event_id,
    'debit_account_id',  v_qty_fg,
    'credit_account_id', v_qty_from,
    'amount',            p_qty,
    'qty',               p_qty,
    'business_date',     p_business_date,
    'idempotency_key',   gen_random_uuid(),
    'posted_by',         p_posted_by
  ));

  v_batch := v_batch || jsonb_build_array(jsonb_build_object(
    'reason',            'wo_complete',
    'document_kind',     'wo_event',
    'document_id',       v_event_id,
    'debit_account_id',  v_val_fg,
    'credit_account_id', v_val_from,
    'qty',               p_qty,
    'business_date',     p_business_date,
    'idempotency_key',   gen_random_uuid(),
    'posted_by',         p_posted_by
  ));

  PERFORM post_transfers(v_batch, FALSE);

  UPDATE work_orders
     SET qty_completed = qty_completed + p_qty
   WHERE id = p_wo_id;

  IF v_will_close THEN
    PERFORM 1 FROM accounts WHERE id = v_val_from FOR UPDATE;
    SELECT (debits_total - credits_total) INTO v_val_balance
      FROM accounts WHERE id = v_val_from;
    v_residual := COALESCE(v_val_balance, 0);

    IF v_residual <> 0 THEN
      SELECT id INTO v_var_close FROM accounts
       WHERE kind='variance_wo_close' AND ledger_kind='value'
         AND currency=v_wo.currency AND NOT is_closed;
      IF v_var_close IS NULL THEN
        RAISE EXCEPTION 'no open variance_wo_close account for ccy=%',
                        v_wo.currency USING ERRCODE = 'P0010';
      END IF;

      IF v_residual > 0 THEN
        PERFORM post_transfers(jsonb_build_array(jsonb_build_object(
          'reason',            'wo_close_v',
          'document_kind',     'wo_event',
          'document_id',       v_event_id,
          'debit_account_id',  v_var_close,
          'credit_account_id', v_val_from,
          'amount',            v_residual,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        )), FALSE);
      ELSE
        PERFORM post_transfers(jsonb_build_array(jsonb_build_object(
          'reason',            'wo_close_v',
          'document_kind',     'wo_event',
          'document_id',       v_event_id,
          'debit_account_id',  v_val_from,
          'credit_account_id', v_var_close,
          'amount',            -v_residual,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        )), FALSE);
      END IF;
    END IF;

    UPDATE work_orders SET status = 'closed' WHERE id = p_wo_id;
  END IF;

  RETURN p_wo_id;
END;
$$;
