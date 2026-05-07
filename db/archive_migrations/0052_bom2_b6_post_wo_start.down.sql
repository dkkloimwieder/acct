-- acct-58w — rollback of B6: restore the migration 0039 version of post_wo_start.

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

  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

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
