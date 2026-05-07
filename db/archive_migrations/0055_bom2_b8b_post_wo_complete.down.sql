-- acct-n7p — rollback of B8b: restore the migration 0039 version of post_wo_complete.

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

  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

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
  SELECT id INTO v_qty_fg FROM accounts
   WHERE kind='stock_available' AND sku_id=v_wo.parent_sku_id
     AND location_id=v_wo.fg_location_id AND NOT is_closed;
  SELECT id INTO v_val_from FROM accounts
   WHERE kind='inv_value_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=v_last_op AND currency=v_wo.currency AND NOT is_closed;
  SELECT id INTO v_val_fg FROM accounts
   WHERE kind='inv_value_fg' AND sku_id=v_wo.parent_sku_id
     AND location_id=v_wo.fg_location_id AND currency=v_wo.currency
     AND NOT is_closed;

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
    'reason', 'wo_complete', 'document_kind', 'wo_event',
    'document_id', v_event_id,
    'debit_account_id', v_qty_fg, 'credit_account_id', v_qty_from,
    'amount', p_qty, 'qty', p_qty,
    'business_date', p_business_date,
    'idempotency_key', gen_random_uuid(), 'posted_by', p_posted_by
  ));
  v_batch := v_batch || jsonb_build_array(jsonb_build_object(
    'reason', 'wo_complete', 'document_kind', 'wo_event',
    'document_id', v_event_id,
    'debit_account_id', v_val_fg, 'credit_account_id', v_val_from,
    'qty', p_qty,
    'business_date', p_business_date,
    'idempotency_key', gen_random_uuid(), 'posted_by', p_posted_by
  ));

  PERFORM post_transfers(v_batch, FALSE);

  UPDATE work_orders SET qty_completed = qty_completed + p_qty
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
      IF v_residual > 0 THEN
        PERFORM post_transfers(jsonb_build_array(jsonb_build_object(
          'reason', 'wo_close_v', 'document_kind', 'wo_event',
          'document_id', v_event_id,
          'debit_account_id', v_var_close, 'credit_account_id', v_val_from,
          'amount', v_residual, 'business_date', p_business_date,
          'idempotency_key', gen_random_uuid(), 'posted_by', p_posted_by
        )), FALSE);
      ELSE
        PERFORM post_transfers(jsonb_build_array(jsonb_build_object(
          'reason', 'wo_close_v', 'document_kind', 'wo_event',
          'document_id', v_event_id,
          'debit_account_id', v_val_from, 'credit_account_id', v_var_close,
          'amount', -v_residual, 'business_date', p_business_date,
          'idempotency_key', gen_random_uuid(), 'posted_by', p_posted_by
        )), FALSE);
      END IF;
    END IF;

    UPDATE work_orders SET status = 'closed' WHERE id = p_wo_id;
  END IF;

  RETURN p_wo_id;
END;
$$;
