-- acct-du2.10 — post_wo_close_unproduced solo-at-pool gate.
--
-- BUG. mig 0056 walks every inv_value_wip(parent_sku, op_*, ccy) pool
-- this WO's routing touches and absorbs the WHOLE residual into THIS
-- WO's wo_close_v / variance_wo_close. When other WOs share parent_sku
-- × routing_op pools and have unfinished WIP, the residual sweep
-- absorbs that other WOs' WIP into THIS WO's variance — same shape as
-- the original acct-69e bug fixed for post_wo_complete in mig 0068.
--
-- post_wo_close_unproduced is called only when qty_completed=0 AND
-- qty_scrapped=qty_target, so THIS WO's stock_wip qty contribution
-- is already drained to stock_scrap. Any non-zero stock_wip(parent,
-- op) qty at call time is OTHER WOs.
--
-- FIX. Gate residual absorption on solo-at-pool: stock_wip(parent, op)
-- qty == 0 before we post wo_close_v. When not solo, skip — variance
-- lands at the LAST sharing WO's close, same approximation pattern as
-- mig 0068's residual sweep (acct-69e). The trade-off is identical:
-- approximate per-WO close-variance attribution when WOs share a pool;
-- pool-value integrity preserved.
--
-- LOCK ORDER. Existing FOR UPDATE on inv_value_wip(parent, op) at
-- mig 0056 line 121 is preserved. The new stock_wip read happens
-- before that, mirroring mig 0068's pre-gate read at line 164. As
-- with acct-du2.1 / .2, the stock_wip read is not under FOR UPDATE
-- (filed for fix as acct-du2.1 / .2 against post_wo_complete; same
-- shape applies here as a follow-up). Concurrent post_op_move on the
-- pool can race the gate decision; correct fix is to extend the lock
-- set, deferred to a separate migration to avoid scope-creep.

CREATE OR REPLACE FUNCTION post_wo_close_unproduced(
  p_wo_id           UUID,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id  UUID;
  v_event_id     UUID;
  v_wo           work_orders%ROWTYPE;
  v_var_close    BIGINT;
  v_residual     BIGINT;
  v_op_residual  RECORD;
  v_pool_qty     BIGINT;
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

  IF v_wo.status <> 'released' THEN
    RAISE EXCEPTION
      'wo_close_unproduced_invalid_state: WO % status=% not released',
      p_wo_id, v_wo.status USING ERRCODE = 'P0034';
  END IF;
  IF v_wo.qty_completed <> 0 THEN
    RAISE EXCEPTION
      'wo_close_unproduced_invalid_state: WO % qty_completed=% (must be 0; '
      'use post_wo_complete if any units finished)',
      p_wo_id, v_wo.qty_completed USING ERRCODE = 'P0034';
  END IF;
  IF v_wo.qty_scrapped <> v_wo.qty_target THEN
    RAISE EXCEPTION
      'wo_close_unproduced_invalid_state: WO % qty_scrapped=% qty_target=% '
      '(must scrap full target before unproduced close)',
      p_wo_id, v_wo.qty_scrapped, v_wo.qty_target USING ERRCODE = 'P0034';
  END IF;

  INSERT INTO wo_events (
    wo_id, event_kind, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_wo_id, 'close_unproduced', p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_event_id;
  IF v_event_id IS NULL THEN RETURN p_wo_id; END IF;

  -- Walk all inv_value_wip(parent, op_*, ccy) for this WO.
  -- acct-du2.10 fix: gate residual absorption on solo-at-pool. When
  -- stock_wip(parent, op) qty > 0, other WOs are present and any
  -- residual on the inv_value_wip pool includes their contribution.
  -- Skip; variance lands at the last sharing WO's close.
  FOR v_op_residual IN
    SELECT a.id AS acct_id, a.routing_op
      FROM accounts a
     WHERE a.kind = 'inv_value_wip'
       AND a.sku_id = v_wo.parent_sku_id
       AND a.currency = v_wo.currency
       AND a.routing_op IN (
         SELECT routing_op FROM wo_routings WHERE wo_id = p_wo_id
       )
       AND NOT a.is_closed
     ORDER BY a.routing_op
  LOOP
    -- Solo-at-pool check: stock_wip(parent, op) qty must be 0 (this WO
    -- has already moved its qty to stock_scrap; non-zero qty here
    -- means OTHER WOs are present).
    SELECT (debits_total - credits_total) INTO v_pool_qty
      FROM accounts
     WHERE kind = 'stock_wip' AND sku_id = v_wo.parent_sku_id
       AND routing_op = v_op_residual.routing_op AND NOT is_closed;
    IF COALESCE(v_pool_qty, 0) <> 0 THEN
      CONTINUE;  -- other WOs present at this op; defer to their close
    END IF;

    PERFORM 1 FROM accounts WHERE id = v_op_residual.acct_id FOR UPDATE;
    SELECT (debits_total - credits_total) INTO v_residual
      FROM accounts WHERE id = v_op_residual.acct_id;
    IF v_residual = 0 OR v_residual IS NULL THEN CONTINUE; END IF;

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
        'credit_account_id', v_op_residual.acct_id,
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
        'debit_account_id',  v_op_residual.acct_id,
        'credit_account_id', v_var_close,
        'amount',            -v_residual,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      )), FALSE);
    END IF;
  END LOOP;

  UPDATE work_orders SET status = 'closed' WHERE id = p_wo_id;

  RETURN p_wo_id;
END;
$$;

COMMENT ON FUNCTION post_wo_close_unproduced(UUID, DATE, UUID, UUID, TEXT) IS
  'Closes a WO that produced zero units (qty_completed=0, qty_scrapped='
  'qty_target). Walks all inv_value_wip(parent, op_*) for this WO and '
  'absorbs residuals via wo_close_v, gated on solo-at-pool '
  '(stock_wip(parent, op) qty == 0). When not solo, defers to the last '
  'sharing WO''s close. Status → closed. P0034 if state is wrong. '
  'acct-7he (BOM2 B10) + acct-du2.10 (mig 0072 solo gate).';
