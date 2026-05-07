-- acct-7he / BOM2 Phase B10 — post_wo_close_unproduced.
--
-- New entry point for closing a fully-scrapped WO that produced zero
-- units. post_wo_complete requires p_qty > 0, so a WO whose qty_target
-- is fully consumed by post_scrap calls cannot reach 'closed' through
-- the normal completion path. This function fills that gap:
--   - validates qty_completed = 0 AND qty_scrapped = qty_target
--   - walks ALL inv_value_wip(parent, op, currency) accounts for this WO
--     and absorbs each non-zero residual via wo_close_v
--   - status → closed
--
-- This is also useful for incident WOs where rework is impossible and
-- the operator scraps the entire lot. Caller still needs to scrap the
-- full qty_target before calling this; otherwise P0034.
--
-- New error code:
--   P0034 wo_close_unproduced_invalid_state — qty_completed > 0 OR
--     qty_scrapped < qty_target OR status not 'released'.
--
-- Schema change: extend wo_events.event_kind CHECK to allow
-- 'close_unproduced' (no routing_op_from/to/qty — pure audit row).

ALTER TABLE wo_events DROP CONSTRAINT wo_events_event_kind_check;
ALTER TABLE wo_events ADD CONSTRAINT wo_events_event_kind_check
  CHECK (event_kind IN ('start','op_move','wo_complete','scrap','close_unproduced'));

ALTER TABLE wo_events DROP CONSTRAINT wo_events_check;
ALTER TABLE wo_events ADD CONSTRAINT wo_events_check CHECK (
  (event_kind = 'start'
   AND routing_op_from IS NULL AND routing_op_to IS NULL AND qty IS NULL)
  OR
  (event_kind = 'op_move'
   AND routing_op_from IS NOT NULL AND routing_op_to IS NOT NULL
   AND qty IS NOT NULL)
  OR
  (event_kind = 'wo_complete'
   AND routing_op_from IS NOT NULL AND routing_op_to IS NULL
   AND qty IS NOT NULL)
  OR
  (event_kind = 'scrap'
   AND routing_op_from IS NOT NULL AND routing_op_to IS NULL
   AND qty IS NOT NULL)
  OR
  (event_kind = 'close_unproduced'
   AND routing_op_from IS NULL AND routing_op_to IS NULL AND qty IS NULL)
);

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
BEGIN
  -- Fast-path replay check.
  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  SELECT * INTO v_wo FROM work_orders WHERE id = p_wo_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: WO % not found', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;

  -- Race-safe replay check after lock.
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

  -- Walk all inv_value_wip(parent, op_*, ccy) for this WO; absorb residuals.
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
  'Closes a WO that produced zero units (qty_completed=0, qty_scrapped=qty_target). '
  'Walks all inv_value_wip(parent, op_*) for this WO and absorbs residuals via '
  'wo_close_v. Status → closed. P0034 if state is wrong. acct-7he (BOM2 B10).';
