-- acct-3uh — re-applying the rollback would re-create post_osp_ship +
-- post_osp_receive verbatim from migration 0057. See that file for the
-- function bodies. Down here mirrors 0057's up.

CREATE OR REPLACE FUNCTION post_osp_ship(
  p_wo_id           UUID,
  p_routing_op      INT,
  p_qty             BIGINT,
  p_vendor_id       UUID,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing      BIGINT;
  v_wo            work_orders%ROWTYPE;
  v_op_count      INT;
  v_qty_from      BIGINT;
  v_qty_to        BIGINT;
  v_wip_balance   BIGINT;
BEGIN
  SELECT id INTO v_existing FROM transfers
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF p_qty IS NULL OR p_qty <= 0 THEN
    RAISE EXCEPTION 'wo_invalid: osp_ship qty must be > 0 (got %)', p_qty
      USING ERRCODE = 'P0026';
  END IF;
  IF p_vendor_id IS NULL THEN
    RAISE EXCEPTION 'wo_invalid: osp_ship requires vendor_id'
      USING ERRCODE = 'P0026';
  END IF;

  SELECT * INTO v_wo FROM work_orders WHERE id = p_wo_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: WO % not found', p_wo_id USING ERRCODE = 'P0026';
  END IF;
  IF v_wo.status <> 'released' THEN
    RAISE EXCEPTION 'wo_invalid: WO % status=% not released',
                    p_wo_id, v_wo.status USING ERRCODE = 'P0026';
  END IF;

  SELECT COUNT(*) INTO v_op_count FROM wo_routings
   WHERE wo_id = p_wo_id AND routing_op = p_routing_op;
  IF v_op_count = 0 THEN
    RAISE EXCEPTION 'routing_op_invalid: op % not in WO % routing',
                    p_routing_op, p_wo_id USING ERRCODE = 'P0028';
  END IF;

  SELECT id INTO v_qty_from FROM accounts
   WHERE kind='stock_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_routing_op AND NOT is_closed;
  IF v_qty_from IS NULL THEN
    RAISE EXCEPTION 'no open stock_wip account for sku=% op=%',
                    v_wo.parent_sku_id, p_routing_op USING ERRCODE = 'P0010';
  END IF;

  SELECT id INTO v_qty_to FROM accounts
   WHERE kind='stock_consigned_at_vendor'
     AND sku_id=v_wo.parent_sku_id
     AND counterparty_id=p_vendor_id
     AND routing_op=p_routing_op
     AND NOT is_closed;
  IF v_qty_to IS NULL THEN
    RAISE EXCEPTION 'no open stock_consigned_at_vendor account for sku=% vendor=% op=%',
                    v_wo.parent_sku_id, p_vendor_id, p_routing_op
      USING ERRCODE = 'P0010';
  END IF;

  PERFORM 1 FROM accounts WHERE id IN (v_qty_from, v_qty_to)
   ORDER BY id FOR UPDATE;
  SELECT (debits_total - credits_total) INTO v_wip_balance
    FROM accounts WHERE id = v_qty_from;
  IF p_qty > v_wip_balance THEN
    RAISE EXCEPTION
      'wo_invalid: osp_ship qty=% > stock_wip balance=% at sku=% op=%',
      p_qty, v_wip_balance, v_wo.parent_sku_id, p_routing_op
      USING ERRCODE = 'P0026';
  END IF;

  PERFORM post_transfers(jsonb_build_array(jsonb_build_object(
    'reason',            'osp_ship',
    'document_kind',     'work_order',
    'document_id',       p_wo_id,
    'debit_account_id',  v_qty_to,
    'credit_account_id', v_qty_from,
    'amount',            p_qty,
    'qty',               p_qty,
    'business_date',     p_business_date,
    'idempotency_key',   p_idempotency_key,
    'posted_by',         p_posted_by,
    'counterparty_id',   p_vendor_id,
    'notes',             p_notes
  )), FALSE);

  RETURN p_wo_id;
END;
$$;

CREATE OR REPLACE FUNCTION post_osp_receive(
  p_wo_id           UUID,
  p_routing_op      INT,
  p_qty             BIGINT,
  p_vendor_id       UUID,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing      BIGINT;
  v_wo            work_orders%ROWTYPE;
  v_op_count      INT;
  v_qty_from      BIGINT;
  v_qty_to        BIGINT;
  v_consigned     BIGINT;
BEGIN
  SELECT id INTO v_existing FROM transfers
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF p_qty IS NULL OR p_qty <= 0 THEN
    RAISE EXCEPTION 'wo_invalid: osp_receive qty must be > 0 (got %)', p_qty
      USING ERRCODE = 'P0026';
  END IF;
  IF p_vendor_id IS NULL THEN
    RAISE EXCEPTION 'wo_invalid: osp_receive requires vendor_id'
      USING ERRCODE = 'P0026';
  END IF;

  SELECT * INTO v_wo FROM work_orders WHERE id = p_wo_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: WO % not found', p_wo_id USING ERRCODE = 'P0026';
  END IF;
  IF v_wo.status <> 'released' THEN
    RAISE EXCEPTION 'wo_invalid: WO % status=% not released',
                    p_wo_id, v_wo.status USING ERRCODE = 'P0026';
  END IF;

  SELECT COUNT(*) INTO v_op_count FROM wo_routings
   WHERE wo_id = p_wo_id AND routing_op = p_routing_op;
  IF v_op_count = 0 THEN
    RAISE EXCEPTION 'routing_op_invalid: op % not in WO % routing',
                    p_routing_op, p_wo_id USING ERRCODE = 'P0028';
  END IF;

  SELECT id INTO v_qty_from FROM accounts
   WHERE kind='stock_consigned_at_vendor'
     AND sku_id=v_wo.parent_sku_id
     AND counterparty_id=p_vendor_id
     AND routing_op=p_routing_op
     AND NOT is_closed;
  IF v_qty_from IS NULL THEN
    RAISE EXCEPTION 'no open stock_consigned_at_vendor account for sku=% vendor=% op=%',
                    v_wo.parent_sku_id, p_vendor_id, p_routing_op
      USING ERRCODE = 'P0010';
  END IF;

  SELECT id INTO v_qty_to FROM accounts
   WHERE kind='stock_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_routing_op AND NOT is_closed;
  IF v_qty_to IS NULL THEN
    RAISE EXCEPTION 'no open stock_wip account for sku=% op=%',
                    v_wo.parent_sku_id, p_routing_op USING ERRCODE = 'P0010';
  END IF;

  PERFORM 1 FROM accounts WHERE id IN (v_qty_from, v_qty_to)
   ORDER BY id FOR UPDATE;
  SELECT (debits_total - credits_total) INTO v_consigned
    FROM accounts WHERE id = v_qty_from;
  IF p_qty > COALESCE(v_consigned, 0) THEN
    RAISE EXCEPTION
      'osp_yield_overflow: osp_receive qty=% > stock_consigned_at_vendor balance=% '
      '(sku=% vendor=% op=%); cannot receive more than was shipped',
      p_qty, v_consigned, v_wo.parent_sku_id, p_vendor_id, p_routing_op
      USING ERRCODE = 'P0030';
  END IF;

  PERFORM post_transfers(jsonb_build_array(jsonb_build_object(
    'reason',            'osp_receive',
    'document_kind',     'work_order',
    'document_id',       p_wo_id,
    'debit_account_id',  v_qty_to,
    'credit_account_id', v_qty_from,
    'amount',            p_qty,
    'qty',               p_qty,
    'business_date',     p_business_date,
    'idempotency_key',   p_idempotency_key,
    'posted_by',         p_posted_by,
    'counterparty_id',   p_vendor_id,
    'notes',             p_notes
  )), FALSE);

  RETURN p_wo_id;
END;
$$;
