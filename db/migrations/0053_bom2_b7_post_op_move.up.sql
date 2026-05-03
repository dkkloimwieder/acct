-- acct-j3r / BOM2 Phase B7 — post_op_move dispatch-by-existence rewrite.
--
-- Adds a NEW path that drives the value-leg amount and the to_op
-- emission via the new BOM model:
--
--   1. std_cum_at_from_op (per-unit-of-parent) computed from
--      _wo_explode_bom + scrap_pct gross-up + per-lot amortization
--      by skus.default_lot_size.
--
--   2. First-arrival detection at p_to_op via wo_events scan
--      (event_kind='op_move' WHERE routing_op_to=p_to_op,
--      OR event_kind='start' if p_to_op is the first_op).
--
--   3. _wo_emit_bom_lines emits the to_op-arrival lines:
--        first arrival      → all fire_at='op_arrival' lines at p_to_op
--                             (per_unit + per_lot)
--        subsequent arrival → only per_unit (per_lot already fired)
--
-- Old (Slice B) path is preserved verbatim from migration 0038 so
-- existing tests stay green. Dispatch is via the same predicate as
-- post_wo_start: parent has primary active bom_header at business_date
-- OR work_orders.bom_id is set.
--
-- std_cum_at_from formula (NEW path, per-unit-of-parent):
--   per_unit  = Σ over basis='per_unit' AND applies_at_op ≤ from_op:
--                 item  → qty_per_parent × resolve_standard_cost_at(comp)
--                         / (1 - scrap_pct/100)            (gross-up)
--                 svc   → std_amount
--   per_lot   = Σ std_amount over basis='per_lot' WHERE fired-by-now:
--                 fire_at='wo_start'                        always
--                 fire_at='op_arrival' AND applies_at_op ≤ from_op
--               / parent_sku.default_lot_size               (amortize)
--   std_cum_at_from = per_unit + per_lot
--   value_amount    = p_qty × std_cum_at_from
--
-- This is the "planned" std_cum and may diverge slightly from actual
-- pool (due to CEIL on grossed-up qty in _wo_emit_bom_lines and
-- per-lot amortization residual on lot-size mismatch). Variance
-- lands in wo_close_v at WO close.

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
  v_std_cum_at_from  BIGINT;
  v_value_amount     BIGINT;
  v_batch            JSONB := '[]'::JSONB;
  v_use_new          BOOLEAN;
  v_bom              bom_headers%ROWTYPE;
  v_first_op         INT;
  v_default_lot_size BIGINT;
  v_per_unit_cum     BIGINT;
  v_per_lot_cum      BIGINT;
  v_first_arrival    BOOLEAN;
  -- old-path locals
  v_rm_per_unit      BIGINT;
  v_burden_at_from   BIGINT;
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

  -- Both ops in routing.
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

  -- WIP qty + value accounts (from / to).
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

  -- Decide path.
  v_use_new := v_wo.bom_id IS NOT NULL OR EXISTS (
    SELECT 1 FROM bom_headers bh
     WHERE bh.parent_sku_id = v_wo.parent_sku_id
       AND bh.is_primary AND bh.status='active'
       AND bh.effective_at <= (p_business_date::TIMESTAMPTZ + INTERVAL '1 day')
       AND bh.obsolete_at  >  p_business_date::TIMESTAMPTZ
  );

  IF v_use_new THEN
    -- ============================================================
    -- NEW PATH: compute std_cum_at_from from bom_lines
    -- ============================================================
    v_bom := _wo_resolve_bom_for(p_wo_id, p_business_date);
    SELECT default_lot_size INTO v_default_lot_size
      FROM skus WHERE id = v_wo.parent_sku_id;
    SELECT MIN(routing_op) INTO v_first_op
      FROM wo_routings WHERE wo_id = p_wo_id;

    -- Per-unit contribution at applies_at_op ≤ from_op.
    SELECT COALESCE(SUM(
      CASE
        WHEN exp.kind = 'item' THEN
          (exp.qty_per_parent::NUMERIC
            * resolve_standard_cost_at(exp.component_sku_id, p_business_date)
            / (1 - exp.scrap_pct / 100.0))::BIGINT
        WHEN exp.kind = 'service' AND exp.basis = 'per_unit' THEN exp.std_amount
        ELSE 0
      END
    ), 0) INTO v_per_unit_cum
      FROM _wo_explode_bom(v_bom.id, p_business_date) exp
     WHERE exp.basis = 'per_unit'
       AND exp.applies_at_op <= p_from_op;

    -- Per-lot contribution amortized (fired-by-now).
    SELECT COALESCE(SUM(exp.std_amount), 0) / v_default_lot_size
      INTO v_per_lot_cum
      FROM _wo_explode_bom(v_bom.id, p_business_date) exp
     WHERE exp.basis = 'per_lot'
       AND (
         exp.fire_at = 'wo_start'
         OR (exp.fire_at = 'op_arrival' AND exp.applies_at_op <= p_from_op)
       );

    v_std_cum_at_from := v_per_unit_cum + v_per_lot_cum;
    v_value_amount    := p_qty * v_std_cum_at_from;

    -- First-arrival detection at p_to_op.
    v_first_arrival := NOT EXISTS (
      SELECT 1 FROM wo_events
       WHERE wo_id = p_wo_id
         AND (
           (event_kind = 'op_move' AND routing_op_to = p_to_op)
           OR (event_kind = 'start' AND p_to_op = v_first_op)
         )
    );
  ELSE
    -- ============================================================
    -- OLD PATH (Slice B): boms + wo_routing_burdens
    -- ============================================================
    SELECT COALESCE(SUM(b.qty_per_parent
                        * resolve_standard_cost_at(b.component_sku_id, p_business_date)), 0)
      INTO v_rm_per_unit
      FROM boms b WHERE b.parent_sku_id = v_wo.parent_sku_id;
    SELECT COALESCE(SUM(std_amount), 0) INTO v_burden_at_from
      FROM wo_routing_burdens
     WHERE wo_id = p_wo_id AND routing_op <= p_from_op;
    v_std_cum_at_from := v_rm_per_unit + v_burden_at_from;
    v_value_amount    := p_qty * v_std_cum_at_from;
  END IF;

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

  -- Qty leg (op_move).
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

  -- Value leg (op_move_v): caller-supplied amount stands.
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

  -- to_op burdens.
  IF v_use_new THEN
    IF v_first_arrival THEN
      -- First arrival: per_unit + per_lot at p_to_op.
      v_batch := v_batch || _wo_emit_bom_lines(
        p_wo_id, v_bom.id, p_to_op, p_qty,
        jsonb_build_object('fire_at', 'op_arrival', 'applies_at_op', p_to_op),
        v_event_id, p_business_date, p_posted_by
      );
    ELSE
      -- Subsequent arrival: per_unit only.
      v_batch := v_batch || _wo_emit_bom_lines(
        p_wo_id, v_bom.id, p_to_op, p_qty,
        jsonb_build_object('fire_at', 'op_arrival', 'applies_at_op', p_to_op,
                           'basis', 'per_unit'),
        v_event_id, p_business_date, p_posted_by
      );
    END IF;
  ELSE
    v_batch := v_batch || _wo_burden_events_for_op(
      p_wo_id, p_to_op, p_qty,
      v_val_to, v_wo.currency, p_business_date,
      v_event_id, p_posted_by
    );
  END IF;

  PERFORM post_transfers(v_batch, FALSE);

  RETURN p_wo_id;
END;
$$;

COMMENT ON FUNCTION post_op_move(UUID, INT, INT, BIGINT, DATE, UUID, UUID, TEXT) IS
  'Moves p_qty units from p_from_op to p_to_op, then applies to_op '
  'lines. Dispatches new vs old BOM model on parent_sku state. NEW: '
  'std_cum from bom_lines (with scrap_pct gross-up + per-lot lot-size '
  'amortization); first-arrival detection drives whether per_lot fires '
  'on this move. OLD: preserved Slice B math via boms + wo_routing_burdens. '
  'acct-j3r (BOM2 B7).';
