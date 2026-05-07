-- acct-n7p / BOM2 Phase B8 (part b) — post_wo_complete rewrite.
--
-- Adds NEW path that:
--   1. Iterates wo_outputs (single primary by default; multi-output for
--      co-products / by-products) and emits one (qty leg, value leg)
--      pair per output.
--   2. Distributes per-output qty proportionally (output.qty × p_qty /
--      qty_target) and per-output value via allocation_pct. The LAST
--      output absorbs any rounding residual so the totals balance
--      against the WIP pool.
--   3. Uses NEW reason 'wo_complete_v' for the value leg with caller-
--      supplied amount (bypasses dispatcher auto-pricing — same trick
--      as op_move_v). Qty leg keeps reason 'wo_complete' (qty-side
--      dispatcher branch already honors caller amount).
--   4. On final close (qty_completed + qty_scrapped + p_qty = qty_target):
--      walks ALL inv_value_wip(parent_sku, op, currency) accounts for
--      this WO (not just last_op) and absorbs any nonzero residual via
--      wo_close_v. Captures intermediate-op truncation residue from
--      op_move integer math.
--
-- OLD path preserved verbatim from migration 0039 (single-output to
-- work_orders.fg_location_id, last-op-only residual sweep).
--
-- Dispatch: NEW path iff parent has primary active bom_header at
-- p_business_date OR work_orders.bom_id is set (same predicate as
-- post_wo_start / post_op_move).
--
-- New error code:
--   P0033 output_allocation_invalid — Σ allocation_pct ≠ 100 at the
--     time of post_wo_complete (re-validated; post_wo_start also
--     validates at start time).

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
  v_use_new        BOOLEAN;
  v_alloc_sum      NUMERIC;
  v_outputs_n      INT;
  v_output         RECORD;
  v_output_idx     INT;
  v_parent_std     BIGINT;
  v_total_drain    BIGINT;
  v_qty_used       BIGINT := 0;
  v_val_used       BIGINT := 0;
  v_q_share        BIGINT;
  v_v_share        BIGINT;
  v_op_residual    RECORD;
BEGIN
  -- Fast-path replay check (no lock).
  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF p_qty IS NULL OR p_qty <= 0 THEN
    RAISE EXCEPTION 'wo_invalid: wo_complete qty must be > 0 (got %)', p_qty
      USING ERRCODE = 'P0026';
  END IF;

  -- Lock work_orders row.
  SELECT * INTO v_wo FROM work_orders WHERE id = p_wo_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: WO % not found', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;

  -- Race-safe replay check AFTER lock.
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

  -- Stock_wip / inv_value_wip @ last_op (source pool, both paths).
  SELECT id INTO v_qty_from FROM accounts
   WHERE kind='stock_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=v_last_op AND NOT is_closed;
  IF v_qty_from IS NULL THEN
    RAISE EXCEPTION 'no open stock_wip account for sku=% op=%',
                    v_wo.parent_sku_id, v_last_op USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_val_from FROM accounts
   WHERE kind='inv_value_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=v_last_op AND currency=v_wo.currency AND NOT is_closed;
  IF v_val_from IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_wip account for sku=% op=% ccy=%',
                    v_wo.parent_sku_id, v_last_op, v_wo.currency
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
    -- NEW PATH: drive via wo_outputs, walk-all-WIP residual at close.
    -- ============================================================

    SELECT COUNT(*), COALESCE(SUM(allocation_pct), 0)
      INTO v_outputs_n, v_alloc_sum
      FROM wo_outputs WHERE wo_id = p_wo_id;
    IF v_outputs_n = 0 THEN
      RAISE EXCEPTION 'wo_invalid: WO % has no wo_outputs rows (new path)',
                      p_wo_id USING ERRCODE = 'P0026';
    END IF;
    IF v_alloc_sum <> 100 THEN
      RAISE EXCEPTION
        'output_allocation_invalid: wo_outputs(wo=%) allocation_pct sums to % (expected 100)',
        p_wo_id, v_alloc_sum USING ERRCODE = 'P0033';
    END IF;

    v_parent_std := resolve_standard_cost_at(v_wo.parent_sku_id, p_business_date);
    v_total_drain := p_qty * v_parent_std;

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

    v_output_idx := 0;
    FOR v_output IN
      SELECT * FROM wo_outputs WHERE wo_id = p_wo_id
       ORDER BY output_no
    LOOP
      v_output_idx := v_output_idx + 1;

      -- Per-output qty share (proportional, last-row residual fix).
      IF v_output_idx = v_outputs_n THEN
        v_q_share := p_qty - v_qty_used;  -- residual to last
      ELSE
        v_q_share := (v_output.qty * p_qty) / v_wo.qty_target;
      END IF;
      v_qty_used := v_qty_used + v_q_share;

      -- Per-output value share (allocation_pct, last-row residual fix).
      IF v_output_idx = v_outputs_n THEN
        v_v_share := v_total_drain - v_val_used;
      ELSE
        v_v_share := (v_total_drain * v_output.allocation_pct)::BIGINT / 100;
      END IF;
      v_val_used := v_val_used + v_v_share;

      -- Resolve output's stock_available + inv_value_fg accounts.
      SELECT id INTO v_qty_fg FROM accounts
       WHERE kind='stock_available' AND sku_id=v_output.output_sku_id
         AND location_id=v_output.fg_location_id AND NOT is_closed;
      IF v_qty_fg IS NULL THEN
        RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                        v_output.output_sku_id, v_output.fg_location_id
          USING ERRCODE = 'P0010';
      END IF;
      SELECT id INTO v_val_fg FROM accounts
       WHERE kind='inv_value_fg' AND sku_id=v_output.output_sku_id
         AND location_id=v_output.fg_location_id AND currency=v_wo.currency
         AND NOT is_closed;
      IF v_val_fg IS NULL THEN
        RAISE EXCEPTION 'no open inv_value_fg account for sku=% loc=% ccy=%',
                        v_output.output_sku_id, v_output.fg_location_id, v_wo.currency
          USING ERRCODE = 'P0010';
      END IF;

      -- Qty leg (reason wo_complete; dispatcher honors caller amount on qty side).
      IF v_q_share > 0 THEN
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'wo_complete',
          'document_kind',     'wo_event',
          'document_id',       v_event_id,
          'debit_account_id',  v_qty_fg,
          'credit_account_id', v_qty_from,
          'amount',            v_q_share,
          'qty',               v_q_share,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        ));
      END IF;

      -- Value leg (reason wo_complete_v; bypasses dispatcher auto-pricing).
      IF v_v_share > 0 THEN
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'wo_complete_v',
          'document_kind',     'wo_event',
          'document_id',       v_event_id,
          'debit_account_id',  v_val_fg,
          'credit_account_id', v_val_from,
          'amount',            v_v_share,
          'qty',               v_q_share,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        ));
      END IF;
    END LOOP;

    PERFORM post_transfers(v_batch, FALSE);

    UPDATE work_orders SET qty_completed = qty_completed + p_qty
     WHERE id = p_wo_id;

    IF v_will_close THEN
      -- Walk ALL inv_value_wip(parent, op_*, ccy) for this WO; absorb residuals.
      FOR v_op_residual IN
        SELECT a.id AS acct_id,
               (a.debits_total - a.credits_total) AS balance
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
    END IF;

    RETURN p_wo_id;
  END IF;

  -- ============================================================
  -- OLD PATH (Slice B): single-output, last-op-only residual.
  -- ============================================================

  SELECT id INTO v_qty_fg FROM accounts
   WHERE kind='stock_available' AND sku_id=v_wo.parent_sku_id
     AND location_id=v_wo.fg_location_id AND NOT is_closed;
  IF v_qty_fg IS NULL THEN
    RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                    v_wo.parent_sku_id, v_wo.fg_location_id
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

COMMENT ON FUNCTION post_wo_complete(UUID, BIGINT, DATE, UUID, UUID, TEXT) IS
  'Completes p_qty units. Dispatches new vs old BOM model. NEW path '
  'drives multi-output distribution via wo_outputs (proportional qty + '
  'allocation_pct value), uses wo_complete_v reason on value leg to '
  'bypass dispatcher auto-pricing, and walks ALL inv_value_wip(parent,op) '
  'residuals at final close. OLD path: single-output, last-op residual. '
  'New error: P0033 output_allocation_invalid. acct-n7p (BOM2 B8).';
