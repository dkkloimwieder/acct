-- ============================================================
-- Phase E2 follow-up — lot_fifo parent + by-products at post_wo_complete
-- (acct-fjxp).
--
-- Lifts the gate that previously raised P0006 when a lot_fifo
-- parent had wo_by_products rows. Three of the four treatment
-- shapes are admitted:
--
--   * nrv_credit: works because the existing flow REDUCES
--     v_total_drain by v_byproduct_drain (mig 0050:1002), so the
--     per-output drain debits inv_value_fg with the post-credit
--     amount. The FG-lot row is then created at the post-NRV
--     unit_cost (v_v_share / v_q_share). Pool stays balanced
--     under FIFO walk; lot residual matches inv_value_fg through
--     full depletion.
--
--   * negligible: bp.unit_value=0 by CHECK, so no value-leg
--     fires; only the qty leg posts (stock_available DR /
--     creation_void CR). Parent FG pool untouched.
--
--   * disposal_cost (period basis): debits disposal_expense and
--     credits accrued_disposal_liability. Doesn't touch parent
--     FG pool. Yield variance flows through variance_yield_byproduct
--     symmetrically. No drift.
--
-- The fourth shape is REJECTED:
--
--   * disposal_cost (inventoriable basis): debits inv_value_fg
--     of each wo_output (parent + co-products) WITHOUT modifying
--     v_total_drain. The per-output drain creates the FG-lot at
--     the pre-disposal unit_cost while the pool carries the
--     extra disposal value — drift on full depletion that the
--     lot subledger can't reconcile (lots are append-only;
--     unit_cost is fixed at receipt). Surfaces as P0006 with a
--     descriptive message; future work needed to either route
--     to a per-lot revaluation account or invert the order so
--     the lot is created at the disposal-inclusive unit_cost.
--
-- By-product SKU's own cost_method is not constrained here.
-- A lot_fifo by-product SKU would fail at apply_event's E2 block
-- (no lot_code in the wo_byproduct_credit JSON) — that's
-- pre-existing behavior, separate from this fix.
-- ============================================================

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
  v_residual       BIGINT;
  v_batch          JSONB := '[]'::JSONB;
  v_event_obj      JSONB;
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
  v_pool_at_last   BIGINT;
  v_prebalance     BIGINT;
  v_cost_method    cost_method;
  v_pool_qty       BIGINT;
  v_unit           BIGINT;
  v_pool_qty_pre   BIGINT;
  v_op_qty_acct    BIGINT;
  v_op_qty         BIGINT;
  v_solo_at_last   BOOLEAN;
  v_lock_first     BIGINT;
  v_lock_second    BIGINT;
  v_bp             wo_by_products%ROWTYPE;
  v_bp_qty_acct    BIGINT;
  v_bp_val_acct    BIGINT;
  v_void_qty       BIGINT;
  v_byproduct_drain BIGINT := 0;
  v_disp_total       BIGINT;
  v_disp_liability   BIGINT;
  v_disp_exp_acct    BIGINT;
  v_disp_exp_kind    account_kind;
  v_disp_share       BIGINT;
  v_disp_used        BIGINT;
  v_disp_output      RECORD;
  v_disp_output_idx  INT;
  v_yield_var_acct   BIGINT;
  v_yield_qty_delta  BIGINT;
  v_yield_amount     BIGINT;
  v_lot_code         TEXT;
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

  SELECT COUNT(*), COALESCE(SUM(allocation_pct), 0)
    INTO v_outputs_n, v_alloc_sum
    FROM wo_outputs WHERE wo_id = p_wo_id;
  IF v_outputs_n = 0 THEN
    RAISE EXCEPTION 'wo_invalid: WO % has no wo_outputs rows', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;
  IF v_alloc_sum <> 100 THEN
    RAISE EXCEPTION
      'output_allocation_invalid: wo_outputs(wo=%) allocation_pct sums to % (expected 100)',
      p_wo_id, v_alloc_sum USING ERRCODE = 'P0033';
  END IF;

  v_lock_first  := LEAST(v_qty_from, v_val_from);
  v_lock_second := GREATEST(v_qty_from, v_val_from);
  PERFORM 1 FROM accounts WHERE id = v_lock_first  FOR UPDATE;
  PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;

  SELECT (debits_total - credits_total) INTO v_pool_qty_pre
    FROM accounts WHERE id = v_qty_from;
  v_solo_at_last := COALESCE(v_pool_qty_pre, 0) = p_qty;

  SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_wo.parent_sku_id;

  -- acct-fjxp: lot_fifo parent + by-products is now SUPPORTED for
  -- nrv_credit, negligible, and disposal_cost(period). The fourth
  -- shape (disposal_cost inventoriable) is rejected at the by-products
  -- pre-pass below.

  IF v_cost_method = 'standard' THEN
    v_parent_std  := _resolve_standard_cost_at(v_wo.parent_sku_id, p_business_date);
    v_total_drain := p_qty * v_parent_std;

  -- L4: 'lot_fifo' joins WAC family for parent FG drain calc.
  ELSIF v_cost_method IN ('wac_perpetual', 'wac_periodic',
                          'wac_retroactive', 'fifo', 'lot_fifo') THEN
    SELECT (debits_total - credits_total) INTO v_pool_at_last
      FROM accounts WHERE id = v_val_from;
    SELECT (debits_total - credits_total) INTO v_pool_qty
      FROM accounts WHERE id = v_qty_from;

    IF v_pool_qty IS NULL OR v_pool_qty <= 0 THEN
      v_unit := 0;
    ELSE
      v_unit := GREATEST(COALESCE(v_pool_at_last, 0), 0) / v_pool_qty;
    END IF;
    v_total_drain := p_qty * v_unit;

  ELSE
    RAISE EXCEPTION
      'wo_invalid: parent_sku % has cost_method=% which post_wo_complete does not handle',
      v_wo.parent_sku_id, v_cost_method USING ERRCODE = 'P0026';
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

  IF v_will_close AND v_solo_at_last THEN
    IF v_cost_method = 'standard' THEN
      SELECT (debits_total - credits_total) INTO v_pool_at_last
        FROM accounts WHERE id = v_val_from;
    END IF;
    v_prebalance := v_total_drain - COALESCE(v_pool_at_last, 0);

    IF v_prebalance <> 0 THEN
      SELECT id INTO v_var_close FROM accounts
       WHERE kind='variance_wo_close' AND ledger_kind='value'
         AND currency=v_wo.currency AND NOT is_closed;
      IF v_var_close IS NULL THEN
        RAISE EXCEPTION 'no open variance_wo_close account for ccy=%',
                        v_wo.currency USING ERRCODE = 'P0010';
      END IF;

      IF v_prebalance > 0 THEN
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'wo_close_v',
          'document_kind',     'wo_complete',
          'document_id',       v_event_id,
          'debit_account_id',  v_val_from,
          'credit_account_id', v_var_close,
          'amount',            v_prebalance,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        ));
      ELSE
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'wo_close_v',
          'document_kind',     'wo_complete',
          'document_id',       v_event_id,
          'debit_account_id',  v_var_close,
          'credit_account_id', v_val_from,
          'amount',            -v_prebalance,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        ));
      END IF;
    END IF;
  END IF;

  -- By-products pre-pass on closing call. acct-nnyl admitted
  -- standard + WAC family; acct-fjxp now admits lot_fifo as well.
  -- (fifo silently skipped, pre-existing behavior — see acct-nnyl
  -- mig 0040 commentary for the rationale.)
  IF v_will_close AND v_cost_method IN
       ('standard', 'wac_perpetual', 'wac_periodic',
        'wac_retroactive', 'lot_fifo') THEN
    SELECT id INTO v_void_qty FROM accounts
     WHERE kind='creation_void' AND ledger_kind='qty' AND NOT is_closed;

    FOR v_bp IN
      SELECT * FROM wo_by_products WHERE wo_id = p_wo_id
       ORDER BY by_product_no
    LOOP
      v_yield_qty_delta := v_bp.actual_qty - v_bp.planned_qty;

      IF v_bp.actual_qty > 0 THEN
        IF v_void_qty IS NULL THEN
          RAISE EXCEPTION 'no creation_void(qty) account configured'
            USING ERRCODE = 'P0010';
        END IF;
        SELECT id INTO v_bp_qty_acct FROM accounts
         WHERE kind='stock_available' AND sku_id=v_bp.output_sku_id
           AND location_id=v_bp.fg_location_id AND NOT is_closed;
        IF v_bp_qty_acct IS NULL THEN
          RAISE EXCEPTION
            'no open stock_available account for by-product sku=% loc=%',
            v_bp.output_sku_id, v_bp.fg_location_id USING ERRCODE = 'P0010';
        END IF;

        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'wo_complete',
          'document_kind',     'wo_complete',
          'document_id',       v_event_id,
          'debit_account_id',  v_bp_qty_acct,
          'credit_account_id', v_void_qty,
          'amount',            v_bp.actual_qty,
          'qty',               v_bp.actual_qty,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        ));
      END IF;

      IF v_bp.treatment = 'nrv_credit' THEN
        SELECT id INTO v_bp_val_acct FROM accounts
         WHERE kind='inv_value_fg' AND sku_id=v_bp.output_sku_id
           AND location_id=v_bp.fg_location_id AND currency=v_wo.currency
           AND NOT is_closed;
        IF v_bp_val_acct IS NULL THEN
          RAISE EXCEPTION
            'no open inv_value_fg account for by-product sku=% loc=% ccy=%',
            v_bp.output_sku_id, v_bp.fg_location_id, v_wo.currency
            USING ERRCODE = 'P0010';
        END IF;

        v_byproduct_drain := v_byproduct_drain + v_bp.unit_value * v_bp.planned_qty;

        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'wo_byproduct_credit',
          'document_kind',     'wo_complete',
          'document_id',       v_event_id,
          'debit_account_id',  v_bp_val_acct,
          'credit_account_id', v_val_from,
          'amount',            v_bp.unit_value * v_bp.planned_qty,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        ));

        IF v_yield_qty_delta <> 0 THEN
          SELECT id INTO v_yield_var_acct FROM accounts
           WHERE kind='variance_yield_byproduct' AND ledger_kind='value'
             AND currency=v_wo.currency AND NOT is_closed;
          IF v_yield_var_acct IS NULL THEN
            RAISE EXCEPTION
              'no open variance_yield_byproduct account for ccy=%',
              v_wo.currency USING ERRCODE = 'P0010';
          END IF;

          v_yield_amount := v_yield_qty_delta * v_bp.unit_value;
          IF v_yield_amount > 0 THEN
            v_batch := v_batch || jsonb_build_array(jsonb_build_object(
              'reason',            'wo_byproduct_credit',
              'document_kind',     'wo_complete',
              'document_id',       v_event_id,
              'debit_account_id',  v_bp_val_acct,
              'credit_account_id', v_yield_var_acct,
              'amount',            v_yield_amount,
              'qty',               v_yield_qty_delta,
              'business_date',     p_business_date,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         p_posted_by
            ));
          ELSE
            v_batch := v_batch || jsonb_build_array(jsonb_build_object(
              'reason',            'wo_byproduct_credit',
              'document_kind',     'wo_complete',
              'document_id',       v_event_id,
              'debit_account_id',  v_yield_var_acct,
              'credit_account_id', v_bp_val_acct,
              'amount',            -v_yield_amount,
              'qty',               -v_yield_qty_delta,
              'business_date',     p_business_date,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         p_posted_by
            ));
          END IF;
        END IF;

      ELSIF v_bp.treatment = 'disposal_cost' THEN
        -- acct-fjxp: lot_fifo parent + disposal_cost(inventoriable)
        -- has structural drift between the per-output drain
        -- (creates lot at pre-disposal unit_cost) and the disposal
        -- debit (inflates inv_value_fg without a paired lot value).
        -- On full lot depletion, disposal_share residue stays in
        -- the pool. Lots are append-only so the unit_cost can't
        -- be retroactively rebased. Reject with a descriptive
        -- error; future work should route the disposal share to a
        -- per-lot revaluation account or invert the order so the
        -- lot is created at the disposal-inclusive amount.
        IF v_cost_method = 'lot_fifo'
           AND v_bp.disposal_basis = 'inventoriable' THEN
          RAISE EXCEPTION
            'cost_method_not_implemented: lot_fifo parent + '
            'disposal_cost(inventoriable) at wo_complete (wo=%, '
            'by_product_no=%); period basis is supported, '
            'inventoriable basis requires per-lot revaluation '
            'infrastructure not yet built',
            p_wo_id, v_bp.by_product_no USING ERRCODE = 'P0006';
        END IF;

        SELECT id INTO v_disp_liability FROM accounts
         WHERE kind = 'accrued_disposal_liability'
           AND counterparty_id = v_bp.disposal_vendor_id
           AND currency = v_wo.currency
           AND NOT is_closed;
        IF v_disp_liability IS NULL THEN
          RAISE EXCEPTION
            'no open accrued_disposal_liability account for vendor=% ccy=%',
            v_bp.disposal_vendor_id, v_wo.currency
            USING ERRCODE = 'P0010';
        END IF;

        v_disp_total := ABS(v_bp.unit_value) * v_bp.planned_qty;

        IF v_bp.disposal_basis = 'period' THEN
          v_disp_exp_kind := COALESCE(
            v_bp.disposal_expense_account_kind,
            'disposal_expense'::account_kind
          );
          SELECT id INTO v_disp_exp_acct FROM accounts
           WHERE kind = v_disp_exp_kind
             AND ledger_kind = 'value'
             AND currency = v_wo.currency
             AND NOT is_closed;
          IF v_disp_exp_acct IS NULL THEN
            RAISE EXCEPTION
              'no open % account for ccy=%',
              v_disp_exp_kind, v_wo.currency USING ERRCODE = 'P0010';
          END IF;

          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'wo_byproduct_credit',
            'document_kind',     'wo_complete',
            'document_id',       v_event_id,
            'debit_account_id',  v_disp_exp_acct,
            'credit_account_id', v_disp_liability,
            'amount',            v_disp_total,
            'qty',               v_bp.planned_qty,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'posted_by',         p_posted_by
          ));

        ELSIF v_bp.disposal_basis = 'inventoriable' THEN
          v_disp_used := 0;
          v_disp_output_idx := 0;
          FOR v_disp_output IN
            SELECT * FROM wo_outputs WHERE wo_id = p_wo_id
             ORDER BY output_no
          LOOP
            v_disp_output_idx := v_disp_output_idx + 1;
            IF v_disp_output_idx = v_outputs_n THEN
              v_disp_share := v_disp_total - v_disp_used;
            ELSE
              v_disp_share := (v_disp_total * v_disp_output.allocation_pct)::BIGINT / 100;
            END IF;
            v_disp_used := v_disp_used + v_disp_share;

            IF v_disp_share = 0 THEN
              CONTINUE;
            END IF;

            SELECT id INTO v_val_fg FROM accounts
             WHERE kind = 'inv_value_fg'
               AND sku_id = v_disp_output.output_sku_id
               AND location_id = v_disp_output.fg_location_id
               AND currency = v_wo.currency
               AND NOT is_closed;
            IF v_val_fg IS NULL THEN
              RAISE EXCEPTION
                'no open inv_value_fg account for sku=% loc=% ccy=%',
                v_disp_output.output_sku_id, v_disp_output.fg_location_id, v_wo.currency
                USING ERRCODE = 'P0010';
            END IF;

            v_batch := v_batch || jsonb_build_array(jsonb_build_object(
              'reason',            'wo_byproduct_credit',
              'document_kind',     'wo_complete',
              'document_id',       v_event_id,
              'debit_account_id',  v_val_fg,
              'credit_account_id', v_disp_liability,
              'amount',            v_disp_share,
              'business_date',     p_business_date,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         p_posted_by
            ));
          END LOOP;
        END IF;

        IF v_yield_qty_delta <> 0 THEN
          SELECT id INTO v_yield_var_acct FROM accounts
           WHERE kind='variance_yield_byproduct' AND ledger_kind='value'
             AND currency=v_wo.currency AND NOT is_closed;
          IF v_yield_var_acct IS NULL THEN
            RAISE EXCEPTION
              'no open variance_yield_byproduct account for ccy=%',
              v_wo.currency USING ERRCODE = 'P0010';
          END IF;

          v_yield_amount := v_yield_qty_delta * ABS(v_bp.unit_value);
          IF v_yield_amount > 0 THEN
            v_batch := v_batch || jsonb_build_array(jsonb_build_object(
              'reason',            'wo_byproduct_credit',
              'document_kind',     'wo_complete',
              'document_id',       v_event_id,
              'debit_account_id',  v_yield_var_acct,
              'credit_account_id', v_disp_liability,
              'amount',            v_yield_amount,
              'qty',               v_yield_qty_delta,
              'business_date',     p_business_date,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         p_posted_by
            ));
          ELSE
            v_batch := v_batch || jsonb_build_array(jsonb_build_object(
              'reason',            'wo_byproduct_credit',
              'document_kind',     'wo_complete',
              'document_id',       v_event_id,
              'debit_account_id',  v_disp_liability,
              'credit_account_id', v_yield_var_acct,
              'amount',            -v_yield_amount,
              'qty',               -v_yield_qty_delta,
              'business_date',     p_business_date,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         p_posted_by
            ));
          END IF;
        END IF;
      END IF;
    END LOOP;

    v_total_drain := v_total_drain - v_byproduct_drain;
  END IF;

  v_output_idx := 0;
  FOR v_output IN
    SELECT * FROM wo_outputs WHERE wo_id = p_wo_id
     ORDER BY output_no
  LOOP
    v_output_idx := v_output_idx + 1;
    IF v_output_idx = v_outputs_n THEN
      v_q_share := p_qty - v_qty_used;
    ELSE
      v_q_share := (v_output.qty * p_qty) / v_wo.qty_target;
    END IF;
    v_qty_used := v_qty_used + v_q_share;

    IF v_output_idx = v_outputs_n THEN
      v_v_share := v_total_drain - v_val_used;
    ELSE
      v_v_share := (v_total_drain * v_output.allocation_pct)::BIGINT / 100;
    END IF;
    v_val_used := v_val_used + v_v_share;

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

    -- L4: lot_code resolution. For lot_fifo parent, prefer
    -- user-supplied wo_outputs.lot_code; otherwise auto-gen using
    -- the wo_event_id for uniqueness across partial completions.
    IF v_cost_method = 'lot_fifo' THEN
      v_lot_code := v_output.lot_code;
      IF v_lot_code IS NULL OR length(v_lot_code) = 0 THEN
        v_lot_code := 'WO-' || substr(v_event_id::TEXT, 1, 8) || '-' || v_output.output_no;
      END IF;
    ELSE
      v_lot_code := NULL;
    END IF;

    IF v_q_share > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'wo_complete',
        'document_kind',     'wo_complete',
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

    IF v_v_share > 0 THEN
      v_event_obj := jsonb_build_object(
        'reason',            'wo_complete_v',
        'document_kind',     'wo_complete',
        'document_id',       v_event_id,
        'debit_account_id',  v_val_fg,
        'credit_account_id', v_val_from,
        'amount',            v_v_share,
        'qty',               v_q_share,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      );
      IF v_lot_code IS NOT NULL THEN
        v_event_obj := v_event_obj || jsonb_build_object('lot_code', v_lot_code);
      END IF;
      v_batch := v_batch || jsonb_build_array(v_event_obj);
    END IF;
  END LOOP;

  PERFORM post_posting_lines(v_batch, FALSE);
  UPDATE work_orders SET qty_completed = qty_completed + p_qty
   WHERE id = p_wo_id;

  IF v_will_close THEN
    FOR v_op_residual IN
      SELECT a.id AS acct_id,
             a.routing_op AS rop,
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
      SELECT id INTO v_op_qty_acct FROM accounts
       WHERE kind = 'stock_wip' AND sku_id = v_wo.parent_sku_id
         AND routing_op = v_op_residual.rop AND NOT is_closed;
      IF v_op_qty_acct IS NULL THEN
        v_op_qty := 0;
      ELSE
        v_lock_first  := LEAST(v_op_qty_acct, v_op_residual.acct_id);
        v_lock_second := GREATEST(v_op_qty_acct, v_op_residual.acct_id);
        PERFORM 1 FROM accounts WHERE id = v_lock_first  FOR UPDATE;
        PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;

        SELECT (debits_total - credits_total) INTO v_op_qty
          FROM accounts WHERE id = v_op_qty_acct;
      END IF;
      IF COALESCE(v_op_qty, 0) <> 0 THEN
        CONTINUE;
      END IF;

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
        PERFORM post_posting_lines(jsonb_build_array(jsonb_build_object(
          'reason',            'wo_close_v',
          'document_kind',     'wo_complete',
          'document_id',       v_event_id,
          'debit_account_id',  v_var_close,
          'credit_account_id', v_op_residual.acct_id,
          'amount',            v_residual,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        )), FALSE);
      ELSE
        PERFORM post_posting_lines(jsonb_build_array(jsonb_build_object(
          'reason',            'wo_close_v',
          'document_kind',     'wo_complete',
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
END;
$$;
