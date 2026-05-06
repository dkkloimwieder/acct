-- acct-6g47 / acct-7t4.4 — disposal_cost treatment in post_wo_complete
-- pre-pass + the bridging accrual machinery (accrued_disposal_liability,
-- disposal_expense).
--
-- Mig 0098 (acct-u1n9) shipped nrv_credit + negligible and stubbed out
-- disposal_cost with `IF v_bp.treatment = 'disposal_cost' THEN CONTINUE`.
-- This migration replaces that CONTINUE with per-disposal_basis handling
-- and adds the two account_kind values that disposal_cost accrues into:
--
--   * accrued_disposal_liability — credit-normal, vendor-partitioned,
--     currency-aware. Mirror of ap_unsettled's GRNI shape: at WO
--     completion the liability accrues to the disposal vendor; at
--     post_ap_bill (acct-3yno, kind='disposal_match') the liability
--     drains to ap. Bridges the WO-time accrual to the AP-time bill.
--
--   * disposal_expense — debit-normal, currency-only. Default destination
--     for disposal_basis='period' value legs when bom_by_products
--     .disposal_expense_account_kind is NULL. Caller may override with
--     any other expense kind for finer GL classification.
--
-- Treatment dispatch in the pre-pass (extending mig 0098):
--
--   * disposal_basis='inventoriable':
--       qty leg:  stock_available(by_product, fg_loc) DR
--                 creation_void(qty) CR
--                 amount=qty=actual_qty, reason='wo_complete'
--       value:    For EACH co-product in wo_outputs (split by
--                 allocation_pct, last gets residual):
--                   inv_value_fg(co_product, co_loc, ccy) DR
--                   accrued_disposal_liability(vendor, ccy) CR
--                   amount = (|unit_value| × actual_qty) × pct/100
--                   reason='wo_complete_v'
--                 inv_value_wip(parent, last_op) is NOT touched.
--                 v_total_drain UNCHANGED. Co-products absorb full
--                 drain (parent_std × p_qty) AND get this second
--                 debit on top — disposal cost INFLATES the
--                 co-product cost basis per mainstream-ERP convention.
--
--   * disposal_basis='period':
--       qty leg:  same shape as inventoriable.
--       value:    disposal_expense_kind(_, ccy) DR
--                 accrued_disposal_liability(vendor, ccy) CR
--                 amount = |unit_value| × actual_qty
--                 reason='wo_complete_v'
--                 inv_value_wip(parent) is NOT touched. P&L hit is
--                 immediate; co-product COGS basis unchanged.
--
-- GATING. Same `v_will_close AND v_cost_method = standard` gate as
-- mig 0098. The cost_method gate is conservative — disposal_cost legs
-- don't credit parent inv_value_wip(last_op) so the acct-nnyl WAC-
-- flagging concern doesn't apply. Lifting the gate for disposal_cost
-- specifically is a future follow-up; consistency with the umbrella
-- (acct-7t4) keeps the gate in place for now.
--
-- VENDOR ACCOUNT RESOLUTION. accrued_disposal_liability is partitioned
-- on (kind, counterparty_id, currency) — same shape as ap_unsettled.
-- Per-vendor accounts are opened by callers (test scaffold or
-- production setup) before the WO completes; the function raises P0010
-- if the vendor's account is missing.
--
-- ERROR CODES. No new ones — P0010 (account_missing) covers vendor
-- account resolution and disposal_expense kind resolution.

ALTER TYPE account_kind ADD VALUE IF NOT EXISTS 'accrued_disposal_liability';
ALTER TYPE account_kind ADD VALUE IF NOT EXISTS 'disposal_expense';

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
  -- acct-7t4.3 by-products pre-pass
  v_bp             wo_by_products%ROWTYPE;
  v_bp_qty_acct    BIGINT;
  v_bp_val_acct    BIGINT;
  v_void_qty       BIGINT;
  v_byproduct_drain BIGINT := 0;
  -- acct-7t4.4 disposal_cost
  v_disp_total       BIGINT;
  v_disp_liability   BIGINT;
  v_disp_exp_acct    BIGINT;
  v_disp_exp_kind    account_kind;
  v_disp_share       BIGINT;
  v_disp_used        BIGINT;
  v_disp_output      RECORD;
  v_disp_output_idx  INT;
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

  IF v_cost_method = 'standard' THEN
    v_parent_std  := resolve_standard_cost_at(v_wo.parent_sku_id, p_business_date);
    v_total_drain := p_qty * v_parent_std;

  ELSIF v_cost_method IN ('wac_perpetual', 'wac_periodic', 'wac_retroactive') THEN
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
      'wo_invalid: parent_sku % has cost_method=% which post_wo_complete '
      'does not handle',
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
          'document_kind',     'wo_event',
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
          'document_kind',     'wo_event',
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

  -- ============================================================
  -- By-products pre-pass (acct-7t4.3 + acct-7t4.4).
  -- ============================================================
  --
  -- Walks wo_by_products and emits per-treatment legs. Gates:
  --   * v_will_close — actual_qty is the cumulative WO snapshot, not
  --     a per-call share. Firing on every partial complete would
  --     multi-emit. Same shape as the pre-balance step above.
  --   * v_cost_method = 'standard' — value legs that credit parent's
  --     inv_value_wip(last_op) (nrv_credit) would be flagged into
  --     transfers_provisional on WAC parents. By-product NRV is FIXED
  --     by bom_by_products and shouldn't be cost-method-dispatched.
  --     disposal_cost legs DON'T credit parent WIP, but the gate stays
  --     for consistency; lifting it for disposal_cost is a future
  --     follow-up. WAC-parent + by-product interaction is tracked as
  --     acct-nnyl.

  IF v_will_close AND v_cost_method = 'standard' THEN
    SELECT id INTO v_void_qty FROM accounts
     WHERE kind='creation_void' AND ledger_kind='qty' AND NOT is_closed;

    FOR v_bp IN
      SELECT * FROM wo_by_products WHERE wo_id = p_wo_id
       ORDER BY by_product_no
    LOOP
      IF v_bp.actual_qty <= 0 THEN
        CONTINUE;
      END IF;

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

      -- Qty leg fires for all three treatments.
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'wo_complete',
        'document_kind',     'wo_event',
        'document_id',       v_event_id,
        'debit_account_id',  v_bp_qty_acct,
        'credit_account_id', v_void_qty,
        'amount',            v_bp.actual_qty,
        'qty',               v_bp.actual_qty,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      ));

      -- Per-treatment value handling.
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

        v_byproduct_drain := v_byproduct_drain + v_bp.unit_value * v_bp.actual_qty;

        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'wo_complete_v',
          'document_kind',     'wo_event',
          'document_id',       v_event_id,
          'debit_account_id',  v_bp_val_acct,
          'credit_account_id', v_val_from,
          'amount',            v_bp.unit_value * v_bp.actual_qty,
          'qty',               v_bp.actual_qty,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        ));

      ELSIF v_bp.treatment = 'disposal_cost' THEN
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

        v_disp_total := ABS(v_bp.unit_value) * v_bp.actual_qty;

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
            'reason',            'wo_complete_v',
            'document_kind',     'wo_event',
            'document_id',       v_event_id,
            'debit_account_id',  v_disp_exp_acct,
            'credit_account_id', v_disp_liability,
            'amount',            v_disp_total,
            'qty',               v_bp.actual_qty,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'posted_by',         p_posted_by
          ));

        ELSIF v_bp.disposal_basis = 'inventoriable' THEN
          -- Per-co-product split. Distribute v_disp_total across
          -- wo_outputs by allocation_pct. Last co-product gets the
          -- residual to absorb integer truncation. Each share posts
          --   inv_value_fg(co_product) DR / accrued_disposal_liability CR.
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
              'reason',            'wo_complete_v',
              'document_kind',     'wo_event',
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
      END IF;
      -- negligible: qty leg only, no value leg.
    END LOOP;

    v_total_drain := v_total_drain - v_byproduct_drain;
  END IF;

  -- ============================================================
  -- Co-product distribution loop (unchanged).
  -- ============================================================

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
END;
$$;

COMMENT ON FUNCTION post_wo_complete(UUID, BIGINT, DATE, UUID, UUID, TEXT) IS
  'WO completion. Drains parent WIP at last_op, distributes value to '
  'co-products via wo_outputs.allocation_pct. By-product pre-pass on '
  'closing call: nrv_credit (acct-7t4.3) drains parent WIP into '
  'inv_value_fg of by-product (reduces co-product share); negligible '
  'posts qty leg only; disposal_cost (acct-7t4.4) accrues vendor '
  'liability — inventoriable inflates co-product fg basis (per-output '
  'split), period posts directly to disposal_expense. Solo-at-pool gate '
  'on pre-balance step. Standard-cost-method gate (WAC follow-up '
  'tracked as acct-nnyl).';
