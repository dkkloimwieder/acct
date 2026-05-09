-- 0040_byproducts_wac_parent — lift acct-nnyl gate.
--
-- post_wo_complete by-products pre-pass was gated to standard parents
-- only (mig 0098 acct-u1n9, preserved by consolidation 0017 then mig
-- 0038's FIFO parent extension). WAC parents with bom_by_products rows
-- silently skipped the pre-pass.
--
-- Root cause: by-product nrv_credit / disposal_cost value legs used
-- reason='wo_complete_v', which is in apply_event's flagging list at
-- mig 0014 line 278. For wac_periodic / wac_retroactive parents the
-- credit-side is the parent's inv_value_wip (a wac_* SKU); flagging
-- routes the row into posting_lines_provisional, and the close hook
-- recomputes it at the parent's final running average. But by-product
-- NRV is FIXED at WO time (caller-supplied salvage value, not cost
-- flow from the parent pool); the recompute is semantically wrong.
--
-- Fix per the issue's option (a):
--   1. ALTER TYPE posting_line_reason ADD VALUE 'wo_byproduct_credit'.
--   2. By-product pre-pass switches all value-leg reasons from
--      'wo_complete_v' to 'wo_byproduct_credit'. The new reason is
--      NOT in apply_event's flagging list (mig 0014 line 278) and NOT
--      in the dispatcher's auto-pricing list (mig 0014 line 416), so
--      caller-supplied amounts stand and no posting_lines_provisional
--      row is created — the close hook never sees it.
--   3. Lift the v_will_close gate from 'standard' to allow WAC parents
--      (wac_perpetual / wac_periodic / wac_retroactive). FIFO parent
--      remains gated out — FIFO + by-products is a separate follow-up
--      because FIFO by-product SKUs would also need apply_event's qty-
--      leg cost_method gate lifted, beyond the scope of this fix.
--   4. Extend _inventory_movement_event_type with
--      'wo_byproduct_credit' → 9 so the D-block continues to write
--      inventory_movements rows for by-product FG inflows (matching
--      the prior 'wo_complete_v' → 9 behavior for standard parents).
--
-- Per-output drain ('wo_complete_v' on line 965 of mig 0038) is the
-- main parent FG drain and stays unchanged — that one DOES want
-- flagging on WAC parents; the close hook correctly recomputes at
-- final running avg.
--
-- ERROR CODES. No new ones.
--
-- TESTS. tests/wo_complete_by_products.rs test #5 (formerly
-- `wac_parent_nrv_credit_silently_skipped`) flips to assert positive
-- behavior; new test cases cover wac_periodic + wac_retroactive
-- across period close to verify no re-dispatch.

ALTER TYPE posting_line_reason ADD VALUE IF NOT EXISTS 'wo_byproduct_credit';

-- ============================================================
-- _inventory_movement_event_type — extend with new reason
-- ============================================================

CREATE OR REPLACE FUNCTION _inventory_movement_event_type(
  p_reason     posting_line_reason,
  p_signed_qty NUMERIC
) RETURNS SMALLINT
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
  RETURN CASE p_reason::TEXT
    WHEN 'po_receipt'              THEN 1
    WHEN 'po_receipt_provisional'  THEN 1
    WHEN 'so_ship'                 THEN 2
    WHEN 'to_release'              THEN 3
    WHEN 'osp_ship'                THEN 3
    WHEN 'to_receipt'              THEN 4
    WHEN 'osp_receive'             THEN 4
    WHEN 'cycle_count_adj'
      THEN CASE WHEN p_signed_qty > 0 THEN 5 ELSE 6 END
    WHEN 'inventory_adjustment'
      THEN CASE WHEN p_signed_qty > 0 THEN 5 ELSE 6 END
    WHEN 'scrap'                   THEN 7
    WHEN 'scrap_v'                 THEN 7
    WHEN 'rm_issue_to_wo'          THEN 8
    WHEN 'wo_complete'             THEN 9
    WHEN 'wo_complete_v'           THEN 9
    WHEN 'wo_byproduct_credit'     THEN 9
    WHEN 'op_move'                 THEN 11
    WHEN 'op_move_v'               THEN 11
    WHEN 'customer_return'         THEN 12
    WHEN 'po_return_to_vendor'     THEN 13
    WHEN 'standard_cost_roll'      THEN 14
    WHEN 'cost_adjustment'         THEN 16
    WHEN 'cost_restate'            THEN 16
    WHEN 'wo_close_v'              THEN 16
    ELSE NULL
  END;
END;
$$;

-- ============================================================
-- post_wo_complete — body identical to mig 0038, two changes:
--   1. Pre-pass gate at line 670 (was: v_cost_method = 'standard')
--      now admits 'standard', 'wac_perpetual', 'wac_periodic',
--      'wac_retroactive'.
--   2. By-product value-leg reasons (7 occurrences) switch from
--      'wo_complete_v' to 'wo_byproduct_credit'.
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
    v_parent_std  := _resolve_standard_cost_at(v_wo.parent_sku_id, p_business_date);
    v_total_drain := p_qty * v_parent_std;

  ELSIF v_cost_method IN ('wac_perpetual', 'wac_periodic',
                          'wac_retroactive', 'fifo') THEN
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

  -- By-products pre-pass on closing call. acct-nnyl: WAC parents now
  -- admitted alongside standard. By-product value-leg uses the new
  -- 'wo_byproduct_credit' reason so apply_event's flagging skips it
  -- (NRV is fixed at WO time; close-hook recompute is semantically
  -- wrong). FIFO parent + by-products remains deferred.
  IF v_will_close AND v_cost_method IN
       ('standard', 'wac_perpetual', 'wac_periodic', 'wac_retroactive') THEN
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

        -- qty omitted: NRV reduces parent WIP value WITHOUT changing
        -- the parent's per-class qty divisor on inv_value_wip. With
        -- qty=planned_qty the credit-side parent WIP would lose
        -- both value (correct) AND units (incorrect — by-product
        -- doesn't drain parent units). qty=NULL signals "value-only
        -- transfer" — apply_event's I3 query, pli writeback, and D-block
        -- all skip on NULL. Subledger inventory_movements row for the
        -- by-product creation isn't written from this leg; the
        -- by-product's qty leg above (DR stock_available[bp]) is qty-
        -- class only and also doesn't write inventory_movements (the
        -- D-block filters on inv_value_*). Future enhancement: split
        -- this into a two-leg wash through creation_void(USD) so
        -- bp_val DR side produces an inventory_movements row.
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
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
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
      ));
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

-- ============================================================
-- wac_periodic_close_hook — subtract wo_byproduct_credit values
-- when computing pool period-avg numerator.
--
-- The hook computes final_avg = pool_value_in / pool_qty_in. Without
-- this fix, v_pool_value_in only sums DEBITS to the pool — by-product
-- credits (CR posts to the parent WIP pool) reduce parent value but
-- don't appear in the avg numerator, so flagged depletions get
-- recomputed against an inflated avg and post phantom variance to
-- variance_wac_periodic, undoing the NRV reduction at close.
--
-- Fix: extend the v_pool_value_in calc to subtract the SUM of
-- 'wo_byproduct_credit' amounts targeting the pool as CR. The
-- by-product credit conceptually reduces the pool's incoming value
-- without reducing units; treating it as a negative debit yields the
-- correct period avg.
--
-- wac_retroactive_close_hook does NOT need this fix: its chronological
-- replay (line 704-707) decrements v_pool_value when it encounters a
-- CR-side event with qty=NULL, so the by-product credit is already
-- properly accounted for in the running pool_value before subsequent
-- depletions recompute.
-- ============================================================

CREATE OR REPLACE FUNCTION wac_periodic_close_hook(
  p_period_id         BIGINT,
  p_force_provisional BOOLEAN DEFAULT FALSE
) RETURNS BIGINT LANGUAGE plpgsql AS $$
DECLARE
  v_period_opens          DATE;
  v_period_closes         DATE;
  v_period_code           TEXT;
  v_count                 BIGINT := 0;
  v_pool_id               BIGINT;
  v_processed             BIGINT[] := ARRAY[]::BIGINT[];
  v_remaining             INT;
  v_progress              INT;
  v_cycle_pools           TEXT;
  v_row                   RECORD;
  v_orig                  RECORD;
  v_pool_acct             accounts%ROWTYPE;
  v_pool_value_in         BIGINT;
  v_pool_byproduct_credit BIGINT;
  v_pool_qty_in           BIGINT;
  v_final_avg             BIGINT;
  v_provisional           BIGINT;
  v_variance              BIGINT;
  v_var_acct              BIGINT;
  v_batch                 JSONB;
  v_var_xfer_id           BIGINT;
  v_orig_debit            BIGINT;
  v_orig_credit           BIGINT;
  v_event_a               JSONB;
  v_event_b               JSONB;
  v_orig_reason           posting_line_reason;
  v_dest_method           TEXT;
  v_mixed                 BOOLEAN;
BEGIN
  SELECT opens_at, closes_at, code
    INTO v_period_opens, v_period_closes, v_period_code
    FROM periods WHERE id = p_period_id;
  IF NOT FOUND THEN
    RETURN 0;
  END IF;

  CREATE TEMP TABLE _wac_pools (
    pool_id BIGINT PRIMARY KEY
  ) ON COMMIT DROP;

  CREATE TEMP TABLE _wac_edges (
    predecessor BIGINT,
    successor   BIGINT,
    PRIMARY KEY (predecessor, successor)
  ) ON COMMIT DROP;

  INSERT INTO _wac_pools (pool_id)
  SELECT DISTINCT t.credit_account_id
    FROM posting_lines_provisional p
    JOIN posting_lines t ON t.id = p.posting_line_id
   WHERE p.period_id = p_period_id
     AND p.cost_method = 'wac_periodic'
     AND p.finalized_at IS NULL
  UNION
  SELECT DISTINCT t.debit_account_id
    FROM posting_lines_provisional p
    JOIN posting_lines t ON t.id = p.posting_line_id
   WHERE p.period_id = p_period_id
     AND p.cost_method = 'wac_periodic'
     AND p.finalized_at IS NULL
     AND t.reason IN ('op_move_v', 'rm_issue_to_wo')
  ON CONFLICT DO NOTHING;

  INSERT INTO _wac_edges (predecessor, successor)
  SELECT DISTINCT t.credit_account_id, t.debit_account_id
    FROM posting_lines_provisional p
    JOIN posting_lines t ON t.id = p.posting_line_id
   WHERE p.period_id = p_period_id
     AND p.cost_method = 'wac_periodic'
     AND p.finalized_at IS NULL
     AND t.reason IN ('op_move_v', 'rm_issue_to_wo')
  ON CONFLICT DO NOTHING;

  LOOP
    SELECT COUNT(*) INTO v_remaining FROM _wac_pools;
    EXIT WHEN v_remaining = 0;

    v_progress := 0;
    FOR v_pool_id IN
      SELECT wp.pool_id
        FROM _wac_pools wp
       WHERE NOT EXISTS (
         SELECT 1 FROM _wac_edges e
          WHERE e.successor = wp.pool_id
            AND e.predecessor IN (SELECT pool_id FROM _wac_pools)
       )
    LOOP
      v_progress := v_progress + 1;

      SELECT * INTO v_pool_acct FROM accounts WHERE id = v_pool_id;

      SELECT COALESCE(SUM(
        t.amount + COALESCE(p.variance_amount, 0)
      ), 0) INTO v_pool_value_in
        FROM posting_lines t
        LEFT JOIN posting_lines_provisional p
               ON p.posting_line_id = t.id
              AND p.finalized_at IS NOT NULL
              AND p.variance_posting_line_id IS NULL
       WHERE t.debit_account_id = v_pool_id
         AND t.business_date BETWEEN v_period_opens AND v_period_closes;

      -- acct-nnyl: subtract by-product credits posted CR-to-pool
      -- during the period. NRV credit reduces pool value without
      -- reducing units; treat it as a negative debit for period-avg
      -- purposes so flagged depletions don't get phantom variance.
      SELECT COALESCE(SUM(t.amount), 0) INTO v_pool_byproduct_credit
        FROM posting_lines t
       WHERE t.credit_account_id = v_pool_id
         AND t.reason = 'wo_byproduct_credit'
         AND t.business_date BETWEEN v_period_opens AND v_period_closes;
      v_pool_value_in := v_pool_value_in - v_pool_byproduct_credit;

      v_pool_qty_in := _wac_close_pool_qty_in(
        v_pool_acct, v_period_opens, v_period_closes
      );
      IF v_pool_qty_in IS NULL THEN
        RAISE EXCEPTION
          'wac_periodic_close: cannot resolve qty account for value pool %',
          v_pool_id USING ERRCODE = 'P0010';
      END IF;

      IF v_pool_qty_in = 0 THEN
        IF p_force_provisional THEN
          DELETE FROM _wac_pools WHERE pool_id = v_pool_id;
          v_processed := array_append(v_processed, v_pool_id);
          CONTINUE;
        END IF;
        RAISE EXCEPTION
          'wac_periodic_close_no_receipts: period % (id=%) has provisional '
          'depletions on pool kind=% sku=% loc=% op=% ccy=% but zero receipts in '
          'period; post receipts and retry the close, or close with '
          'p_force_provisional=TRUE.',
          v_period_code, p_period_id, v_pool_acct.kind, v_pool_acct.sku_id,
          v_pool_acct.location_id, v_pool_acct.routing_op, v_pool_acct.currency
          USING ERRCODE = 'P0020';
      END IF;

      v_final_avg := v_pool_value_in / v_pool_qty_in;

      FOR v_row IN
        SELECT *
          FROM posting_lines_provisional
         WHERE period_id = p_period_id
           AND cost_method = 'wac_periodic'
           AND finalized_at IS NULL
         ORDER BY posting_line_id
           FOR UPDATE
      LOOP
        SELECT * INTO v_orig FROM posting_lines WHERE id = v_row.posting_line_id;
        IF v_orig.credit_account_id <> v_pool_id THEN
          CONTINUE;
        END IF;
        v_orig_reason := v_orig.reason;

        v_provisional := v_orig.amount / v_row.qty;
        v_variance    := (v_final_avg - v_provisional) * v_row.qty;

        v_mixed := FALSE;
        IF v_orig_reason = 'rm_issue_to_wo' THEN
          SELECT s.cost_method::TEXT INTO v_dest_method
            FROM accounts a
            JOIN skus s ON s.id = a.sku_id
           WHERE a.id = v_orig.debit_account_id;
          IF v_dest_method IS DISTINCT FROM 'wac_periodic' THEN
            v_mixed := TRUE;
          END IF;
        END IF;

        IF v_mixed THEN
          IF v_variance = 0 THEN
            UPDATE posting_lines_provisional
               SET finalized_at = clock_timestamp(),
                   variance_amount = 0,
                   variance_posting_line_id = NULL
             WHERE posting_line_id = v_row.posting_line_id;
            v_count := v_count + 1;
            CONTINUE;
          END IF;

          SELECT id INTO v_var_acct FROM accounts
           WHERE kind = 'variance_material_mixed'
             AND ledger_kind = 'value'
             AND currency = v_pool_acct.currency
             AND NOT is_closed;
          IF v_var_acct IS NULL THEN
            RAISE EXCEPTION
              'wac_periodic_close: no variance_material_mixed(value, ccy=%) '
              'account configured (acct-7eo)',
              v_pool_acct.currency USING ERRCODE = 'P0010';
          END IF;

          v_orig_credit := v_orig.credit_account_id;
          IF v_variance > 0 THEN
            v_event_a := jsonb_build_object(
              'reason',            'cost_restate',
              'document_kind',     'wac_periodic_close_mixed',
              'document_id',       gen_random_uuid(),
              'debit_account_id',  v_var_acct,
              'credit_account_id', v_orig_credit,
              'amount',            v_variance,
              'business_date',     v_period_closes,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         '00000000-0000-0000-0000-000000000000'
            );
          ELSE
            v_event_a := jsonb_build_object(
              'reason',            'cost_restate',
              'document_kind',     'wac_periodic_close_mixed',
              'document_id',       gen_random_uuid(),
              'debit_account_id',  v_orig_credit,
              'credit_account_id', v_var_acct,
              'amount',            -v_variance,
              'business_date',     v_period_closes,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         '00000000-0000-0000-0000-000000000000'
            );
          END IF;
          v_batch := jsonb_build_array(v_event_a);
          PERFORM post_posting_lines(v_batch, TRUE);
          SELECT id INTO v_var_xfer_id
            FROM posting_lines
           WHERE idempotency_key = (v_event_a->>'idempotency_key')::UUID;
          UPDATE posting_lines_provisional
             SET finalized_at = clock_timestamp(),
                 variance_amount = v_variance,
                 variance_posting_line_id = v_var_xfer_id
           WHERE posting_line_id = v_row.posting_line_id;
          v_count := v_count + 1;
        ELSIF v_orig_reason IN ('op_move_v', 'rm_issue_to_wo') THEN
          UPDATE posting_lines_provisional
             SET finalized_at = clock_timestamp(),
                 variance_amount = v_variance,
                 variance_posting_line_id = NULL
           WHERE posting_line_id = v_row.posting_line_id;
          v_count := v_count + 1;
        ELSIF v_variance = 0 THEN
          UPDATE posting_lines_provisional
             SET finalized_at = clock_timestamp(),
                 variance_amount = 0,
                 variance_posting_line_id = NULL
           WHERE posting_line_id = v_row.posting_line_id;
          v_count := v_count + 1;
        ELSE
          SELECT id INTO v_var_acct FROM accounts
           WHERE kind = 'variance_wac_periodic' AND ledger_kind = 'value'
             AND currency = v_pool_acct.currency AND NOT is_closed;
          IF v_var_acct IS NULL THEN
            RAISE EXCEPTION
              'wac_periodic_close: no variance_wac_periodic(value, ccy=%) account configured',
              v_pool_acct.currency USING ERRCODE = 'P0010';
          END IF;

          IF v_pool_acct.kind = 'inv_value_wip' THEN
            IF v_variance > 0 THEN
              v_event_a := jsonb_build_object(
                'reason',            'cost_restate',
                'document_kind',     'wac_periodic_close',
                'document_id',       gen_random_uuid(),
                'debit_account_id',  v_orig.debit_account_id,
                'credit_account_id', v_var_acct,
                'amount',            v_variance,
                'business_date',     v_period_closes,
                'idempotency_key',   gen_random_uuid(),
                'posted_by',         '00000000-0000-0000-0000-000000000000'
              );
            ELSE
              v_event_a := jsonb_build_object(
                'reason',            'cost_restate',
                'document_kind',     'wac_periodic_close',
                'document_id',       gen_random_uuid(),
                'debit_account_id',  v_var_acct,
                'credit_account_id', v_orig.debit_account_id,
                'amount',            -v_variance,
                'business_date',     v_period_closes,
                'idempotency_key',   gen_random_uuid(),
                'posted_by',         '00000000-0000-0000-0000-000000000000'
              );
            END IF;
            v_batch := jsonb_build_array(v_event_a);
          ELSE
            v_orig_debit  := v_orig.debit_account_id;
            v_orig_credit := v_orig.credit_account_id;
            IF v_variance > 0 THEN
              v_event_a := jsonb_build_object(
                'reason',            'cost_restate',
                'document_kind',     'wac_periodic_close',
                'document_id',       gen_random_uuid(),
                'debit_account_id',  v_orig_debit,
                'credit_account_id', v_var_acct,
                'amount',            v_variance,
                'business_date',     v_period_closes,
                'idempotency_key',   gen_random_uuid(),
                'posted_by',         '00000000-0000-0000-0000-000000000000'
              );
              v_event_b := jsonb_build_object(
                'reason',            'cost_restate',
                'document_kind',     'wac_periodic_close',
                'document_id',       gen_random_uuid(),
                'debit_account_id',  v_var_acct,
                'credit_account_id', v_orig_credit,
                'amount',            v_variance,
                'business_date',     v_period_closes,
                'idempotency_key',   gen_random_uuid(),
                'posted_by',         '00000000-0000-0000-0000-000000000000'
              );
            ELSE
              v_event_a := jsonb_build_object(
                'reason',            'cost_restate',
                'document_kind',     'wac_periodic_close',
                'document_id',       gen_random_uuid(),
                'debit_account_id',  v_var_acct,
                'credit_account_id', v_orig_debit,
                'amount',            -v_variance,
                'business_date',     v_period_closes,
                'idempotency_key',   gen_random_uuid(),
                'posted_by',         '00000000-0000-0000-0000-000000000000'
              );
              v_event_b := jsonb_build_object(
                'reason',            'cost_restate',
                'document_kind',     'wac_periodic_close',
                'document_id',       gen_random_uuid(),
                'debit_account_id',  v_orig_credit,
                'credit_account_id', v_var_acct,
                'amount',            -v_variance,
                'business_date',     v_period_closes,
                'idempotency_key',   gen_random_uuid(),
                'posted_by',         '00000000-0000-0000-0000-000000000000'
              );
            END IF;
            v_batch := jsonb_build_array(v_event_a, v_event_b);
          END IF;

          PERFORM post_posting_lines(v_batch, TRUE);
          SELECT id INTO v_var_xfer_id
            FROM posting_lines
           WHERE idempotency_key = (v_event_a->>'idempotency_key')::UUID;
          UPDATE posting_lines_provisional
             SET finalized_at = clock_timestamp(),
                 variance_amount = v_variance,
                 variance_posting_line_id = v_var_xfer_id
           WHERE posting_line_id = v_row.posting_line_id;
          v_count := v_count + 1;
        END IF;
      END LOOP;

      DELETE FROM _wac_pools WHERE pool_id = v_pool_id;
      v_processed := array_append(v_processed, v_pool_id);
    END LOOP;

    IF v_progress = 0 THEN
      SELECT string_agg(pool_id::TEXT, ', ' ORDER BY pool_id)
        INTO v_cycle_pools FROM _wac_pools;
      RAISE EXCEPTION
        'wac_periodic_pool_cycle: period % (id=%) has cycles in pool DAG; '
        'unprocessed pool ids: %; rework / cyclic op_move detected. '
        'Track resolution in acct-p7v rework follow-up.',
        v_period_code, p_period_id, v_cycle_pools
        USING ERRCODE = 'P0036';
    END IF;
  END LOOP;

  RETURN v_count;
END;
$$;
