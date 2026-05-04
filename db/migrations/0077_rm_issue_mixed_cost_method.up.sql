-- acct-7eo — rm_issue_to_wo with mixed parent/component cost methods.
--
-- BACKGROUND. acct-7py (mig 0067) and acct-rso (mig 0070) lifted
-- rm_issue_to_wo's component dispatch to wac_periodic and
-- wac_retroactive components, but ONLY when the parent's cost_method
-- matched the component's. Mixed shapes (standard or wac_perpetual
-- parent + wac_periodic / wac_retroactive component) raised P0026
-- 'rm_issue_mixed_cost_method' deferred here.
--
-- The deferral existed because the close hook's variance routing for
-- wac_periodic / wac_retroactive components on rm_issue_to_wo was
-- INTERNAL-CHAIN: variance recorded on transfers_provisional but no
-- transfer posted. Cumulative cost shift propagated downstream via
-- the LEFT JOIN cache to leaf depletions on the destination WIP pool
-- (which had to be wac_periodic / wac_retroactive for the propagation
-- to land somewhere). For non-matching destinations the cache had no
-- consumer; variance was effectively lost.
--
-- DESIGN. Treat mixed-method rm_issue_to_wo as a LEAF DEPLETION of
-- the component pool — not internal-chain. At close, post a single-leg
-- variance transfer between the component pool and a NEW
-- variance_material_mixed P&L account:
--
--   variance > 0 (final_avg > prov_unit; component pool was UNDER-
--                 depleted at issue time):
--     dr variance_material_mixed
--     cr orig_credit (= component pool)
--     amount = v_variance
--   variance < 0 (final_avg < prov_unit; component pool was OVER-
--                 depleted at issue time):
--     dr orig_credit (= component pool)
--     cr variance_material_mixed
--     amount = -v_variance
--
-- Net effect: component pool ends at final_avg × (per-class qty)
-- (same as raw/fg leaf depletions in the homogeneous case). The
-- material price variance for the period is recognized in P&L at
-- close time. The destination WIP pool (non-matching cost_method) is
-- NOT touched — it keeps the provisional cost it received at issue
-- time, and downstream wo_complete / wo_close_v / scrap_v already
-- drained it at the original input cost. This is the documented
-- accounting interpretation: "consuming a wac_*-tracked component
-- through a non-wac_* WIP path generates a period-end material price
-- variance, recognized as P&L in the period of the close."
--
-- WHY NOT 2-LEG WASH? The 2-leg wash pattern from mig 0029 / 0031
-- (used for raw/fg leaf depletions in homogeneous case) routes
-- through both orig_debit and orig_credit. orig_debit here is the
-- destination WIP pool, which is debit-normal and typically drained
-- to 0 by wo_complete. A 2-leg wash with negative variance would push
-- WIP below 0, violating the debit-normal CHECK. Single-leg through
-- orig_credit (component pool, retains balance) avoids the overdrain
-- per CLAUDE.md R5.
--
-- WHY NEW VARIANCE ACCOUNT? Re-using variance_wac_period or
-- variance_wac_retroactive for mixed-case routing would merge the
-- mixed-case material price variance with the homogeneous-case
-- close-time recompute variance on the income statement. Distinct
-- accounting stories deserve distinct kinds. variance_material_mixed
-- is bidirectional (unrestricted) and shared between both close
-- hooks — period and retroactive distinguish themselves via the
-- transfer's document_kind ('wac_periodic_close_mixed' /
-- 'wac_retroactive_close_mixed').
--
-- POOL/EDGE SET FILTER. The close hooks' _wac_pools / _wac_edges
-- temp tables collect destinations of internal-chain provisional
-- transfers (rm_issue_to_wo, op_move_v) so successor pools are
-- walked too. For mixed-case rm_issue_to_wo the destination is
-- non-matching and needn't be walked — the inner FOR loop on it
-- yields no rows because no flagged provisional credits a non-
-- matching destination. Walking it is wasteful but harmless; the
-- inner-loop short-circuit makes filtering optional. We leave the
-- pool set as-is for code parity with mig 0067 / 0070.

-- ============================================================
-- 1) New account kind: variance_material_mixed.
-- ============================================================

ALTER TYPE account_kind ADD VALUE IF NOT EXISTS 'variance_material_mixed';

-- ============================================================
-- 2) _wo_emit_bom_lines — lift the two P0026 gates.
-- ============================================================

CREATE OR REPLACE FUNCTION _wo_emit_bom_lines(
  p_wo_id          UUID,
  p_bom_id         BIGINT,
  p_routing_op     INT,
  p_qty            BIGINT,
  p_filter         JSONB,
  p_event_id       UUID,
  p_business_date  DATE,
  p_posted_by      UUID
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
  v_wo                   work_orders%ROWTYPE;
  v_val_acct_wip         BIGINT;
  v_batch                JSONB := '[]'::JSONB;
  v_line                 RECORD;
  v_filter_kind          TEXT;
  v_filter_basis         TEXT;
  v_filter_fire_at       TEXT;
  v_filter_applies_at_op INT;
  v_adj_qty              BIGINT;
  v_value                BIGINT;
  v_amount               BIGINT;
  v_reason               transfer_reason;
  v_comp_consumed        BIGINT;
  v_comp_qty_acct        BIGINT;
  v_comp_val_acct        BIGINT;
  v_applied_kind         account_kind;
  v_applied_acct         BIGINT;
  v_comp_std_cost        BIGINT;
  v_comp_cost_method     cost_method;
  v_parent_cost_method   cost_method;
  v_pool_qty             BIGINT;
  v_pool_value           BIGINT;
  v_unit                 BIGINT;
BEGIN
  IF p_qty IS NULL OR p_qty <= 0 THEN
    RAISE EXCEPTION 'wo_invalid: _wo_emit_bom_lines requires positive p_qty (got %)', p_qty
      USING ERRCODE = 'P0026';
  END IF;
  IF p_bom_id IS NULL THEN
    RAISE EXCEPTION 'wo_invalid: _wo_emit_bom_lines requires non-NULL p_bom_id'
      USING ERRCODE = 'P0026';
  END IF;

  SELECT * INTO v_wo FROM work_orders WHERE id = p_wo_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: WO % not found', p_wo_id USING ERRCODE = 'P0026';
  END IF;

  SELECT cost_method INTO v_parent_cost_method
    FROM skus WHERE id = v_wo.parent_sku_id;

  SELECT id INTO v_val_acct_wip FROM accounts
   WHERE kind='inv_value_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_routing_op AND currency=v_wo.currency
     AND NOT is_closed;
  IF v_val_acct_wip IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_wip account for sku=% op=% ccy=%',
                    v_wo.parent_sku_id, p_routing_op, v_wo.currency
      USING ERRCODE = 'P0010';
  END IF;

  v_filter_kind          := p_filter->>'kind';
  v_filter_basis         := p_filter->>'basis';
  v_filter_fire_at       := p_filter->>'fire_at';
  v_filter_applies_at_op := NULLIF(p_filter->>'applies_at_op', '')::INT;

  FOR v_line IN
    SELECT exp.*
      FROM _wo_explode_bom(p_bom_id, p_business_date) exp
     WHERE (v_filter_kind          IS NULL OR exp.kind          = v_filter_kind)
       AND (v_filter_basis         IS NULL OR exp.basis         = v_filter_basis)
       AND (v_filter_fire_at       IS NULL OR exp.fire_at       = v_filter_fire_at)
       AND (v_filter_applies_at_op IS NULL OR exp.applies_at_op = v_filter_applies_at_op)
     ORDER BY exp.source_bom_id, exp.source_line_no, exp.depth
  LOOP
    IF v_line.kind = 'item' THEN
      v_adj_qty := p_qty * v_line.qty_per_parent;

      SELECT id INTO v_comp_consumed FROM accounts
       WHERE kind='stock_consumed' AND sku_id=v_line.component_sku_id
         AND ledger_kind='qty' AND NOT is_closed;
      IF v_comp_consumed IS NULL THEN
        RAISE EXCEPTION 'no open stock_consumed account for sku=%',
                        v_line.component_sku_id USING ERRCODE = 'P0010';
      END IF;

      SELECT id INTO v_comp_qty_acct FROM accounts
       WHERE kind='stock_available' AND sku_id=v_line.component_sku_id
         AND location_id=v_line.component_loc_id AND NOT is_closed;
      IF v_comp_qty_acct IS NULL THEN
        RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                        v_line.component_sku_id, v_line.component_loc_id
          USING ERRCODE = 'P0010';
      END IF;

      SELECT id INTO v_comp_val_acct FROM accounts
       WHERE kind='inv_value_raw' AND sku_id=v_line.component_sku_id
         AND location_id=v_line.component_loc_id AND currency=v_wo.currency
         AND NOT is_closed;
      IF v_comp_val_acct IS NULL THEN
        RAISE EXCEPTION 'no open inv_value_raw account for sku=% loc=% ccy=%',
                        v_line.component_sku_id, v_line.component_loc_id, v_wo.currency
          USING ERRCODE = 'P0010';
      END IF;

      SELECT cost_method INTO v_comp_cost_method
        FROM skus WHERE id = v_line.component_sku_id;

      CASE v_comp_cost_method
        WHEN 'standard' THEN
          v_comp_std_cost := resolve_standard_cost_at(
            v_line.component_sku_id, p_business_date
          );
          v_value := v_adj_qty * v_comp_std_cost;

        WHEN 'wac_perpetual' THEN
          PERFORM 1 FROM accounts WHERE id = v_comp_val_acct FOR UPDATE;
          SELECT COALESCE(SUM(
            CASE
              WHEN t.debit_account_id  = v_comp_val_acct THEN  t.qty
              WHEN t.credit_account_id = v_comp_val_acct THEN -t.qty
            END
          ), 0)
            INTO v_pool_qty
            FROM transfers t
           WHERE v_comp_val_acct IN (t.debit_account_id, t.credit_account_id)
             AND t.qty IS NOT NULL;

          IF v_pool_qty <= 0 THEN
            RAISE EXCEPTION
              'rm_issue_empty_pool: wac_perpetual component % at sku=% loc=% '
              'has empty inv_value_raw pool (per-class qty=%); cannot issue '
              '% units to WO %',
              v_line.component_sku_id, v_line.component_sku_id,
              v_line.component_loc_id, v_pool_qty, v_adj_qty, p_wo_id
              USING ERRCODE = 'P0010';
          END IF;

          SELECT (debits_total - credits_total) INTO v_pool_value
            FROM accounts WHERE id = v_comp_val_acct;
          v_unit  := GREATEST(COALESCE(v_pool_value, 0), 0) / v_pool_qty;
          v_value := v_adj_qty * v_unit;

        WHEN 'wac_periodic' THEN
          -- acct-7eo (mig 0077): mixed parent/component cost methods now
          -- supported. The rm_issue value-leg is flagged into
          -- transfers_provisional with cost_method='wac_periodic' (driven
          -- by credit-side SKU per acct-7py mig 0067). At close,
          -- wac_periodic_close_hook detects mixed case (destination's
          -- SKU cost_method != 'wac_periodic') and posts single-leg
          -- variance through variance_material_mixed instead of the
          -- internal-chain record-only path.
          PERFORM 1 FROM accounts WHERE id = v_comp_val_acct FOR UPDATE;
          SELECT COALESCE(SUM(
            CASE
              WHEN t.debit_account_id  = v_comp_val_acct THEN  t.qty
              WHEN t.credit_account_id = v_comp_val_acct THEN -t.qty
            END
          ), 0)
            INTO v_pool_qty
            FROM transfers t
           WHERE v_comp_val_acct IN (t.debit_account_id, t.credit_account_id)
             AND t.qty IS NOT NULL;

          IF v_pool_qty <= 0 THEN
            RAISE EXCEPTION
              'rm_issue_empty_pool: wac_periodic component % at sku=% loc=% '
              'has empty inv_value_raw pool (per-class qty=%); cannot issue '
              '% units to WO %',
              v_line.component_sku_id, v_line.component_sku_id,
              v_line.component_loc_id, v_pool_qty, v_adj_qty, p_wo_id
              USING ERRCODE = 'P0010';
          END IF;

          SELECT (debits_total - credits_total) INTO v_pool_value
            FROM accounts WHERE id = v_comp_val_acct;
          v_unit  := GREATEST(COALESCE(v_pool_value, 0), 0) / v_pool_qty;
          v_value := v_adj_qty * v_unit;

        WHEN 'wac_retroactive' THEN
          -- acct-7eo (mig 0077): mixed parent/component supported via
          -- single-leg variance routing in wac_retroactive_close_hook.
          PERFORM 1 FROM accounts WHERE id = v_comp_val_acct FOR UPDATE;
          SELECT COALESCE(SUM(
            CASE
              WHEN t.debit_account_id  = v_comp_val_acct THEN  t.qty
              WHEN t.credit_account_id = v_comp_val_acct THEN -t.qty
            END
          ), 0)
            INTO v_pool_qty
            FROM transfers t
           WHERE v_comp_val_acct IN (t.debit_account_id, t.credit_account_id)
             AND t.qty IS NOT NULL;

          IF v_pool_qty <= 0 THEN
            RAISE EXCEPTION
              'rm_issue_empty_pool: wac_retroactive component % at sku=% loc=% '
              'has empty inv_value_raw pool (per-class qty=%); cannot issue '
              '% units to WO %',
              v_line.component_sku_id, v_line.component_sku_id,
              v_line.component_loc_id, v_pool_qty, v_adj_qty, p_wo_id
              USING ERRCODE = 'P0010';
          END IF;

          SELECT (debits_total - credits_total) INTO v_pool_value
            FROM accounts WHERE id = v_comp_val_acct;
          v_unit  := GREATEST(COALESCE(v_pool_value, 0), 0) / v_pool_qty;
          v_value := v_adj_qty * v_unit;

        WHEN 'fifo', 'lot' THEN
          RAISE EXCEPTION
            'cost_method_not_implemented: % for component % (acct-8gg)',
            v_comp_cost_method, v_line.component_sku_id
            USING ERRCODE = 'P0006';

        ELSE
          RAISE EXCEPTION
            'unknown cost_method % for component %',
            v_comp_cost_method, v_line.component_sku_id
            USING ERRCODE = 'P0011';
      END CASE;

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'rm_issue_to_wo',
        'document_kind',     'wo_event',
        'document_id',       p_event_id,
        'debit_account_id',  v_comp_consumed,
        'credit_account_id', v_comp_qty_acct,
        'amount',            v_adj_qty,
        'qty',               v_adj_qty,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      ));

      IF v_value > 0 THEN
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'rm_issue_to_wo',
          'document_kind',     'wo_event',
          'document_id',       p_event_id,
          'debit_account_id',  v_val_acct_wip,
          'credit_account_id', v_comp_val_acct,
          'amount',            v_value,
          'qty',               v_adj_qty,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        ));
      END IF;

    ELSIF v_line.kind IN ('service', 'charge') THEN
      IF v_line.basis = 'per_unit' THEN
        v_amount := p_qty * v_line.std_amount;
      ELSE
        v_amount := v_line.std_amount;
      END IF;
      IF v_amount <= 0 THEN
        CONTINUE;
      END IF;

      v_reason := _wo_apply_reason_for(v_line.absorption_class_id, v_line.basis);

      SELECT applied_account_kind INTO v_applied_kind FROM absorption_classes
       WHERE id = v_line.absorption_class_id;
      IF v_applied_kind IS NULL THEN
        RAISE EXCEPTION 'wo_invalid: absorption_class id=% not found',
                        v_line.absorption_class_id USING ERRCODE = 'P0026';
      END IF;

      SELECT id INTO v_applied_acct FROM accounts
       WHERE kind = v_applied_kind AND ledger_kind='value'
         AND currency = v_wo.currency AND NOT is_closed
       LIMIT 1;
      IF v_applied_acct IS NULL THEN
        RAISE EXCEPTION 'no open % account for ccy=%',
                        v_applied_kind, v_wo.currency
          USING ERRCODE = 'P0010';
      END IF;

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            v_reason,
        'document_kind',     'wo_event',
        'document_id',       p_event_id,
        'debit_account_id',  v_val_acct_wip,
        'credit_account_id', v_applied_acct,
        'amount',            v_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      ));
    END IF;
  END LOOP;

  RETURN v_batch;
END;
$$;

COMMENT ON FUNCTION _wo_emit_bom_lines(UUID, BIGINT, INT, BIGINT, JSONB, UUID, DATE, UUID) IS
  'Generic BOM-line emitter. Dispatches item value-leg cost on the '
  'COMPONENT''s cost_method. tier 1 (acct-24b): standard / wac_perpetual. '
  'tier 2 (acct-7py): wac_periodic, parent must match. tier 3 (acct-rso): '
  'wac_retroactive, parent must match. acct-7eo (mig 0077): mixed parent/'
  'component cost methods now permitted for wac_periodic / wac_retroactive '
  'components — close hook posts single-leg variance through '
  'variance_material_mixed at period close. fifo / lot raise P0006 '
  '(acct-8gg).';

-- ============================================================
-- 3) wac_periodic_close_hook — add mixed-case branch.
-- ============================================================

CREATE OR REPLACE FUNCTION wac_periodic_close_hook(
  p_period_id         BIGINT,
  p_force_provisional BOOLEAN DEFAULT FALSE
) RETURNS BIGINT LANGUAGE plpgsql AS $$
DECLARE
  v_period_opens   DATE;
  v_period_closes  DATE;
  v_period_code    TEXT;
  v_count          BIGINT := 0;
  v_pool_id        BIGINT;
  v_processed      BIGINT[] := ARRAY[]::BIGINT[];
  v_remaining      INT;
  v_progress       INT;
  v_cycle_pools    TEXT;
  v_row            RECORD;
  v_orig           RECORD;
  v_pool_acct      accounts%ROWTYPE;
  v_pool_value_in  BIGINT;
  v_pool_qty_in    BIGINT;
  v_final_avg      BIGINT;
  v_provisional    BIGINT;
  v_variance       BIGINT;
  v_var_acct       BIGINT;
  v_batch          JSONB;
  v_var_xfer_id    BIGINT;
  v_orig_debit     BIGINT;
  v_orig_credit    BIGINT;
  v_event_a        JSONB;
  v_event_b        JSONB;
  v_orig_reason    transfer_reason;
  v_dest_method    TEXT;
  v_mixed          BOOLEAN;
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
    FROM transfers_provisional p
    JOIN transfers t ON t.id = p.transfer_id
   WHERE p.period_id = p_period_id
     AND p.cost_method = 'wac_periodic'
     AND p.finalized_at IS NULL
  UNION
  SELECT DISTINCT t.debit_account_id
    FROM transfers_provisional p
    JOIN transfers t ON t.id = p.transfer_id
   WHERE p.period_id = p_period_id
     AND p.cost_method = 'wac_periodic'
     AND p.finalized_at IS NULL
     AND t.reason IN ('op_move_v', 'rm_issue_to_wo')
  ON CONFLICT DO NOTHING;

  INSERT INTO _wac_edges (predecessor, successor)
  SELECT DISTINCT t.credit_account_id, t.debit_account_id
    FROM transfers_provisional p
    JOIN transfers t ON t.id = p.transfer_id
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
        FROM transfers t
        LEFT JOIN transfers_provisional p
               ON p.transfer_id = t.id
              AND p.finalized_at IS NOT NULL
              AND p.variance_transfer_id IS NULL
       WHERE t.debit_account_id = v_pool_id
         AND t.business_date BETWEEN v_period_opens AND v_period_closes;

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
          FROM transfers_provisional
         WHERE period_id = p_period_id
           AND cost_method = 'wac_periodic'
           AND finalized_at IS NULL
         ORDER BY transfer_id
           FOR UPDATE
      LOOP
        SELECT * INTO v_orig FROM transfers WHERE id = v_row.transfer_id;
        IF v_orig.credit_account_id <> v_pool_id THEN
          CONTINUE;
        END IF;
        v_orig_reason := v_orig.reason;

        v_provisional := v_orig.amount / v_row.qty;
        v_variance    := (v_final_avg - v_provisional) * v_row.qty;

        -- acct-7eo: detect mixed-method rm_issue_to_wo. Source pool is
        -- wac_periodic (we're walking it). Destination pool's SKU may
        -- not be wac_periodic — that's the mixed shape.
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
          -- Mixed-method rm_issue_to_wo: post single-leg variance
          -- through variance_material_mixed against the component pool.
          -- Destination WIP pool untouched (debit-normal, drained at WO
          -- close — single-leg per CLAUDE.md R5).
          IF v_variance = 0 THEN
            UPDATE transfers_provisional
               SET finalized_at = clock_timestamp(),
                   variance_amount = 0,
                   variance_transfer_id = NULL
             WHERE transfer_id = v_row.transfer_id;
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
          PERFORM post_transfers(v_batch, TRUE);
          SELECT id INTO v_var_xfer_id
            FROM transfers
           WHERE idempotency_key = (v_event_a->>'idempotency_key')::UUID;
          UPDATE transfers_provisional
             SET finalized_at = clock_timestamp(),
                 variance_amount = v_variance,
                 variance_transfer_id = v_var_xfer_id
           WHERE transfer_id = v_row.transfer_id;
          v_count := v_count + 1;
          CONTINUE;
        END IF;

        -- Homogeneous internal-chain (op_move_v + rm_issue_to_wo where
        -- destination is wac_periodic). Variance recorded; no transfer.
        IF v_orig_reason IN ('op_move_v', 'rm_issue_to_wo') THEN
          UPDATE transfers_provisional
             SET finalized_at = clock_timestamp(),
                 variance_amount = v_variance,
                 variance_transfer_id = NULL
           WHERE transfer_id = v_row.transfer_id;
          v_count := v_count + 1;
          CONTINUE;
        END IF;

        IF v_variance = 0 THEN
          UPDATE transfers_provisional
             SET finalized_at = clock_timestamp(),
                 variance_amount = 0,
                 variance_transfer_id = NULL
           WHERE transfer_id = v_row.transfer_id;
          v_count := v_count + 1;
          CONTINUE;
        END IF;

        SELECT id INTO v_var_acct
          FROM accounts
         WHERE kind = 'variance_wac_period'
           AND ledger_kind = 'value'
           AND currency = v_pool_acct.currency
           AND NOT is_closed;
        IF v_var_acct IS NULL THEN
          RAISE EXCEPTION
            'wac_periodic_close: no variance_wac_period(value, ccy=%) account configured',
            v_pool_acct.currency USING ERRCODE = 'P0010';
        END IF;

        v_orig_debit  := v_orig.debit_account_id;
        v_orig_credit := v_orig.credit_account_id;

        IF v_pool_acct.kind = 'inv_value_wip' THEN
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
          END IF;
          v_batch := jsonb_build_array(v_event_a);
        ELSE
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

        PERFORM post_transfers(v_batch, TRUE);

        SELECT id INTO v_var_xfer_id
          FROM transfers
         WHERE idempotency_key = (v_event_a->>'idempotency_key')::UUID;

        UPDATE transfers_provisional
           SET finalized_at = clock_timestamp(),
               variance_amount = v_variance,
               variance_transfer_id = v_var_xfer_id
         WHERE transfer_id = v_row.transfer_id;
        v_count := v_count + 1;
      END LOOP;

      DELETE FROM _wac_pools WHERE pool_id = v_pool_id;
      v_processed := array_append(v_processed, v_pool_id);
    END LOOP;

    IF v_progress = 0 THEN
      SELECT string_agg(pool_id::TEXT, ', ' ORDER BY pool_id)
        INTO v_cycle_pools
        FROM _wac_pools;
      RAISE EXCEPTION
        'wac_periodic_pool_cycle: period % (id=%) has rework cycles in '
        'wac_periodic op_move_v / rm_issue_to_wo flow involving pools [%]; '
        'iterative-fixed-point handling deferred to acct-p7v-rework.',
        v_period_code, p_period_id, v_cycle_pools
        USING ERRCODE = 'P0036';
    END IF;
  END LOOP;

  RETURN v_count;
END;
$$;

COMMENT ON FUNCTION wac_periodic_close_hook(BIGINT, BOOLEAN) IS
  'wac_periodic period-close recompute. Tier 2 (acct-smn): topological '
  'pool walk over op_move_v edges. Tier 2 (acct-7py): rm_issue_to_wo '
  'edges + internal-chain treatment. acct-7eo (mig 0077): mixed-method '
  'rm_issue_to_wo (component is wac_periodic, destination WIP is not) '
  'posts single-leg variance through variance_material_mixed at the '
  'component pool — destination WIP untouched. Rework cycles raise '
  'P0036; deferred to acct-p7v-rework.';

-- ============================================================
-- 4) wac_retroactive_close_hook — add mixed-case branch.
-- ============================================================

CREATE OR REPLACE FUNCTION wac_retroactive_close_hook(
  p_period_id         BIGINT,
  p_force_provisional BOOLEAN DEFAULT FALSE
) RETURNS BIGINT LANGUAGE plpgsql AS $$
DECLARE
  v_period_opens   DATE;
  v_period_closes  DATE;
  v_period_code    TEXT;
  v_count          BIGINT := 0;
  v_pool_id        BIGINT;
  v_processed      BIGINT[] := ARRAY[]::BIGINT[];
  v_remaining      INT;
  v_progress       INT;
  v_cycle_pools    TEXT;
  v_pool_acct      accounts%ROWTYPE;
  v_qty_pool_id    BIGINT;
  v_pool_value     BIGINT;
  v_pool_qty       BIGINT;
  v_event          RECORD;
  v_recomputed_avg BIGINT;
  v_recomputed_amt BIGINT;
  v_variance       BIGINT;
  v_var_acct       BIGINT;
  v_batch          JSONB;
  v_var_xfer_id    BIGINT;
  v_event_a        JSONB;
  v_event_b        JSONB;
  v_orig_reason    transfer_reason;
  v_dest_method    TEXT;
  v_mixed          BOOLEAN;
BEGIN
  SELECT opens_at, closes_at, code
    INTO v_period_opens, v_period_closes, v_period_code
    FROM periods WHERE id = p_period_id;
  IF NOT FOUND THEN
    RETURN 0;
  END IF;

  CREATE TEMP TABLE _wac_retro_pools (
    pool_id BIGINT PRIMARY KEY
  ) ON COMMIT DROP;

  CREATE TEMP TABLE _wac_retro_edges (
    predecessor BIGINT,
    successor   BIGINT,
    PRIMARY KEY (predecessor, successor)
  ) ON COMMIT DROP;

  INSERT INTO _wac_retro_pools (pool_id)
  SELECT DISTINCT t.credit_account_id
    FROM transfers_provisional p
    JOIN transfers t ON t.id = p.transfer_id
   WHERE p.period_id = p_period_id
     AND p.cost_method = 'wac_retroactive'
     AND p.finalized_at IS NULL
  UNION
  SELECT DISTINCT t.debit_account_id
    FROM transfers_provisional p
    JOIN transfers t ON t.id = p.transfer_id
   WHERE p.period_id = p_period_id
     AND p.cost_method = 'wac_retroactive'
     AND p.finalized_at IS NULL
     AND t.reason IN ('op_move_v', 'rm_issue_to_wo')
  ON CONFLICT DO NOTHING;

  INSERT INTO _wac_retro_edges (predecessor, successor)
  SELECT DISTINCT t.credit_account_id, t.debit_account_id
    FROM transfers_provisional p
    JOIN transfers t ON t.id = p.transfer_id
   WHERE p.period_id = p_period_id
     AND p.cost_method = 'wac_retroactive'
     AND p.finalized_at IS NULL
     AND t.reason IN ('op_move_v', 'rm_issue_to_wo')
  ON CONFLICT DO NOTHING;

  LOOP
    SELECT COUNT(*) INTO v_remaining FROM _wac_retro_pools;
    EXIT WHEN v_remaining = 0;

    v_progress := 0;
    FOR v_pool_id IN
      SELECT wp.pool_id
        FROM _wac_retro_pools wp
       WHERE NOT EXISTS (
         SELECT 1 FROM _wac_retro_edges e
          WHERE e.successor = wp.pool_id
            AND e.predecessor IN (SELECT pool_id FROM _wac_retro_pools)
       )
    LOOP
      v_progress := v_progress + 1;

      SELECT * INTO v_pool_acct FROM accounts WHERE id = v_pool_id;

      IF v_pool_acct.kind = 'inv_value_wip' THEN
        v_qty_pool_id := _post_transfers_lookup_qty_account(v_pool_acct);
        IF v_qty_pool_id IS NULL THEN
          RAISE EXCEPTION
            'wac_retroactive_close: cannot resolve stock_wip qty account '
            'for inv_value_wip pool % (sku=% op=%)',
            v_pool_id, v_pool_acct.sku_id, v_pool_acct.routing_op
            USING ERRCODE = 'P0010';
        END IF;
      ELSE
        v_qty_pool_id := v_pool_id;
      END IF;

      SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = v_pool_id THEN  t.amount
                               WHEN t.credit_account_id = v_pool_id THEN -t.amount END), 0)
        INTO v_pool_value
        FROM transfers t
       WHERE v_pool_id IN (t.debit_account_id, t.credit_account_id)
         AND t.business_date < v_period_opens;

      SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = v_qty_pool_id THEN  t.qty
                               WHEN t.credit_account_id = v_qty_pool_id THEN -t.qty END), 0)
        INTO v_pool_qty
        FROM transfers t
       WHERE v_qty_pool_id IN (t.debit_account_id, t.credit_account_id)
         AND t.qty IS NOT NULL
         AND t.business_date < v_period_opens;

      FOR v_event IN
        WITH value_events AS (
          SELECT t.id,
                 CASE
                   WHEN t.debit_account_id = v_pool_id
                        THEN t.amount + COALESCE(p_cache.variance_amount, 0)
                   ELSE t.amount
                 END AS adj_amount,
                 t.amount AS orig_amount,
                 t.qty,
                 t.debit_account_id,
                 t.credit_account_id,
                 t.business_date,
                 t.posted_at,
                 t.document_id,
                 t.reason,
                 (p_my.transfer_id IS NOT NULL) AS is_prov,
                 1 AS sub_priority,
                 'value'::TEXT AS leg
            FROM transfers t
            LEFT JOIN transfers_provisional p_cache
                   ON p_cache.transfer_id = t.id
                  AND p_cache.finalized_at IS NOT NULL
                  AND p_cache.variance_transfer_id IS NULL
            LEFT JOIN transfers_provisional p_my
                   ON p_my.transfer_id = t.id
                  AND p_my.cost_method = 'wac_retroactive'
                  AND p_my.finalized_at IS NULL
                  AND t.credit_account_id = v_pool_id
           WHERE v_pool_id IN (t.debit_account_id, t.credit_account_id)
             AND t.business_date BETWEEN v_period_opens AND v_period_closes
        ),
        qty_events AS (
          SELECT t.id,
                 t.amount AS adj_amount,
                 t.amount AS orig_amount,
                 t.qty,
                 t.debit_account_id,
                 t.credit_account_id,
                 t.business_date,
                 t.posted_at,
                 t.document_id,
                 t.reason,
                 FALSE AS is_prov,
                 CASE WHEN t.debit_account_id = v_qty_pool_id THEN 0 ELSE 2 END AS sub_priority,
                 'qty'::TEXT AS leg
            FROM transfers t
           WHERE v_pool_acct.kind = 'inv_value_wip'
             AND v_qty_pool_id <> v_pool_id
             AND v_qty_pool_id IN (t.debit_account_id, t.credit_account_id)
             AND t.qty IS NOT NULL
             AND t.business_date BETWEEN v_period_opens AND v_period_closes
        ),
        merged AS (
          SELECT * FROM value_events
          UNION ALL
          SELECT * FROM qty_events
        ),
        ordered AS (
          SELECT *,
                 MIN(posted_at) OVER (PARTITION BY document_id) AS doc_chrono
            FROM merged
        )
        SELECT * FROM ordered
        ORDER BY business_date, doc_chrono, document_id, sub_priority, id
      LOOP
        IF v_event.leg = 'qty' THEN
          IF v_event.debit_account_id = v_qty_pool_id THEN
            v_pool_qty := v_pool_qty + v_event.qty;
          ELSE
            v_pool_qty := v_pool_qty - v_event.qty;
          END IF;
          CONTINUE;
        END IF;

        IF v_event.debit_account_id = v_pool_id THEN
          v_pool_value := v_pool_value + v_event.adj_amount;
          IF v_pool_acct.kind <> 'inv_value_wip' AND v_event.qty IS NOT NULL THEN
            v_pool_qty := v_pool_qty + v_event.qty;
          END IF;
          CONTINUE;
        END IF;

        IF v_event.qty IS NULL THEN
          v_pool_value := v_pool_value - v_event.orig_amount;
          CONTINUE;
        END IF;

        IF v_pool_qty <= 0 THEN
          IF p_force_provisional AND v_event.is_prov THEN
            CONTINUE;
          END IF;
          RAISE EXCEPTION
            'wac_retroactive_replay_pool_empty: period % (id=%) pool kind=% sku=% '
            'loc=% op=% ccy=%: running qty went non-positive at depletion of transfer %; '
            'this indicates the perpetual chain has an inconsistency (more depletions '
            'than receipts of valid age). Pass p_force_provisional=TRUE to skip this row.',
            v_period_code, p_period_id, v_pool_acct.kind, v_pool_acct.sku_id,
            v_pool_acct.location_id, v_pool_acct.routing_op, v_pool_acct.currency,
            v_event.id
            USING ERRCODE = 'P0006';
        END IF;

        v_recomputed_avg := v_pool_value / v_pool_qty;
        v_recomputed_amt := v_event.qty * v_recomputed_avg;

        IF v_event.is_prov THEN
          v_variance    := v_recomputed_amt - v_event.orig_amount;
          v_orig_reason := v_event.reason;

          -- acct-7eo: detect mixed-method rm_issue_to_wo.
          v_mixed := FALSE;
          IF v_orig_reason = 'rm_issue_to_wo' THEN
            SELECT s.cost_method::TEXT INTO v_dest_method
              FROM accounts a
              JOIN skus s ON s.id = a.sku_id
             WHERE a.id = v_event.debit_account_id;
            IF v_dest_method IS DISTINCT FROM 'wac_retroactive' THEN
              v_mixed := TRUE;
            END IF;
          END IF;

          IF v_mixed THEN
            -- Mixed-method: post single-leg variance through
            -- variance_material_mixed against the component pool.
            IF v_variance = 0 THEN
              UPDATE transfers_provisional
                 SET finalized_at = clock_timestamp(),
                     variance_amount = 0,
                     variance_transfer_id = NULL
               WHERE transfer_id = v_event.id;
              v_count := v_count + 1;
            ELSE
              SELECT id INTO v_var_acct FROM accounts
               WHERE kind = 'variance_material_mixed'
                 AND ledger_kind = 'value'
                 AND currency = v_pool_acct.currency
                 AND NOT is_closed;
              IF v_var_acct IS NULL THEN
                RAISE EXCEPTION
                  'wac_retroactive_close: no variance_material_mixed(value, ccy=%) '
                  'account configured (acct-7eo)',
                  v_pool_acct.currency USING ERRCODE = 'P0010';
              END IF;

              IF v_variance > 0 THEN
                v_event_a := jsonb_build_object(
                  'reason',            'cost_restate',
                  'document_kind',     'wac_retroactive_close_mixed',
                  'document_id',       gen_random_uuid(),
                  'debit_account_id',  v_var_acct,
                  'credit_account_id', v_event.credit_account_id,
                  'amount',            v_variance,
                  'business_date',     v_period_closes,
                  'idempotency_key',   gen_random_uuid(),
                  'posted_by',         '00000000-0000-0000-0000-000000000000'
                );
              ELSE
                v_event_a := jsonb_build_object(
                  'reason',            'cost_restate',
                  'document_kind',     'wac_retroactive_close_mixed',
                  'document_id',       gen_random_uuid(),
                  'debit_account_id',  v_event.credit_account_id,
                  'credit_account_id', v_var_acct,
                  'amount',            -v_variance,
                  'business_date',     v_period_closes,
                  'idempotency_key',   gen_random_uuid(),
                  'posted_by',         '00000000-0000-0000-0000-000000000000'
                );
              END IF;
              v_batch := jsonb_build_array(v_event_a);
              PERFORM post_transfers(v_batch, TRUE);
              SELECT id INTO v_var_xfer_id
                FROM transfers
               WHERE idempotency_key = (v_event_a->>'idempotency_key')::UUID;
              UPDATE transfers_provisional
                 SET finalized_at = clock_timestamp(),
                     variance_amount = v_variance,
                     variance_transfer_id = v_var_xfer_id
               WHERE transfer_id = v_event.id;
              v_count := v_count + 1;
            END IF;
          ELSIF v_orig_reason IN ('op_move_v', 'rm_issue_to_wo') THEN
            UPDATE transfers_provisional
               SET finalized_at = clock_timestamp(),
                   variance_amount = v_variance,
                   variance_transfer_id = NULL
             WHERE transfer_id = v_event.id;
            v_count := v_count + 1;
          ELSIF v_variance = 0 THEN
            UPDATE transfers_provisional
               SET finalized_at = clock_timestamp(),
                   variance_amount = 0,
                   variance_transfer_id = NULL
             WHERE transfer_id = v_event.id;
            v_count := v_count + 1;
          ELSE
            SELECT id INTO v_var_acct FROM accounts
             WHERE kind = 'variance_wac_retroactive' AND ledger_kind = 'value'
               AND currency = v_pool_acct.currency AND NOT is_closed;
            IF v_var_acct IS NULL THEN
              RAISE EXCEPTION
                'wac_retroactive_close: no variance_wac_retroactive(value, ccy=%) account configured',
                v_pool_acct.currency USING ERRCODE = 'P0010';
            END IF;

            IF v_pool_acct.kind = 'inv_value_wip' THEN
              IF v_variance > 0 THEN
                v_event_a := jsonb_build_object(
                  'reason',            'cost_restate',
                  'document_kind',     'wac_retroactive_close',
                  'document_id',       gen_random_uuid(),
                  'debit_account_id',  v_event.debit_account_id,
                  'credit_account_id', v_var_acct,
                  'amount',            v_variance,
                  'business_date',     v_period_closes,
                  'idempotency_key',   gen_random_uuid(),
                  'posted_by',         '00000000-0000-0000-0000-000000000000'
                );
              ELSE
                v_event_a := jsonb_build_object(
                  'reason',            'cost_restate',
                  'document_kind',     'wac_retroactive_close',
                  'document_id',       gen_random_uuid(),
                  'debit_account_id',  v_var_acct,
                  'credit_account_id', v_event.debit_account_id,
                  'amount',            -v_variance,
                  'business_date',     v_period_closes,
                  'idempotency_key',   gen_random_uuid(),
                  'posted_by',         '00000000-0000-0000-0000-000000000000'
                );
              END IF;
              v_batch := jsonb_build_array(v_event_a);
            ELSE
              IF v_variance > 0 THEN
                v_event_a := jsonb_build_object(
                  'reason',            'cost_restate',
                  'document_kind',     'wac_retroactive_close',
                  'document_id',       gen_random_uuid(),
                  'debit_account_id',  v_event.debit_account_id,
                  'credit_account_id', v_var_acct,
                  'amount',            v_variance,
                  'business_date',     v_period_closes,
                  'idempotency_key',   gen_random_uuid(),
                  'posted_by',         '00000000-0000-0000-0000-000000000000'
                );
                v_event_b := jsonb_build_object(
                  'reason',            'cost_restate',
                  'document_kind',     'wac_retroactive_close',
                  'document_id',       gen_random_uuid(),
                  'debit_account_id',  v_var_acct,
                  'credit_account_id', v_pool_id,
                  'amount',            v_variance,
                  'business_date',     v_period_closes,
                  'idempotency_key',   gen_random_uuid(),
                  'posted_by',         '00000000-0000-0000-0000-000000000000'
                );
              ELSE
                v_event_a := jsonb_build_object(
                  'reason',            'cost_restate',
                  'document_kind',     'wac_retroactive_close',
                  'document_id',       gen_random_uuid(),
                  'debit_account_id',  v_var_acct,
                  'credit_account_id', v_event.debit_account_id,
                  'amount',            -v_variance,
                  'business_date',     v_period_closes,
                  'idempotency_key',   gen_random_uuid(),
                  'posted_by',         '00000000-0000-0000-0000-000000000000'
                );
                v_event_b := jsonb_build_object(
                  'reason',            'cost_restate',
                  'document_kind',     'wac_retroactive_close',
                  'document_id',       gen_random_uuid(),
                  'debit_account_id',  v_pool_id,
                  'credit_account_id', v_var_acct,
                  'amount',            -v_variance,
                  'business_date',     v_period_closes,
                  'idempotency_key',   gen_random_uuid(),
                  'posted_by',         '00000000-0000-0000-0000-000000000000'
                );
              END IF;
              v_batch := jsonb_build_array(v_event_a, v_event_b);
            END IF;

            PERFORM post_transfers(v_batch, TRUE);

            SELECT id INTO v_var_xfer_id
              FROM transfers
             WHERE idempotency_key = (v_event_a->>'idempotency_key')::UUID;

            UPDATE transfers_provisional
               SET finalized_at = clock_timestamp(),
                   variance_amount = v_variance,
                   variance_transfer_id = v_var_xfer_id
             WHERE transfer_id = v_event.id;
            v_count := v_count + 1;
          END IF;

          v_pool_value := v_pool_value - v_recomputed_amt;
          IF v_pool_acct.kind <> 'inv_value_wip' THEN
            v_pool_qty := v_pool_qty - v_event.qty;
          END IF;
        ELSE
          v_pool_value := v_pool_value - v_event.orig_amount;
          IF v_pool_acct.kind <> 'inv_value_wip' THEN
            v_pool_qty := v_pool_qty - v_event.qty;
          END IF;
        END IF;
      END LOOP;

      DELETE FROM _wac_retro_pools WHERE pool_id = v_pool_id;
      v_processed := array_append(v_processed, v_pool_id);
    END LOOP;

    IF v_progress = 0 THEN
      SELECT string_agg(pool_id::TEXT, ', ' ORDER BY pool_id)
        INTO v_cycle_pools
        FROM _wac_retro_pools;
      RAISE EXCEPTION
        'wac_retroactive_pool_cycle: period % (id=%) has rework cycles in '
        'wac_retroactive op_move_v / rm_issue_to_wo flow involving pools [%]; '
        'iterative-fixed-point handling deferred to acct-p7v-rework.',
        v_period_code, p_period_id, v_cycle_pools
        USING ERRCODE = 'P0036';
    END IF;
  END LOOP;

  RETURN v_count;
END;
$$;

COMMENT ON FUNCTION wac_retroactive_close_hook(BIGINT, BOOLEAN) IS
  'wac_retroactive period-close recompute. Tier 3 (acct-rso): topological '
  'pool walk + per-pool chronological replay (merged value/qty stream). '
  'acct-7eo (mig 0077): mixed-method rm_issue_to_wo posts single-leg '
  'variance through variance_material_mixed at the component pool — '
  'destination WIP untouched. Rework cycles raise P0036.';
