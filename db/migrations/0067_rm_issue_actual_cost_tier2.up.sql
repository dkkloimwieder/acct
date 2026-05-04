-- acct-7py — Tier 2 of acct-rgb: rm_issue_to_wo on a wac_periodic
-- component lifts to running-pool-avg at issue time, with the resulting
-- value-leg flagged into transfers_provisional. The wac_periodic close
-- hook's topological pool walk extends to include rm_issue_to_wo edges
-- (raw_component_pool → destination_inv_value_wip), so the period-end
-- recompute on the raw pool propagates correctly to downstream WIP via
-- the LEFT JOIN cache, and lands at leaf wo_complete_v.
--
-- WHAT CHANGES.
--
-- 1. _wo_emit_bom_lines: lifts the wac_periodic gate from tier 1
--    (mig 0066). Mid-period math is identical to wac_perpetual — read
--    the component value pool's running avg under FOR UPDATE, value =
--    adj_qty × (pool_value / pool_qty). The flagging happens via
--    _post_transfers_apply_event downstream.
--    SCOPE GATE: the parent SKU MUST also be wac_periodic. Mixed parent
--    cost_methods (standard or wac_perpetual parent + wac_periodic
--    component) raise P0026 deferred to acct-7eo. Reason: the
--    rm_issue value-leg's variance has no clean landing on a non-
--    wac-periodic destination WIP pool (drained to 0 by wo_close_v
--    or by wac_perpetual wo_complete_v running avg, neither of which
--    is flagged for retroactive correction).
--
-- 2. _post_transfers_apply_event: TWO changes.
--    a. Cost-event reason list extends to include 'rm_issue_to_wo' so
--       the apply-step flags rm_issue value legs into
--       transfers_provisional when the COMPONENT SKU is wac_periodic.
--    b. SKU resolution switches from debit-first to credit-first.
--       Rationale: for every flagged reason, credit_account_id is the
--       depletion source whose cost_method drives flagging. Specifically:
--         * op_move/op_move_v: credit=stock_wip|inv_value_wip(parent) → parent's cost_method
--         * scrap/scrap_v: credit=stock_wip|inv_value_wip(parent) → parent's cost_method
--         * wo_complete/wo_complete_v: credit=stock_wip|inv_value_wip(parent) → parent's
--           cost_method (was debit-first which picked output_sku in multi-output
--           co-product WOs — latent bug, fixed by this switch)
--         * so_ship: credit=stock_available|inv_value_fg(sold) → sold SKU's cost_method
--         * rm_issue_to_wo: credit=inv_value_raw(component) → component's cost_method (NEW)
--       For canonical reasons where debit and credit share the same SKU
--       (all single-output WO events) or where debit has NULL sku_id
--       (scrap_v, so_ship, wo_complete_v with no FG before drain), the
--       switch is a no-op. It only changes behavior for rm_issue_to_wo
--       and for multi-output wo_complete_v.
--
-- 3. wac_periodic_close_hook: extend the topological pool walk to
--    include rm_issue_to_wo edges. Three additive changes:
--    a. Pool set: also include debit_account_id (= destination WIP) of
--       rm_issue_to_wo provisional rows. Previously only op_move_v
--       contributed to the debit-side pool inclusion; rm_issue_to_wo
--       must too so the destination WIP gets visited and downstream
--       op_move_v / wo_complete_v transfers debiting it pick up the
--       upstream variance via cache.
--    b. Edge set: also include rm_issue_to_wo's (credit, debit) tuple
--       (raw_component → destination_WIP). Same Kahn topological sort
--       handles raw + WIP in one walk.
--    c. Internal-chain branch: extend the IF v_orig_reason = 'op_move_v'
--       check to (op_move_v, rm_issue_to_wo). Both record variance
--       on transfers_provisional with variance_transfer_id = NULL and
--       DO NOT post a variance transfer. The cost shift propagates to
--       downstream pools' corrected_value_in via the LEFT JOIN on
--       transfers_provisional.variance_amount.
--
-- WHY rm_issue_to_wo IS INTERNAL-CHAIN.
-- The rm_issue value-leg's destination pool is inv_value_wip on a
-- wac_periodic parent (gate enforced in _wo_emit_bom_lines above). That
-- WIP pool is itself flagged via op_move_v / wo_complete_v provisionals.
-- The retroactive variance from the raw pool's recompute must flow
-- through to the leaf depletion (wo_complete_v drains FG at WIP-pool
-- final_avg). Posting a variance transfer at the rm_issue level would
-- either:
--   * 2-leg-wash through WIP destination: pushes drained WIP negative on
--     net-negative variance under multi-WO drift (debit-normal CHECK).
--   * Single-leg between variance_wac_period and raw: works numerically
--     but breaks the cache (downstream WIP's corrected_value_in would
--     double-count the variance — once via t.amount of the new variance
--     transfer, once via p.variance_amount of the original rm_issue TP
--     row).
-- Internal-chain is the correct treatment: record variance, propagate
-- via cache, post nothing. The cumulative cost shift surfaces at the
-- leaf wo_complete_v's final_avg computation on the inv_value_wip
-- destination (already debit-normal; tier 1 single-leg pattern).
--
-- 3-TIER CHAIN. raw (wac_periodic) → WIP@op10 → WIP@op20 → FG.
--   * Process raw pool first (Kahn). Compute final_avg from PO/seed
--     receipts. For each rm_issue_to_wo with credit=raw: variance
--     recorded.
--   * Process WIP@op10. corrected_value_in = SUM(amount + variance) over
--     debit transfers; the rm_issue's variance enters via LEFT JOIN.
--     final_avg(op10) reflects raw final_avg propagated forward. For
--     each op_move_v with credit=WIP@op10: variance recorded.
--   * Process WIP@op20. corrected_value_in includes the op_move_v
--     transfer's amount + variance (which already chained the raw
--     correction). final_avg(op20) reflects the full chain. For each
--     wo_complete_v with credit=WIP@op20: variance posted single-leg
--     (FG side is debit-normal and retains balance).
--
-- MIXED COMPONENTS IN SAME BOM (one std, one wac_periodic). Standard
-- component's rm_issue is unflagged (component cost_method = 'standard'
-- → not in flagging list). wac_periodic component's rm_issue is flagged.
-- Close hook walks only the wac side. Standard side stays at its
-- original amount.
--
-- LOCK ORDER. _wo_emit_bom_lines acquires FOR UPDATE on the component
-- value pool before reading running avg. post_transfers's lock pre-scan
-- re-acquires the same lock idempotently (single-account; no deadlock
-- because the dispatcher locks accounts in ID order, the emitter's
-- earlier single-account lock doesn't violate the global ordering).

-- ============================================================
-- _wo_emit_bom_lines — lift wac_periodic gate
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
      -- LITERAL emission per acct-6jq (mig 0061): scrap_pct is planning
      -- metadata, not a per-issue gross-up. Pool flow is qty_per_parent
      -- × p_qty regardless of scrap_pct or skus.yield_mode.
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

      -- Dispatch on COMPONENT's cost_method (acct-24b tier 1, acct-7py tier 2).
      SELECT cost_method INTO v_comp_cost_method
        FROM skus WHERE id = v_line.component_sku_id;

      CASE v_comp_cost_method
        WHEN 'standard' THEN
          v_comp_std_cost := resolve_standard_cost_at(
            v_line.component_sku_id, p_business_date
          );
          v_value := v_adj_qty * v_comp_std_cost;

        WHEN 'wac_perpetual' THEN
          -- Lock the component pool before reading running avg. The
          -- subsequent post_transfers lock pre-scan re-acquires the same
          -- lock idempotently.
          PERFORM 1 FROM accounts WHERE id = v_comp_val_acct FOR UPDATE;

          -- Per-class qty divisor (acct-1vr / mig 0030 pattern).
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
          -- Tier 2 (acct-7py): supported only when parent is also
          -- wac_periodic. Mixed cost methods raise P0026; deferred to
          -- acct-7eo because the close-hook variance on rm_issue cannot
          -- propagate cleanly into a non-wac-periodic destination WIP
          -- pool (drained to 0 by wo_close_v on standard parent or by
          -- wac_perpetual wo_complete_v that isn't flagged for
          -- retroactive correction).
          IF v_parent_cost_method <> 'wac_periodic' THEN
            RAISE EXCEPTION
              'rm_issue_mixed_cost_method: wac_periodic component % at '
              'sku=% requires wac_periodic parent (parent % has '
              'cost_method=%). Mixed parent/component cost methods '
              'deferred to acct-7eo.',
              v_line.component_sku_id, v_line.component_sku_id,
              v_wo.parent_sku_id, v_parent_cost_method
              USING ERRCODE = 'P0026';
          END IF;

          -- Identical math to wac_perpetual — read running avg under
          -- FOR UPDATE. The flagging into transfers_provisional happens
          -- in _post_transfers_apply_event (extended below to include
          -- rm_issue_to_wo with credit-first SKU resolution).
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
          RAISE EXCEPTION
            'rm_issue_to_wo from wac_retroactive component % deferred to '
            'acct-rso (tier 3 of acct-rgb / acct-8in): per-event '
            'chronological replay across consumption events',
            v_line.component_sku_id USING ERRCODE = 'P0026';

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
  'tier 2 (acct-7py): wac_periodic — same running-avg math as wac_perpetual '
  'but only when parent is also wac_periodic (mixed cost methods raise '
  'P0026 → acct-7eo). wac_retroactive / fifo / lot defer further (acct-rso, '
  'acct-8gg).';

-- ============================================================
-- _post_transfers_apply_event — credit-first SKU resolution +
-- rm_issue_to_wo in cost-event flagging list.
-- ============================================================

CREATE OR REPLACE FUNCTION _post_transfers_apply_event(
  p_event           JSONB,
  p_idx             INT,
  p_amount          BIGINT,
  p_d_acct          accounts,
  p_c_acct          accounts,
  p_cost_method     cost_method,
  p_override_closed BOOLEAN
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_period_id     BIGINT;
  v_period_closed TIMESTAMPTZ;
  v_business_date DATE;
  v_qty_for_row   BIGINT;
  v_reason        transfer_reason;
  v_idem_key      UUID;
  v_new_id        BIGINT;
  v_event_qty     BIGINT;
  v_resolved_cm   cost_method;
  v_cost_sku      UUID;
BEGIN
  v_business_date := (p_event->>'business_date')::DATE;
  v_reason        := (p_event->>'reason')::transfer_reason;
  v_idem_key      := (p_event->>'idempotency_key')::UUID;

  IF p_d_acct.is_closed OR p_c_acct.is_closed THEN
    RAISE EXCEPTION 'account_closed: event index %', p_idx
      USING ERRCODE = 'P0001';
  END IF;
  IF p_d_acct.ledger_kind <> p_c_acct.ledger_kind THEN
    RAISE EXCEPTION 'ledger_mismatch: event index % (% vs %)',
                    p_idx, p_d_acct.ledger_kind, p_c_acct.ledger_kind
      USING ERRCODE = 'P0002';
  END IF;
  IF p_d_acct.ledger_kind = 'value'
     AND p_d_acct.currency <> p_c_acct.currency THEN
    RAISE EXCEPTION 'currency_mismatch: event index % (% vs %)',
                    p_idx, p_d_acct.currency, p_c_acct.currency
      USING ERRCODE = 'P0003';
  END IF;

  SELECT id, closed_at INTO v_period_id, v_period_closed
    FROM periods
   WHERE opens_at <= v_business_date AND closes_at >= v_business_date;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'period_missing: event index % business_date %',
                    p_idx, v_business_date USING ERRCODE = 'P0004';
  END IF;
  IF v_period_closed IS NOT NULL AND NOT p_override_closed THEN
    RAISE EXCEPTION 'period_closed: event index % business_date %',
                    p_idx, v_business_date USING ERRCODE = 'P0005';
  END IF;

  v_qty_for_row := (p_event->>'qty')::BIGINT;
  IF v_qty_for_row IS NULL
     AND p_d_acct.ledger_kind = 'qty'
     AND p_c_acct.ledger_kind = 'qty' THEN
    v_qty_for_row := p_amount;
  END IF;

  UPDATE accounts SET debits_total  = debits_total  + p_amount
    WHERE id = p_d_acct.id;
  UPDATE accounts SET credits_total = credits_total + p_amount
    WHERE id = p_c_acct.id;
  INSERT INTO transfers (
    reason, document_kind, document_id, document_line_id,
    debit_account_id, credit_account_id, amount, qty,
    routing_op, counterparty_id, period_id, business_date,
    idempotency_key, posted_by
  ) VALUES (
    v_reason, p_event->>'document_kind', (p_event->>'document_id')::UUID,
    (p_event->>'document_line_id')::UUID, p_d_acct.id, p_c_acct.id,
    p_amount, v_qty_for_row,
    (p_event->>'routing_op')::INT, (p_event->>'counterparty_id')::UUID,
    v_period_id, v_business_date, v_idem_key,
    (p_event->>'posted_by')::UUID
  ) RETURNING id INTO v_new_id;

  -- Provisional flag for wac_periodic / wac_retroactive depletions.
  -- Cost-event reasons flagged: canonical (op_move/scrap/wo_complete/
  -- so_ship), BOM2 *_v reasons (op_move_v/scrap_v/wo_complete_v), and
  -- — added by tier 2 (acct-7py) — rm_issue_to_wo. Resolves cost_method
  -- on-demand when caller (post_transfers) passes NULL — which it does
  -- for every reason except the canonical four since they bypass the
  -- dispatcher's reason list.
  --
  -- SKU resolution: credit-first. Credit account is the depletion source
  -- whose cost_method drives flagging across all listed reasons:
  --   * op_move / op_move_v / scrap / scrap_v / wo_complete / wo_complete_v:
  --     credit = stock_wip|inv_value_wip (parent SKU)
  --   * so_ship: credit = stock_available|inv_value_fg (sold SKU)
  --   * rm_issue_to_wo: credit = inv_value_raw (component SKU) ← NEW
  -- Falls back to debit when credit has NULL sku_id (variance accounts
  -- on debit side of *_v reasons).
  IF v_reason IN ('op_move','scrap','wo_complete','so_ship',
                  'op_move_v','scrap_v','wo_complete_v',
                  'rm_issue_to_wo')
     AND p_d_acct.ledger_kind = 'value' THEN
    v_resolved_cm := p_cost_method;
    IF v_resolved_cm IS NULL THEN
      v_cost_sku := COALESCE(p_c_acct.sku_id, p_d_acct.sku_id);
      IF v_cost_sku IS NOT NULL THEN
        SELECT cost_method INTO v_resolved_cm FROM skus WHERE id = v_cost_sku;
      END IF;
    END IF;
    IF v_resolved_cm IN ('wac_periodic', 'wac_retroactive') THEN
      v_event_qty := (p_event->>'qty')::BIGINT;
      INSERT INTO transfers_provisional (transfer_id, period_id, cost_method, qty)
      VALUES (v_new_id, v_period_id, v_resolved_cm, v_event_qty);
    END IF;
  END IF;

  RETURN v_new_id;
END;
$$;

COMMENT ON FUNCTION _post_transfers_apply_event(JSONB, INT, BIGINT, accounts, accounts, cost_method, BOOLEAN) IS
  'Single-event apply step. Tier 2 (acct-7py): adds rm_issue_to_wo to the '
  'flagged cost-event reasons; switches SKU resolution to credit-first '
  '(credit account is the depletion source for every flagged reason). '
  'The credit-first switch is a no-op for canonical reasons where '
  'debit and credit share the same SKU, and for *_v reasons where the '
  'debit is a variance account with NULL sku_id; it changes behavior '
  'only for rm_issue_to_wo (parent vs component) and for multi-output '
  'wo_complete_v (output_sku vs parent_sku) — the latter being a latent '
  'bug fixed in passing.';

-- ============================================================
-- wac_periodic_close_hook — extend pool/edge sets and internal-chain
-- branch to cover rm_issue_to_wo.
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
BEGIN
  SELECT opens_at, closes_at, code
    INTO v_period_opens, v_period_closes, v_period_code
    FROM periods WHERE id = p_period_id;
  IF NOT FOUND THEN
    RETURN 0;
  END IF;

  -- Build a transient working set: the pools (credit accounts on flagged
  -- depletions) and the edge set (op_move_v + rm_issue_to_wo predecessor
  -- → successor). Use TEMP TABLES with ON COMMIT DROP since the function
  -- runs inside a close_period transaction.

  CREATE TEMP TABLE _wac_pools (
    pool_id BIGINT PRIMARY KEY
  ) ON COMMIT DROP;

  CREATE TEMP TABLE _wac_edges (
    predecessor BIGINT,
    successor   BIGINT,
    PRIMARY KEY (predecessor, successor)
  ) ON COMMIT DROP;

  -- Pool set: credit_account_id of every wac_periodic provisional row,
  -- plus debit_account_id of every internal-chain provisional
  -- (op_move_v + rm_issue_to_wo) so successor pools that have no own
  -- depletion-out still get processed.
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

  -- Edge set: op_move_v + rm_issue_to_wo transfers in-period drive the
  -- predecessor → successor relationships.
  INSERT INTO _wac_edges (predecessor, successor)
  SELECT DISTINCT t.credit_account_id, t.debit_account_id
    FROM transfers_provisional p
    JOIN transfers t ON t.id = p.transfer_id
   WHERE p.period_id = p_period_id
     AND p.cost_method = 'wac_periodic'
     AND p.finalized_at IS NULL
     AND t.reason IN ('op_move_v', 'rm_issue_to_wo')
  ON CONFLICT DO NOTHING;

  -- Iterative Kahn's algorithm. At each step, pick a pool with no
  -- unprocessed predecessor; process it; record processed; remove
  -- edges that now have a finished tail. If no pool has zero unprocessed
  -- predecessors and pools remain → cycle.
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

      -- corrected_value_in: SUM over debit transfers t to this pool;
      -- t.amount adjusted by any matching transfers_provisional.variance_amount
      -- (which is non-NULL only after upstream finalization in this hook,
      -- and only meaningful for internal-chain rows where no transfer
      -- was posted — the variance_transfer_id IS NULL filter prevents
      -- double-counting variance that was already posted as a separate
      -- transfer, though tier 2 doesn't post any leaf transfers for
      -- internal-chain reasons).
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

      -- Iterate provisionals from this pool. Lock for update; preserve
      -- transfer_id ordering for determinism.
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

        -- Internal-chain (op_move_v + rm_issue_to_wo): record variance,
        -- do NOT post a variance transfer. Cost shift propagates via the
        -- LEFT JOIN on variance_amount when computing downstream
        -- value_in. Tier 2 (acct-7py) extends this branch to cover
        -- rm_issue_to_wo. Both reasons have inv_value_wip destinations
        -- on wac_periodic parents; both are typically drained at WO
        -- close; posting variance through them would push them negative
        -- on signed-mixed multi-WO drift (debit-normal CHECK).
        IF v_orig_reason IN ('op_move_v', 'rm_issue_to_wo') THEN
          UPDATE transfers_provisional
             SET finalized_at = clock_timestamp(),
                 variance_amount = v_variance,
                 variance_transfer_id = NULL
           WHERE transfer_id = v_row.transfer_id;
          v_count := v_count + 1;
          CONTINUE;
        END IF;

        -- Leaf depletion path (wo_complete_v / scrap_v / so_ship / etc.).
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

        -- Single-leg variance posting for inv_value_wip source pools
        -- (tier 1 pattern). The WIP pool is typically drained at WO close;
        -- 2-leg wash through it would violate debit-normal CHECK on
        -- multi-WO drift. variance routes between v_orig_debit and
        -- variance_wac_period; pool untouched. v_orig_debit for leaf
        -- depletions is FG / variance_scrap / cogs (debit-normal accounts
        -- that retain balance through period).
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
          -- Raw / fg source (non-WIP wac_periodic on canonical reasons).
          -- Keep the 2-leg wash from migrations 0029 / 0030 / 0064 since
          -- raw and fg pools retain balance.
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
      -- No pool with zero unprocessed predecessors: cycle. Surface the
      -- remaining pools' identifiers to the caller.
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
  'pool walk over op_move_v edges. Tier 2 (acct-7py): extended to include '
  'rm_issue_to_wo edges (raw_component → destination_WIP) and treat '
  'rm_issue_to_wo as internal-chain (record variance, do not post; cost '
  'shift propagates via cache to leaf wo_complete_v). Per-pool: '
  'corrected_value_in via LEFT JOIN on variance_amount (filtered to '
  'variance_transfer_id IS NULL so leaf-posted variance is not double-'
  'counted), final_avg, then iterates provisional depletions. Leaf '
  'depletions on inv_value_wip source: single-leg variance; on raw/fg '
  'source: 2-leg wash. Rework cycles raise P0036; deferred to '
  'acct-p7v-rework.';
