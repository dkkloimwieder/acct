-- ============================================================
-- Phase D D2 — standard-cost dispatcher writes inventory_movements
-- (acct-wb75.3.2).
--
-- Extends `_post_posting_lines_apply_event` (mig 0024 baseline) with
-- a D-block immediately after the C inventory extension write. The
-- D-block emits one inventory_movements row per qualifying inv_value_*
-- value-leg posting on a STANDARD-cost SKU. WAC (D3) and FIFO/lot
-- (Phase E) extend the gate.
--
-- Design calls (from plan-phase-d-inventory-movements memory +
-- research/posting-lines-convergence-plan.md §4.D):
--
--   - One row per inv_value_* value-leg posting_line. Internal
--     posts where BOTH sides are inv_value_* (rm_issue_to_wo,
--     op_move_v) still emit one row, with sign and event_type
--     resolved from credit-side perspective (mirrors the §4.D D4
--     backfill template). Per-routing-op flow visibility is at
--     posting_lines grain; subledger aggregates at (product,
--     location).
--
--   - quantity SIGNED. Negative when the credit side is the
--     inventory leg (value going OUT of inventory); positive
--     otherwise. Mirrors the D4 backfill template's
--     `CASE WHEN c.kind LIKE 'stock_%' OR c.kind LIKE 'inv_value_%'
--       THEN -ABS(t.qty) ELSE ABS(t.qty) END` rule.
--
--   - actual_unit_cost = posting_line.amount / ABS(qty). For a
--     standard SKU this equals the standard cost at posting time —
--     the inv_value_* posting_line was capitalized at standard, so
--     subledger and GL agree on cost-flow value (D5 recon invariant).
--     Per-receipt PPV separation lives on `variance_ppv`
--     posting_lines (acct-7mg / acct-b8n) — we deliberately do NOT
--     mirror that into ppv_amount here; mainstream-ERP convention
--     keeps the subledger at inventory book value, with PPV as a
--     separate GL account. ppv_amount stays 0 for D2; future
--     enhancement can populate it for receipt-side analytics.
--
--   - standard_unit_cost = _resolve_standard_cost_at(sku, business_date).
--     Same lookup the dispatcher's standard branch uses, so the
--     inventory_movements row references the same authoritative
--     cost the GL was capitalized at. Lookup raises P0018 if no
--     standard exists at business_date — but the gate
--     v_inv_cost_method='standard' implies the SKU is on standard
--     cost, and any post on a standard-cost SKU has already cleared
--     the dispatcher's standard branch, which calls the same lookup.
--     So if we got here, the lookup succeeds.
--
--   - event_type via centralized helper `_inventory_movement_event_type`.
--     Returns NULL for unmapped reasons (caller skips the INSERT)
--     so D2 doesn't have to enumerate every posting_line_reason in
--     the apply_event body. Reasons mapped:
--
--         po_receipt / po_receipt_provisional        →  1 receipt
--         so_ship                                    →  2 issue
--         to_release / osp_ship                      →  3 transfer_out
--         to_receipt / osp_receive                   →  4 transfer_in
--         cycle_count_adj / inventory_adjustment     →  5 adj_in (qty>0)
--                                                       6 adj_out (qty<0)
--         scrap / scrap_v                            →  7 scrap
--         rm_issue_to_wo                             →  8 wo_consume
--         wo_complete / wo_complete_v                →  9 wo_produce
--         op_move / op_move_v                        → 11 op_move_in
--         customer_return                            → 12 return_in
--         po_return_to_vendor                        → 13 return_out
--         standard_cost_roll                         → 14 standard_revaluation
--         cost_adjustment / cost_restate / wo_close_v→ 16 cost_adjustment
--
--     Unmapped reasons (labor_apply, oh_apply, burden_apply,
--     lot_charge_apply, ar_*, ap_*, fx_*, ppv, muv, lv, ohv,
--     phantom_explode, etc.) return NULL — the apply_event D-block
--     skips the inventory_movements INSERT. Burdens fire on
--     stock_wip without a qty leg in apply_event's value-leg
--     pathway anyway (the qty-leg is a separate qty-only posting).
--
-- Non-goals for D2 (deferred to D3 / D4 / D5 / D6):
--   - WAC dispatchers (D3, mig 0027).
--   - Backfill of pre-D2 historical posts (D4, mig 0028).
--   - Recon check #7 subledger ↔ GL divergence (D5, mig 0029).
--   - Close-hook variance corrections as append-only movements
--     (D6, mig 0030).
-- ============================================================

CREATE OR REPLACE FUNCTION _inventory_movement_event_type(
  p_reason     posting_line_reason,
  p_signed_qty NUMERIC
) RETURNS SMALLINT
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
  -- See header comment for rationale and the full mapping table.
  -- NULL return signals "skip writing a movement row for this reason"
  -- — used by apply_event's D-block.
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
-- _post_posting_lines_apply_event — extended with D-block.
--
-- Body identical to mig 0024 through the C inventory extension
-- write, with a new D-block appended before RETURN. The D-block
-- gates on cost_method='standard' (D3 will lift); WAC and FIFO
-- still skip until D3 / Phase E land.
-- ============================================================

CREATE OR REPLACE FUNCTION _post_posting_lines_apply_event(
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
  v_period_id          BIGINT;
  v_period_closed      TIMESTAMPTZ;
  v_business_date      DATE;
  v_qty_for_row        BIGINT;
  v_reason             posting_line_reason;
  v_idem_key           UUID;
  v_new_id             BIGINT;
  v_event_qty          BIGINT;
  v_resolved_cm        cost_method;
  v_cost_sku           UUID;
  v_reverses_id        BIGINT;
  v_parent_doc         UUID;
  v_ic_pair            UUID;
  v_proc               VARCHAR;
  v_functional_ccy     CHAR(3);
  v_fx_rate            NUMERIC(20, 10);
  v_dim_sku            UUID;
  v_dim_loc            UUID;
  v_dim_routing_op     INT;
  v_event_cp           UUID;
  v_dim_cp             UUID;
  v_dim_cp_type        SMALLINT;
  v_inv_unit_cost      NUMERIC(19, 4);
  v_inv_cost_method    cost_method;
  v_im_quantity        NUMERIC(19, 6);
  v_im_event_type      SMALLINT;
  v_im_std_unit_cost   NUMERIC(19, 4);
  v_im_product_id      UUID;
  v_im_location_id     UUID;
BEGIN
  v_business_date := (p_event->>'business_date')::DATE;
  v_reason        := (p_event->>'reason')::posting_line_reason;
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
  INSERT INTO posting_lines (
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
      INSERT INTO posting_lines_provisional (posting_line_id, period_id, cost_method, qty)
      VALUES (v_new_id, v_period_id, v_resolved_cm, v_event_qty);
    END IF;
  END IF;

  -- B1 extension write.
  v_reverses_id := (p_event->>'reverses_posting_line_id')::BIGINT;
  v_parent_doc  := (p_event->>'parent_document_id')::UUID;
  v_ic_pair     := (p_event->>'intercompany_pair_id')::UUID;
  v_proc        := p_event->>'created_by_process';
  IF v_reverses_id IS NOT NULL
     OR v_parent_doc  IS NOT NULL
     OR v_ic_pair     IS NOT NULL
     OR v_proc        IS NOT NULL THEN
    INSERT INTO posting_line_sources (
      posting_line_id, reverses_posting_line_id, parent_document_id,
      intercompany_pair_id, created_by_process
    ) VALUES (
      v_new_id, v_reverses_id, v_parent_doc, v_ic_pair, v_proc
    );
  END IF;

  -- B2 extension write.
  IF p_c_acct.ledger_kind = 'value' THEN
    SELECT functional_currency INTO v_functional_ccy
      FROM legal_entities WHERE id = p_c_acct.legal_entity_id;

    IF v_functional_ccy IS NOT NULL
       AND p_c_acct.currency <> v_functional_ccy THEN
      SELECT rate INTO v_fx_rate
        FROM fx_rates
       WHERE from_currency = p_c_acct.currency
         AND to_currency   = v_functional_ccy
         AND effective_at::DATE <= v_business_date
       ORDER BY effective_at DESC LIMIT 1;
      IF v_fx_rate IS NULL THEN
        RAISE EXCEPTION
          'missing_fx_rate: no fx_rates row found for % → % effective_at <= %',
          p_c_acct.currency, v_functional_ccy, v_business_date
          USING ERRCODE = 'P0050';
      END IF;

      INSERT INTO posting_line_currencies (
        posting_line_id, amount_transaction, currency_transaction,
        fx_rate_to_functional
      ) VALUES (
        v_new_id, p_amount, p_c_acct.currency, v_fx_rate
      );
    END IF;
  END IF;

  -- B3 extension writes. Credit-first composition resolution per R2.
  v_dim_sku := COALESCE(p_c_acct.sku_id, p_d_acct.sku_id);
  IF v_dim_sku IS NOT NULL THEN
    INSERT INTO posting_line_dimensions
      (posting_line_id, dimension_type, dimension_value_uuid)
      VALUES (v_new_id, 3, v_dim_sku);
  END IF;

  v_dim_loc := COALESCE(p_c_acct.location_id, p_d_acct.location_id);
  IF v_dim_loc IS NOT NULL THEN
    INSERT INTO posting_line_dimensions
      (posting_line_id, dimension_type, dimension_value_uuid)
      VALUES (v_new_id, 4, v_dim_loc);
  END IF;

  v_dim_routing_op := COALESCE(
    (p_event->>'routing_op')::INT,
    p_c_acct.routing_op,
    p_d_acct.routing_op
  );
  IF v_dim_routing_op IS NOT NULL THEN
    INSERT INTO posting_line_dimensions
      (posting_line_id, dimension_type, dimension_value)
      VALUES (v_new_id, 5, v_dim_routing_op::BIGINT);
  END IF;

  v_event_cp := (p_event->>'counterparty_id')::UUID;
  v_dim_cp := COALESCE(v_event_cp, p_c_acct.counterparty_id, p_d_acct.counterparty_id);
  IF v_dim_cp IS NOT NULL THEN
    IF p_c_acct.kind IN ('ar','ar_unsettled','customer_pool')
       OR p_d_acct.kind IN ('ar','ar_unsettled','customer_pool') THEN
      v_dim_cp_type := 1;
    ELSIF p_c_acct.kind IN ('ap','ap_unsettled','vendor_pool','accrued_disposal_liability')
       OR p_d_acct.kind IN ('ap','ap_unsettled','vendor_pool','accrued_disposal_liability') THEN
      v_dim_cp_type := 2;
    ELSE
      v_dim_cp_type := NULL;
    END IF;
    IF v_dim_cp_type IS NOT NULL THEN
      INSERT INTO posting_line_dimensions
        (posting_line_id, dimension_type, dimension_value_uuid)
        VALUES (v_new_id, v_dim_cp_type, v_dim_cp);
    END IF;
  END IF;

  -- C extension write. One row per qty-bearing inventory posting_line.
  -- Schema requires product_id NOT NULL; emit only when a SKU resolves.
  -- Postings that carry qty as audit metadata but have no inventory
  -- semantics on either side (e.g., by-product disposal_cost period-
  -- basis: disposal_expense ↔ accrued_disposal_liability with
  -- planned_qty for traceability) are deliberately skipped — the qty
  -- is reporting metadata, not an inventory movement. Recon check #6
  -- mirrors this filter so non-inventory qty postings don't false-
  -- positive.
  IF v_qty_for_row IS NOT NULL
     AND COALESCE(p_c_acct.sku_id, p_d_acct.sku_id) IS NOT NULL THEN
    -- unit_cost only meaningful on value-leg postings (ledger_kind='value');
    -- on qty-leg postings amount IS the qty so amount/qty == 1, not a cost.
    IF p_d_acct.ledger_kind = 'value' AND v_qty_for_row <> 0 THEN
      v_inv_unit_cost := p_amount::NUMERIC / ABS(v_qty_for_row)::NUMERIC;
    ELSE
      v_inv_unit_cost := NULL;
    END IF;

    SELECT cost_method INTO v_inv_cost_method
      FROM skus
     WHERE id = COALESCE(p_c_acct.sku_id, p_d_acct.sku_id);

    INSERT INTO posting_line_inventory (
      posting_line_id, product_id, quantity, qty_uom,
      unit_cost, cost_method_at_event
    ) VALUES (
      v_new_id,
      COALESCE(p_c_acct.sku_id, p_d_acct.sku_id),
      ABS(v_qty_for_row)::NUMERIC,
      'EA',
      v_inv_unit_cost,
      v_inv_cost_method
    );

    -- D extension write — STANDARD COST ONLY (D2). One row per
    -- qualifying inv_value_* value-leg posting on a standard-cost
    -- SKU. WAC handled by D3; FIFO/lot by Phase E. See header
    -- comment for the full rationale + event_type mapping table.
    --
    -- Additional gate: location_id MUST resolve. inv_value_wip
    -- accounts are partitioned by (sku, routing_op, currency)
    -- with location_id NULL — postings with inv_value_wip on
    -- BOTH sides (op_move_v, wo_close_v) have no location to
    -- attribute the movement to at subledger grain. Per-routing-op
    -- WIP flow is captured at posting_lines grain (B3 dimension);
    -- the inventory_movements subledger aggregates at (product,
    -- location) so we deliberately skip those rows. Half-WIP
    -- postings (e.g., wo_complete: DR inv_value_fg, CR inv_value_wip)
    -- DO write — the FG side carries location.
    IF v_inv_cost_method = 'standard'
       AND p_d_acct.ledger_kind = 'value'
       AND v_qty_for_row <> 0
       AND (p_d_acct.kind::TEXT LIKE 'inv_value_%'
            OR p_c_acct.kind::TEXT LIKE 'inv_value_%')
       AND COALESCE(p_c_acct.location_id, p_d_acct.location_id) IS NOT NULL THEN

      v_im_quantity := CASE
        WHEN p_c_acct.kind::TEXT LIKE 'inv_value_%'
          OR p_c_acct.kind::TEXT LIKE 'stock_%'
        THEN -ABS(v_qty_for_row)::NUMERIC
        ELSE  ABS(v_qty_for_row)::NUMERIC
      END;

      v_im_event_type := _inventory_movement_event_type(v_reason, v_im_quantity);

      IF v_im_event_type IS NOT NULL THEN
        v_im_product_id  := COALESCE(p_c_acct.sku_id, p_d_acct.sku_id);
        v_im_location_id := COALESCE(p_c_acct.location_id, p_d_acct.location_id);

        -- Look up standard tolerantly. Normally a 'standard' SKU
        -- has a standard_costs row at business_date — but a SKU
        -- whose cost_method changed FROM another method TO standard
        -- without a standard ever being established is a legitimate
        -- transitional state (e.g., a return at the historical WAC
        -- snapshot when the current SKU is on standard). In that
        -- case we record the movement at actual_unit_cost only and
        -- leave standard_unit_cost NULL — the column allows it,
        -- recon (D5) skips standard comparison when NULL.
        SELECT cost::NUMERIC INTO v_im_std_unit_cost
          FROM standard_costs
         WHERE sku_id = v_im_product_id
           AND effective_at <= v_business_date
         ORDER BY effective_at DESC
         LIMIT 1;

        INSERT INTO inventory_movements (
          product_id, legal_entity_id, location_id,
          event_type, movement_date, quantity,
          standard_unit_cost, actual_unit_cost,
          cost_currency, posting_line_id
        ) VALUES (
          v_im_product_id,
          p_c_acct.legal_entity_id,
          v_im_location_id,
          v_im_event_type,
          v_business_date,
          v_im_quantity,
          v_im_std_unit_cost,
          v_inv_unit_cost,
          p_c_acct.currency,
          v_new_id
        );
      END IF;
    END IF;
  END IF;

  RETURN v_new_id;
END;
$$;
