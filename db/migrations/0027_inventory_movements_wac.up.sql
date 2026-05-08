-- ============================================================
-- Phase D D3 — WAC dispatchers write inventory_movements
-- (acct-wb75.3.3).
--
-- Lifts the D-block's cost_method gate from `= 'standard'` to
--   IN ('standard', 'wac_perpetual', 'wac_periodic', 'wac_retroactive')
-- so movements are emitted for all four wired methods. FIFO and
-- lot remain blocked at the dispatcher level (P0006) so Phase E
-- handles them separately.
--
-- Why no other body changes:
--
--   - actual_unit_cost = posting amount / ABS(qty) is method-
--     agnostic. For WAC the dispatcher (_compute_amount_wac_*_
--     outbound) computes amount = qty × running_avg, so amount/qty
--     recovers the running avg automatically. Same shape for
--     receipts (caller supplies amount = po_unit_cost × qty).
--
--   - standard_unit_cost via the tolerant standard_costs lookup.
--     WAC SKUs typically don't have a standard_costs row →
--     NULL standard_unit_cost. Recon (D5) skips standard
--     comparison when NULL.
--
--   - posting_lines_provisional flagging for wac_periodic /
--     wac_retroactive depletions is unchanged (already in place
--     above the D-block). Close hooks (mig 0015) continue to
--     recompute period-end avg and post variance via
--     variance_wac_periodic / variance_wac_retroactive on
--     posting_lines. D6 (mig 0030) extends the close hooks to
--     ALSO write append-only correction movements (event_type=16
--     cost_adjustment) so the subledger stays in sync with the
--     post-close GL state.
--
--   - ppv_amount stays 0 for WAC. Period-end variance lives on
--     variance_wac_* posting_lines (and, after D6, on correction
--     movements with event_type=16).
--
-- The mig 0026 design call about location_id resolution still
-- applies: rm_issue_to_wo on a wac_* component crosses
-- inv_value_raw (location-bearing) → inv_value_wip (no location);
-- the COALESCE picks raw's location, so the gate passes. op_move_v
-- on a wac_* parent (both sides inv_value_wip, no location) still
-- skips — per-routing-op WIP flow stays at posting_lines grain.
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
  IF v_qty_for_row IS NOT NULL
     AND COALESCE(p_c_acct.sku_id, p_d_acct.sku_id) IS NOT NULL THEN
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

    -- D extension write — STANDARD + WAC family (D2 + D3). One row
    -- per qualifying inv_value_* value-leg posting on a SKU using
    -- one of the four wired cost methods. FIFO/lot still blocked
    -- at dispatcher level (P0006); their movements ship in Phase E.
    IF v_inv_cost_method IN ('standard', 'wac_perpetual',
                             'wac_periodic', 'wac_retroactive')
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

        -- Tolerant lookup. Standard SKUs normally have a row;
        -- WAC SKUs typically don't. NULL means recon skips
        -- standard comparison.
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
