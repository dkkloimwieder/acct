-- ============================================================
-- Phase D D5 — subledger ↔ GL recon invariant + apply_event
-- D-block redesign (acct-wb75.3.5).
--
-- Implementing D5's recon revealed a design bug in the D2/D3
-- D-block's credit-first attribution. For "internal" postings
-- where BOTH sides are inv_value_* (wo_complete: DR inv_value_fg,
-- CR inv_value_wip; rm_issue_to_wo: DR inv_value_wip, CR inv_value_raw),
-- credit-first attribution put the movement at the wrong (sku,
-- location) bucket OR with the wrong sign relative to GL state.
--
-- Concrete example (kitchen_sink_lifecycle): a single-output
-- wo_complete posts DR inv_value_fg(output_sku, MAIN) +amount,
-- CR inv_value_wip(parent_sku, no_loc) -amount. The actual GL
-- state has bucket (output_sku, MAIN) up by amount. But credit-
-- first attribution wrote a movement at (parent_sku via COALESCE,
-- MAIN via COALESCE, -qty), which the recon would compute as
-- -amount at (output_sku=parent_sku, MAIN). Same direction
-- magnitude but FLIPPED SIGN relative to GL.
--
-- Redesign: write up to TWO movement rows per posting_line, one
-- per qualifying inv_value_* leg.
--
--   - DR side: if d.kind LIKE 'inv_value_%' AND d.sku_id IS NOT NULL
--     AND d.location_id IS NOT NULL → row with quantity = +ABS(qty).
--   - CR side: if c.kind LIKE 'inv_value_%' AND c.sku_id IS NOT NULL
--     AND c.location_id IS NOT NULL → row with quantity = -ABS(qty).
--
-- For most cases (po_receipt, so_ship, scrap, cycle_count_adj,
-- cost_adjustment) only ONE side is inv_value_* — exactly one row,
-- same as before. For internal flows (rm_issue_to_wo, wo_complete,
-- op_move_v) up to TWO rows depending on which sides have
-- location_id resolved. Common subcases:
--
--   wo_complete: DR inv_value_fg(MAIN) writes +qty; CR
--     inv_value_wip(no_loc) skips. One row at (output_sku, MAIN, +qty).
--
--   rm_issue_to_wo: DR inv_value_wip(no_loc) skips; CR
--     inv_value_raw(raw_loc) writes -qty. One row at
--     (component_sku, raw_loc, -qty).
--
--   op_move_v op10 → op20: both inv_value_wip without location;
--     both skip. Zero rows. Per-routing-op flow stays at
--     posting_lines / B3 dimensions grain.
--
-- The event_type uses the same helper but now passes side-aware
-- signed_qty: DR side gets +1, CR side gets -1. This routes
-- adjustment_in vs adjustment_out correctly for ambiguous reasons.
--
-- Recon (check #7): both subledger and GL views can now use the
-- natural per-account grain. GL is a UNION ALL of DR-side
-- contributions (+amount) and CR-side contributions (-amount),
-- each at the inv_value_* account's own (sku_id, location_id).
-- Subledger groups by (product_id, location_id) directly. They
-- match. Filter pl.qty IS NOT NULL excludes close-hook variance
-- posts (qty NULL by design — pure value adjustments without a
-- material flow); D6 will write append-only correction movements
-- with quantity=0 that are also out of scope for cost-flow recon.
--
-- The backfill helper is rewritten to use the same two-row pattern,
-- so backfilled rows are indistinguishable from dispatcher-written
-- rows.
-- ============================================================

-- ============================================================
-- _post_posting_lines_apply_event — D-block redesigned to two-row
-- per-leg attribution. Body identical to mig 0027 EXCEPT the
-- D-block.
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
  v_im_event_type      SMALLINT;
  v_im_std_unit_cost   NUMERIC(19, 4);
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

    -- D extension write — TWO-ROW per-leg attribution. Each
    -- inv_value_* leg of the posting that has both sku_id and
    -- location_id resolved gets its own movement row. DR side
    -- gets positive quantity (value flowing INTO the inv account);
    -- CR side gets negative (value flowing OUT). For most posts
    -- only one side qualifies → one row. Internal flows get up
    -- to two rows. inv_value_wip without location still skips
    -- (per-routing-op grain stays at posting_lines / B3 dims).
    --
    -- Standard SKUs use _resolve_standard_cost_at via tolerant
    -- subquery; WAC SKUs typically have NULL standard.
    IF v_inv_cost_method IN ('standard', 'wac_perpetual',
                             'wac_periodic', 'wac_retroactive')
       AND p_d_acct.ledger_kind = 'value'
       AND v_qty_for_row <> 0 THEN

      -- DR side
      IF p_d_acct.kind::TEXT LIKE 'inv_value_%'
         AND p_d_acct.sku_id IS NOT NULL
         AND p_d_acct.location_id IS NOT NULL THEN

        v_im_event_type := _inventory_movement_event_type(
          v_reason, ABS(v_qty_for_row)::NUMERIC);
        IF v_im_event_type IS NOT NULL THEN
          SELECT cost::NUMERIC INTO v_im_std_unit_cost
            FROM standard_costs
           WHERE sku_id = p_d_acct.sku_id
             AND effective_at <= v_business_date
           ORDER BY effective_at DESC LIMIT 1;

          INSERT INTO inventory_movements (
            product_id, legal_entity_id, location_id,
            event_type, movement_date, quantity,
            standard_unit_cost, actual_unit_cost,
            cost_currency, posting_line_id
          ) VALUES (
            p_d_acct.sku_id,
            p_d_acct.legal_entity_id,
            p_d_acct.location_id,
            v_im_event_type,
            v_business_date,
            ABS(v_qty_for_row)::NUMERIC,
            v_im_std_unit_cost,
            v_inv_unit_cost,
            p_d_acct.currency,
            v_new_id
          );
        END IF;
      END IF;

      -- CR side
      IF p_c_acct.kind::TEXT LIKE 'inv_value_%'
         AND p_c_acct.sku_id IS NOT NULL
         AND p_c_acct.location_id IS NOT NULL THEN

        v_im_event_type := _inventory_movement_event_type(
          v_reason, -ABS(v_qty_for_row)::NUMERIC);
        IF v_im_event_type IS NOT NULL THEN
          SELECT cost::NUMERIC INTO v_im_std_unit_cost
            FROM standard_costs
           WHERE sku_id = p_c_acct.sku_id
             AND effective_at <= v_business_date
           ORDER BY effective_at DESC LIMIT 1;

          INSERT INTO inventory_movements (
            product_id, legal_entity_id, location_id,
            event_type, movement_date, quantity,
            standard_unit_cost, actual_unit_cost,
            cost_currency, posting_line_id
          ) VALUES (
            p_c_acct.sku_id,
            p_c_acct.legal_entity_id,
            p_c_acct.location_id,
            v_im_event_type,
            v_business_date,
            -ABS(v_qty_for_row)::NUMERIC,
            v_im_std_unit_cost,
            v_inv_unit_cost,
            p_c_acct.currency,
            v_new_id
          );
        END IF;
      END IF;
    END IF;
  END IF;

  RETURN v_new_id;
END;
$$;

-- ============================================================
-- _backfill_inventory_movements — rewritten to two-row pattern.
--
-- UNION ALL of two SELECTs: one for DR-side contributions
-- (positive quantity) and one for CR-side contributions
-- (negative quantity). LEFT JOIN inventory_movements with both
-- (posting_line_id, side-distinguishing key) — but since we don't
-- have a side column on the movements table, we dedup by checking
-- both per-leg attributions: a row already exists at the leg's
-- (product, location) for this posting_line_id.
-- ============================================================

CREATE OR REPLACE FUNCTION _backfill_inventory_movements()
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_count BIGINT;
BEGIN
  WITH ins AS (
    INSERT INTO inventory_movements (
      product_id, legal_entity_id, cost_book_id, location_id,
      event_type, movement_date, quantity,
      standard_unit_cost, actual_unit_cost,
      cost_currency, posting_line_id, created_at
    )
    -- DR side rows
    SELECT
      d.sku_id,
      d.legal_entity_id,
      1,
      d.location_id,
      _inventory_movement_event_type(pl.reason, ABS(pl.qty)::NUMERIC),
      pl.business_date,
      ABS(pl.qty)::NUMERIC,
      (SELECT cost::NUMERIC
         FROM standard_costs sc
        WHERE sc.sku_id = d.sku_id AND sc.effective_at <= pl.business_date
        ORDER BY sc.effective_at DESC LIMIT 1),
      pli.unit_cost,
      d.currency,
      pl.id,
      pl.posted_at
      FROM posting_lines pl
      INNER JOIN posting_line_inventory pli ON pli.posting_line_id = pl.id
      INNER JOIN accounts d ON d.id = pl.debit_account_id
     WHERE pli.cost_method_at_event IN
             ('standard', 'wac_perpetual', 'wac_periodic', 'wac_retroactive')
       AND d.ledger_kind = 'value'
       AND pl.qty IS NOT NULL
       AND pl.qty <> 0
       AND d.kind::TEXT LIKE 'inv_value_%'
       AND d.sku_id IS NOT NULL
       AND d.location_id IS NOT NULL
       AND _inventory_movement_event_type(pl.reason, ABS(pl.qty)::NUMERIC) IS NOT NULL
       AND NOT EXISTS (
         SELECT 1 FROM inventory_movements im
          WHERE im.posting_line_id = pl.id
            AND im.product_id     = d.sku_id
            AND im.location_id    = d.location_id
            AND im.quantity       > 0
       )

    UNION ALL

    -- CR side rows
    SELECT
      c.sku_id,
      c.legal_entity_id,
      1,
      c.location_id,
      _inventory_movement_event_type(pl.reason, -ABS(pl.qty)::NUMERIC),
      pl.business_date,
      -ABS(pl.qty)::NUMERIC,
      (SELECT cost::NUMERIC
         FROM standard_costs sc
        WHERE sc.sku_id = c.sku_id AND sc.effective_at <= pl.business_date
        ORDER BY sc.effective_at DESC LIMIT 1),
      pli.unit_cost,
      c.currency,
      pl.id,
      pl.posted_at
      FROM posting_lines pl
      INNER JOIN posting_line_inventory pli ON pli.posting_line_id = pl.id
      INNER JOIN accounts c ON c.id = pl.credit_account_id
     WHERE pli.cost_method_at_event IN
             ('standard', 'wac_perpetual', 'wac_periodic', 'wac_retroactive')
       AND c.ledger_kind = 'value'
       AND pl.qty IS NOT NULL
       AND pl.qty <> 0
       AND c.kind::TEXT LIKE 'inv_value_%'
       AND c.sku_id IS NOT NULL
       AND c.location_id IS NOT NULL
       AND _inventory_movement_event_type(pl.reason, -ABS(pl.qty)::NUMERIC) IS NOT NULL
       AND NOT EXISTS (
         SELECT 1 FROM inventory_movements im
          WHERE im.posting_line_id = pl.id
            AND im.product_id     = c.sku_id
            AND im.location_id    = c.location_id
            AND im.quantity       < 0
       )
    RETURNING 1
  )
  SELECT COUNT(*)::BIGINT INTO v_count FROM ins;
  RETURN v_count;
END;
$$;

-- ============================================================
-- run_daily_reconciliation — adds check #7 subledger ↔ GL
-- divergence. After the D-block redesign above, the GL view can
-- naturally use per-account (sku, location) grain via UNION of
-- DR and CR contributions. Same shape on the subledger side.
-- ============================================================

CREATE OR REPLACE FUNCTION run_daily_reconciliation() RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
  v_total INT := 0;
  v_step  INT;
BEGIN
  -- Check 1: per-ledger double-entry.
  WITH imbalances AS (
    SELECT ledger_kind, currency,
           SUM(debits_total)::BIGINT  AS dr,
           SUM(credits_total)::BIGINT AS cr
      FROM accounts
     GROUP BY ledger_kind, currency
    HAVING SUM(debits_total) <> SUM(credits_total)
  )
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'double_entry_imbalance',
         jsonb_build_object(
           'ledger_kind', ledger_kind,
           'currency',    currency,
           'debits',      dr,
           'credits',     cr,
           'imbalance',   dr - cr
         )
    FROM imbalances;
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 2: reservation over-promise.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'reservation_over_promise',
         jsonb_build_object(
           'sku_id',      a.sku_id,
           'location_id', a.location_id,
           'on_hand',     (a.debits_total - a.credits_total),
           'reserved',    COALESCE(r.total, 0),
           'deficit',     (a.debits_total - a.credits_total) - COALESCE(r.total, 0)
         )
    FROM accounts a
    LEFT JOIN (
      SELECT sku_id, location_id, SUM(qty)::BIGINT AS total
        FROM inventory_reservations
       WHERE status = 'active'
       GROUP BY sku_id, location_id
    ) r ON r.sku_id = a.sku_id AND r.location_id = a.location_id
   WHERE a.kind = 'stock_available'
     AND NOT a.is_closed
     AND (a.debits_total - a.credits_total) < COALESCE(r.total, 0);
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 3: B2 currency extension amount consistency.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'currency_extension_amount_mismatch',
         jsonb_build_object(
           'posting_line_id',      plc.posting_line_id,
           'amount_transaction',   plc.amount_transaction,
           'amount',               pl.amount,
           'currency_transaction', plc.currency_transaction,
           'fx_rate_to_functional',plc.fx_rate_to_functional::TEXT
         )
    FROM posting_line_currencies plc
    JOIN posting_lines pl ON pl.id = plc.posting_line_id
   WHERE plc.amount_transaction <> pl.amount;
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 4: B3 dimensions — product coverage.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'dimension_product_missing',
         jsonb_build_object(
           'posting_line_id',  pl.id,
           'expected_sku_id',  COALESCE(c.sku_id, d.sku_id)
         )
    FROM posting_lines pl
    JOIN accounts c ON c.id = pl.credit_account_id
    JOIN accounts d ON d.id = pl.debit_account_id
    LEFT JOIN posting_line_dimensions pld
      ON pld.posting_line_id = pl.id AND pld.dimension_type = 3
   WHERE COALESCE(c.sku_id, d.sku_id) IS NOT NULL
     AND pld.posting_line_id IS NULL;
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 5: B3 dimensions — counterparty coverage.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'dimension_counterparty_missing',
         jsonb_build_object(
           'posting_line_id', pl.id,
           'expected_kind',
              CASE
                WHEN c.kind IN ('ar','ar_unsettled','customer_pool')
                  OR d.kind IN ('ar','ar_unsettled','customer_pool')
                  THEN 'customer'
                ELSE 'vendor'
              END
         )
    FROM posting_lines pl
    JOIN accounts c ON c.id = pl.credit_account_id
    JOIN accounts d ON d.id = pl.debit_account_id
    LEFT JOIN posting_line_dimensions pld
      ON pld.posting_line_id = pl.id AND pld.dimension_type IN (1, 2)
   WHERE COALESCE(pl.counterparty_id, c.counterparty_id, d.counterparty_id) IS NOT NULL
     AND (c.kind IN ('ar','ar_unsettled','customer_pool',
                     'ap','ap_unsettled','vendor_pool','accrued_disposal_liability')
       OR d.kind IN ('ar','ar_unsettled','customer_pool',
                     'ap','ap_unsettled','vendor_pool','accrued_disposal_liability'))
     AND pld.posting_line_id IS NULL;
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 6: C inventory extension count match.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'inventory_extension_missing',
         jsonb_build_object(
           'posting_line_id', pl.id,
           'reason',          pl.reason::TEXT,
           'qty',             pl.qty
         )
    FROM posting_lines pl
    JOIN accounts c ON c.id = pl.credit_account_id
    JOIN accounts d ON d.id = pl.debit_account_id
    LEFT JOIN posting_line_inventory pli ON pli.posting_line_id = pl.id
   WHERE pl.qty IS NOT NULL
     AND COALESCE(c.sku_id, d.sku_id) IS NOT NULL
     AND pli.posting_line_id IS NULL;
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 7: D subledger ↔ GL divergence per (product, location,
  -- period). Per-bucket tolerance 1 cent. Only qty-bearing
  -- inventory posts are in scope; close-hook variance posts
  -- (qty IS NULL) are pure value adjustments tracked separately
  -- on posting_lines and out of scope for cost-flow recon.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  WITH gl_inv AS (
    -- DR side contributions (+amount).
    SELECT
      d.sku_id        AS product_id,
      d.location_id   AS location_id,
      pl.period_id    AS period_id,
      pl.amount::NUMERIC AS gl_net
      FROM posting_lines pl
      JOIN accounts d ON d.id = pl.debit_account_id
     WHERE d.kind::TEXT LIKE 'inv_value_%'
       AND d.sku_id IS NOT NULL
       AND d.location_id IS NOT NULL
       AND pl.qty IS NOT NULL
    UNION ALL
    -- CR side contributions (-amount).
    SELECT
      c.sku_id,
      c.location_id,
      pl.period_id,
      -pl.amount::NUMERIC
      FROM posting_lines pl
      JOIN accounts c ON c.id = pl.credit_account_id
     WHERE c.kind::TEXT LIKE 'inv_value_%'
       AND c.sku_id IS NOT NULL
       AND c.location_id IS NOT NULL
       AND pl.qty IS NOT NULL
  ),
  gl AS (
    SELECT product_id, location_id, period_id,
           SUM(gl_net) AS gl_net
      FROM gl_inv
     GROUP BY 1, 2, 3
  ),
  sub AS (
    SELECT
      im.product_id,
      im.location_id,
      p.id AS period_id,
      SUM(im.quantity * im.actual_unit_cost)::NUMERIC AS sub_net
      FROM inventory_movements im
      JOIN periods p
        ON p.opens_at  <= im.movement_date
       AND p.closes_at >= im.movement_date
     GROUP BY 1, 2, 3
  )
  SELECT 'subledger_gl_divergence',
         jsonb_build_object(
           'product_id',  COALESCE(g.product_id, s.product_id),
           'location_id', COALESCE(g.location_id, s.location_id),
           'period_id',   COALESCE(g.period_id, s.period_id),
           'gl_net',      COALESCE(g.gl_net, 0)::TEXT,
           'sub_net',     COALESCE(s.sub_net, 0)::TEXT,
           'diff',        (COALESCE(g.gl_net, 0) - COALESCE(s.sub_net, 0))::TEXT
         )
    FROM gl g
    FULL OUTER JOIN sub s
      ON g.product_id  = s.product_id
     AND g.location_id = s.location_id
     AND g.period_id   = s.period_id
   WHERE ABS(COALESCE(g.gl_net, 0) - COALESCE(s.sub_net, 0)) > 1;
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  RETURN v_total;
END;
$$;
