-- sxl2.2 (acct-sxl2.2): _post_posting_lines_apply_event extension
-- for tracked_by='lot_and_serial' — create inventory_units rows +
-- emit inventory_unit_events type=1 (receipt) at receipt-side.
--
-- WHY: sxl2.1 (mig 0061) shipped the schema. This mig wires the
-- dispatcher hook so that when a posting touches inv_value_raw /
-- inv_value_fg on the DR side AND the receipt SKU is tracked_by=
-- 'lot_and_serial' AND cost_method='lot_fifo', per-unit rows are
-- created alongside the inventory_lots row that
-- _lot_create_from_event already produces.
--
-- DESIGN:
--   Event JSON optional keys:
--     'unit_serials':     TEXT[] — caller-supplied system serials.
--                         If omitted, auto-generated as
--                         <lot_code>-U<padded-seq>. Length must
--                         match ABS(qty) when supplied.
--     'external_serials': TEXT[] — supplier/manufacturer labels.
--                         Optional; per-element NULL allowed via
--                         array of NULLs from caller side.
--                         When supplied, length must match ABS(qty).
--
--   Issue-side unit consumption (UPDATE status, emit type=2 events)
--   is deferred to wrapper layer (sxl2.3-sxl2.5) where the caller
--   specifies which unit_ids to consume.
--
-- VERBATIM-COPY DISCIPLINE: full body copied from mig 0057 (acct-
-- fzzw) — latest CREATE OR REPLACE before this mig. Surgical
-- additions:
--   1. Extend SKU lookup to also resolve v_tracked_by.
--   2. New variables v_tracked_by, v_unit_serials, v_external_serials,
--      v_unit_count, v_lot_code.
--   3. New block "E2.5 — per-unit (lot_and_serial) writes" inside the
--      lot_fifo receipt branch, AFTER _lot_create_from_event and the
--      pli.lot_id + im.lot_id stamps.

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
  v_fifo_first_layer   BIGINT;
  v_lot_first          BIGINT;
  v_specific_lot_id    BIGINT;
  -- sxl2.2 additions:
  v_tracked_by         inventory_tracking;
  v_unit_serials       TEXT[];
  v_external_serials   TEXT[];
  v_unit_count         INT;
  v_lot_code           VARCHAR(64);
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

  IF v_qty_for_row IS NOT NULL
     AND COALESCE(p_c_acct.sku_id, p_d_acct.sku_id) IS NOT NULL THEN
    IF p_d_acct.ledger_kind = 'value' AND v_qty_for_row <> 0 THEN
      v_inv_unit_cost := p_amount::NUMERIC / ABS(v_qty_for_row)::NUMERIC;
    ELSE
      v_inv_unit_cost := NULL;
    END IF;

    -- sxl2.2: extend SKU lookup to resolve tracked_by alongside
    -- cost_method (single SELECT, same R2 credit-first source).
    SELECT cost_method, tracked_by
      INTO v_inv_cost_method, v_tracked_by
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

    IF v_inv_cost_method IN ('standard', 'wac_perpetual',
                             'wac_periodic', 'wac_retroactive',
                             'fifo', 'lot_fifo')
       AND p_d_acct.ledger_kind = 'value'
       AND v_qty_for_row <> 0 THEN

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

    -- E1 block — FIFO layer state.
    IF v_inv_cost_method = 'fifo' AND v_qty_for_row <> 0 THEN

      IF p_d_acct.kind IN ('inv_value_raw', 'inv_value_fg')
         AND p_d_acct.sku_id IS NOT NULL
         AND p_d_acct.location_id IS NOT NULL THEN
        INSERT INTO cost_layers (
          product_id, legal_entity_id, location_id,
          receipt_posting_line_id, receipt_date,
          original_quantity, unit_cost, cost_currency
        ) VALUES (
          p_d_acct.sku_id,
          p_d_acct.legal_entity_id,
          p_d_acct.location_id,
          v_new_id,
          v_business_date,
          ABS(v_qty_for_row)::NUMERIC,
          v_inv_unit_cost,
          p_d_acct.currency
        );
      END IF;

      IF p_c_acct.kind IN ('inv_value_raw', 'inv_value_fg')
         AND p_c_acct.sku_id IS NOT NULL
         AND p_c_acct.location_id IS NOT NULL THEN
        v_fifo_first_layer := _fifo_write_depletions(
          v_new_id,
          p_c_acct.sku_id,
          p_c_acct.location_id,
          1::SMALLINT,
          ABS(v_qty_for_row)::NUMERIC,
          v_business_date
        );
        UPDATE posting_line_inventory
           SET cost_layer_id = v_fifo_first_layer
         WHERE posting_line_id = v_new_id;
      END IF;
    END IF;

    -- E2 block — lot subledger writes.
    --
    -- acct-fzzw: gate on v_reason <> 'lot_transfer'. The
    -- post_lot_transfer wrapper owns lot subledger writes for
    -- transfers (multi-lot walk needs to copy per-source-lot
    -- metadata to per-dest-lot rows; the receipt-from-event JSON
    -- pattern below can't express that). Skipping the entire E2
    -- block also bypasses the bilateral-rejection that previously
    -- raised P0006 'lot_transfer_not_implemented' for transfer
    -- postings.
    --
    -- Receipt-side (DR inv_value_raw/_fg, lot_fifo SKU):
    --   create one inventory_lots row from event JSON metadata.
    -- Issue-side (CR inv_value_raw/_fg, lot_fifo SKU):
    --   walk lots, INSERT inventory_lot_events 'issue' rows.
    -- A bilateral posting for any reason OTHER than 'lot_transfer'
    -- (i.e., a same-SKU/same-cost-method posting that touches both
    -- inv_value_* sides outside of the transfer wrapper path) is
    -- still rejected — the wrapper is the only sanctioned bilateral
    -- path.
    IF v_inv_cost_method = 'lot_fifo'
       AND v_qty_for_row <> 0
       AND v_reason <> 'lot_transfer' THEN

      IF p_d_acct.kind IN ('inv_value_raw', 'inv_value_fg')
         AND p_c_acct.kind IN ('inv_value_raw', 'inv_value_fg')
         AND p_d_acct.sku_id IS NOT NULL
         AND p_c_acct.sku_id IS NOT NULL THEN
        RAISE EXCEPTION
          'lot_transfer_not_implemented: posting touches both inv_value_* '
          'sides for lot_fifo SKU at event index % outside post_lot_transfer',
          p_idx
          USING ERRCODE = 'P0006';
      END IF;

      v_specific_lot_id := (p_event->>'lot_id')::BIGINT;

      -- Receipt: inflow on DR inv_value_*.
      IF p_d_acct.kind IN ('inv_value_raw', 'inv_value_fg')
         AND p_d_acct.sku_id IS NOT NULL
         AND p_d_acct.location_id IS NOT NULL THEN
        v_lot_first := _lot_create_from_event(
          p_event, v_new_id, p_d_acct,
          ABS(v_qty_for_row)::NUMERIC, v_inv_unit_cost,
          v_business_date, p_idx
        );
        UPDATE posting_line_inventory
           SET lot_id = v_lot_first
         WHERE posting_line_id = v_new_id;
        UPDATE inventory_movements
           SET lot_id = v_lot_first
         WHERE posting_line_id = v_new_id
           AND product_id = p_d_acct.sku_id
           AND location_id = p_d_acct.location_id;

        -- ============================================================
        -- E2.5 block (sxl2.2) — per-unit (lot_and_serial) writes.
        -- ============================================================
        --
        -- For SKUs tracked_by='lot_and_serial', create one
        -- inventory_units row per qty unit alongside the lot,
        -- and emit a type=1 (receipt) inventory_unit_events row.
        --
        -- Event JSON optional keys:
        --   'unit_serials': TEXT[] caller-supplied system serials
        --                   (length must equal ABS(qty)); auto-
        --                   generated as <lot_code>-U<padded-seq>
        --                   when key absent.
        --   'external_serials': TEXT[] supplier/manufacturer labels
        --                   (length must equal ABS(qty); per-element
        --                   NULL allowed for unlabelled units).
        --
        -- Issue-side unit consumption (UPDATE status + emit type=2
        -- events) is deferred to wrapper layer (sxl2.3-sxl2.5);
        -- this hook owns receipt-side only.
        IF v_tracked_by = 'lot_and_serial' THEN
          v_unit_count := ABS(v_qty_for_row)::INT;

          IF p_event ? 'unit_serials' THEN
            SELECT array_agg(s) INTO v_unit_serials
              FROM jsonb_array_elements_text(p_event->'unit_serials') AS s;
            IF COALESCE(array_length(v_unit_serials, 1), 0) <> v_unit_count THEN
              RAISE EXCEPTION
                'lot_and_serial_serial_count_mismatch: event index % '
                'expected % serials (matching qty), got %',
                p_idx, v_unit_count,
                COALESCE(array_length(v_unit_serials, 1), 0)
                USING ERRCODE = 'P0006';
            END IF;
          ELSE
            -- Auto-generate <lot_code>-U<padded-seq>.
            SELECT lot_code INTO v_lot_code
              FROM inventory_lots
             WHERE lot_id = v_lot_first
               AND receipt_date = v_business_date;
            SELECT array_agg(v_lot_code || '-U' || lpad(seq::TEXT, 6, '0')
                             ORDER BY seq)
              INTO v_unit_serials
              FROM generate_series(1, v_unit_count) AS seq;
          END IF;

          IF p_event ? 'external_serials' THEN
            SELECT array_agg(s ORDER BY ord) INTO v_external_serials
              FROM jsonb_array_elements_text(p_event->'external_serials')
                   WITH ORDINALITY AS t(s, ord);
            IF COALESCE(array_length(v_external_serials, 1), 0) <> v_unit_count THEN
              RAISE EXCEPTION
                'lot_and_serial_external_count_mismatch: event index % '
                'expected % external serials (matching qty), got %',
                p_idx, v_unit_count,
                COALESCE(array_length(v_external_serials, 1), 0)
                USING ERRCODE = 'P0006';
            END IF;
          END IF;

          -- INSERT N units + their type=1 receipt events. We use
          -- a CTE so that the events INSERT picks up the just-
          -- generated unit_ids in the same statement.
          WITH new_units AS (
            INSERT INTO inventory_units (
              product_id, lot_id, lot_receipt_date,
              serial_no, external_serial_no,
              current_location_id, receipt_posting_line_id
            )
            SELECT
              p_d_acct.sku_id,
              v_lot_first,
              v_business_date,
              v_unit_serials[seq],
              v_external_serials[seq],   -- NULL when array NULL or NULL element
              p_d_acct.location_id,
              v_new_id
            FROM generate_series(1, v_unit_count) AS seq
            RETURNING unit_id
          )
          INSERT INTO inventory_unit_events (
            unit_id, event_date, event_type,
            posting_line_id, new_status, location_id_to
          )
          SELECT
            unit_id, v_business_date, 1,
            v_new_id, 'available', p_d_acct.location_id
          FROM new_units;
        END IF;
      END IF;

      -- Issue: outflow on CR inv_value_*.
      IF p_c_acct.kind IN ('inv_value_raw', 'inv_value_fg')
         AND p_c_acct.sku_id IS NOT NULL
         AND p_c_acct.location_id IS NOT NULL THEN
        v_lot_first := _lot_write_issues(
          v_new_id,
          p_c_acct.sku_id,
          p_c_acct.location_id,
          1::SMALLINT,
          ABS(v_qty_for_row)::NUMERIC,
          v_business_date,
          v_specific_lot_id
        );
        UPDATE posting_line_inventory
           SET lot_id = v_lot_first
         WHERE posting_line_id = v_new_id;
        UPDATE inventory_movements
           SET lot_id = v_lot_first
         WHERE posting_line_id = v_new_id
           AND product_id = p_c_acct.sku_id
           AND location_id = p_c_acct.location_id;
      END IF;
    END IF;
  END IF;

  RETURN v_new_id;
END;
$$;
