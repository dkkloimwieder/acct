-- sxl2.4 (acct-sxl2.4): wrappers B — post_so_ship + post_wo_complete
-- extended for tracked_by='lot_and_serial'.
--
-- post_so_ship:
--   Each line of p_lines may carry 'unit_ids' BIGINT[]. REQUIRED on
--   lot_and_serial (P0037 if missing). Wrapper validates length =
--   qty_shipped, all units active and at ship_location_id, same lot.
--   Derives v_specific_lot_id from units (overrides 'lot_id' key).
--   Post-PERFORM: UPDATE units status='shipped' + customer_id; INSERT
--   inventory_unit_events type=2 with location_id_from + posting_line_id;
--   stamp so_shipment_lines.unit_ids audit column.
--   Rejects unit_ids on non-lot_and_serial (P0006).
--
-- post_wo_complete:
--   Signature gains p_output_serials JSONB DEFAULT NULL (DROP +
--   CREATE per pg-forbids-renaming-function-parameters memory).
--   p_output_serials is a dictionary keyed by output_no::TEXT, each
--   entry {"unit_serials": TEXT[], "external_serials": TEXT[]}.
--   For each output, if the entry exists, length must match q_share.
--   Forwarded into per-output value-leg event JSON; apply_event's
--   E2.5 block creates units against the new FG lot (or auto-gens
--   serials when entry absent).
--
-- VERBATIM-COPY DISCIPLINE: post_so_ship body from mig 0053 (acct-
-- L4); post_wo_complete body from mig 0059 (acct-3j3z). Surgical
-- additions in clearly-marked sxl2.4 blocks.

-- ---------- 1. Audit column on so_shipment_lines ----------

ALTER TABLE so_shipment_lines ADD COLUMN unit_ids BIGINT[];

CREATE INDEX so_shipment_lines_unit_ids
  ON so_shipment_lines USING GIN (unit_ids)
  WHERE unit_ids IS NOT NULL;

COMMENT ON COLUMN so_shipment_lines.unit_ids IS
  'Audit pointer: inventory_units shipped against this line for '
  'tracked_by=''lot_and_serial'' SKUs. NULL otherwise.';

-- ---------- 2. post_so_ship ----------

CREATE OR REPLACE FUNCTION post_so_ship(
  p_so_id           UUID,
  p_lines           JSONB,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id    UUID;
  v_doc_id         UUID;
  v_customer_id    UUID;
  v_n              INT;
  v_idx            INT;
  v_line           JSONB;
  v_so_line_id     UUID;
  v_qty_shipped    BIGINT;
  v_unit_price     BIGINT;
  v_tax_amount     BIGINT;
  v_sl             RECORD;
  v_already_ship   BIGINT;
  v_cost_method    cost_method;
  v_tracked_by     inventory_tracking;
  v_unit_cost      BIGINT;
  v_walk_total     BIGINT;
  v_qty_acct       BIGINT;
  v_val_acct       BIGINT;
  v_cust_qty       BIGINT;
  v_cust_unsettled BIGINT;
  v_revenue_acct   BIGINT;
  v_cogs_acct      BIGINT;
  v_tax_acct       BIGINT;
  v_qty_balance    BIGINT;
  v_value_balance  BIGINT;
  v_ship_line_id   UUID;
  v_batch          JSONB := '[]'::JSONB;
  v_specific_lot_id BIGINT;
  v_first_lot      BIGINT;
  v_walk           RECORD;
  v_value_event    JSONB;
  v_audit_lot_id   BIGINT;
  v_caller_lot_id  BIGINT;
  v_pin_count      INT;
  v_pin_lot_id     BIGINT;
  v_pin_match_ok   BOOLEAN;
  -- sxl2.4 scratch:
  v_unit_ids_json  JSONB;
  v_unit_ids       BIGINT[];
  v_unit_lot_min   BIGINT;
  v_unit_lot_max   BIGINT;
  v_unit_match     INT;
BEGIN
  SELECT id INTO v_existing_id FROM so_shipments
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT customer_id INTO v_customer_id FROM sales_orders WHERE id = p_so_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'so_ship_invalid: SO % not found', p_so_id
      USING ERRCODE = 'P0037';
  END IF;
  IF v_customer_id IS NULL THEN
    RAISE EXCEPTION 'so_ship_invalid: SO % has no customer_id', p_so_id
      USING ERRCODE = 'P0037';
  END IF;

  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'so_ship_invalid: empty lines for SO %', p_so_id
      USING ERRCODE = 'P0037';
  END IF;

  INSERT INTO so_shipments (so_id, business_date, posted_by, idempotency_key, notes)
  VALUES (p_so_id, p_business_date, p_posted_by, p_idempotency_key, p_notes)
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM so_shipments WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  FOR v_idx IN 1..v_n LOOP
    v_line        := p_lines -> (v_idx - 1);
    v_so_line_id  := (v_line->>'so_line_id')::UUID;
    v_qty_shipped := (v_line->>'qty_shipped')::BIGINT;

    IF v_qty_shipped IS NULL OR v_qty_shipped <= 0 THEN
      RAISE EXCEPTION 'so_ship_invalid: line % qty_shipped must be > 0',
                      v_idx USING ERRCODE = 'P0037';
    END IF;

    SELECT so_id, sku_id, ship_location_id, qty_ordered, unit_price,
           currency, tax_amount
      INTO v_sl
      FROM sales_order_lines WHERE id = v_so_line_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'so_ship_invalid: so_line % not found', v_so_line_id
        USING ERRCODE = 'P0037';
    END IF;
    IF v_sl.so_id <> p_so_id THEN
      RAISE EXCEPTION 'so_ship_invalid: so_line % belongs to SO % not %',
                      v_so_line_id, v_sl.so_id, p_so_id
        USING ERRCODE = 'P0037';
    END IF;

    SELECT COALESCE(SUM(qty_shipped), 0) INTO v_already_ship
      FROM so_shipment_lines WHERE so_line_id = v_so_line_id;
    IF v_already_ship + v_qty_shipped > v_sl.qty_ordered THEN
      RAISE EXCEPTION
        'so_line_overshipped: so_line %: ordered=%, already shipped=%, '
        'this shipment=%; cumulative would exceed qty_ordered',
        v_so_line_id, v_sl.qty_ordered, v_already_ship, v_qty_shipped
        USING ERRCODE = 'P0038';
    END IF;

    v_unit_price := COALESCE((v_line->>'unit_price')::BIGINT, v_sl.unit_price);
    v_tax_amount := COALESCE((v_line->>'tax_amount')::BIGINT, v_sl.tax_amount);

    SELECT cost_method, tracked_by INTO v_cost_method, v_tracked_by
      FROM skus WHERE id = v_sl.sku_id;

    IF v_cost_method = 'lot' THEN
      RAISE EXCEPTION
        'cost_method_not_implemented: lot for so_ship (sku=%); see acct-uze',
        v_sl.sku_id USING ERRCODE = 'P0006';
    END IF;

    -- sxl2.4: parse line.unit_ids; reject on non-lot_and_serial.
    v_unit_ids_json := v_line->'unit_ids';
    v_unit_ids      := NULL;

    IF v_unit_ids_json IS NOT NULL THEN
      IF v_tracked_by <> 'lot_and_serial' THEN
        RAISE EXCEPTION
          'so_ship_invalid: line % carries unit_ids but sku=% is '
          'tracked_by=% (only ''lot_and_serial'' accepts unit_ids)',
          v_idx, v_sl.sku_id, v_tracked_by USING ERRCODE = 'P0006';
      END IF;
      SELECT array_agg((x)::BIGINT ORDER BY ord)
        INTO v_unit_ids
        FROM jsonb_array_elements_text(v_unit_ids_json)
             WITH ORDINALITY AS t(x, ord);
    END IF;

    SELECT id INTO v_qty_acct FROM accounts
     WHERE kind='stock_available' AND sku_id=v_sl.sku_id
       AND location_id=v_sl.ship_location_id AND NOT is_closed;
    IF v_qty_acct IS NULL THEN
      RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                      v_sl.sku_id, v_sl.ship_location_id USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_val_acct FROM accounts
     WHERE kind='inv_value_fg' AND sku_id=v_sl.sku_id
       AND location_id=v_sl.ship_location_id
       AND currency=v_sl.currency AND NOT is_closed;
    IF v_val_acct IS NULL THEN
      RAISE EXCEPTION 'no open inv_value_fg account for sku=% loc=% ccy=%',
                      v_sl.sku_id, v_sl.ship_location_id, v_sl.currency
        USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_cust_qty FROM accounts
     WHERE kind='customer_pool' AND counterparty_id=v_customer_id
       AND ledger_kind='qty' AND NOT is_closed;
    IF v_cust_qty IS NULL THEN
      RAISE EXCEPTION 'no open customer_pool(qty) account for customer=%',
                      v_customer_id USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_cust_unsettled FROM accounts
     WHERE kind='ar_unsettled' AND counterparty_id=v_customer_id
       AND currency=v_sl.currency AND NOT is_closed;
    IF v_cust_unsettled IS NULL THEN
      RAISE EXCEPTION 'no open ar_unsettled account for customer=% ccy=%',
                      v_customer_id, v_sl.currency USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_revenue_acct FROM accounts
     WHERE kind='revenue' AND ledger_kind='value'
       AND currency=v_sl.currency AND NOT is_closed;
    IF v_revenue_acct IS NULL THEN
      RAISE EXCEPTION 'no open revenue account for ccy=%',
                      v_sl.currency USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_cogs_acct FROM accounts
     WHERE kind='cogs' AND ledger_kind='value'
       AND currency=v_sl.currency AND NOT is_closed;
    IF v_cogs_acct IS NULL THEN
      RAISE EXCEPTION 'no open cogs account for ccy=%',
                      v_sl.currency USING ERRCODE = 'P0010';
    END IF;

    v_specific_lot_id := NULL;
    v_first_lot       := NULL;
    v_audit_lot_id    := NULL;

    IF v_cost_method = 'standard' THEN
      v_unit_cost := _resolve_standard_cost_at(v_sl.sku_id, p_business_date);

    ELSIF v_cost_method = 'fifo' THEN
      PERFORM 1 FROM accounts WHERE id = v_val_acct FOR UPDATE;
      SELECT COALESCE(SUM(cost_amount), 0)::BIGINT
        INTO v_walk_total
        FROM _fifo_walk_layers(v_sl.sku_id, v_sl.ship_location_id,
                               1::SMALLINT, v_qty_shipped::NUMERIC);
      v_unit_cost := v_walk_total / v_qty_shipped;

    ELSIF v_cost_method = 'lot_fifo' THEN
      PERFORM 1 FROM accounts WHERE id = v_val_acct FOR UPDATE;

      -- sxl2.4: tracked_by='lot_and_serial' requires unit_ids; the
      -- units pin the lot directly. Otherwise fall through to legacy
      -- W2 pin-resolution.
      IF v_tracked_by = 'lot_and_serial' THEN
        IF v_unit_ids IS NULL THEN
          RAISE EXCEPTION
            'so_ship_invalid: line % sku=% is tracked_by=''lot_and_serial'' '
            'but no unit_ids supplied',
            v_idx, v_sl.sku_id USING ERRCODE = 'P0037';
        END IF;
        IF COALESCE(array_length(v_unit_ids, 1), 0) <> v_qty_shipped THEN
          RAISE EXCEPTION
            'so_ship_invalid: line % unit_ids length % does not match '
            'qty_shipped %',
            v_idx, COALESCE(array_length(v_unit_ids, 1), 0), v_qty_shipped
            USING ERRCODE = 'P0006';
        END IF;

        PERFORM 1 FROM inventory_units
         WHERE unit_id = ANY(v_unit_ids)
         ORDER BY unit_id
           FOR UPDATE;

        SELECT MIN(lot_id), MAX(lot_id), COUNT(*)
          INTO v_unit_lot_min, v_unit_lot_max, v_unit_match
          FROM inventory_units
         WHERE unit_id = ANY(v_unit_ids)
           AND product_id = v_sl.sku_id
           AND current_location_id = v_sl.ship_location_id
           AND status IN ('available', 'reserved', 'allocated');

        IF v_unit_match <> COALESCE(array_length(v_unit_ids, 1), 0) THEN
          RAISE EXCEPTION
            'so_ship_invalid: line % one or more unit_ids are not '
            'active / not at sku=% / not at loc=% (matched %/%)',
            v_idx, v_sl.sku_id, v_sl.ship_location_id, v_unit_match,
            COALESCE(array_length(v_unit_ids, 1), 0)
            USING ERRCODE = 'P0006';
        END IF;
        IF v_unit_lot_min <> v_unit_lot_max THEN
          RAISE EXCEPTION
            'so_ship_invalid: line % unit_ids span multiple lots '
            '(% to %); one shipment line must ship from a single lot',
            v_idx, v_unit_lot_min, v_unit_lot_max USING ERRCODE = 'P0006';
        END IF;
        v_specific_lot_id := v_unit_lot_min;
      ELSE
        v_caller_lot_id := (v_line->>'lot_id')::BIGINT;
        SELECT COUNT(*), MAX(lot_id)
          INTO v_pin_count, v_pin_lot_id
          FROM inventory_reservations
         WHERE so_line_id   = v_so_line_id
           AND status       IN ('active', 'allocated')
           AND lot_specific = TRUE;

        IF v_pin_count = 0 THEN
          v_specific_lot_id := v_caller_lot_id;
        ELSIF v_caller_lot_id IS NULL THEN
          IF v_pin_count = 1 THEN
            v_specific_lot_id := v_pin_lot_id;
          ELSE
            RAISE EXCEPTION
              'ambiguous_pinned_reservation: so_line=% has % pinned reservations; caller must specify lot_id',
              v_so_line_id, v_pin_count
              USING ERRCODE = 'P0055';
          END IF;
        ELSE
          SELECT EXISTS (
            SELECT 1 FROM inventory_reservations
             WHERE so_line_id   = v_so_line_id
               AND status       IN ('active', 'allocated')
               AND lot_specific = TRUE
               AND lot_id       = v_caller_lot_id
          ) INTO v_pin_match_ok;
          IF NOT v_pin_match_ok THEN
            RAISE EXCEPTION
              'ship_lot_pin_conflict: so_line=% caller_lot=% does not match any of % pinned reservations',
              v_so_line_id, v_caller_lot_id, v_pin_count
              USING ERRCODE = 'P0054';
          END IF;
          v_specific_lot_id := v_caller_lot_id;
        END IF;
      END IF;

      v_walk_total := 0;
      FOR v_walk IN
        SELECT * FROM _lot_walk_layers(
          v_sl.sku_id, v_sl.ship_location_id,
          1::SMALLINT, v_qty_shipped::NUMERIC, v_specific_lot_id
        )
      LOOP
        IF v_first_lot IS NULL THEN v_first_lot := v_walk.lot_id; END IF;
        v_walk_total := v_walk_total + v_walk.cost_amount;
      END LOOP;
      v_unit_cost := v_walk_total / v_qty_shipped;
      v_audit_lot_id := COALESCE(v_specific_lot_id, v_first_lot);

    ELSE
      PERFORM 1 FROM accounts WHERE id = v_val_acct FOR UPDATE;
      SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = v_val_acct THEN  t.qty
                               WHEN t.credit_account_id = v_val_acct THEN -t.qty END), 0)
        INTO v_qty_balance
        FROM posting_lines t
       WHERE v_val_acct IN (t.debit_account_id, t.credit_account_id)
         AND t.qty IS NOT NULL;

      IF v_qty_balance <= 0 THEN
        RAISE EXCEPTION
          'wac so_ship qty balance is %, cannot price (sku=%, loc=%, ccy=%)',
          v_qty_balance, v_sl.sku_id, v_sl.ship_location_id, v_sl.currency
          USING ERRCODE = 'P0006';
      END IF;
      SELECT debits_total - credits_total INTO v_value_balance
        FROM accounts WHERE id = v_val_acct;
      IF v_value_balance < 0 THEN v_value_balance := 0; END IF;
      v_unit_cost := v_value_balance / v_qty_balance;
    END IF;

    IF v_tax_amount > 0 THEN
      SELECT id INTO v_tax_acct FROM accounts
       WHERE kind='sales_tax_payable' AND ledger_kind='value'
         AND currency=v_sl.currency AND NOT is_closed;
      IF v_tax_acct IS NULL THEN
        RAISE EXCEPTION 'no open sales_tax_payable account for ccy=%',
                        v_sl.currency USING ERRCODE = 'P0010';
      END IF;
    END IF;

    INSERT INTO so_shipment_lines (
      shipment_id, so_line_id, qty_shipped, unit_cost, unit_price, tax_amount,
      cost_method_at_ship, lot_id
    ) VALUES (
      v_doc_id, v_so_line_id, v_qty_shipped, v_unit_cost, v_unit_price, v_tax_amount,
      v_cost_method, v_audit_lot_id
    ) RETURNING id INTO v_ship_line_id;

    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'so_ship',
      'document_kind',     'so_ship',
      'document_id',       v_doc_id,
      'document_line_id',  v_ship_line_id,
      'debit_account_id',  v_cust_qty,
      'credit_account_id', v_qty_acct,
      'amount',            v_qty_shipped,
      'qty',               v_qty_shipped,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   v_customer_id,
      'posted_by',         p_posted_by
    ));

    v_value_event := jsonb_build_object(
      'reason',            'so_ship',
      'document_kind',     'so_ship',
      'document_id',       v_doc_id,
      'document_line_id',  v_ship_line_id,
      'debit_account_id',  v_cogs_acct,
      'credit_account_id', v_val_acct,
      'amount',            v_qty_shipped * v_unit_cost,
      'qty',               v_qty_shipped,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   v_customer_id,
      'posted_by',         p_posted_by
    );
    IF v_cost_method = 'lot_fifo' THEN
      v_value_event := v_value_event || jsonb_build_object(
        'lot_id', v_specific_lot_id
      );
    END IF;
    v_batch := v_batch || jsonb_build_array(v_value_event);

    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'so_ship',
      'document_kind',     'so_ship',
      'document_id',       v_doc_id,
      'document_line_id',  v_ship_line_id,
      'debit_account_id',  v_cust_unsettled,
      'credit_account_id', v_revenue_acct,
      'amount',            v_qty_shipped * v_unit_price,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   v_customer_id,
      'posted_by',         p_posted_by
    ));

    IF v_tax_amount > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'so_ship',
        'document_kind',     'so_ship',
        'document_id',       v_doc_id,
        'document_line_id',  v_ship_line_id,
        'debit_account_id',  v_cust_unsettled,
        'credit_account_id', v_tax_acct,
        'amount',            v_tax_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   v_customer_id,
        'posted_by',         p_posted_by
      ));
    END IF;

    -- sxl2.4: stash unit_ids on the shipment-line audit column so the
    -- post-PERFORM block can iterate to mark units + emit events.
    IF v_unit_ids IS NOT NULL THEN
      UPDATE so_shipment_lines
         SET unit_ids = v_unit_ids
       WHERE id = v_ship_line_id;
    END IF;
  END LOOP;

  UPDATE inventory_reservations
     SET status = 'shipped',
         resolved_at = clock_timestamp()
   WHERE so_id = p_so_id
     AND status IN ('active', 'allocated');

  PERFORM post_posting_lines(v_batch, FALSE);

  -- sxl2.4: post-PERFORM unit lifecycle.
  UPDATE inventory_units iu
     SET status = 'shipped',
         customer_id = v_customer_id,
         updated_at = clock_timestamp()
    FROM so_shipment_lines ssl
   WHERE ssl.shipment_id = v_doc_id
     AND ssl.unit_ids IS NOT NULL
     AND iu.unit_id = ANY(ssl.unit_ids);

  INSERT INTO inventory_unit_events (
    unit_id, event_date, event_type,
    posting_line_id, new_status, location_id_from, customer_id
  )
  SELECT iu.unit_id, p_business_date, 2,
         pl.id, 'shipped', iu.current_location_id, v_customer_id
    FROM so_shipment_lines ssl
    JOIN posting_lines pl ON pl.document_line_id = ssl.id
                         AND pl.document_kind = 'so_ship'
                         AND pl.reason = 'so_ship'
                         AND pl.qty IS NOT NULL
                         AND EXISTS (
                           SELECT 1 FROM accounts a
                            WHERE a.id = pl.credit_account_id
                              AND a.kind = 'inv_value_fg'
                         )
    JOIN inventory_units iu ON iu.unit_id = ANY(ssl.unit_ids)
   WHERE ssl.shipment_id = v_doc_id
     AND ssl.unit_ids IS NOT NULL;

  RETURN v_doc_id;
END;
$$;

-- ---------- 3. post_wo_complete ----------
--
-- Signature change adds p_output_serials JSONB DEFAULT NULL — DROP +
-- CREATE per pg-forbids-renaming-function-parameters memory.

DROP FUNCTION IF EXISTS post_wo_complete(UUID, BIGINT, DATE, UUID, UUID, TEXT);

CREATE FUNCTION post_wo_complete(
  p_wo_id           UUID,
  p_qty             BIGINT,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL,
  p_output_serials  JSONB DEFAULT NULL
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
  v_output_value_idem UUID;
  v_output_recs       JSONB := '[]'::JSONB;
  v_qty_share         NUMERIC;
  -- sxl2.4 scratch:
  v_output_serial_payload JSONB;
  v_output_unit_serials   JSONB;
  v_output_ext_serials    JSONB;
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

  -- By-products pre-pass on closing call.
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

    IF v_cost_method = 'lot_fifo' THEN
      v_lot_code := v_output.lot_code;
      IF v_lot_code IS NULL OR length(v_lot_code) = 0 THEN
        v_lot_code := 'WO-' || substr(v_event_id::TEXT, 1, 8) || '-' || v_output.output_no;
      END IF;
    ELSE
      v_lot_code := NULL;
    END IF;

    -- sxl2.4: resolve per-output serial payload + validate lengths
    -- against q_share. Forward into value-leg event JSON.
    v_output_unit_serials := NULL;
    v_output_ext_serials  := NULL;
    IF p_output_serials IS NOT NULL THEN
      v_output_serial_payload := p_output_serials -> v_output.output_no::TEXT;
      IF v_output_serial_payload IS NOT NULL THEN
        v_output_unit_serials := v_output_serial_payload->'unit_serials';
        v_output_ext_serials  := v_output_serial_payload->'external_serials';

        IF v_output_unit_serials IS NOT NULL THEN
          IF jsonb_array_length(v_output_unit_serials) <> v_q_share THEN
            RAISE EXCEPTION
              'wo_invalid: output_no=% unit_serials length % does not '
              'match q_share %',
              v_output.output_no,
              jsonb_array_length(v_output_unit_serials), v_q_share
              USING ERRCODE = 'P0006';
          END IF;
        END IF;
        IF v_output_ext_serials IS NOT NULL THEN
          IF jsonb_array_length(v_output_ext_serials) <> v_q_share THEN
            RAISE EXCEPTION
              'wo_invalid: output_no=% external_serials length % does '
              'not match q_share %',
              v_output.output_no,
              jsonb_array_length(v_output_ext_serials), v_q_share
              USING ERRCODE = 'P0006';
          END IF;
        END IF;
      END IF;
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
      v_output_value_idem := gen_random_uuid();
      v_event_obj := jsonb_build_object(
        'reason',            'wo_complete_v',
        'document_kind',     'wo_complete',
        'document_id',       v_event_id,
        'debit_account_id',  v_val_fg,
        'credit_account_id', v_val_from,
        'amount',            v_v_share,
        'qty',               v_q_share,
        'business_date',     p_business_date,
        'idempotency_key',   v_output_value_idem,
        'posted_by',         p_posted_by
      );
      IF v_lot_code IS NOT NULL THEN
        v_event_obj := v_event_obj || jsonb_build_object('lot_code', v_lot_code);
        v_output_recs := v_output_recs || jsonb_build_array(jsonb_build_object(
          'output_sku_id',    v_output.output_sku_id,
          'fg_location_id',   v_output.fg_location_id,
          'allocation_pct',   v_output.allocation_pct,
          'value_idem_key',   v_output_value_idem
        ));

        -- sxl2.4: forward per-output serials. apply_event's E2.5
        -- reads them when output SKU is tracked_by='lot_and_serial';
        -- auto-gen otherwise.
        IF v_output_unit_serials IS NOT NULL THEN
          v_event_obj := v_event_obj || jsonb_build_object(
            'unit_serials', v_output_unit_serials
          );
        END IF;
        IF v_output_ext_serials IS NOT NULL THEN
          v_event_obj := v_event_obj || jsonb_build_object(
            'external_serials', v_output_ext_serials
          );
        END IF;
      END IF;
      v_batch := v_batch || jsonb_build_array(v_event_obj);
    END IF;
  END LOOP;

  PERFORM post_posting_lines(v_batch, FALSE);

  v_qty_share := p_qty::NUMERIC / v_wo.qty_target::NUMERIC;
  PERFORM _wo_write_lot_genealogy(p_wo_id, v_output_recs, v_qty_share);

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
