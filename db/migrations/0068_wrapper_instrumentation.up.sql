-- acct-h73o: profile 1s6r wrappers to decompose p99 tail.
--
-- Adds an UNLOGGED instrumentation table + extends four hot wrappers
-- (post_so_ship, post_customer_invoice, post_wo_start, post_op_move)
-- with per-section timing captures. Each successful (non-idempotent-
-- replay) invocation writes 3 rows: setup, post_posting_lines, followup.
--
-- Aggregate query at the end of a load_phase1_mixed_workload run reports
-- per-wrapper per-section p50/p95/p99/max in µs. Verdict feeds acct-c4p
-- pseudo-sync pivot decision.
--
-- Wrappers are CREATE OR REPLACE'd verbatim from their latest sources
-- (post_so_ship: mig 0064; post_customer_invoice: mig 0018;
-- post_wo_start + post_op_move: mig 0058). Only the surgical timing
-- declarations and INSERT block are new. Verbatim-copy discipline per
-- state-2026-05-08-acct-vohc lesson.

-- ============================================================
-- 1. _wrapper_section_timings — UNLOGGED instrumentation table
-- ============================================================
--
-- UNLOGGED to skip WAL (~600k INSERTs over a 10-min run would otherwise
-- inflate WAL ~10%); we don't care about crash durability for an
-- ephemeral profiling table. BIGSERIAL is fine: PG sequences use cached
-- nextval, no row-level lock contention on the counter.

CREATE UNLOGGED TABLE _wrapper_section_timings (
  id           BIGSERIAL PRIMARY KEY,
  wrapper_name TEXT      NOT NULL,
  section      TEXT      NOT NULL
    CHECK (section IN ('setup', 'post_posting_lines', 'followup')),
  elapsed_us   BIGINT    NOT NULL,
  captured_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX _wrapper_section_timings_wrapper_section
  ON _wrapper_section_timings (wrapper_name, section);

-- ============================================================
-- 2. post_so_ship — verbatim from mig 0064 + section timings
-- ============================================================

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
  v_unit_ids_json  JSONB;
  v_unit_ids       BIGINT[];
  v_unit_lot_min   BIGINT;
  v_unit_lot_max   BIGINT;
  v_unit_match     INT;
  -- acct-h73o instrumentation:
  v_t0             TIMESTAMPTZ;
  v_t1             TIMESTAMPTZ;
  v_t2             TIMESTAMPTZ;
  v_t3             TIMESTAMPTZ;
BEGIN
  v_t0 := clock_timestamp();

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

  v_t1 := clock_timestamp();
  PERFORM post_posting_lines(v_batch, FALSE);
  v_t2 := clock_timestamp();

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

  v_t3 := clock_timestamp();

  INSERT INTO _wrapper_section_timings (wrapper_name, section, elapsed_us) VALUES
    ('post_so_ship', 'setup',              (EXTRACT(EPOCH FROM v_t1 - v_t0) * 1e6)::BIGINT),
    ('post_so_ship', 'post_posting_lines', (EXTRACT(EPOCH FROM v_t2 - v_t1) * 1e6)::BIGINT),
    ('post_so_ship', 'followup',           (EXTRACT(EPOCH FROM v_t3 - v_t2) * 1e6)::BIGINT);

  RETURN v_doc_id;
END;
$$;

-- ============================================================
-- 3. post_customer_invoice — verbatim from mig 0018 + timings
-- ============================================================

CREATE OR REPLACE FUNCTION post_customer_invoice(
  p_customer_id     UUID,
  p_currency        CHAR(3),
  p_lines           JSONB,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id     UUID;
  v_doc_id          UUID;
  v_customer_check  UUID;
  v_tolerance_pct   NUMERIC(5,2);
  v_n               INT;
  v_idx             INT;
  v_line            JSONB;
  v_kind            TEXT;
  v_so_line_id      UUID;
  v_qty             BIGINT;
  v_unit_price      BIGINT;
  v_amount          BIGINT;
  v_tax_amount      BIGINT;
  v_revenue_acct_id BIGINT;
  v_sl              RECORD;
  v_total_shipped   BIGINT;
  v_total_invoiced  BIGINT;
  v_returns_to_us   BIGINT;
  v_avail           BIGINT;
  v_cust_unsettled  BIGINT;
  v_cust_ar         BIGINT;
  v_cust_tax        BIGINT;
  v_match_tol_acct  BIGINT;
  v_rev_acct        accounts%ROWTYPE;
  v_inv_line_id     UUID;
  v_diff_total      BIGINT;
  v_diff_pct        NUMERIC(10,4);
  v_amount_at_so    BIGINT;
  v_batch           JSONB := '[]'::JSONB;
  -- acct-h73o instrumentation:
  v_t0              TIMESTAMPTZ;
  v_t1              TIMESTAMPTZ;
  v_t2              TIMESTAMPTZ;
  v_t3              TIMESTAMPTZ;
BEGIN
  v_t0 := clock_timestamp();

  SELECT id INTO v_existing_id FROM customer_invoices
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT id, unit_price_tolerance_pct INTO v_customer_check, v_tolerance_pct
    FROM customers WHERE id = p_customer_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'customer_invoice_invalid_line: customer % not found',
                    p_customer_id USING ERRCODE = 'P0041';
  END IF;

  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'customer_invoice_invalid_line: empty invoice for customer %',
                    p_customer_id USING ERRCODE = 'P0041';
  END IF;

  SELECT id INTO v_cust_ar FROM accounts
   WHERE kind='ar' AND counterparty_id=p_customer_id
     AND currency=p_currency AND NOT is_closed;
  IF v_cust_ar IS NULL THEN
    RAISE EXCEPTION 'no open ar account for customer=% ccy=%',
                    p_customer_id, p_currency USING ERRCODE = 'P0010';
  END IF;

  INSERT INTO customer_invoices (
    customer_id, currency, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_customer_id, p_currency, p_business_date, p_posted_by,
    p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM customer_invoices
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  FOR v_idx IN 1..v_n LOOP
    v_line       := p_lines -> (v_idx - 1);
    v_kind       := v_line->>'kind';
    v_amount     := (v_line->>'amount')::BIGINT;
    v_tax_amount := COALESCE((v_line->>'tax_amount')::BIGINT, 0);

    IF v_kind = 'so_match' THEN
      v_so_line_id := (v_line->>'so_line_id')::UUID;
      v_qty        := (v_line->>'qty')::BIGINT;
      v_unit_price := (v_line->>'unit_price')::BIGINT;

      SELECT sl.so_id, sl.unit_price, sl.currency, so.customer_id
        INTO v_sl
        FROM sales_order_lines sl
        JOIN sales_orders so ON so.id = sl.so_id
       WHERE sl.id = v_so_line_id
         FOR UPDATE OF sl;
      IF NOT FOUND THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % so_line % not found',
          v_idx, v_so_line_id USING ERRCODE = 'P0041';
      END IF;
      IF v_sl.customer_id IS DISTINCT FROM p_customer_id THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % so_line % belongs to '
          'customer % but invoice is for customer %',
          v_idx, v_so_line_id, v_sl.customer_id, p_customer_id
          USING ERRCODE = 'P0041';
      END IF;
      IF v_sl.currency <> p_currency THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % so_line currency=% but invoice currency=%',
          v_idx, v_sl.currency, p_currency USING ERRCODE = 'P0041';
      END IF;

      IF v_unit_price <> v_sl.unit_price THEN
        IF v_tolerance_pct = 0 THEN
          RAISE EXCEPTION
            'customer_invoice_three_way_mismatch: line % unit_price % does '
            'not match so_line.unit_price %',
            v_idx, v_unit_price, v_sl.unit_price USING ERRCODE = 'P0040';
        END IF;
        IF v_sl.unit_price = 0 THEN
          RAISE EXCEPTION
            'customer_invoice_three_way_mismatch: line % so_line.unit_price '
            'is 0 but invoice unit_price is % (zero-baseline; out of '
            'tolerance, customer tolerance %%%)',
            v_idx, v_unit_price, v_tolerance_pct
            USING ERRCODE = 'P0040';
        END IF;
        v_diff_pct := ABS(v_unit_price - v_sl.unit_price) * 100.0 / v_sl.unit_price;
        IF v_diff_pct > v_tolerance_pct THEN
          RAISE EXCEPTION
            'customer_invoice_three_way_mismatch: line % unit_price % differs from '
            'so_line.unit_price % by %%% (customer tolerance %%%)',
            v_idx, v_unit_price, v_sl.unit_price, v_diff_pct, v_tolerance_pct
            USING ERRCODE = 'P0040';
        END IF;
      END IF;
      IF v_amount <> v_qty * v_unit_price THEN
        RAISE EXCEPTION
          'customer_invoice_three_way_mismatch: line % amount % <> qty % × unit_price %',
          v_idx, v_amount, v_qty, v_unit_price USING ERRCODE = 'P0040';
      END IF;

      SELECT COALESCE(SUM(qty_shipped), 0) INTO v_total_shipped
        FROM so_shipment_lines WHERE so_line_id = v_so_line_id;
      SELECT COALESCE(SUM(qty), 0) INTO v_total_invoiced
        FROM customer_invoice_lines
       WHERE so_line_id = v_so_line_id AND kind = 'so_match';
      SELECT COALESCE(SUM(crl.qty_to_ar_unsettled), 0) INTO v_returns_to_us
        FROM customer_return_lines crl
        JOIN so_shipment_lines     ssl ON ssl.id = crl.ship_line_id
       WHERE ssl.so_line_id = v_so_line_id;
      v_avail := v_total_shipped - v_total_invoiced - v_returns_to_us;
      IF v_qty > v_avail THEN
        RAISE EXCEPTION
          'customer_invoice_three_way_mismatch: line % qty % exceeds '
          'shipped-not-invoiced-not-returned remainder % for so_line % '
          '(shipped=%, already invoiced=%, prior returns to ar_unsettled=%)',
          v_idx, v_qty, v_avail, v_so_line_id, v_total_shipped,
          v_total_invoiced, v_returns_to_us
          USING ERRCODE = 'P0040';
      END IF;

      SELECT id INTO v_cust_unsettled FROM accounts
       WHERE kind='ar_unsettled' AND counterparty_id=p_customer_id
         AND currency=p_currency AND NOT is_closed;
      IF v_cust_unsettled IS NULL THEN
        RAISE EXCEPTION 'no open ar_unsettled account for customer=% ccy=%',
                        p_customer_id, p_currency USING ERRCODE = 'P0010';
      END IF;

      INSERT INTO customer_invoice_lines (
        invoice_id, line_no, kind, so_line_id, qty, unit_price, amount, tax_amount
      ) VALUES (
        v_doc_id, v_idx, 'so_match', v_so_line_id, v_qty, v_unit_price,
        v_amount, v_tax_amount
      ) RETURNING id INTO v_inv_line_id;

      v_amount_at_so := v_qty * v_sl.unit_price;
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ar_invoice',
        'document_kind',     'customer_invoice',
        'document_id',       v_doc_id,
        'document_line_id',  v_inv_line_id,
        'debit_account_id',  v_cust_ar,
        'credit_account_id', v_cust_unsettled,
        'amount',            v_amount_at_so,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_customer_id,
        'posted_by',         p_posted_by
      ));

      v_diff_total := v_amount - v_amount_at_so;
      IF v_diff_total <> 0 THEN
        SELECT id INTO v_match_tol_acct FROM accounts
         WHERE kind='variance_match_tolerance' AND ledger_kind='value'
           AND currency=p_currency AND NOT is_closed;
        IF v_match_tol_acct IS NULL THEN
          RAISE EXCEPTION 'no open variance_match_tolerance account for ccy=%',
                          p_currency USING ERRCODE = 'P0010';
        END IF;

        IF v_diff_total > 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'ar_invoice',
            'document_kind',     'customer_invoice',
            'document_id',       v_doc_id,
            'document_line_id',  v_inv_line_id,
            'debit_account_id',  v_cust_ar,
            'credit_account_id', v_match_tol_acct,
            'amount',            v_diff_total,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_customer_id,
            'posted_by',         p_posted_by
          ));
        ELSE
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'ar_invoice',
            'document_kind',     'customer_invoice',
            'document_id',       v_doc_id,
            'document_line_id',  v_inv_line_id,
            'debit_account_id',  v_match_tol_acct,
            'credit_account_id', v_cust_ar,
            'amount',            -v_diff_total,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_customer_id,
            'posted_by',         p_posted_by
          ));
        END IF;
      END IF;

    ELSIF v_kind = 'service' THEN
      v_revenue_acct_id := (v_line->>'revenue_account_id')::BIGINT;

      SELECT * INTO v_rev_acct FROM accounts WHERE id = v_revenue_acct_id;
      IF NOT FOUND THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % revenue_account_id % not found',
          v_idx, v_revenue_acct_id USING ERRCODE = 'P0041';
      END IF;
      IF v_rev_acct.is_closed THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % revenue account % is closed',
          v_idx, v_revenue_acct_id USING ERRCODE = 'P0041';
      END IF;
      IF v_rev_acct.ledger_kind <> 'value' THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % revenue account % is %, expected value',
          v_idx, v_revenue_acct_id, v_rev_acct.ledger_kind
          USING ERRCODE = 'P0041';
      END IF;
      IF v_rev_acct.currency <> p_currency THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % revenue account ccy=% but invoice ccy=%',
          v_idx, v_rev_acct.currency, p_currency USING ERRCODE = 'P0041';
      END IF;

      INSERT INTO customer_invoice_lines (
        invoice_id, line_no, kind, revenue_account_id, amount, tax_amount
      ) VALUES (
        v_doc_id, v_idx, 'service', v_revenue_acct_id, v_amount, v_tax_amount
      ) RETURNING id INTO v_inv_line_id;

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ar_invoice',
        'document_kind',     'customer_invoice',
        'document_id',       v_doc_id,
        'document_line_id',  v_inv_line_id,
        'debit_account_id',  v_cust_ar,
        'credit_account_id', v_revenue_acct_id,
        'amount',            v_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_customer_id,
        'posted_by',         p_posted_by
      ));

      IF v_tax_amount > 0 THEN
        SELECT id INTO v_cust_tax FROM accounts
         WHERE kind='sales_tax_payable' AND ledger_kind='value'
           AND currency=p_currency AND NOT is_closed;
        IF v_cust_tax IS NULL THEN
          RAISE EXCEPTION 'no open sales_tax_payable account for ccy=%',
                          p_currency USING ERRCODE = 'P0010';
        END IF;
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'ar_invoice',
          'document_kind',     'customer_invoice',
          'document_id',       v_doc_id,
          'document_line_id',  v_inv_line_id,
          'debit_account_id',  v_cust_ar,
          'credit_account_id', v_cust_tax,
          'amount',            v_tax_amount,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'counterparty_id',   p_customer_id,
          'posted_by',         p_posted_by
        ));
      END IF;

    ELSE
      RAISE EXCEPTION 'customer_invoice_invalid_line: line % unknown kind %',
                      v_idx, v_kind USING ERRCODE = 'P0041';
    END IF;
  END LOOP;

  v_t1 := clock_timestamp();
  PERFORM post_posting_lines(v_batch, FALSE);
  v_t2 := clock_timestamp();
  v_t3 := v_t2;

  INSERT INTO _wrapper_section_timings (wrapper_name, section, elapsed_us) VALUES
    ('post_customer_invoice', 'setup',              (EXTRACT(EPOCH FROM v_t1 - v_t0) * 1e6)::BIGINT),
    ('post_customer_invoice', 'post_posting_lines', (EXTRACT(EPOCH FROM v_t2 - v_t1) * 1e6)::BIGINT),
    ('post_customer_invoice', 'followup',           (EXTRACT(EPOCH FROM v_t3 - v_t2) * 1e6)::BIGINT);

  RETURN v_doc_id;
END;
$$;

-- ============================================================
-- 4. post_wo_start — verbatim from mig 0058 + section timings
-- ============================================================

CREATE OR REPLACE FUNCTION post_wo_start(
  p_wo_id              UUID,
  p_business_date      DATE,
  p_posted_by          UUID,
  p_idempotency_key    UUID,
  p_notes              TEXT DEFAULT NULL,
  p_component_lot_pins JSONB DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id     UUID;
  v_event_id        UUID;
  v_wo              work_orders%ROWTYPE;
  v_first_op        INT;
  v_op_count        INT;
  v_cost_method     cost_method;
  v_qty_acct_wip    BIGINT;
  v_void_qty        BIGINT;
  v_val_acct_wip    BIGINT;
  v_bom             bom_headers%ROWTYPE;
  v_bad_op          INT;
  v_alloc_sum       NUMERIC;
  v_batch           JSONB := '[]'::JSONB;
  v_walks           JSONB := '[]'::JSONB;
  v_emit_batch      JSONB;
  v_emit_walks      JSONB;
  -- acct-h73o instrumentation:
  v_t0              TIMESTAMPTZ;
  v_t1              TIMESTAMPTZ;
  v_t2              TIMESTAMPTZ;
  v_t3              TIMESTAMPTZ;
BEGIN
  v_t0 := clock_timestamp();

  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  SELECT * INTO v_wo FROM work_orders WHERE id = p_wo_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: WO % not found', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;

  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF v_wo.status <> 'draft' THEN
    RAISE EXCEPTION 'wo_invalid: WO % status=% not draft (already started)',
                    p_wo_id, v_wo.status USING ERRCODE = 'P0026';
  END IF;

  SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_wo.parent_sku_id;
  IF v_cost_method NOT IN ('standard', 'wac_perpetual', 'wac_periodic',
                           'wac_retroactive', 'fifo', 'lot_fifo') THEN
    RAISE EXCEPTION
      'wo_invalid: parent_sku % has cost_method=% which post_wo_start does not handle',
      v_wo.parent_sku_id, v_cost_method USING ERRCODE = 'P0026';
  END IF;

  SELECT MIN(routing_op), COUNT(*) INTO v_first_op, v_op_count
    FROM wo_routings WHERE wo_id = p_wo_id;
  IF v_op_count = 0 THEN
    RAISE EXCEPTION 'wo_invalid: WO % has no routing operations', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;

  SELECT id INTO v_qty_acct_wip FROM accounts
   WHERE kind='stock_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=v_first_op AND NOT is_closed;
  IF v_qty_acct_wip IS NULL THEN
    RAISE EXCEPTION 'no open stock_wip account for sku=% op=%',
                    v_wo.parent_sku_id, v_first_op USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_void_qty FROM accounts
   WHERE kind='creation_void' AND ledger_kind='qty' AND NOT is_closed;
  IF v_void_qty IS NULL THEN
    RAISE EXCEPTION 'no creation_void(qty) account configured'
      USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_val_acct_wip FROM accounts
   WHERE kind='inv_value_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=v_first_op AND currency=v_wo.currency
     AND NOT is_closed;
  IF v_val_acct_wip IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_wip account for sku=% op=% ccy=%',
                    v_wo.parent_sku_id, v_first_op, v_wo.currency
      USING ERRCODE = 'P0010';
  END IF;

  v_bom := _wo_resolve_bom_for(p_wo_id, p_business_date);

  SELECT exp.applies_at_op INTO v_bad_op
    FROM _wo_explode_bom(v_bom.id, p_business_date) exp
   WHERE NOT EXISTS (
     SELECT 1 FROM wo_routings wr
      WHERE wr.wo_id = p_wo_id AND wr.routing_op = exp.applies_at_op
   )
   LIMIT 1;
  IF v_bad_op IS NOT NULL THEN
    RAISE EXCEPTION
      'wo_start_op_mismatch: bom_lines reference applies_at_op=% '
      'which is not in wo_routings(wo=%)',
      v_bad_op, p_wo_id USING ERRCODE = 'P0028';
  END IF;

  PERFORM 1 FROM wo_outputs WHERE wo_id = p_wo_id LIMIT 1;
  IF NOT FOUND THEN
    INSERT INTO wo_outputs (
      wo_id, output_no, output_sku_id, fg_location_id, qty,
      allocation_method, allocation_pct
    ) VALUES (
      p_wo_id, 1, v_wo.parent_sku_id, v_wo.fg_location_id, v_wo.qty_target,
      'primary', 100
    );
  ELSE
    SELECT COALESCE(SUM(allocation_pct), 0)
      INTO v_alloc_sum
      FROM wo_outputs WHERE wo_id = p_wo_id;
    IF v_alloc_sum <> 100 THEN
      RAISE EXCEPTION
        'output_allocation_invalid: wo_outputs(wo=%) allocation_pct sums to % (expected 100)',
        p_wo_id, v_alloc_sum USING ERRCODE = 'P0033';
    END IF;
  END IF;

  PERFORM 1 FROM wo_by_products WHERE wo_id = p_wo_id LIMIT 1;
  IF NOT FOUND THEN
    INSERT INTO wo_by_products (
      wo_id, by_product_no, output_sku_id, fg_location_id,
      planned_qty, actual_qty, unit_value, treatment,
      disposal_basis, disposal_vendor_id, disposal_expense_account_kind
    )
    SELECT
      p_wo_id,
      bbp.by_product_no,
      bbp.output_sku_id,
      bbp.fg_location_id,
      ROUND(bbp.qty_per_parent * v_wo.qty_target)::BIGINT AS planned_qty,
      ROUND(bbp.qty_per_parent * v_wo.qty_target)::BIGINT AS actual_qty,
      bbp.unit_value,
      bbp.treatment,
      bbp.disposal_basis,
      bbp.disposal_vendor_id,
      bbp.disposal_expense_account_kind
    FROM bom_by_products bbp
   WHERE bbp.bom_id = v_bom.id
     AND ROUND(bbp.qty_per_parent * v_wo.qty_target) >= 1;
  END IF;

  INSERT INTO wo_events (
    wo_id, event_kind, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_wo_id, 'start', p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_event_id;
  IF v_event_id IS NULL THEN RETURN p_wo_id; END IF;

  v_batch := v_batch || jsonb_build_array(jsonb_build_object(
    'reason',            'wo_start',
    'document_kind',     'wo_start',
    'document_id',       v_event_id,
    'debit_account_id',  v_qty_acct_wip,
    'credit_account_id', v_void_qty,
    'amount',            v_wo.qty_target,
    'qty',               v_wo.qty_target,
    'business_date',     p_business_date,
    'idempotency_key',   gen_random_uuid(),
    'posted_by',         p_posted_by
  ));

  SELECT b.batch, b.walks INTO v_emit_batch, v_emit_walks
    FROM _wo_emit_bom_lines(
      p_wo_id, v_bom.id, v_first_op, v_wo.qty_target,
      jsonb_build_object('fire_at', 'wo_start'),
      v_event_id, p_business_date, p_posted_by, 'wo_start',
      p_component_lot_pins
    ) b;
  v_batch := v_batch || v_emit_batch;
  v_walks := v_walks || v_emit_walks;

  SELECT b.batch, b.walks INTO v_emit_batch, v_emit_walks
    FROM _wo_emit_bom_lines(
      p_wo_id, v_bom.id, v_first_op, v_wo.qty_target,
      jsonb_build_object('fire_at', 'op_arrival', 'applies_at_op', v_first_op),
      v_event_id, p_business_date, p_posted_by, 'wo_start',
      p_component_lot_pins
    ) b;
  v_batch := v_batch || v_emit_batch;
  v_walks := v_walks || v_emit_walks;

  v_t1 := clock_timestamp();
  PERFORM post_posting_lines(v_batch, FALSE);
  v_t2 := clock_timestamp();

  PERFORM _wo_write_lot_consumption(v_walks);
  UPDATE work_orders SET status = 'released' WHERE id = p_wo_id;

  v_t3 := clock_timestamp();

  INSERT INTO _wrapper_section_timings (wrapper_name, section, elapsed_us) VALUES
    ('post_wo_start', 'setup',              (EXTRACT(EPOCH FROM v_t1 - v_t0) * 1e6)::BIGINT),
    ('post_wo_start', 'post_posting_lines', (EXTRACT(EPOCH FROM v_t2 - v_t1) * 1e6)::BIGINT),
    ('post_wo_start', 'followup',           (EXTRACT(EPOCH FROM v_t3 - v_t2) * 1e6)::BIGINT);

  RETURN p_wo_id;
END;
$$;

-- ============================================================
-- 5. post_op_move — verbatim from mig 0058 + section timings
-- ============================================================

CREATE OR REPLACE FUNCTION post_op_move(
  p_wo_id           UUID,
  p_from_op         INT,
  p_to_op           INT,
  p_qty             BIGINT,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id      UUID;
  v_event_id         UUID;
  v_wo               work_orders%ROWTYPE;
  v_from_count       INT;
  v_to_count         INT;
  v_qty_from         BIGINT;
  v_qty_to           BIGINT;
  v_val_from         BIGINT;
  v_val_to           BIGINT;
  v_value_amount     BIGINT;
  v_batch            JSONB := '[]'::JSONB;
  v_walks            JSONB := '[]'::JSONB;
  v_emit_batch       JSONB;
  v_emit_walks       JSONB;
  v_bom              bom_headers%ROWTYPE;
  v_first_op         INT;
  v_default_lot_size BIGINT;
  v_per_unit_cum     BIGINT;
  v_per_lot_cum      BIGINT;
  v_first_arrival    BOOLEAN;
  v_cost_method      cost_method;
  v_pool_value       BIGINT;
  v_pool_qty         BIGINT;
  v_unit             BIGINT;
  v_lock_first       BIGINT;
  v_lock_second      BIGINT;
  -- acct-h73o instrumentation:
  v_t0               TIMESTAMPTZ;
  v_t1               TIMESTAMPTZ;
  v_t2               TIMESTAMPTZ;
  v_t3               TIMESTAMPTZ;
BEGIN
  v_t0 := clock_timestamp();

  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF p_qty IS NULL OR p_qty <= 0 THEN
    RAISE EXCEPTION 'wo_invalid: op_move qty must be > 0 (got %)', p_qty
      USING ERRCODE = 'P0026';
  END IF;
  IF p_from_op = p_to_op THEN
    RAISE EXCEPTION 'routing_op_invalid: from_op (%) = to_op (%)',
                    p_from_op, p_to_op USING ERRCODE = 'P0028';
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

  SELECT COUNT(*) INTO v_from_count FROM wo_routings
   WHERE wo_id = p_wo_id AND routing_op = p_from_op;
  IF v_from_count = 0 THEN
    RAISE EXCEPTION 'routing_op_invalid: from_op % not in WO % routing',
                    p_from_op, p_wo_id USING ERRCODE = 'P0028';
  END IF;
  SELECT COUNT(*) INTO v_to_count FROM wo_routings
   WHERE wo_id = p_wo_id AND routing_op = p_to_op;
  IF v_to_count = 0 THEN
    RAISE EXCEPTION 'routing_op_invalid: to_op % not in WO % routing',
                    p_to_op, p_wo_id USING ERRCODE = 'P0028';
  END IF;

  SELECT id INTO v_qty_from FROM accounts
   WHERE kind='stock_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_from_op AND NOT is_closed;
  IF v_qty_from IS NULL THEN
    RAISE EXCEPTION 'no open stock_wip account for sku=% op=%',
                    v_wo.parent_sku_id, p_from_op USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_qty_to FROM accounts
   WHERE kind='stock_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_to_op AND NOT is_closed;
  IF v_qty_to IS NULL THEN
    RAISE EXCEPTION 'no open stock_wip account for sku=% op=%',
                    v_wo.parent_sku_id, p_to_op USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_val_from FROM accounts
   WHERE kind='inv_value_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_from_op AND currency=v_wo.currency AND NOT is_closed;
  IF v_val_from IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_wip account for sku=% op=% ccy=%',
                    v_wo.parent_sku_id, p_from_op, v_wo.currency
      USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_val_to FROM accounts
   WHERE kind='inv_value_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_to_op AND currency=v_wo.currency AND NOT is_closed;
  IF v_val_to IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_wip account for sku=% op=% ccy=%',
                    v_wo.parent_sku_id, p_to_op, v_wo.currency
      USING ERRCODE = 'P0010';
  END IF;

  v_bom := _wo_resolve_bom_for(p_wo_id, p_business_date);
  SELECT cost_method, default_lot_size INTO v_cost_method, v_default_lot_size
    FROM skus WHERE id = v_wo.parent_sku_id;
  SELECT MIN(routing_op) INTO v_first_op
    FROM wo_routings WHERE wo_id = p_wo_id;

  IF v_cost_method = 'standard' THEN
    SELECT COALESCE(SUM(
      CASE
        WHEN exp.kind = 'item' THEN
          (exp.qty_per_parent
            * _resolve_standard_cost_at(exp.component_sku_id, p_business_date))
        WHEN exp.kind = 'service' AND exp.basis = 'per_unit' THEN exp.std_amount
        ELSE 0
      END
    ), 0) INTO v_per_unit_cum
      FROM _wo_explode_bom(v_bom.id, p_business_date) exp
     WHERE exp.basis = 'per_unit'
       AND exp.applies_at_op <= p_from_op;

    SELECT COALESCE(SUM(exp.std_amount), 0) / v_default_lot_size
      INTO v_per_lot_cum
      FROM _wo_explode_bom(v_bom.id, p_business_date) exp
     WHERE exp.basis = 'per_lot'
       AND (
         exp.fire_at = 'wo_start'
         OR (exp.fire_at = 'op_arrival' AND exp.applies_at_op <= p_from_op)
       );

    v_value_amount := p_qty * (v_per_unit_cum + v_per_lot_cum);

  ELSIF v_cost_method IN ('wac_perpetual', 'wac_periodic',
                          'wac_retroactive', 'fifo', 'lot_fifo') THEN
    v_lock_first  := LEAST(v_qty_from, v_val_from);
    v_lock_second := GREATEST(v_qty_from, v_val_from);
    PERFORM 1 FROM accounts WHERE id = v_lock_first  FOR UPDATE;
    PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;
    SELECT (debits_total - credits_total) INTO v_pool_value
      FROM accounts WHERE id = v_val_from;
    SELECT (debits_total - credits_total) INTO v_pool_qty
      FROM accounts WHERE id = v_qty_from;

    IF v_pool_qty IS NULL OR v_pool_qty <= 0 THEN
      v_value_amount := 0;
    ELSE
      v_unit := GREATEST(COALESCE(v_pool_value, 0), 0) / v_pool_qty;
      v_value_amount := p_qty * v_unit;
    END IF;

  ELSE
    RAISE EXCEPTION
      'wo_invalid: parent_sku % has cost_method=% which post_op_move does not handle',
      v_wo.parent_sku_id, v_cost_method USING ERRCODE = 'P0026';
  END IF;

  v_first_arrival := NOT EXISTS (
    SELECT 1 FROM wo_events
     WHERE wo_id = p_wo_id
       AND (
         (event_kind = 'op_move' AND routing_op_to = p_to_op)
         OR (event_kind = 'start' AND p_to_op = v_first_op)
       )
  );

  INSERT INTO wo_events (
    wo_id, event_kind, routing_op_from, routing_op_to, qty,
    business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_wo_id, 'op_move', p_from_op, p_to_op, p_qty,
    p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_event_id;
  IF v_event_id IS NULL THEN RETURN p_wo_id; END IF;

  v_batch := v_batch || jsonb_build_array(jsonb_build_object(
    'reason',            'op_move',
    'document_kind',     'op_move',
    'document_id',       v_event_id,
    'debit_account_id',  v_qty_to,
    'credit_account_id', v_qty_from,
    'amount',            p_qty,
    'qty',               p_qty,
    'business_date',     p_business_date,
    'idempotency_key',   gen_random_uuid(),
    'posted_by',         p_posted_by
  ));

  IF v_value_amount > 0 THEN
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'op_move_v',
      'document_kind',     'op_move',
      'document_id',       v_event_id,
      'debit_account_id',  v_val_to,
      'credit_account_id', v_val_from,
      'amount',            v_value_amount,
      'qty',               p_qty,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'posted_by',         p_posted_by
    ));
  END IF;

  IF v_first_arrival THEN
    SELECT b.batch, b.walks INTO v_emit_batch, v_emit_walks
      FROM _wo_emit_bom_lines(
        p_wo_id, v_bom.id, p_to_op, p_qty,
        jsonb_build_object('fire_at', 'op_arrival', 'applies_at_op', p_to_op),
        v_event_id, p_business_date, p_posted_by, 'op_move'
      ) b;
  ELSE
    SELECT b.batch, b.walks INTO v_emit_batch, v_emit_walks
      FROM _wo_emit_bom_lines(
        p_wo_id, v_bom.id, p_to_op, p_qty,
        jsonb_build_object('fire_at',        'op_arrival',
                           'applies_at_op',  p_to_op,
                           'basis',          'per_unit',
                           'kind',           'service'),
        v_event_id, p_business_date, p_posted_by, 'op_move'
      ) b;
  END IF;
  v_batch := v_batch || v_emit_batch;
  v_walks := v_walks || v_emit_walks;

  v_t1 := clock_timestamp();
  PERFORM post_posting_lines(v_batch, FALSE);
  v_t2 := clock_timestamp();

  PERFORM _wo_write_lot_consumption(v_walks);

  v_t3 := clock_timestamp();

  INSERT INTO _wrapper_section_timings (wrapper_name, section, elapsed_us) VALUES
    ('post_op_move', 'setup',              (EXTRACT(EPOCH FROM v_t1 - v_t0) * 1e6)::BIGINT),
    ('post_op_move', 'post_posting_lines', (EXTRACT(EPOCH FROM v_t2 - v_t1) * 1e6)::BIGINT),
    ('post_op_move', 'followup',           (EXTRACT(EPOCH FROM v_t3 - v_t2) * 1e6)::BIGINT);

  RETURN p_wo_id;
END;
$$;
