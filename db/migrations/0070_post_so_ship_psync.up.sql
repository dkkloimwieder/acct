-- acct-c4p: shape-L pseudo-sync entry point for post_so_ship.
--
-- Why: 1s6r + h73o showed that 3 of 4 hot wrappers spend >99% of p99
-- inside the in-transaction PERFORM post_posting_lines call. shape-L
-- (load_outbox_pseudo_sync) moves the transport off the caller's wait
-- by: writer INSERTs events to ledger_outbox + commits tx 1; single
-- drainer reads pending rows + commits ledger postings + emits
-- pg_notify; caller blocks on LISTEN/NOTIFY rendezvous via a Rust-side
-- Dispatcher.
--
-- This migration ships ONE wrapper migrated as proof-of-concept:
-- post_so_ship_psync. Existing post_so_ship (mig 0068) stays synchronous
-- for the ~20 existing test call sites; new psync entry point is opt-in
-- via the T4_USE_PSYNC=1 env in load_phase1_mixed_workload.
--
-- Body sources: mig 0068 lines 43-516 (verbatim post_so_ship). Surgical
-- changes:
--   - Skip the PERFORM post_posting_lines call; INSERT events to
--     ledger_outbox instead.
--   - RETURN both document_id and outbox_id (RECORD) so the Rust caller
--     can rendezvous via the dispatcher.
--   - h73o timing wrappers preserved with names suffixed _psync so
--     decomposed measurements distinguish the two paths.
--   - Reject lot_and_serial: the followup inventory_unit_events INSERT
--     JOINs posting_lines, which don't exist yet in tx 1. (Acceptable
--     MVP limitation — file as acct-c4p-lot-and-serial-followup if a
--     real driver surfaces.)
--
-- Consistency model: tx 1 (so_shipments + so_shipment_lines + outbox
-- row) commits BEFORE tx 2 (drainer's post_posting_lines). If tx 2
-- fails (e.g. P0001, 23514 from stock depletion), tx 1's documents are
-- orphaned — caller receives status='failed' + sqlstate via the
-- dispatcher and is responsible for surfacing/compensating. Recon would
-- catch ledger_outbox rows with status='failed' against committed
-- so_shipments. Filed as acct-c4p-recon followup if it doesn't ship
-- in this MVP.

-- Extend the h73o instrumentation CHECK to allow 'enqueue' section.
ALTER TABLE _wrapper_section_timings DROP CONSTRAINT _wrapper_section_timings_section_check;
ALTER TABLE _wrapper_section_timings ADD CONSTRAINT _wrapper_section_timings_section_check
  CHECK (section IN ('setup','post_posting_lines','followup','enqueue'));

CREATE OR REPLACE FUNCTION post_so_ship_psync(
  p_so_id           UUID,
  p_lines           JSONB,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS TABLE (so_shipment_id UUID, outbox_id BIGINT)
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
  v_outbox_id      BIGINT;
  -- acct-h73o instrumentation, _psync variant:
  v_t0             TIMESTAMPTZ;
  v_t1             TIMESTAMPTZ;
  v_t2             TIMESTAMPTZ;
BEGIN
  v_t0 := clock_timestamp();

  SELECT id INTO v_existing_id FROM so_shipments
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN
    -- Idempotent replay: return the existing doc + a sentinel outbox_id (-1)
    -- meaning "no rendezvous needed". Caller skips the wait.
    so_shipment_id := v_existing_id;
    outbox_id := -1;
    RETURN NEXT;
    RETURN;
  END IF;

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
    so_shipment_id := v_doc_id;
    outbox_id := -1;
    RETURN NEXT;
    RETURN;
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

    IF v_tracked_by = 'lot_and_serial' THEN
      RAISE EXCEPTION
        'cost_method_not_implemented: psync entry does not yet support '
        'tracked_by=lot_and_serial — followup inventory_unit_events INSERT '
        'JOINs posting_lines which do not exist in caller tx; use '
        'post_so_ship (sync) instead'
        USING ERRCODE = 'P0006';
    END IF;

    v_unit_ids_json := v_line->'unit_ids';
    IF v_unit_ids_json IS NOT NULL THEN
      RAISE EXCEPTION
        'so_ship_invalid: line % carries unit_ids but post_so_ship_psync '
        'does not support lot_and_serial — use post_so_ship instead',
        v_idx USING ERRCODE = 'P0006';
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
  END LOOP;

  UPDATE inventory_reservations
     SET status = 'shipped',
         resolved_at = clock_timestamp()
   WHERE so_id = p_so_id
     AND status IN ('active', 'allocated');

  -- acct-c4p: instead of PERFORM post_posting_lines, enqueue to outbox.
  -- The Rust caller's dispatcher waits on the pg_notify rendezvous.
  v_t1 := clock_timestamp();
  INSERT INTO ledger_outbox (events, override_closed_period)
  VALUES (v_batch, FALSE)
  RETURNING id INTO v_outbox_id;
  v_t2 := clock_timestamp();

  INSERT INTO _wrapper_section_timings (wrapper_name, section, elapsed_us) VALUES
    ('post_so_ship_psync', 'setup',              (EXTRACT(EPOCH FROM v_t1 - v_t0) * 1e6)::BIGINT),
    ('post_so_ship_psync', 'enqueue',            (EXTRACT(EPOCH FROM v_t2 - v_t1) * 1e6)::BIGINT),
    ('post_so_ship_psync', 'followup',           0);

  so_shipment_id := v_doc_id;
  outbox_id := v_outbox_id;
  RETURN NEXT;
END;
$$;
