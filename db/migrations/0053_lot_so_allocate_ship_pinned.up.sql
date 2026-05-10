-- ============================================================
-- Phase E2 E2.5-followup — post_so_allocate + post_so_ship
-- pinned-reservation handling (acct-5vh9, sub-issue of acct-uze).
--
-- E2.5 (mig 0052, acct-knuu) added p_lot_id + p_lot_specific to
-- reserve_inventory; mig 0044 (acct-ua3r) laid the schema columns
-- inventory_reservations.lot_id + lot_specific. But neither
-- post_so_allocate (mig 0018) nor post_so_ship (mig 0050) consults
-- those pins. This migration completes the lifecycle.
--
-- W1 — post_so_allocate:
--
--   For each pinned reservation in (so_id, status='active'),
--   verify the lot still has enough residual to honor the pin:
--
--     residual = _inventory_lot_remaining_qty(lot_id, receipt_date)
--     if residual < reservation.qty: raise P0053
--                                    'pinned_lot_underfulfilled'
--
--   Why: between reserve and allocate, concurrent ships /
--   adjustments / non-pinned reservations resolving to this lot
--   may have depleted it. Catching at allocate prevents a later
--   ship-time failure from leaving the SO line in a half-state.
--
--   Lock pattern: PERFORM 1 ... FOR UPDATE on stock_available
--   rows for the reservation's (sku, location). Mirrors
--   reserve_inventory's serialization against issue-time
--   dispatchers.
--
-- W2 — post_so_ship lot_fifo branch:
--
--   Before reading p_lines->>'lot_id', look up pinned
--   reservations matching so_line_id (status IN active /
--   allocated, lot_specific = TRUE).
--
--   Resolution table (caller_lot = p_lines->>'lot_id'):
--
--     pins | caller_lot | action
--     -----+------------+-----------------------------------
--      0   | NULL       | FIFO walk (existing behavior)
--      0   | given      | use caller's (existing behavior)
--      1   | NULL       | use pin's lot_id
--      1   | matches    | use it (consistent)
--      1   | mismatches | P0054 ship_lot_pin_conflict
--      >1  | NULL       | P0055 ambiguous_pinned_reservation
--      >1  | matches    | use caller's (resolves ambiguity)
--      >1  | mismatches | P0054 (caller bypasses pins)
--
--   "Caller matches" means caller_lot equals at least one pin's
--   lot_id. This is the schema-permitted multi-pin-per-line
--   case; the caller resolves it by naming which pin they want.
--
-- Errors:
--   P0053 — pinned_lot_underfulfilled (W1)
--   P0054 — ship_lot_pin_conflict (W2)
--   P0055 — ambiguous_pinned_reservation (W2)
--
-- Out of scope (separate follow-ups):
--   - FEFO allocator + skus.allocation_strategy (E2.7).
--   - Auto-pinning non-pinned reservations to specific lots at
--     allocate time (E2.7).
--   - Reservation transfers between SOs.
--   - Multi-cost-book pinning (acct-zf80).
-- ============================================================

CREATE OR REPLACE FUNCTION post_so_allocate(
  p_so_id           UUID,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id UUID;
  v_doc_id      UUID;
  v_so_check    UUID;
  v_pin         RECORD;
  v_recv_date   DATE;
  v_residual    NUMERIC;
BEGIN
  SELECT id INTO v_existing_id FROM so_allocations
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT id INTO v_so_check FROM sales_orders WHERE id = p_so_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'so_allocate_invalid: SO % not found', p_so_id
      USING ERRCODE = 'P0043';
  END IF;

  -- W1: validate every pinned active reservation. Lock the
  -- relevant stock_available rows so a concurrent issue-time
  -- dispatcher can't deplete the lot between our residual read
  -- and the status flip below.
  FOR v_pin IN
    SELECT id, sku_id, location_id, qty, lot_id
      FROM inventory_reservations
     WHERE so_id        = p_so_id
       AND status       = 'active'
       AND lot_specific = TRUE
  LOOP
    PERFORM 1
       FROM accounts
      WHERE kind        = 'stock_available'
        AND sku_id      = v_pin.sku_id
        AND location_id = v_pin.location_id
        AND NOT is_closed
      FOR UPDATE;

    SELECT receipt_date
      INTO v_recv_date
      FROM inventory_lots
     WHERE lot_id = v_pin.lot_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION
        'pinned_lot_underfulfilled: reservation=% lot=% not found',
        v_pin.id, v_pin.lot_id
        USING ERRCODE = 'P0053';
    END IF;

    v_residual := COALESCE(
      _inventory_lot_remaining_qty(v_pin.lot_id, v_recv_date), 0
    );

    IF v_residual < v_pin.qty THEN
      RAISE EXCEPTION
        'pinned_lot_underfulfilled: reservation=% lot=% residual=% qty=%',
        v_pin.id, v_pin.lot_id, v_residual, v_pin.qty
        USING ERRCODE = 'P0053';
    END IF;
  END LOOP;

  INSERT INTO so_allocations (so_id, business_date, posted_by, idempotency_key, notes)
  VALUES (p_so_id, p_business_date, p_posted_by, p_idempotency_key, p_notes)
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM so_allocations
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  UPDATE inventory_reservations
     SET status = 'allocated'
   WHERE so_id = p_so_id
     AND status = 'active';

  RETURN v_doc_id;
END;
$$;

-- ============================================================
-- post_so_ship — extend lot_fifo branch to consult reservation
-- pins. Body is identical to mig 0050's verbatim except for the
-- lot_fifo branch's pin-resolution block (lines marked W2).
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
  -- W2 pin-resolution scratch:
  v_caller_lot_id  BIGINT;
  v_pin_count      INT;
  v_pin_lot_id     BIGINT;
  v_pin_match_ok   BOOLEAN;
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

    SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_sl.sku_id;

    IF v_cost_method = 'lot' THEN
      RAISE EXCEPTION
        'cost_method_not_implemented: lot for so_ship (sku=%); see acct-uze',
        v_sl.sku_id USING ERRCODE = 'P0006';
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

    -- Reset per-line lot state.
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
      -- L4: walk FG lots under FOR UPDATE on inv_value_fg (mirrors
      -- the FIFO branch's lock pattern). The dispatcher walks again
      -- post-lock; apply_event re-walks for inventory_lot_events
      -- writeback. All three see identical allocations.
      PERFORM 1 FROM accounts WHERE id = v_val_acct FOR UPDATE;

      -- W2: resolve the lot pin from caller and active pinned
      -- reservations on this so_line. Caller's lot_id (if any)
      -- must match a pin when pins exist; absent caller, exactly
      -- one pin is required to disambiguate.
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
      -- Audit pointer prefers explicit pin, else first walked lot.
      v_audit_lot_id := COALESCE(v_specific_lot_id, v_first_lot);

    ELSE
      -- WAC family. acct-5prc / R4 + R7 — lock value pool BEFORE
      -- reading per-class qty divisor + value balance.
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
    -- L4: forward lot_id pin (or NULL = FIFO walk) to dispatcher +
    -- apply_event so all three walks see the same allocations.
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

  PERFORM post_posting_lines(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;
