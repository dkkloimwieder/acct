-- acct-th7 / Slice C Part C — outflow-cycle functions.
--
-- post_so_ship          — qty + COGS + revenue→ar_unsettled + tax legs
-- post_customer_invoice — ar_unsettled→ar with three-way match
-- post_ar_payment       — cash→ar
--
-- Includes a refactor to post_transfers (mig 0033 pattern) to allow
-- so_ship value legs without SKU on either side (revenue / tax legs
-- through ar_unsettled / revenue / sales_tax_payable, none of which
-- carry SKU) to pass through with caller-supplied amount instead of
-- raising P0006. Behavior-preserving for canonical cost-event reasons
-- (op_move / scrap / wo_complete and so_ship's qty + COGS legs which
-- DO have SKU on at least one side) — those still resolve a SKU and
-- the dispatcher prices.
--
-- Also switches the SKU resolution in post_transfers to credit-first
-- (mirroring acct-du2.4 mig 0075 which switched _post_transfers_
-- compute_amount and acct-7py mig 0067 which switched _post_transfers_
-- apply_event). Functionally equivalent for canonical reasons today
-- (single SKU-bearing leg or both legs same SKU); aligns with the
-- credit-first pattern across the post_transfers family.
--
-- Idempotency: fast-path replay on idempotency_key + ON CONFLICT DO
-- NOTHING on header INSERT. UNIQUE(idempotency_key) on each header
-- table serializes concurrent callers; the loser sees its INSERT
-- skipped and returns the existing id. Same pattern Slice A used for
-- post_po_receipt / post_ap_bill (acct-7mg). Satisfies CLAUDE.md R6
-- via the unique-constraint race tiebreaker — no FOR UPDATE-then-
-- recheck loop is required because the UNIQUE rejects the second
-- caller atomically.
--
-- Error codes (additive to Slice A's P0022–P0025):
--   P0037 — so_ship_invalid: SO missing, no customer_id, empty lines,
--           so_line missing or wrong SO, qty_shipped <= 0.
--   P0038 — so_line_overshipped: cumulative qty would exceed
--           qty_ordered.
--   P0039 — ar_payment_invalid: amount <= 0, customer missing.
--   P0040 — customer_invoice_three_way_mismatch: line qty exceeds
--           shipped-not-invoiced remainder; unit_price diverges from
--           so_line; amount diverges from qty × unit_price.
--   P0041 — customer_invoice_invalid_line: customer mismatch,
--           currency mismatch, service line missing revenue_account,
--           account state violations, etc.

-- ============================================================
-- post_transfers refactor — allow so_ship non-SKU value legs.
-- ============================================================
--
-- Single change vs mig 0033: when reason is in the cost-event list
-- and the value leg has NO resolvable SKU on either account, fall
-- through to caller-supplied amount instead of raising P0006. The
-- non-SKU case targets so_ship's revenue / tax legs which post
-- ar_unsettled DR / revenue (or sales_tax_payable) CR — both
-- accounts have NULL sku_id by partition.
--
-- Refactor also switches v_cost_sku COALESCE to credit-first for
-- consistency with mig 0075 (compute_amount) and mig 0067 (apply_
-- event). For canonical cost-event reasons today (op_move / scrap /
-- wo_complete) the switch is a no-op because both legs share the
-- same SKU.

CREATE OR REPLACE FUNCTION post_transfers(
  p_events                 JSONB,
  p_override_closed_period BOOLEAN DEFAULT FALSE
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
  v_results        JSONB := '[]'::JSONB;
  v_n              INT;
  v_idx            INT;
  v_event          JSONB;
  v_d_acct         accounts%ROWTYPE;
  v_c_acct         accounts%ROWTYPE;
  v_d_id           BIGINT;
  v_c_id           BIGINT;
  v_idem_key       UUID;
  v_reason         transfer_reason;
  v_cost_sku       UUID;
  v_cost_method    cost_method;
  v_amount         BIGINT;
  v_amounts        BIGINT[];
  v_aux_qty_id     BIGINT;
  v_aux_qty_ids    BIGINT[] := '{}';
  v_has_wac        BOOLEAN  := FALSE;
  v_has_cost_event BOOLEAN;
BEGIN
  v_n := jsonb_array_length(p_events);
  IF v_n = 0 THEN RETURN '[]'::JSONB; END IF;

  v_has_cost_event := EXISTS (
    SELECT 1 FROM jsonb_array_elements(p_events) ev
     WHERE (ev->>'reason')::transfer_reason
           IN ('op_move','scrap','wo_complete','so_ship')
  );

  IF v_has_cost_event THEN
    FOR v_idx IN 1..v_n LOOP
      v_event  := p_events -> (v_idx - 1);
      v_reason := (v_event->>'reason')::transfer_reason;
      IF v_reason NOT IN ('op_move','scrap','wo_complete','so_ship') THEN
        CONTINUE;
      END IF;
      v_c_id := (v_event->>'credit_account_id')::BIGINT;
      SELECT * INTO v_c_acct FROM accounts WHERE id = v_c_id;
      IF v_c_acct.ledger_kind <> 'value' THEN CONTINUE; END IF;
      v_cost_sku := v_c_acct.sku_id;
      IF v_cost_sku IS NULL THEN
        v_d_id := (v_event->>'debit_account_id')::BIGINT;
        SELECT * INTO v_d_acct FROM accounts WHERE id = v_d_id;
        v_cost_sku := v_d_acct.sku_id;
      END IF;
      IF v_cost_sku IS NULL THEN CONTINUE; END IF;
      SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_cost_sku;
      IF v_cost_method IN ('wac_perpetual', 'wac_periodic', 'wac_retroactive') THEN
        v_has_wac := TRUE;
        v_aux_qty_id := _post_transfers_lookup_qty_account(v_c_acct);
        IF v_aux_qty_id IS NOT NULL THEN
          v_aux_qty_ids := array_append(v_aux_qty_ids, v_aux_qty_id);
        END IF;
      END IF;
    END LOOP;
  END IF;

  PERFORM _post_transfers_lock_pre_scan(p_events, v_aux_qty_ids);

  IF NOT v_has_wac THEN
    -- Single-pass.
    FOR v_idx IN 1..v_n LOOP
      v_event    := p_events -> (v_idx - 1);
      v_idem_key := (v_event->>'idempotency_key')::UUID;
      IF EXISTS (SELECT 1 FROM transfers WHERE idempotency_key = v_idem_key) THEN
        v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'exists');
        CONTINUE;
      END IF;
      v_d_id := (v_event->>'debit_account_id')::BIGINT;
      v_c_id := (v_event->>'credit_account_id')::BIGINT;
      v_reason := (v_event->>'reason')::transfer_reason;
      v_cost_method := NULL;
      SELECT * INTO v_d_acct FROM accounts WHERE id = v_d_id;
      SELECT * INTO v_c_acct FROM accounts WHERE id = v_c_id;
      IF v_reason IN ('op_move','scrap','wo_complete','so_ship') THEN
        -- Credit-first SKU resolution. Aligns with mig 0075 (compute_
        -- amount) and mig 0067 (apply_event).
        v_cost_sku := COALESCE(v_c_acct.sku_id, v_d_acct.sku_id);
        IF v_cost_sku IS NULL THEN
          -- so_ship's revenue / tax legs through ar_unsettled /
          -- revenue / sales_tax_payable carry no SKU on either side.
          -- Caller-supplied amount stands. Other cost-event reasons
          -- (op_move / scrap / wo_complete) MUST resolve an SKU
          -- because they're inventory ops; without one the dispatcher
          -- can't price them and a missing SKU is a caller bug.
          IF v_reason = 'so_ship' AND v_d_acct.ledger_kind = 'value' THEN
            v_amount := (v_event->>'amount')::BIGINT;
          ELSE
            RAISE EXCEPTION
              'cost_method_not_implemented: sku not resolvable for reason % at event index %',
              v_reason, v_idx USING ERRCODE = 'P0006';
          END IF;
        ELSE
          SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_cost_sku;
          IF v_d_acct.ledger_kind = 'value' THEN
            v_amount := _post_transfers_compute_amount(
              v_event, v_d_acct, v_c_acct, v_cost_method, v_idx
            );
          ELSE
            IF v_cost_method NOT IN
               ('standard', 'wac_perpetual', 'wac_periodic', 'wac_retroactive') THEN
              RAISE EXCEPTION
                'cost_method_not_implemented: % for reason % at event index %',
                v_cost_method, v_reason, v_idx USING ERRCODE = 'P0006';
            END IF;
            v_amount := (v_event->>'amount')::BIGINT;
          END IF;
        END IF;
      ELSE
        v_amount := (v_event->>'amount')::BIGINT;
      END IF;
      PERFORM _post_transfers_apply_event(
        v_event, v_idx, v_amount, v_d_acct, v_c_acct,
        v_cost_method, p_override_closed_period
      );
      v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'ok');
    END LOOP;
    RETURN v_results;
  END IF;

  -- Two-pass for WAC batches: pass-1 compute amounts; pass-2 apply.
  v_amounts := array_fill(NULL::BIGINT, ARRAY[v_n]);
  FOR v_idx IN 1..v_n LOOP
    v_event  := p_events -> (v_idx - 1);
    v_reason := (v_event->>'reason')::transfer_reason;
    IF v_reason NOT IN ('op_move','scrap','wo_complete','so_ship') THEN
      v_amounts[v_idx] := (v_event->>'amount')::BIGINT;
      CONTINUE;
    END IF;
    v_idem_key := (v_event->>'idempotency_key')::UUID;
    IF EXISTS (SELECT 1 FROM transfers WHERE idempotency_key = v_idem_key) THEN
      CONTINUE;
    END IF;
    v_d_id := (v_event->>'debit_account_id')::BIGINT;
    v_c_id := (v_event->>'credit_account_id')::BIGINT;
    SELECT * INTO v_d_acct FROM accounts WHERE id = v_d_id;
    SELECT * INTO v_c_acct FROM accounts WHERE id = v_c_id;
    v_cost_sku := COALESCE(v_c_acct.sku_id, v_d_acct.sku_id);
    IF v_cost_sku IS NULL THEN
      IF v_reason = 'so_ship' AND v_d_acct.ledger_kind = 'value' THEN
        v_amounts[v_idx] := (v_event->>'amount')::BIGINT;
      ELSE
        RAISE EXCEPTION
          'cost_method_not_implemented: sku not resolvable for reason % at event index %',
          v_reason, v_idx USING ERRCODE = 'P0006';
      END IF;
    ELSE
      SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_cost_sku;
      IF v_d_acct.ledger_kind = 'value' THEN
        v_amounts[v_idx] := _post_transfers_compute_amount(
          v_event, v_d_acct, v_c_acct, v_cost_method, v_idx
        );
      ELSE
        IF v_cost_method NOT IN
           ('standard', 'wac_perpetual', 'wac_periodic', 'wac_retroactive') THEN
          RAISE EXCEPTION
            'cost_method_not_implemented: % for reason % at event index %',
            v_cost_method, v_reason, v_idx USING ERRCODE = 'P0006';
        END IF;
        v_amounts[v_idx] := (v_event->>'amount')::BIGINT;
      END IF;
    END IF;
  END LOOP;

  FOR v_idx IN 1..v_n LOOP
    v_event    := p_events -> (v_idx - 1);
    v_idem_key := (v_event->>'idempotency_key')::UUID;
    IF EXISTS (SELECT 1 FROM transfers WHERE idempotency_key = v_idem_key) THEN
      v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'exists');
      CONTINUE;
    END IF;
    v_d_id := (v_event->>'debit_account_id')::BIGINT;
    v_c_id := (v_event->>'credit_account_id')::BIGINT;
    v_reason := (v_event->>'reason')::transfer_reason;
    v_amount := v_amounts[v_idx];
    v_cost_method := NULL;
    SELECT * INTO v_d_acct FROM accounts WHERE id = v_d_id;
    SELECT * INTO v_c_acct FROM accounts WHERE id = v_c_id;
    IF v_reason IN ('op_move','scrap','wo_complete','so_ship') THEN
      v_cost_sku := COALESCE(v_c_acct.sku_id, v_d_acct.sku_id);
      IF v_cost_sku IS NOT NULL THEN
        SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_cost_sku;
      END IF;
    END IF;
    PERFORM _post_transfers_apply_event(
      v_event, v_idx, v_amount, v_d_acct, v_c_acct,
      v_cost_method, p_override_closed_period
    );
    v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'ok');
  END LOOP;
  RETURN v_results;
END;
$$;

COMMENT ON FUNCTION post_transfers(JSONB, BOOLEAN) IS
  'Append-only ledger primitive. Validates a JSONB array of transfer '
  'events, applies each as one row in transfers + paired UPDATE on '
  'accounts.debits_total/credits_total. Single-pass for non-WAC '
  'batches; two-pass for batches that touch a WAC SKU. Apply step '
  'extracted to _post_transfers_apply_event (acct-7mg). SKU '
  'resolution is credit-first across the family (acct-du2.4 / acct-'
  '7py / acct-th7). Cost-event value legs without resolvable SKU '
  '(so_ship revenue / tax legs through ar_unsettled / revenue / '
  'sales_tax_payable) pass through with caller-supplied amount '
  '(acct-th7 / Slice C).';

-- ============================================================
-- post_so_ship
-- ============================================================
--
-- Per line: posts qty (customer_pool DR / stock_available CR), COGS
-- (cogs DR / inv_value_fg CR — dispatcher-priced via cost_method),
-- revenue (ar_unsettled DR / revenue CR — caller-supplied), and tax
-- (ar_unsettled DR / sales_tax_payable CR — caller-supplied) when
-- tax_amount > 0. Reservation status flips 'active' → 'shipped'
-- atomically with the so_shipment INSERT.
--
-- All four legs use reason='so_ship' per design doc §3.3 verbatim;
-- the post_transfers refactor above lets the non-SKU value legs
-- pass through.

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
BEGIN
  -- Fast-path replay.
  SELECT id INTO v_existing_id FROM so_shipments
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  -- SO + customer.
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

  -- Reserve doc id (race-safe via UNIQUE on idempotency_key).
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

    -- SO line + ownership check.
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

    -- Strict over-ship gate.
    SELECT COALESCE(SUM(qty_shipped), 0) INTO v_already_ship
      FROM so_shipment_lines WHERE so_line_id = v_so_line_id;
    IF v_already_ship + v_qty_shipped > v_sl.qty_ordered THEN
      RAISE EXCEPTION
        'so_line_overshipped: so_line %: ordered=%, already shipped=%, '
        'this shipment=%; cumulative would exceed qty_ordered',
        v_so_line_id, v_sl.qty_ordered, v_already_ship, v_qty_shipped
        USING ERRCODE = 'P0038';
    END IF;

    -- Line override or so_line defaults for unit_price / tax_amount.
    v_unit_price := COALESCE((v_line->>'unit_price')::BIGINT, v_sl.unit_price);
    v_tax_amount := COALESCE((v_line->>'tax_amount')::BIGINT, v_sl.tax_amount);

    -- Cost method on the FG SKU drives COGS pricing.
    SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_sl.sku_id;

    IF v_cost_method IN ('fifo', 'lot') THEN
      RAISE EXCEPTION
        'cost_method_not_implemented: % for so_ship (sku=%); see acct-8gg',
        v_cost_method, v_sl.sku_id USING ERRCODE = 'P0006';
    END IF;

    -- Resolve accounts.
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

    -- Compute COGS unit_cost externally (mirrors dispatcher logic).
    -- Persisted in so_shipment_lines for audit. The post_transfers
    -- COGS event below recomputes via _post_transfers_compute_amount
    -- under the same lock, producing the same value.
    IF v_cost_method = 'standard' THEN
      v_unit_cost := resolve_standard_cost_at(v_sl.sku_id, p_business_date);
    ELSE
      -- WAC family: running avg from inv_value_fg per-class qty signed
      -- SUM (R1; mirror of mig 0030 / acct-1vr). Class-isolated when
      -- raw + fg share a location.
      SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = v_val_acct THEN  t.qty
                               WHEN t.credit_account_id = v_val_acct THEN -t.qty END), 0)
        INTO v_qty_balance
        FROM transfers t
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

    -- Resolve sales_tax_payable if tax_amount > 0.
    IF v_tax_amount > 0 THEN
      SELECT id INTO v_tax_acct FROM accounts
       WHERE kind='sales_tax_payable' AND ledger_kind='value'
         AND currency=v_sl.currency AND NOT is_closed;
      IF v_tax_acct IS NULL THEN
        RAISE EXCEPTION 'no open sales_tax_payable account for ccy=%',
                        v_sl.currency USING ERRCODE = 'P0010';
      END IF;
    END IF;

    -- INSERT shipment-line first so events can carry document_line_id.
    INSERT INTO so_shipment_lines (
      shipment_id, so_line_id, qty_shipped, unit_cost, unit_price, tax_amount
    ) VALUES (
      v_doc_id, v_so_line_id, v_qty_shipped, v_unit_cost, v_unit_price, v_tax_amount
    ) RETURNING id INTO v_ship_line_id;

    -- Event 1: qty leg. customer_pool DR / stock_available CR.
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'so_ship',
      'document_kind',     'so_shipment',
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

    -- Event 2: COGS leg. cogs DR / inv_value_fg CR. Dispatcher-priced
    -- (amount field is overridden by _post_transfers_compute_amount).
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'so_ship',
      'document_kind',     'so_shipment',
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
    ));

    -- Event 3: Revenue leg. ar_unsettled DR / revenue CR. Caller-
    -- supplied amount; both accounts have NULL sku_id so the
    -- post_transfers refactor above lets it pass through.
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'so_ship',
      'document_kind',     'so_shipment',
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

    -- Event 4: Tax leg (if any). ar_unsettled DR / sales_tax_payable CR.
    IF v_tax_amount > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'so_ship',
        'document_kind',     'so_shipment',
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

  -- Flip active reservations on this SO to 'shipped'. Atomic with
  -- the shipment INSERT above. The 'allocated' state is reserved for
  -- a future post_so_allocate workflow (Phase 2).
  UPDATE inventory_reservations
     SET status = 'shipped',
         resolved_at = clock_timestamp()
   WHERE so_id = p_so_id
     AND status = 'active';

  PERFORM post_transfers(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;

COMMENT ON FUNCTION post_so_ship(UUID, JSONB, DATE, UUID, UUID, TEXT) IS
  'Ship goods against an SO. Per line: posts qty (customer_pool DR / '
  'stock_available CR), COGS (cogs DR / inv_value_fg CR — dispatcher-'
  'priced via cost_method), revenue (ar_unsettled DR / revenue CR), '
  'and tax (ar_unsettled DR / sales_tax_payable CR) when tax > 0. '
  'Strict over-ship rejection (P0038). GRNI-mirror: ar_unsettled is '
  'debited, not ar (cleared by post_customer_invoice). Active '
  'reservations flip to shipped atomically.';

-- ============================================================
-- post_customer_invoice
-- ============================================================
--
-- Per line:
--   * so_match: ar_unsettled DR / ar CR (clears the staging line).
--     Strict three-way match on (qty ≤ shipped-not-invoiced
--     remainder, unit_price = so_line.unit_price, amount = qty ×
--     unit_price). Mirror of post_ap_bill po_match.
--   * service: revenue_account DR / ar CR (caller-supplied revenue
--     account; consulting fees, subscriptions, etc.). Mirror of
--     post_ap_bill service.
-- Mixed so_match + service in one invoice supported.

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
  v_so_customer    UUID;
  v_total_shipped  BIGINT;
  v_total_invoiced BIGINT;
  v_avail          BIGINT;
  v_cust_unsettled BIGINT;
  v_cust_ar        BIGINT;
  v_cust_tax       BIGINT;
  v_rev_acct       accounts%ROWTYPE;
  v_inv_line_id    UUID;
  v_batch          JSONB := '[]'::JSONB;
BEGIN
  -- Fast-path replay.
  SELECT id INTO v_existing_id FROM customer_invoices
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  -- Customer exists.
  SELECT id INTO v_customer_check FROM customers WHERE id = p_customer_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'customer_invoice_invalid_line: customer % not found',
                    p_customer_id USING ERRCODE = 'P0041';
  END IF;

  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'customer_invoice_invalid_line: empty invoice for customer %',
                    p_customer_id USING ERRCODE = 'P0041';
  END IF;

  -- Resolve customer ar (one invoice = one customer = one ar account).
  SELECT id INTO v_cust_ar FROM accounts
   WHERE kind='ar' AND counterparty_id=p_customer_id
     AND currency=p_currency AND NOT is_closed;
  IF v_cust_ar IS NULL THEN
    RAISE EXCEPTION 'no open ar account for customer=% ccy=%',
                    p_customer_id, p_currency USING ERRCODE = 'P0010';
  END IF;

  -- Reserve doc id.
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

      -- Look up so_line + verify SO customer matches invoice customer.
      SELECT sl.so_id, sl.unit_price, sl.currency, so.customer_id
        INTO v_sl
        FROM sales_order_lines sl
        JOIN sales_orders so ON so.id = sl.so_id
       WHERE sl.id = v_so_line_id;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'customer_invoice_invalid_line: line % so_line % not found',
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

      -- Three-way match: strict equality on unit_price.
      IF v_unit_price <> v_sl.unit_price THEN
        RAISE EXCEPTION
          'customer_invoice_three_way_mismatch: line % unit_price % does '
          'not match so_line.unit_price %',
          v_idx, v_unit_price, v_sl.unit_price USING ERRCODE = 'P0040';
      END IF;
      -- Three-way match: strict equality on amount = qty × unit_price.
      IF v_amount <> v_qty * v_unit_price THEN
        RAISE EXCEPTION
          'customer_invoice_three_way_mismatch: line % amount % <> qty % × unit_price %',
          v_idx, v_amount, v_qty, v_unit_price USING ERRCODE = 'P0040';
      END IF;

      -- Cumulative invoiced ≤ cumulative shipped per so_line.
      SELECT COALESCE(SUM(qty_shipped), 0) INTO v_total_shipped
        FROM so_shipment_lines WHERE so_line_id = v_so_line_id;
      SELECT COALESCE(SUM(qty), 0) INTO v_total_invoiced
        FROM customer_invoice_lines
       WHERE so_line_id = v_so_line_id AND kind = 'so_match';
      v_avail := v_total_shipped - v_total_invoiced;
      IF v_qty > v_avail THEN
        RAISE EXCEPTION
          'customer_invoice_three_way_mismatch: line % qty % exceeds '
          'shipped-not-invoiced remainder % for so_line % (shipped=%, '
          'already invoiced=%)',
          v_idx, v_qty, v_avail, v_so_line_id, v_total_shipped, v_total_invoiced
          USING ERRCODE = 'P0040';
      END IF;

      -- ar_unsettled side.
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

      -- so_match clear: ar DR / ar_unsettled CR.
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ar_invoice',
        'document_kind',     'customer_invoice',
        'document_id',       v_doc_id,
        'document_line_id',  v_inv_line_id,
        'debit_account_id',  v_cust_ar,
        'credit_account_id', v_cust_unsettled,
        'amount',            v_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_customer_id,
        'posted_by',         p_posted_by
      ));

      -- Tax leg if invoice asserts tax: ar DR / sales_tax_payable CR.
      -- (Tax was previously parked in ar_unsettled at ship; invoice
      -- migrates it to ar. NOTE: this requires the ship-side tax to
      -- have been into ar_unsettled, then invoice clears tax from
      -- ar_unsettled to sales_tax_payable. Simpler MVP: caller passes
      -- tax_amount on invoice line, we credit sales_tax_payable
      -- directly as an additional ar receivable. This duplicates the
      -- ship-side tax post if also asserted there. Phase 1 scope:
      -- caller decides whether tax fires at ship (immediate) or at
      -- invoice (deferred); not both. Service lines only.)
      -- For now: tax on so_match invoice lines is NOT separately
      -- posted — assume ship-side tax is the source of truth.

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

      -- service: ar DR / revenue_account CR.
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

      -- Service-line tax leg if asserted: ar DR / sales_tax_payable CR.
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

  PERFORM post_transfers(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;

COMMENT ON FUNCTION post_customer_invoice(UUID, CHAR, JSONB, DATE, UUID, UUID, TEXT) IS
  'Customer invoice. so_match lines clear ar_unsettled → ar with '
  'strict three-way match (qty within shipped-not-invoiced '
  'remainder, unit_price matches so_line, amount matches qty × '
  'unit_price; mismatch → P0040). service lines post caller-supplied '
  'revenue_account → ar with optional tax leg. Mixed so_match + '
  'service in one invoice is supported. Tax for so_match lines is '
  'assumed posted at ship time (ar_unsettled DR / sales_tax_payable '
  'CR by post_so_ship); invoice does not double-post.';

-- ============================================================
-- post_ar_payment
-- ============================================================
--
-- Single-event document: cash DR / ar CR. Per design doc §3.7
-- verbatim. No detail table.

CREATE OR REPLACE FUNCTION post_ar_payment(
  p_customer_id     UUID,
  p_currency        CHAR(3),
  p_amount          BIGINT,
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
  v_customer_check UUID;
  v_cash_acct      BIGINT;
  v_cust_ar        BIGINT;
  v_batch          JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM ar_payments
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  IF p_amount IS NULL OR p_amount <= 0 THEN
    RAISE EXCEPTION 'ar_payment_invalid: amount must be > 0 (got %)', p_amount
      USING ERRCODE = 'P0039';
  END IF;

  SELECT id INTO v_customer_check FROM customers WHERE id = p_customer_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'ar_payment_invalid: customer % not found', p_customer_id
      USING ERRCODE = 'P0039';
  END IF;

  SELECT id INTO v_cash_acct FROM accounts
   WHERE kind='cash' AND ledger_kind='value'
     AND currency=p_currency AND NOT is_closed;
  IF v_cash_acct IS NULL THEN
    RAISE EXCEPTION 'no open cash account for ccy=%', p_currency
      USING ERRCODE = 'P0010';
  END IF;

  SELECT id INTO v_cust_ar FROM accounts
   WHERE kind='ar' AND counterparty_id=p_customer_id
     AND currency=p_currency AND NOT is_closed;
  IF v_cust_ar IS NULL THEN
    RAISE EXCEPTION 'no open ar account for customer=% ccy=%',
                    p_customer_id, p_currency USING ERRCODE = 'P0010';
  END IF;

  INSERT INTO ar_payments (
    customer_id, currency, amount, business_date, posted_by,
    idempotency_key, notes
  ) VALUES (
    p_customer_id, p_currency, p_amount, p_business_date, p_posted_by,
    p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM ar_payments
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  v_batch := jsonb_build_array(jsonb_build_object(
    'reason',            'ar_payment',
    'document_kind',     'ar_payment',
    'document_id',       v_doc_id,
    'debit_account_id',  v_cash_acct,
    'credit_account_id', v_cust_ar,
    'amount',            p_amount,
    'business_date',     p_business_date,
    'idempotency_key',   gen_random_uuid(),
    'counterparty_id',   p_customer_id,
    'posted_by',         p_posted_by
  ));

  PERFORM post_transfers(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;

COMMENT ON FUNCTION post_ar_payment(UUID, CHAR, BIGINT, DATE, UUID, UUID, TEXT) IS
  'AR payment. Single event: cash(currency) DR / ar(customer, '
  'currency) CR. Per design doc §3.7. No three-way match; partial '
  'payments accumulate against ar balance. Phase 2 follow-up: '
  'allocate-against-invoice workflow that drains specific '
  'customer_invoices vs. running ar balance.';
