-- acct-5ox / acct-sb6 — Phase 1 Epic A: cost_method enum cleanup.
--
-- Following user pushback (2026-04-30) about WAC variants per Oracle/PeopleSoft
-- documentation: there are three distinct WAC modes (Periodic, Perpetual,
-- Retroactive Perpetual). Our previous 'wac' enum value was implemented as
-- Perpetual WAC plus a non-standard "asserted cost on OUT" extension. The
-- extension was a custom hybrid that conflated specific-identification (lot
-- cost) with WAC pool semantics. This migration:
--
--   1. Renames 'wac' → 'wac_perpetual' so the variant is explicit.
--   2. Adds 'wac_periodic' and 'wac_retroactive' enum values; both raise
--      P0006 in the dispatcher until their period-close machinery ships
--      (Epics B and C — acct-qfj and acct-9tw).
--   3. Reverts the explicit-cost-on-OUT path in post_inventory_adjustment.
--      WAC OUT now only accepts NULL p_unit_cost (uses pool average,
--      classic WAC). Asserted-cost-per-transaction belongs in 'lot'
--      cost_method (acct-8gg), not in WAC.
--
-- The functions _post_transfers_compute_amount, post_transfers, and
-- post_inventory_adjustment all reference 'wac' as a string literal that
-- is cast to cost_method at execution time. After the rename, those casts
-- would fail for SKUs with the old name, so the function bodies are
-- recreated in this same migration. The down-migration embeds the prior
-- versions verbatim to keep the round-trip clean.

-- ============================================================
-- Enum changes
-- ============================================================

ALTER TYPE cost_method RENAME VALUE 'wac' TO 'wac_perpetual';
ALTER TYPE cost_method ADD VALUE IF NOT EXISTS 'wac_periodic';
ALTER TYPE cost_method ADD VALUE IF NOT EXISTS 'wac_retroactive';

-- ============================================================
-- _post_transfers_compute_amount: dispatcher with renamed wac branch
-- and new variants raising P0006.
-- ============================================================

CREATE OR REPLACE FUNCTION _post_transfers_compute_amount(
  p_event        JSONB,
  p_d_acct       accounts,
  p_c_acct       accounts,
  p_cost_method  cost_method,
  p_idx          INT
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_qty            BIGINT;
  v_sku            UUID;
  v_unit           BIGINT;
  v_qty_id         BIGINT;
  v_qty_balance    BIGINT;
  v_value_balance  BIGINT;
BEGIN
  v_qty := (p_event->>'qty')::BIGINT;
  IF v_qty IS NULL THEN
    RAISE EXCEPTION 'cost_method_not_implemented: cost-relevant value event missing qty at event index %',
                    p_idx
      USING ERRCODE = 'P0006';
  END IF;

  v_sku := COALESCE(p_d_acct.sku_id, p_c_acct.sku_id);
  IF v_sku IS NULL THEN
    RAISE EXCEPTION 'cost_method_not_implemented: sku not resolvable in compute_amount at event index %',
                    p_idx
      USING ERRCODE = 'P0006';
  END IF;

  CASE p_cost_method
    WHEN 'standard' THEN
      SELECT v_qty * standard_cost INTO v_unit
        FROM skus WHERE id = v_sku;
      RETURN v_unit;

    WHEN 'wac_perpetual' THEN
      IF p_c_acct.ledger_kind <> 'value' THEN
        RAISE EXCEPTION 'wac_perpetual requires credit-side value account, got % at event index %',
                        p_c_acct.kind, p_idx
          USING ERRCODE = 'P0006';
      END IF;

      v_qty_id := _post_transfers_lookup_qty_account(p_c_acct);
      IF v_qty_id IS NULL THEN
        RAISE EXCEPTION 'wac_perpetual cannot resolve matching qty account for credit-side % at event index %',
                        p_c_acct.kind, p_idx
          USING ERRCODE = 'P0006';
      END IF;

      SELECT (debits_total - credits_total) INTO v_qty_balance
        FROM accounts WHERE id = v_qty_id;

      IF v_qty_balance IS NULL OR v_qty_balance <= 0 THEN
        RAISE EXCEPTION 'wac_perpetual qty balance is %, cannot divide for unit cost at event index %',
                        v_qty_balance, p_idx
          USING ERRCODE = 'P0006';
      END IF;

      v_value_balance := p_c_acct.debits_total - p_c_acct.credits_total;
      IF v_value_balance < 0 THEN
        v_value_balance := 0;
      END IF;

      v_unit := v_value_balance / v_qty_balance;
      RETURN v_qty * v_unit;

    WHEN 'wac_periodic' THEN
      RAISE EXCEPTION 'cost_method_not_implemented: wac_periodic (tracked as acct-qfj; depends on period-close machinery) at event index %',
                      p_idx
        USING ERRCODE = 'P0006';

    WHEN 'wac_retroactive' THEN
      RAISE EXCEPTION 'cost_method_not_implemented: wac_retroactive (tracked as acct-9tw; depends on period-close machinery) at event index %',
                      p_idx
        USING ERRCODE = 'P0006';

    WHEN 'lot' THEN
      RAISE EXCEPTION 'cost_method_not_implemented: lot (tracked as acct-8gg + lot infrastructure) at event index %',
                      p_idx
        USING ERRCODE = 'P0006';

    WHEN 'fifo' THEN
      RAISE EXCEPTION 'cost_method_not_implemented: fifo (tracked as acct-8gg + lot infrastructure) at event index %',
                      p_idx
        USING ERRCODE = 'P0006';
  END CASE;

  RAISE EXCEPTION 'cost_method_not_implemented: unhandled cost_method % at event index %',
                  p_cost_method, p_idx
    USING ERRCODE = 'P0006';
END;
$$;

-- ============================================================
-- post_transfers: lock pre-scan + two-pass when WAC present.
-- Diff vs migration 0021: 'wac' → 'wac_perpetual' literal substitution
-- in three places (pass-0 trigger, qty-side gate single-pass, qty-side
-- gate two-pass). New variants 'wac_periodic'/'wac_retroactive' do not
-- engage the two-pass path since the dispatcher raises P0006 before
-- amount computation.
-- ============================================================

CREATE OR REPLACE FUNCTION post_transfers(
  p_events                 JSONB,
  p_override_closed_period BOOLEAN DEFAULT FALSE
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
  v_results       JSONB := '[]'::JSONB;
  v_n             INT;
  v_idx           INT;
  v_event         JSONB;
  v_d_acct        accounts%ROWTYPE;
  v_c_acct        accounts%ROWTYPE;
  v_d_id          BIGINT;
  v_c_id          BIGINT;
  v_period_id     BIGINT;
  v_period_closed TIMESTAMPTZ;
  v_business_date DATE;
  v_idem_key      UUID;
  v_reason        transfer_reason;
  v_cost_sku      UUID;
  v_cost_method   cost_method;
  v_amount        BIGINT;
  v_amounts          BIGINT[];
  v_aux_qty_id       BIGINT;
  v_aux_qty_ids      BIGINT[] := '{}';
  v_has_wac          BOOLEAN  := FALSE;
  v_has_cost_event   BOOLEAN;
BEGIN
  v_n := jsonb_array_length(p_events);
  IF v_n = 0 THEN
    RETURN '[]'::JSONB;
  END IF;

  v_has_cost_event := EXISTS (
    SELECT 1 FROM jsonb_array_elements(p_events) ev
     WHERE (ev->>'reason')::transfer_reason IN ('op_move','scrap','wo_complete','so_ship')
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
      IF v_c_acct.ledger_kind <> 'value' THEN
        CONTINUE;
      END IF;
      v_cost_sku := v_c_acct.sku_id;
      IF v_cost_sku IS NULL THEN
        v_d_id := (v_event->>'debit_account_id')::BIGINT;
        SELECT * INTO v_d_acct FROM accounts WHERE id = v_d_id;
        v_cost_sku := v_d_acct.sku_id;
      END IF;
      IF v_cost_sku IS NULL THEN
        CONTINUE;
      END IF;
      SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_cost_sku;
      IF v_cost_method = 'wac_perpetual' THEN
        v_has_wac := TRUE;
        v_aux_qty_id := _post_transfers_lookup_qty_account(v_c_acct);
        IF v_aux_qty_id IS NOT NULL THEN
          v_aux_qty_ids := array_append(v_aux_qty_ids, v_aux_qty_id);
        END IF;
      END IF;
    END LOOP;
  END IF;

  IF v_has_wac THEN
    PERFORM 1 FROM accounts
     WHERE id IN (
       SELECT (ev->>'debit_account_id')::BIGINT  FROM jsonb_array_elements(p_events) ev
       UNION
       SELECT (ev->>'credit_account_id')::BIGINT FROM jsonb_array_elements(p_events) ev
       UNION
       SELECT unnest(v_aux_qty_ids)
     )
     ORDER BY id
     FOR UPDATE;
  ELSE
    PERFORM 1 FROM accounts
     WHERE id IN (
       SELECT (ev->>'debit_account_id')::BIGINT  FROM jsonb_array_elements(p_events) ev
       UNION
       SELECT (ev->>'credit_account_id')::BIGINT FROM jsonb_array_elements(p_events) ev
     )
     ORDER BY id
     FOR UPDATE;
  END IF;

  IF NOT v_has_wac THEN
    -- Single-pass: identical to migration 0019 except for the qty-side gate
    -- now naming 'wac_perpetual'.
    FOR v_idx IN 1..v_n LOOP
      v_event    := p_events -> (v_idx - 1);
      v_idem_key := (v_event->>'idempotency_key')::UUID;

      IF EXISTS (SELECT 1 FROM transfers WHERE idempotency_key = v_idem_key) THEN
        v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'exists');
        CONTINUE;
      END IF;

      v_d_id          := (v_event->>'debit_account_id')::BIGINT;
      v_c_id          := (v_event->>'credit_account_id')::BIGINT;
      v_business_date := (v_event->>'business_date')::DATE;
      v_reason        := (v_event->>'reason')::transfer_reason;

      SELECT * INTO v_d_acct FROM accounts WHERE id = v_d_id;
      SELECT * INTO v_c_acct FROM accounts WHERE id = v_c_id;

      IF v_reason IN ('op_move','scrap','wo_complete','so_ship') THEN
        v_cost_sku := COALESCE(v_d_acct.sku_id, v_c_acct.sku_id);
        IF v_cost_sku IS NULL THEN
          RAISE EXCEPTION 'cost_method_not_implemented: sku not resolvable for reason % at event index %',
                          v_reason, v_idx
            USING ERRCODE = 'P0006';
        END IF;
        SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_cost_sku;
        IF v_d_acct.ledger_kind = 'value' THEN
          v_amount := _post_transfers_compute_amount(
                        v_event, v_d_acct, v_c_acct, v_cost_method, v_idx);
        ELSE
          IF v_cost_method NOT IN ('standard', 'wac_perpetual') THEN
            RAISE EXCEPTION 'cost_method_not_implemented: % for reason % at event index %',
                            v_cost_method, v_reason, v_idx
              USING ERRCODE = 'P0006';
          END IF;
          v_amount := (v_event->>'amount')::BIGINT;
        END IF;
      ELSE
        v_amount := (v_event->>'amount')::BIGINT;
      END IF;

      IF v_d_acct.is_closed OR v_c_acct.is_closed THEN
        RAISE EXCEPTION 'account_closed: event index %', v_idx
          USING ERRCODE = 'P0001';
      END IF;

      IF v_d_acct.ledger_kind <> v_c_acct.ledger_kind THEN
        RAISE EXCEPTION 'ledger_mismatch: event index % (% vs %)',
                        v_idx, v_d_acct.ledger_kind, v_c_acct.ledger_kind
          USING ERRCODE = 'P0002';
      END IF;

      IF v_d_acct.ledger_kind = 'value' AND v_d_acct.currency <> v_c_acct.currency THEN
        RAISE EXCEPTION 'currency_mismatch: event index % (% vs %)',
                        v_idx, v_d_acct.currency, v_c_acct.currency
          USING ERRCODE = 'P0003';
      END IF;

      SELECT id, closed_at INTO v_period_id, v_period_closed
        FROM periods
       WHERE opens_at <= v_business_date AND closes_at >= v_business_date;

      IF NOT FOUND THEN
        RAISE EXCEPTION 'period_missing: event index % business_date %', v_idx, v_business_date
          USING ERRCODE = 'P0004';
      END IF;

      IF v_period_closed IS NOT NULL AND NOT p_override_closed_period THEN
        RAISE EXCEPTION 'period_closed: event index % business_date %', v_idx, v_business_date
          USING ERRCODE = 'P0005';
      END IF;

      UPDATE accounts SET debits_total  = debits_total  + v_amount WHERE id = v_d_id;
      UPDATE accounts SET credits_total = credits_total + v_amount WHERE id = v_c_id;

      INSERT INTO transfers (
        reason, document_kind, document_id, document_line_id,
        debit_account_id, credit_account_id, amount,
        routing_op, counterparty_id,
        period_id, business_date,
        idempotency_key, posted_by
      ) VALUES (
        v_reason,
        v_event->>'document_kind',
        (v_event->>'document_id')::UUID,
        (v_event->>'document_line_id')::UUID,
        v_d_id, v_c_id, v_amount,
        (v_event->>'routing_op')::INT,
        (v_event->>'counterparty_id')::UUID,
        v_period_id, v_business_date,
        v_idem_key,
        (v_event->>'posted_by')::UUID
      );

      v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'ok');
    END LOOP;

    RETURN v_results;
  END IF;

  -- Two-pass (WAC perpetual present in batch).
  v_amounts := array_fill(NULL::BIGINT, ARRAY[v_n]);
  FOR v_idx IN 1..v_n LOOP
    v_event    := p_events -> (v_idx - 1);
    v_reason   := (v_event->>'reason')::transfer_reason;

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

    v_cost_sku := COALESCE(v_d_acct.sku_id, v_c_acct.sku_id);
    IF v_cost_sku IS NULL THEN
      RAISE EXCEPTION 'cost_method_not_implemented: sku not resolvable for reason % at event index %',
                      v_reason, v_idx
        USING ERRCODE = 'P0006';
    END IF;
    SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_cost_sku;

    IF v_d_acct.ledger_kind = 'value' THEN
      v_amounts[v_idx] := _post_transfers_compute_amount(
                            v_event, v_d_acct, v_c_acct, v_cost_method, v_idx);
    ELSE
      IF v_cost_method NOT IN ('standard', 'wac_perpetual') THEN
        RAISE EXCEPTION 'cost_method_not_implemented: % for reason % at event index %',
                        v_cost_method, v_reason, v_idx
          USING ERRCODE = 'P0006';
      END IF;
      v_amounts[v_idx] := (v_event->>'amount')::BIGINT;
    END IF;
  END LOOP;

  FOR v_idx IN 1..v_n LOOP
    v_event    := p_events -> (v_idx - 1);
    v_idem_key := (v_event->>'idempotency_key')::UUID;

    IF EXISTS (SELECT 1 FROM transfers WHERE idempotency_key = v_idem_key) THEN
      v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'exists');
      CONTINUE;
    END IF;

    v_d_id          := (v_event->>'debit_account_id')::BIGINT;
    v_c_id          := (v_event->>'credit_account_id')::BIGINT;
    v_business_date := (v_event->>'business_date')::DATE;
    v_reason        := (v_event->>'reason')::transfer_reason;
    v_amount        := v_amounts[v_idx];

    SELECT * INTO v_d_acct FROM accounts WHERE id = v_d_id;
    SELECT * INTO v_c_acct FROM accounts WHERE id = v_c_id;

    IF v_d_acct.is_closed OR v_c_acct.is_closed THEN
      RAISE EXCEPTION 'account_closed: event index %', v_idx
        USING ERRCODE = 'P0001';
    END IF;

    IF v_d_acct.ledger_kind <> v_c_acct.ledger_kind THEN
      RAISE EXCEPTION 'ledger_mismatch: event index % (% vs %)',
                      v_idx, v_d_acct.ledger_kind, v_c_acct.ledger_kind
        USING ERRCODE = 'P0002';
    END IF;

    IF v_d_acct.ledger_kind = 'value' AND v_d_acct.currency <> v_c_acct.currency THEN
      RAISE EXCEPTION 'currency_mismatch: event index % (% vs %)',
                      v_idx, v_d_acct.currency, v_c_acct.currency
        USING ERRCODE = 'P0003';
    END IF;

    SELECT id, closed_at INTO v_period_id, v_period_closed
      FROM periods
     WHERE opens_at <= v_business_date AND closes_at >= v_business_date;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'period_missing: event index % business_date %', v_idx, v_business_date
        USING ERRCODE = 'P0004';
    END IF;

    IF v_period_closed IS NOT NULL AND NOT p_override_closed_period THEN
      RAISE EXCEPTION 'period_closed: event index % business_date %', v_idx, v_business_date
        USING ERRCODE = 'P0005';
    END IF;

    UPDATE accounts SET debits_total  = debits_total  + v_amount WHERE id = v_d_id;
    UPDATE accounts SET credits_total = credits_total + v_amount WHERE id = v_c_id;

    INSERT INTO transfers (
      reason, document_kind, document_id, document_line_id,
      debit_account_id, credit_account_id, amount,
      routing_op, counterparty_id,
      period_id, business_date,
      idempotency_key, posted_by
    ) VALUES (
      v_reason,
      v_event->>'document_kind',
      (v_event->>'document_id')::UUID,
      (v_event->>'document_line_id')::UUID,
      v_d_id, v_c_id, v_amount,
      (v_event->>'routing_op')::INT,
      (v_event->>'counterparty_id')::UUID,
      v_period_id, v_business_date,
      v_idem_key,
      (v_event->>'posted_by')::UUID
    );

    v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'ok');
  END LOOP;

  RETURN v_results;
END;
$$;

-- ============================================================
-- post_inventory_adjustment: revert explicit-cost-on-OUT.
-- New contract:
--   wac_perpetual IN  + NULL → pool avg (or P0011 if pool empty)
--   wac_perpetual IN  + value → re-averages the pool
--   wac_perpetual OUT + NULL → pool avg (classic WAC)
--   wac_perpetual OUT + value → P0011 (asserted-cost-on-depletion belongs
--                                      in 'lot' cost_method, acct-8gg)
-- ============================================================

CREATE OR REPLACE FUNCTION post_inventory_adjustment(
  p_sku_id          UUID,
  p_location_id     UUID,
  p_qty_delta       BIGINT,
  p_unit_cost       BIGINT,
  p_currency        TEXT,
  p_inventory_class TEXT,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id   UUID;
  v_doc_id        UUID;
  v_cost_method   cost_method;
  v_std_cost      BIGINT;
  v_qty_acct      BIGINT;
  v_val_acct      BIGINT;
  v_void_qty      BIGINT;
  v_void_val      BIGINT;
  v_value_kind    TEXT;
  v_lock_first    BIGINT;
  v_lock_second   BIGINT;
  v_qty_balance   BIGINT;
  v_val_balance   BIGINT;
  v_effective_uc  BIGINT;
  v_qty_amount    BIGINT;
  v_val_amount    BIGINT;
  v_qty_debit     BIGINT;
  v_qty_credit    BIGINT;
  v_val_debit     BIGINT;
  v_val_credit    BIGINT;
  v_batch         JSONB;
BEGIN
  SELECT id INTO v_existing_id
    FROM inventory_adjustments
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN
    RETURN v_existing_id;
  END IF;

  SELECT cost_method, standard_cost INTO v_cost_method, v_std_cost
    FROM skus WHERE id = p_sku_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'sku % not found', p_sku_id USING ERRCODE = 'P0010';
  END IF;

  SELECT id INTO v_qty_acct
    FROM accounts
   WHERE kind        = 'stock_available'
     AND sku_id      = p_sku_id
     AND location_id = p_location_id
     AND NOT is_closed;
  IF v_qty_acct IS NULL THEN
    RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                    p_sku_id, p_location_id
      USING ERRCODE = 'P0010';
  END IF;

  v_value_kind := 'inv_value_' || p_inventory_class;
  EXECUTE format(
    'SELECT id FROM accounts
      WHERE kind = %L AND sku_id = $1 AND location_id = $2
        AND currency = $3 AND NOT is_closed',
    v_value_kind
  )
  INTO v_val_acct
  USING p_sku_id, p_location_id, p_currency;
  IF v_val_acct IS NULL THEN
    RAISE EXCEPTION 'no open % account for sku=% loc=% ccy=%',
                    v_value_kind, p_sku_id, p_location_id, p_currency
      USING ERRCODE = 'P0010';
  END IF;

  SELECT id INTO v_void_qty
    FROM accounts
   WHERE kind = 'creation_void' AND ledger_kind = 'qty' AND NOT is_closed;
  IF v_void_qty IS NULL THEN
    RAISE EXCEPTION 'no creation_void(qty) account configured'
      USING ERRCODE = 'P0010';
  END IF;

  SELECT id INTO v_void_val
    FROM accounts
   WHERE kind = 'inv_adj_expense' AND ledger_kind = 'value'
     AND currency = p_currency AND NOT is_closed;
  IF v_void_val IS NULL THEN
    RAISE EXCEPTION 'no inv_adj_expense(value, ccy=%) account configured', p_currency
      USING ERRCODE = 'P0010';
  END IF;

  CASE v_cost_method
  WHEN 'standard' THEN
    IF p_unit_cost IS NOT NULL THEN
      RAISE EXCEPTION
        'standard SKU % has a fixed standard_cost; do not pass p_unit_cost (got %)',
        p_sku_id, p_unit_cost
        USING ERRCODE = 'P0011';
    END IF;
    v_effective_uc := v_std_cost;

  WHEN 'wac_perpetual' THEN
    -- Lock qty + value accounts in ascending id order (matches
    -- post_transfers' lock-order invariant).
    v_lock_first  := LEAST(v_qty_acct, v_val_acct);
    v_lock_second := GREATEST(v_qty_acct, v_val_acct);
    PERFORM 1 FROM accounts WHERE id = v_lock_first  FOR UPDATE;
    PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;

    SELECT debits_total - credits_total INTO v_qty_balance
      FROM accounts WHERE id = v_qty_acct;
    SELECT debits_total - credits_total INTO v_val_balance
      FROM accounts WHERE id = v_val_acct;

    IF p_qty_delta > 0 THEN
      -- IN: NULL → pool avg (or P0011 if empty); explicit → re-averages.
      IF p_unit_cost IS NULL THEN
        IF v_qty_balance <= 0 THEN
          RAISE EXCEPTION
            'wac_perpetual SKU % at sku=% loc=% has empty pool (qty_balance=%); '
            'caller must pass p_unit_cost on first adjustment-in to seed',
            p_sku_id, p_sku_id, p_location_id, v_qty_balance
            USING ERRCODE = 'P0011';
        END IF;
        v_effective_uc := v_val_balance / v_qty_balance;
      ELSE
        v_effective_uc := p_unit_cost;
      END IF;
    ELSE
      -- OUT: NULL → pool avg only. Explicit → P0011 (asserted-cost-on-
      -- depletion belongs in 'lot' cost_method, acct-8gg). Pool empty → P0010.
      IF p_unit_cost IS NOT NULL THEN
        RAISE EXCEPTION
          'wac_perpetual depletion does not accept asserted unit_cost '
          '(got % on sku=% loc=%); use lot cost_method (acct-8gg) for '
          'asserted-cost-per-transaction needs',
          p_unit_cost, p_sku_id, p_location_id
          USING ERRCODE = 'P0011';
      END IF;
      IF v_qty_balance <= 0 THEN
        RAISE EXCEPTION
          'wac_perpetual SKU % at sku=% loc=% has empty pool; cannot deplete',
          p_sku_id, p_sku_id, p_location_id
          USING ERRCODE = 'P0010';
      END IF;
      v_effective_uc := v_val_balance / v_qty_balance;
    END IF;

  WHEN 'wac_periodic' THEN
    RAISE EXCEPTION
      'cost_method_not_implemented: wac_periodic (acct-qfj; depends on period-close machinery) for sku=%',
      p_sku_id USING ERRCODE = 'P0006';

  WHEN 'wac_retroactive' THEN
    RAISE EXCEPTION
      'cost_method_not_implemented: wac_retroactive (acct-9tw; depends on period-close machinery) for sku=%',
      p_sku_id USING ERRCODE = 'P0006';

  WHEN 'fifo', 'lot' THEN
    RAISE EXCEPTION
      'cost_method_not_implemented: % (sku=%); see acct-8gg',
      v_cost_method, p_sku_id
      USING ERRCODE = 'P0006';

  ELSE
    RAISE EXCEPTION
      'unknown cost_method % for sku=%', v_cost_method, p_sku_id
      USING ERRCODE = 'P0011';
  END CASE;

  v_qty_amount := abs(p_qty_delta);
  v_val_amount := v_qty_amount * v_effective_uc;

  IF p_qty_delta > 0 THEN
    v_qty_debit  := v_qty_acct;  v_qty_credit := v_void_qty;
    v_val_debit  := v_val_acct;  v_val_credit := v_void_val;
  ELSE
    v_qty_debit  := v_void_qty;  v_qty_credit := v_qty_acct;
    v_val_debit  := v_void_val;  v_val_credit := v_val_acct;
  END IF;

  INSERT INTO inventory_adjustments (
    sku_id, location_id, qty_delta, unit_cost, currency,
    inventory_class, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_sku_id, p_location_id, p_qty_delta, v_effective_uc, p_currency,
    p_inventory_class, p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id
      FROM inventory_adjustments
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  IF v_val_amount > 0 THEN
    v_batch := jsonb_build_array(
      jsonb_build_object(
        'reason',            'inventory_adjustment',
        'document_kind',     'inventory_adjustment',
        'document_id',       v_doc_id,
        'debit_account_id',  v_qty_debit,
        'credit_account_id', v_qty_credit,
        'amount',            v_qty_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      ),
      jsonb_build_object(
        'reason',            'inventory_adjustment',
        'document_kind',     'inventory_adjustment',
        'document_id',       v_doc_id,
        'debit_account_id',  v_val_debit,
        'credit_account_id', v_val_credit,
        'amount',            v_val_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      )
    );
  ELSE
    v_batch := jsonb_build_array(
      jsonb_build_object(
        'reason',            'inventory_adjustment',
        'document_kind',     'inventory_adjustment',
        'document_id',       v_doc_id,
        'debit_account_id',  v_qty_debit,
        'credit_account_id', v_qty_credit,
        'amount',            v_qty_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      )
    );
  END IF;

  PERFORM post_transfers(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;
