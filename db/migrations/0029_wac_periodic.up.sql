-- acct-3e1 / acct-qfj.1 — Phase 1 Epic B: wac_periodic implementation.
--
-- Replaces the s6n stub wac_periodic_close_hook with a real body, plus
-- the dispatcher refactors that flag mid-period depletions in
-- transfers_provisional for re-costing at period close.
--
-- LOCKED-IN DESIGN DECISIONS (acct-qfj memo):
--
-- D1 — Receipts don't flag. po_receipt and similar pass caller-asserted
-- amount; they never go through _post_transfers_compute_amount. The
-- close hook can reconstruct the period's value_in / qty_in by
-- querying transfers WHERE debit_account_id = inv_value_*(sku, loc, ccy)
-- AND business_date IN period. No flagging needed for receipts.
--
-- D2 — Provisional cost = running pool average. Same math as
-- wac_perpetual: read pool from credit-side value account under FOR
-- UPDATE, divide value by qty. The difference: the value-leg transfer
-- gets flagged in transfers_provisional with cost_method='wac_periodic'
-- and qty so the close hook can re-cost it. Smallest close-time
-- variance and reuses existing wac_perpetual code. Alternate sources
-- tracked as Epic I (acct-cms).
--
-- D3 — Pool granularity = (sku, location, currency, kind). Period avg
-- computed per pool, not per-SKU globally. The kind dimension matters
-- because inv_value_raw vs inv_value_fg vs inv_value_wip are distinct
-- pools that share the same (sku, location, currency).
--
-- D4 — Empty pool depletion = strict P0006. Same as wac_perpetual.
-- Negative inventory tracked as Epic H (acct-9ij).
--
-- D5 — transfers_provisional gains a `qty BIGINT` column (NULLABLE for
-- backward-compat with s6n stub-era rows). The close hook needs qty to
-- compute variance: variance = (final_avg - provisional_unit_cost) × qty.
-- Without it, neither transfers nor transfers_provisional carry the
-- qty data the hook needs.
--
-- D6 — WIP pools out of scope for Phase 1 wac_periodic. Depletions
-- from inv_value_wip raise P0006 because periodic re-costing of WIP
-- requires recursive cost-chain analysis (op_move from a wac_periodic
-- WIP pool feeds another wac_periodic WIP pool). File 'wac_periodic
-- across WIP pools' as a future epic if real workflows pressure it.
-- Phase 1 supports wac_periodic depletions only from inv_value_raw
-- and inv_value_fg.
--
-- D7 — Zero-receipt edge case. If a (sku, loc, ccy, kind) pool has
-- provisional depletions but zero receipts in the period, raise
-- P0020 (wac_periodic_close_no_receipts) identifying the pool.
-- Operator can post receipts and retry, or use p_force_provisional=TRUE.
--
-- D8 — Variance posting routes through variance_wac_period. Two
-- transfers per provisional row: one between original-debit and
-- variance_wac_period, one between variance_wac_period and
-- original-credit. variance_wac_period nets to zero per close but
-- provides income-statement visibility (queryable by reason/account).
-- Both legs use reason='cost_restate' (existing reason for cost
-- corrections; close enough semantically that we don't add a new one
-- this epic).
--
-- LOCK ORDER. The hook holds transfers_provisional rows under FOR
-- UPDATE; variance posting via post_transfers handles the per-event
-- account lock-order invariant.

-- ============================================================
-- Schema
-- ============================================================

ALTER TABLE transfers_provisional
  ADD COLUMN qty BIGINT;

-- ============================================================
-- _post_transfers_compute_amount: wac_periodic branch added.
-- All other branches identical to migration 0027.
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
  v_business_date  DATE;
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
      v_business_date := (p_event->>'business_date')::DATE;
      v_unit := resolve_standard_cost_at(v_sku, v_business_date);
      RETURN v_qty * v_unit;

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
      -- D6: WIP pools out of scope for wac_periodic Phase 1.
      IF p_c_acct.kind = 'inv_value_wip' THEN
        RAISE EXCEPTION
          'wac_periodic depletions from inv_value_wip not supported in Phase 1; '
          'file future epic for wac_periodic across WIP pools (event index %)',
          p_idx USING ERRCODE = 'P0006';
      END IF;
      IF p_c_acct.ledger_kind <> 'value' THEN
        RAISE EXCEPTION 'wac_periodic requires credit-side value account, got % at event index %',
                        p_c_acct.kind, p_idx
          USING ERRCODE = 'P0006';
      END IF;

      v_qty_id := _post_transfers_lookup_qty_account(p_c_acct);
      IF v_qty_id IS NULL THEN
        RAISE EXCEPTION 'wac_periodic cannot resolve matching qty account for credit-side % at event index %',
                        p_c_acct.kind, p_idx
          USING ERRCODE = 'P0006';
      END IF;

      SELECT (debits_total - credits_total) INTO v_qty_balance
        FROM accounts WHERE id = v_qty_id;

      IF v_qty_balance IS NULL OR v_qty_balance <= 0 THEN
        RAISE EXCEPTION 'wac_periodic qty balance is %, cannot divide for unit cost at event index %',
                        v_qty_balance, p_idx
          USING ERRCODE = 'P0006';
      END IF;

      v_value_balance := p_c_acct.debits_total - p_c_acct.credits_total;
      IF v_value_balance < 0 THEN
        v_value_balance := 0;
      END IF;

      v_unit := v_value_balance / v_qty_balance;
      RETURN v_qty * v_unit;

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
-- post_transfers: add wac_periodic to lock pre-scan + qty-side gate +
-- post-INSERT flagging into transfers_provisional.
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
  v_new_transfer_id  BIGINT;
  v_event_qty        BIGINT;
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
      IF v_cost_method IN ('wac_perpetual', 'wac_periodic') THEN
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
    -- Single-pass.
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
      v_cost_method   := NULL;

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
          IF v_cost_method NOT IN ('standard', 'wac_perpetual', 'wac_periodic') THEN
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
      ) RETURNING id INTO v_new_transfer_id;

      -- Flag wac_periodic depletion value-leg transfers in transfers_provisional.
      IF v_cost_method = 'wac_periodic'
         AND v_reason IN ('op_move','scrap','wo_complete','so_ship')
         AND v_d_acct.ledger_kind = 'value'
      THEN
        v_event_qty := (v_event->>'qty')::BIGINT;
        INSERT INTO transfers_provisional (transfer_id, period_id, cost_method, qty)
        VALUES (v_new_transfer_id, v_period_id, 'wac_periodic', v_event_qty);
      END IF;

      v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'ok');
    END LOOP;

    RETURN v_results;
  END IF;

  -- Two-pass (WAC perpetual or WAC periodic present in batch).
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
      IF v_cost_method NOT IN ('standard', 'wac_perpetual', 'wac_periodic') THEN
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
    v_cost_method   := NULL;

    SELECT * INTO v_d_acct FROM accounts WHERE id = v_d_id;
    SELECT * INTO v_c_acct FROM accounts WHERE id = v_c_id;

    -- Re-resolve cost_method for the flagging step at end of this loop.
    IF v_reason IN ('op_move','scrap','wo_complete','so_ship') THEN
      v_cost_sku := COALESCE(v_d_acct.sku_id, v_c_acct.sku_id);
      IF v_cost_sku IS NOT NULL THEN
        SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_cost_sku;
      END IF;
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
    ) RETURNING id INTO v_new_transfer_id;

    -- Flag wac_periodic depletion value-leg transfers in transfers_provisional.
    IF v_cost_method = 'wac_periodic'
       AND v_reason IN ('op_move','scrap','wo_complete','so_ship')
       AND v_d_acct.ledger_kind = 'value'
    THEN
      v_event_qty := (v_event->>'qty')::BIGINT;
      INSERT INTO transfers_provisional (transfer_id, period_id, cost_method, qty)
      VALUES (v_new_transfer_id, v_period_id, 'wac_periodic', v_event_qty);
    END IF;

    v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'ok');
  END LOOP;

  RETURN v_results;
END;
$$;


-- ============================================================
-- post_inventory_adjustment: add wac_periodic branch + flag depletions.
-- All other branches identical to migration 0027.
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
  v_existing_id      UUID;
  v_doc_id           UUID;
  v_cost_method      cost_method;
  v_qty_acct         BIGINT;
  v_val_acct         BIGINT;
  v_void_qty         BIGINT;
  v_void_val         BIGINT;
  v_value_kind       TEXT;
  v_lock_first       BIGINT;
  v_lock_second      BIGINT;
  v_qty_balance      BIGINT;
  v_val_balance      BIGINT;
  v_effective_uc     BIGINT;
  v_qty_amount       BIGINT;
  v_val_amount       BIGINT;
  v_qty_debit        BIGINT;
  v_qty_credit       BIGINT;
  v_val_debit        BIGINT;
  v_val_credit       BIGINT;
  v_batch            JSONB;
  v_needs_provisional BOOLEAN := FALSE;
  v_value_transfer_id BIGINT;
  v_period_id        BIGINT;
BEGIN
  SELECT id INTO v_existing_id
    FROM inventory_adjustments
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN
    RETURN v_existing_id;
  END IF;

  SELECT cost_method INTO v_cost_method
    FROM skus WHERE id = p_sku_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'sku % not found', p_sku_id USING ERRCODE = 'P0010';
  END IF;

  -- Early WIP-class guard for wac_periodic (D6). Surfaces the deferral
  -- before the account lookups which would otherwise raise P0010 for
  -- the (likely-missing) inv_value_wip account.
  IF v_cost_method = 'wac_periodic' AND p_inventory_class = 'wip' THEN
    RAISE EXCEPTION
      'wac_periodic adjustment on inv_value_wip class not supported in Phase 1 '
      '(file future epic for wac_periodic across WIP pools); sku=%',
      p_sku_id USING ERRCODE = 'P0006';
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
        'standard SKU % has a fixed standard cost; do not pass p_unit_cost (got %)',
        p_sku_id, p_unit_cost
        USING ERRCODE = 'P0011';
    END IF;
    v_effective_uc := resolve_standard_cost_at(p_sku_id, p_business_date);

  WHEN 'wac_perpetual' THEN
    v_lock_first  := LEAST(v_qty_acct, v_val_acct);
    v_lock_second := GREATEST(v_qty_acct, v_val_acct);
    PERFORM 1 FROM accounts WHERE id = v_lock_first  FOR UPDATE;
    PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;

    SELECT debits_total - credits_total INTO v_qty_balance
      FROM accounts WHERE id = v_qty_acct;
    SELECT debits_total - credits_total INTO v_val_balance
      FROM accounts WHERE id = v_val_acct;

    IF p_qty_delta > 0 THEN
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
    -- D6: WIP class out of scope for wac_periodic Phase 1.
    IF p_inventory_class = 'wip' THEN
      RAISE EXCEPTION
        'wac_periodic adjustment on inv_value_wip class not supported in Phase 1 '
        '(file future epic for wac_periodic across WIP pools); sku=%',
        p_sku_id USING ERRCODE = 'P0006';
    END IF;

    v_lock_first  := LEAST(v_qty_acct, v_val_acct);
    v_lock_second := GREATEST(v_qty_acct, v_val_acct);
    PERFORM 1 FROM accounts WHERE id = v_lock_first  FOR UPDATE;
    PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;

    SELECT debits_total - credits_total INTO v_qty_balance
      FROM accounts WHERE id = v_qty_acct;
    SELECT debits_total - credits_total INTO v_val_balance
      FROM accounts WHERE id = v_val_acct;

    IF p_qty_delta > 0 THEN
      -- IN: same shape as wac_perpetual IN. Receipts are not flagged.
      IF p_unit_cost IS NULL THEN
        IF v_qty_balance <= 0 THEN
          RAISE EXCEPTION
            'wac_periodic SKU % at sku=% loc=% has empty pool (qty_balance=%); '
            'caller must pass p_unit_cost on first adjustment-in to seed',
            p_sku_id, p_sku_id, p_location_id, v_qty_balance
            USING ERRCODE = 'P0011';
        END IF;
        v_effective_uc := v_val_balance / v_qty_balance;
      ELSE
        v_effective_uc := p_unit_cost;
      END IF;
    ELSE
      -- OUT: provisional cost = pool avg; flag for re-cost at close.
      IF p_unit_cost IS NOT NULL THEN
        RAISE EXCEPTION
          'wac_periodic depletion does not accept asserted unit_cost '
          '(got % on sku=% loc=%); use lot cost_method (acct-8gg) for '
          'asserted-cost-per-transaction needs',
          p_unit_cost, p_sku_id, p_location_id
          USING ERRCODE = 'P0011';
      END IF;
      IF v_qty_balance <= 0 THEN
        RAISE EXCEPTION
          'wac_periodic SKU % at sku=% loc=% has empty pool; cannot deplete',
          p_sku_id, p_sku_id, p_location_id
          USING ERRCODE = 'P0010';
      END IF;
      v_effective_uc := v_val_balance / v_qty_balance;
      v_needs_provisional := TRUE;
    END IF;

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

  -- Flag the value-leg transfer for wac_periodic depletions (post_transfers'
  -- own flagging only fires for cost-relevant reasons; inventory_adjustment
  -- isn't in that set, so we flag here).
  IF v_needs_provisional THEN
    SELECT id INTO v_value_transfer_id
      FROM transfers
     WHERE document_id = v_doc_id
       AND reason = 'inventory_adjustment'
       AND credit_account_id = v_val_acct;

    SELECT id INTO v_period_id
      FROM periods
     WHERE opens_at <= p_business_date AND closes_at >= p_business_date;

    INSERT INTO transfers_provisional (transfer_id, period_id, cost_method, qty)
    VALUES (v_value_transfer_id, v_period_id, 'wac_periodic', v_qty_amount);
  END IF;

  RETURN v_doc_id;
END;
$$;

-- ============================================================
-- wac_periodic_close_hook: real body. Replaces the s6n stub.
-- ============================================================

-- Drop the s6n 1-arg stub before creating the 2-arg real body. PG
-- treats different argument lists as different functions; CREATE OR
-- REPLACE alone would leave both versions live and ambiguous.
DROP FUNCTION IF EXISTS wac_periodic_close_hook(BIGINT);

CREATE OR REPLACE FUNCTION wac_periodic_close_hook(
  p_period_id         BIGINT,
  p_force_provisional BOOLEAN DEFAULT FALSE
) RETURNS BIGINT LANGUAGE plpgsql AS $$
DECLARE
  v_period_opens   DATE;
  v_period_closes  DATE;
  v_period_code    TEXT;
  v_count          BIGINT := 0;
  v_row            RECORD;
  v_orig           RECORD;
  v_pool_acct      accounts%ROWTYPE;
  v_pool_value_in  BIGINT;
  v_pool_qty_in    BIGINT;
  v_final_avg      BIGINT;
  v_provisional    BIGINT;
  v_variance       BIGINT;
  v_var_acct       BIGINT;
  v_batch          JSONB;
  v_var_xfer_id    BIGINT;
  v_orig_debit     BIGINT;
  v_orig_credit    BIGINT;
  v_event_a        JSONB;
  v_event_b        JSONB;
BEGIN
  SELECT opens_at, closes_at, code
    INTO v_period_opens, v_period_closes, v_period_code
    FROM periods WHERE id = p_period_id;
  IF NOT FOUND THEN
    -- close_period already validates; defensive only.
    RETURN 0;
  END IF;

  -- Walk un-finalized wac_periodic provisional rows in this period.
  FOR v_row IN
    SELECT *
      FROM transfers_provisional
     WHERE period_id = p_period_id
       AND cost_method = 'wac_periodic'
       AND finalized_at IS NULL
     ORDER BY transfer_id
       FOR UPDATE
  LOOP
    -- Read the original transfer to identify its source pool.
    SELECT * INTO v_orig FROM transfers WHERE id = v_row.transfer_id;

    -- For wac_periodic depletions, the credit side is the source pool
    -- (the inv_value_* account being depleted). For inventory_adjustment
    -- OUT same: credit side is val_acct.
    SELECT * INTO v_pool_acct
      FROM accounts WHERE id = v_orig.credit_account_id;

    -- Compute final period avg per pool: Σ(value_in)/Σ(qty_in) over the
    -- period's receipts to this pool. Receipts are debits to the pool.
    SELECT COALESCE(SUM(amount), 0) INTO v_pool_value_in
      FROM transfers
     WHERE debit_account_id = v_pool_acct.id
       AND business_date BETWEEN v_period_opens AND v_period_closes;

    -- qty_in: corresponding qty-side debits to the paired stock_available
    -- account. Use _post_transfers_lookup_qty_account to find it, then
    -- sum debits for the period.
    DECLARE
      v_qty_acct_id BIGINT;
    BEGIN
      v_qty_acct_id := _post_transfers_lookup_qty_account(v_pool_acct);
      IF v_qty_acct_id IS NULL THEN
        RAISE EXCEPTION
          'wac_periodic_close: cannot resolve qty account for value pool %',
          v_pool_acct.id USING ERRCODE = 'P0010';
      END IF;
      SELECT COALESCE(SUM(amount), 0) INTO v_pool_qty_in
        FROM transfers
       WHERE debit_account_id = v_qty_acct_id
         AND business_date BETWEEN v_period_opens AND v_period_closes;
    END;

    IF v_pool_qty_in = 0 THEN
      IF p_force_provisional THEN
        -- Force-bypass: skip this row (leave un-finalized for forensics).
        -- The provisional gate will see it; force_provisional bypasses
        -- there too, so close_period proceeds.
        CONTINUE;
      END IF;
      RAISE EXCEPTION
        'wac_periodic_close_no_receipts: period % (id=%) has provisional '
        'depletions on pool kind=% sku=% loc=% ccy=% but zero receipts in '
        'period; post receipts and retry the close, or close with '
        'p_force_provisional=TRUE.',
        v_period_code, p_period_id, v_pool_acct.kind, v_pool_acct.sku_id,
        v_pool_acct.location_id, v_pool_acct.currency
        USING ERRCODE = 'P0020';
    END IF;

    v_final_avg := v_pool_value_in / v_pool_qty_in;

    -- provisional_unit_cost = orig_amount / qty
    v_provisional := v_orig.amount / v_row.qty;
    v_variance    := (v_final_avg - v_provisional) * v_row.qty;

    IF v_variance = 0 THEN
      -- No correction needed. Mark finalized with variance_amount=0.
      UPDATE transfers_provisional
         SET finalized_at = clock_timestamp(),
             variance_amount = 0,
             variance_transfer_id = NULL
       WHERE transfer_id = v_row.transfer_id;
      v_count := v_count + 1;
      CONTINUE;
    END IF;

    -- Resolve variance_wac_period(currency) account.
    SELECT id INTO v_var_acct
      FROM accounts
     WHERE kind = 'variance_wac_period'
       AND ledger_kind = 'value'
       AND currency = v_pool_acct.currency
       AND NOT is_closed;
    IF v_var_acct IS NULL THEN
      RAISE EXCEPTION
        'wac_periodic_close: no variance_wac_period(value, ccy=%) account configured',
        v_pool_acct.currency USING ERRCODE = 'P0010';
    END IF;

    -- D8: post 2 transfers routing through variance_wac_period.
    -- Net effect: original_debit += variance, original_credit -= variance.
    -- Both legs use reason='cost_restate' (closest existing semantic).
    v_orig_debit  := v_orig.debit_account_id;
    v_orig_credit := v_orig.credit_account_id;

    IF v_variance > 0 THEN
      -- Write-up: dr orig_debit / cr variance_wac_period
      v_event_a := jsonb_build_object(
        'reason',            'cost_restate',
        'document_kind',     'wac_periodic_close',
        'document_id',       gen_random_uuid(),
        'debit_account_id',  v_orig_debit,
        'credit_account_id', v_var_acct,
        'amount',            v_variance,
        'business_date',     v_period_closes,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         '00000000-0000-0000-0000-000000000000'
      );
      -- dr variance_wac_period / cr orig_credit
      v_event_b := jsonb_build_object(
        'reason',            'cost_restate',
        'document_kind',     'wac_periodic_close',
        'document_id',       gen_random_uuid(),
        'debit_account_id',  v_var_acct,
        'credit_account_id', v_orig_credit,
        'amount',            v_variance,
        'business_date',     v_period_closes,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         '00000000-0000-0000-0000-000000000000'
      );
    ELSE
      -- Write-down: dr variance_wac_period / cr orig_debit
      v_event_a := jsonb_build_object(
        'reason',            'cost_restate',
        'document_kind',     'wac_periodic_close',
        'document_id',       gen_random_uuid(),
        'debit_account_id',  v_var_acct,
        'credit_account_id', v_orig_debit,
        'amount',            -v_variance,
        'business_date',     v_period_closes,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         '00000000-0000-0000-0000-000000000000'
      );
      -- dr orig_credit / cr variance_wac_period
      v_event_b := jsonb_build_object(
        'reason',            'cost_restate',
        'document_kind',     'wac_periodic_close',
        'document_id',       gen_random_uuid(),
        'debit_account_id',  v_orig_credit,
        'credit_account_id', v_var_acct,
        'amount',            -v_variance,
        'business_date',     v_period_closes,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         '00000000-0000-0000-0000-000000000000'
      );
    END IF;

    v_batch := jsonb_build_array(v_event_a, v_event_b);
    PERFORM post_transfers(v_batch, FALSE);

    -- Capture the second transfer's id (the one that touches the source pool)
    -- for variance_transfer_id audit.
    SELECT id INTO v_var_xfer_id
      FROM transfers
     WHERE idempotency_key = (v_event_b->>'idempotency_key')::UUID;

    UPDATE transfers_provisional
       SET finalized_at = clock_timestamp(),
           variance_amount = v_variance,
           variance_transfer_id = v_var_xfer_id
     WHERE transfer_id = v_row.transfer_id;

    v_count := v_count + 1;
  END LOOP;

  RETURN v_count;
END;
$$;

-- ============================================================
-- close_period: re-create with p_force_provisional threaded through
-- to wac_periodic_close_hook. Other hook calls unchanged.
-- ============================================================

CREATE OR REPLACE FUNCTION close_period(
  p_period_id         BIGINT,
  p_actor             UUID,
  p_force_provisional BOOLEAN DEFAULT FALSE,
  p_force_recon       BOOLEAN DEFAULT FALSE
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
  v_period_code            TEXT;
  v_already_closed         TIMESTAMPTZ;
  v_wac_period_count       BIGINT;
  v_wac_retro_count        BIGINT;
  v_cost_adj_retro_count   BIGINT;
  v_finalized_count        BIGINT;
  v_unfinalized_remaining  BIGINT;
  v_alerts                 INT;
  v_now                    TIMESTAMPTZ;
BEGIN
  SELECT code, closed_at INTO v_period_code, v_already_closed
    FROM periods WHERE id = p_period_id FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'period_close_invalid: period id=% not found', p_period_id
      USING ERRCODE = 'P0014';
  END IF;

  IF v_already_closed IS NOT NULL THEN
    RAISE EXCEPTION
      'period_close_invalid: period % (id=%) already closed at %',
      v_period_code, p_period_id, v_already_closed
      USING ERRCODE = 'P0014';
  END IF;

  -- wac_periodic_close_hook needs force_provisional to know whether to
  -- skip rows it can't process (vs raise P0020). Other hooks remain
  -- on the simpler (period_id) signature until they get real bodies.
  v_wac_period_count     := wac_periodic_close_hook(p_period_id, p_force_provisional);
  v_wac_retro_count      := wac_retroactive_close_hook(p_period_id);
  v_cost_adj_retro_count := cost_adjust_retroactive_hook(p_period_id);
  v_finalized_count      := v_wac_period_count
                          + v_wac_retro_count
                          + v_cost_adj_retro_count;

  SELECT COUNT(*) INTO v_unfinalized_remaining
    FROM transfers_provisional
   WHERE period_id = p_period_id
     AND finalized_at IS NULL;

  IF v_unfinalized_remaining > 0 AND NOT p_force_provisional THEN
    RAISE EXCEPTION
      'period_close_provisional: % un-finalized provisional rows '
      'remain in period % (id=%); pass p_force_provisional=TRUE to override',
      v_unfinalized_remaining, v_period_code, p_period_id
      USING ERRCODE = 'P0015';
  END IF;

  v_alerts := run_daily_reconciliation();

  IF v_alerts > 0 AND NOT p_force_recon THEN
    RAISE EXCEPTION
      'period_close_reconciliation: % new reconciliation alert(s) raised '
      'while closing period % (id=%); pass p_force_recon=TRUE to override',
      v_alerts, v_period_code, p_period_id
      USING ERRCODE = 'P0016';
  END IF;

  v_now := clock_timestamp();
  UPDATE periods
     SET closed_at = v_now,
         closed_by = p_actor
   WHERE id = p_period_id;

  RETURN jsonb_build_object(
    'period_id',              p_period_id,
    'period_code',            v_period_code,
    'closed_at',              v_now,
    'closed_by',              p_actor,
    'finalized_count',        v_finalized_count,
    'hook_results',           jsonb_build_object(
      'wac_periodic',            v_wac_period_count,
      'wac_retroactive',         v_wac_retro_count,
      'cost_adjust_retroactive', v_cost_adj_retro_count
    ),
    'unfinalized_remaining',  v_unfinalized_remaining,
    'alerts',                 v_alerts,
    'forced',                 jsonb_build_object(
      'provisional',             p_force_provisional,
      'recon',                   p_force_recon
    )
  );
END;
$$;
