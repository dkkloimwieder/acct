-- acct-7mb / acct-9tw.1 — Phase 1 Epic C: wac_retroactive implementation.
--
-- Replaces the s6n stub wac_retroactive_close_hook with a real body
-- that does chronological replay of in-period events, plus the
-- dispatcher additions that flag wac_retroactive depletions for
-- re-costing at close.
--
-- HOW IT DIFFERS FROM wac_periodic. Both use the same
-- transfers_provisional flagging pattern at posting time. The close
-- hook differs:
--
--   - wac_periodic close: ONE final period avg = Σ(in-period receipts
--     value) / Σ(in-period qty), applied to ALL depletions in the
--     period. Per Oracle PAC / SAP S/4 convention.
--
--   - wac_retroactive close: chronological replay. Each depletion
--     gets re-costed at THE RUNNING AVG IT SHOULD HAVE HAD given
--     full-period data, including late-arriving receipts that were
--     originally booked out of order. Hybrid of perpetual (per-event
--     chain) and periodic (correction at close).
--
-- USE CASE. Receipt physically arrived 2026-04-05 but paperwork posted
-- 2026-04-15 (after the 2026-04-10 depletion). Mid-period, the
-- depletion used a running avg that DIDN'T include the late receipt.
-- At close, walking events by business_date places the receipt before
-- the depletion — depletion's recomputed avg DOES include it. Variance
-- corrects the difference.
--
-- LOCKED-IN DESIGN DECISIONS (acct-9tw memo, 2026-05-01, user confirmed):
--
-- D1: Replay order by (business_date ASC, posted_at ASC, id ASC).
--     business_date primary; posted_at (TIMESTAMPTZ, microsecond
--     resolution) breaks same-day ties; transfer.id as final
--     deterministic tiebreaker.
-- D2: ALL events affecting the pool count for replay, not just
--     wac_retroactive-flagged ones. Replays the "true" perpetual
--     chain.
-- D3: WIP class (inv_value_wip) deferred. P0006 with reference to
--     acct-p7v (Phase 2 Epic J: wac across WIP pools).
-- D4: Variance routing through variance_wac_retroactive (already
--     seeded in s6n). Same 2-transfer pattern as wac_periodic; nets
--     to zero per close; provides audit visibility.
-- D5: Pre-period running state from transfers WHERE pool affected
--     AND business_date < period_opens (signed by debit/credit).
--     Reuses the per-class qty SUM pattern from migration 0030.
-- D6: Replay loop is sequential per pool. Receipts add to running
--     state; depletions recompute avg from running state, post
--     variance per provisional row, update running state with
--     recomputed amount.
-- D7: Truncating BIGINT division (matches wac_perpetual semantics).
--
-- Hook signature changes from 1-arg (s6n stub) to 2-arg
-- (with p_force_provisional). close_period is updated to thread
-- the force flag. Same DROP-then-CREATE pattern as wac_periodic in 0029.

-- ============================================================
-- _post_transfers_compute_amount: add wac_retroactive branch.
-- All other branches identical to migration 0030.
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

      SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = p_c_acct.id THEN  t.qty
                               WHEN t.credit_account_id = p_c_acct.id THEN -t.qty END), 0)
        INTO v_qty_balance
        FROM transfers t
       WHERE p_c_acct.id IN (t.debit_account_id, t.credit_account_id)
         AND t.qty IS NOT NULL;

      IF v_qty_balance <= 0 THEN
        RAISE EXCEPTION 'wac_perpetual qty balance is %, cannot divide for unit cost at event index %',
                        v_qty_balance, p_idx
          USING ERRCODE = 'P0006';
      END IF;

      v_value_balance := p_c_acct.debits_total - p_c_acct.credits_total;
      IF v_value_balance < 0 THEN v_value_balance := 0; END IF;

      v_unit := v_value_balance / v_qty_balance;
      RETURN v_qty * v_unit;

    WHEN 'wac_periodic' THEN
      IF p_c_acct.kind = 'inv_value_wip' THEN
        RAISE EXCEPTION
          'wac_periodic depletions from inv_value_wip not supported in Phase 1 '
          '(see acct-p7v Phase 2 Epic J); event index %',
          p_idx USING ERRCODE = 'P0006';
      END IF;
      IF p_c_acct.ledger_kind <> 'value' THEN
        RAISE EXCEPTION 'wac_periodic requires credit-side value account, got % at event index %',
                        p_c_acct.kind, p_idx
          USING ERRCODE = 'P0006';
      END IF;

      SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = p_c_acct.id THEN  t.qty
                               WHEN t.credit_account_id = p_c_acct.id THEN -t.qty END), 0)
        INTO v_qty_balance
        FROM transfers t
       WHERE p_c_acct.id IN (t.debit_account_id, t.credit_account_id)
         AND t.qty IS NOT NULL;

      IF v_qty_balance <= 0 THEN
        RAISE EXCEPTION 'wac_periodic qty balance is %, cannot divide for unit cost at event index %',
                        v_qty_balance, p_idx
          USING ERRCODE = 'P0006';
      END IF;

      v_value_balance := p_c_acct.debits_total - p_c_acct.credits_total;
      IF v_value_balance < 0 THEN v_value_balance := 0; END IF;

      v_unit := v_value_balance / v_qty_balance;
      RETURN v_qty * v_unit;

    WHEN 'wac_retroactive' THEN
      -- Same mid-period math as wac_perpetual / wac_periodic. The
      -- difference is at close: chronological replay (acct-9tw).
      -- D3: WIP class deferred (acct-p7v).
      IF p_c_acct.kind = 'inv_value_wip' THEN
        RAISE EXCEPTION
          'wac_retroactive depletions from inv_value_wip not supported in Phase 1 '
          '(see acct-p7v Phase 2 Epic J); event index %',
          p_idx USING ERRCODE = 'P0006';
      END IF;
      IF p_c_acct.ledger_kind <> 'value' THEN
        RAISE EXCEPTION 'wac_retroactive requires credit-side value account, got % at event index %',
                        p_c_acct.kind, p_idx
          USING ERRCODE = 'P0006';
      END IF;

      SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = p_c_acct.id THEN  t.qty
                               WHEN t.credit_account_id = p_c_acct.id THEN -t.qty END), 0)
        INTO v_qty_balance
        FROM transfers t
       WHERE p_c_acct.id IN (t.debit_account_id, t.credit_account_id)
         AND t.qty IS NOT NULL;

      IF v_qty_balance <= 0 THEN
        RAISE EXCEPTION 'wac_retroactive qty balance is %, cannot divide for unit cost at event index %',
                        v_qty_balance, p_idx
          USING ERRCODE = 'P0006';
      END IF;

      v_value_balance := p_c_acct.debits_total - p_c_acct.credits_total;
      IF v_value_balance < 0 THEN v_value_balance := 0; END IF;

      v_unit := v_value_balance / v_qty_balance;
      RETURN v_qty * v_unit;

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
-- post_transfers: include wac_retroactive in trigger + gate + flagging.
-- Diff vs 0030: wac_retroactive added everywhere wac_periodic is.
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
  v_qty_for_row   BIGINT;
  v_amounts          BIGINT[];
  v_aux_qty_id       BIGINT;
  v_aux_qty_ids      BIGINT[] := '{}';
  v_has_wac          BOOLEAN  := FALSE;
  v_has_cost_event   BOOLEAN;
  v_new_transfer_id  BIGINT;
  v_event_qty        BIGINT;
BEGIN
  v_n := jsonb_array_length(p_events);
  IF v_n = 0 THEN RETURN '[]'::JSONB; END IF;

  v_has_cost_event := EXISTS (
    SELECT 1 FROM jsonb_array_elements(p_events) ev
     WHERE (ev->>'reason')::transfer_reason IN ('op_move','scrap','wo_complete','so_ship')
  );

  IF v_has_cost_event THEN
    FOR v_idx IN 1..v_n LOOP
      v_event  := p_events -> (v_idx - 1);
      v_reason := (v_event->>'reason')::transfer_reason;
      IF v_reason NOT IN ('op_move','scrap','wo_complete','so_ship') THEN CONTINUE; END IF;
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

  IF v_has_wac THEN
    PERFORM 1 FROM accounts
     WHERE id IN (
       SELECT (ev->>'debit_account_id')::BIGINT  FROM jsonb_array_elements(p_events) ev
       UNION SELECT (ev->>'credit_account_id')::BIGINT FROM jsonb_array_elements(p_events) ev
       UNION SELECT unnest(v_aux_qty_ids))
     ORDER BY id FOR UPDATE;
  ELSE
    PERFORM 1 FROM accounts
     WHERE id IN (
       SELECT (ev->>'debit_account_id')::BIGINT  FROM jsonb_array_elements(p_events) ev
       UNION SELECT (ev->>'credit_account_id')::BIGINT FROM jsonb_array_elements(p_events) ev)
     ORDER BY id FOR UPDATE;
  END IF;

  IF NOT v_has_wac THEN
    FOR v_idx IN 1..v_n LOOP
      v_event    := p_events -> (v_idx - 1);
      v_idem_key := (v_event->>'idempotency_key')::UUID;
      IF EXISTS (SELECT 1 FROM transfers WHERE idempotency_key = v_idem_key) THEN
        v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'exists');
        CONTINUE;
      END IF;
      v_d_id := (v_event->>'debit_account_id')::BIGINT;
      v_c_id := (v_event->>'credit_account_id')::BIGINT;
      v_business_date := (v_event->>'business_date')::DATE;
      v_reason := (v_event->>'reason')::transfer_reason;
      v_cost_method := NULL;
      SELECT * INTO v_d_acct FROM accounts WHERE id = v_d_id;
      SELECT * INTO v_c_acct FROM accounts WHERE id = v_c_id;
      IF v_reason IN ('op_move','scrap','wo_complete','so_ship') THEN
        v_cost_sku := COALESCE(v_d_acct.sku_id, v_c_acct.sku_id);
        IF v_cost_sku IS NULL THEN
          RAISE EXCEPTION 'cost_method_not_implemented: sku not resolvable for reason % at event index %',
                          v_reason, v_idx USING ERRCODE = 'P0006';
        END IF;
        SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_cost_sku;
        IF v_d_acct.ledger_kind = 'value' THEN
          v_amount := _post_transfers_compute_amount(v_event, v_d_acct, v_c_acct, v_cost_method, v_idx);
        ELSE
          IF v_cost_method NOT IN ('standard', 'wac_perpetual', 'wac_periodic', 'wac_retroactive') THEN
            RAISE EXCEPTION 'cost_method_not_implemented: % for reason % at event index %',
                            v_cost_method, v_reason, v_idx USING ERRCODE = 'P0006';
          END IF;
          v_amount := (v_event->>'amount')::BIGINT;
        END IF;
      ELSE
        v_amount := (v_event->>'amount')::BIGINT;
      END IF;
      IF v_d_acct.is_closed OR v_c_acct.is_closed THEN
        RAISE EXCEPTION 'account_closed: event index %', v_idx USING ERRCODE = 'P0001';
      END IF;
      IF v_d_acct.ledger_kind <> v_c_acct.ledger_kind THEN
        RAISE EXCEPTION 'ledger_mismatch: event index % (% vs %)', v_idx, v_d_acct.ledger_kind, v_c_acct.ledger_kind USING ERRCODE = 'P0002';
      END IF;
      IF v_d_acct.ledger_kind = 'value' AND v_d_acct.currency <> v_c_acct.currency THEN
        RAISE EXCEPTION 'currency_mismatch: event index % (% vs %)', v_idx, v_d_acct.currency, v_c_acct.currency USING ERRCODE = 'P0003';
      END IF;
      SELECT id, closed_at INTO v_period_id, v_period_closed
        FROM periods WHERE opens_at <= v_business_date AND closes_at >= v_business_date;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'period_missing: event index % business_date %', v_idx, v_business_date USING ERRCODE = 'P0004';
      END IF;
      IF v_period_closed IS NOT NULL AND NOT p_override_closed_period THEN
        RAISE EXCEPTION 'period_closed: event index % business_date %', v_idx, v_business_date USING ERRCODE = 'P0005';
      END IF;
      v_qty_for_row := (v_event->>'qty')::BIGINT;
      IF v_qty_for_row IS NULL AND v_d_acct.ledger_kind = 'qty' AND v_c_acct.ledger_kind = 'qty' THEN
        v_qty_for_row := v_amount;
      END IF;
      UPDATE accounts SET debits_total  = debits_total  + v_amount WHERE id = v_d_id;
      UPDATE accounts SET credits_total = credits_total + v_amount WHERE id = v_c_id;
      INSERT INTO transfers (
        reason, document_kind, document_id, document_line_id,
        debit_account_id, credit_account_id, amount, qty,
        routing_op, counterparty_id, period_id, business_date, idempotency_key, posted_by
      ) VALUES (
        v_reason, v_event->>'document_kind', (v_event->>'document_id')::UUID,
        (v_event->>'document_line_id')::UUID, v_d_id, v_c_id, v_amount, v_qty_for_row,
        (v_event->>'routing_op')::INT, (v_event->>'counterparty_id')::UUID,
        v_period_id, v_business_date, v_idem_key, (v_event->>'posted_by')::UUID
      ) RETURNING id INTO v_new_transfer_id;
      -- Flag wac_periodic AND wac_retroactive depletion value-leg transfers.
      IF v_cost_method IN ('wac_periodic', 'wac_retroactive')
         AND v_reason IN ('op_move','scrap','wo_complete','so_ship')
         AND v_d_acct.ledger_kind = 'value' THEN
        v_event_qty := (v_event->>'qty')::BIGINT;
        INSERT INTO transfers_provisional (transfer_id, period_id, cost_method, qty)
        VALUES (v_new_transfer_id, v_period_id, v_cost_method, v_event_qty);
      END IF;
      v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'ok');
    END LOOP;
    RETURN v_results;
  END IF;

  -- Two-pass.
  v_amounts := array_fill(NULL::BIGINT, ARRAY[v_n]);
  FOR v_idx IN 1..v_n LOOP
    v_event := p_events -> (v_idx - 1);
    v_reason := (v_event->>'reason')::transfer_reason;
    IF v_reason NOT IN ('op_move','scrap','wo_complete','so_ship') THEN
      v_amounts[v_idx] := (v_event->>'amount')::BIGINT;
      CONTINUE;
    END IF;
    v_idem_key := (v_event->>'idempotency_key')::UUID;
    IF EXISTS (SELECT 1 FROM transfers WHERE idempotency_key = v_idem_key) THEN CONTINUE; END IF;
    v_d_id := (v_event->>'debit_account_id')::BIGINT;
    v_c_id := (v_event->>'credit_account_id')::BIGINT;
    SELECT * INTO v_d_acct FROM accounts WHERE id = v_d_id;
    SELECT * INTO v_c_acct FROM accounts WHERE id = v_c_id;
    v_cost_sku := COALESCE(v_d_acct.sku_id, v_c_acct.sku_id);
    IF v_cost_sku IS NULL THEN
      RAISE EXCEPTION 'cost_method_not_implemented: sku not resolvable for reason % at event index %',
                      v_reason, v_idx USING ERRCODE = 'P0006';
    END IF;
    SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_cost_sku;
    IF v_d_acct.ledger_kind = 'value' THEN
      v_amounts[v_idx] := _post_transfers_compute_amount(v_event, v_d_acct, v_c_acct, v_cost_method, v_idx);
    ELSE
      IF v_cost_method NOT IN ('standard', 'wac_perpetual', 'wac_periodic', 'wac_retroactive') THEN
        RAISE EXCEPTION 'cost_method_not_implemented: % for reason % at event index %',
                        v_cost_method, v_reason, v_idx USING ERRCODE = 'P0006';
      END IF;
      v_amounts[v_idx] := (v_event->>'amount')::BIGINT;
    END IF;
  END LOOP;

  FOR v_idx IN 1..v_n LOOP
    v_event := p_events -> (v_idx - 1);
    v_idem_key := (v_event->>'idempotency_key')::UUID;
    IF EXISTS (SELECT 1 FROM transfers WHERE idempotency_key = v_idem_key) THEN
      v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'exists');
      CONTINUE;
    END IF;
    v_d_id := (v_event->>'debit_account_id')::BIGINT;
    v_c_id := (v_event->>'credit_account_id')::BIGINT;
    v_business_date := (v_event->>'business_date')::DATE;
    v_reason := (v_event->>'reason')::transfer_reason;
    v_amount := v_amounts[v_idx];
    v_cost_method := NULL;
    SELECT * INTO v_d_acct FROM accounts WHERE id = v_d_id;
    SELECT * INTO v_c_acct FROM accounts WHERE id = v_c_id;
    IF v_reason IN ('op_move','scrap','wo_complete','so_ship') THEN
      v_cost_sku := COALESCE(v_d_acct.sku_id, v_c_acct.sku_id);
      IF v_cost_sku IS NOT NULL THEN
        SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_cost_sku;
      END IF;
    END IF;
    IF v_d_acct.is_closed OR v_c_acct.is_closed THEN
      RAISE EXCEPTION 'account_closed: event index %', v_idx USING ERRCODE = 'P0001';
    END IF;
    IF v_d_acct.ledger_kind <> v_c_acct.ledger_kind THEN
      RAISE EXCEPTION 'ledger_mismatch: event index % (% vs %)', v_idx, v_d_acct.ledger_kind, v_c_acct.ledger_kind USING ERRCODE = 'P0002';
    END IF;
    IF v_d_acct.ledger_kind = 'value' AND v_d_acct.currency <> v_c_acct.currency THEN
      RAISE EXCEPTION 'currency_mismatch: event index % (% vs %)', v_idx, v_d_acct.currency, v_c_acct.currency USING ERRCODE = 'P0003';
    END IF;
    SELECT id, closed_at INTO v_period_id, v_period_closed
      FROM periods WHERE opens_at <= v_business_date AND closes_at >= v_business_date;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'period_missing: event index % business_date %', v_idx, v_business_date USING ERRCODE = 'P0004';
    END IF;
    IF v_period_closed IS NOT NULL AND NOT p_override_closed_period THEN
      RAISE EXCEPTION 'period_closed: event index % business_date %', v_idx, v_business_date USING ERRCODE = 'P0005';
    END IF;
    v_qty_for_row := (v_event->>'qty')::BIGINT;
    IF v_qty_for_row IS NULL AND v_d_acct.ledger_kind = 'qty' AND v_c_acct.ledger_kind = 'qty' THEN
      v_qty_for_row := v_amount;
    END IF;
    UPDATE accounts SET debits_total  = debits_total  + v_amount WHERE id = v_d_id;
    UPDATE accounts SET credits_total = credits_total + v_amount WHERE id = v_c_id;
    INSERT INTO transfers (
      reason, document_kind, document_id, document_line_id,
      debit_account_id, credit_account_id, amount, qty,
      routing_op, counterparty_id, period_id, business_date, idempotency_key, posted_by
    ) VALUES (
      v_reason, v_event->>'document_kind', (v_event->>'document_id')::UUID,
      (v_event->>'document_line_id')::UUID, v_d_id, v_c_id, v_amount, v_qty_for_row,
      (v_event->>'routing_op')::INT, (v_event->>'counterparty_id')::UUID,
      v_period_id, v_business_date, v_idem_key, (v_event->>'posted_by')::UUID
    ) RETURNING id INTO v_new_transfer_id;
    IF v_cost_method IN ('wac_periodic', 'wac_retroactive')
       AND v_reason IN ('op_move','scrap','wo_complete','so_ship')
       AND v_d_acct.ledger_kind = 'value' THEN
      v_event_qty := (v_event->>'qty')::BIGINT;
      INSERT INTO transfers_provisional (transfer_id, period_id, cost_method, qty)
      VALUES (v_new_transfer_id, v_period_id, v_cost_method, v_event_qty);
    END IF;
    v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'ok');
  END LOOP;
  RETURN v_results;
END;
$$;

-- ============================================================
-- post_inventory_adjustment: add wac_retroactive branch.
-- Mirror of wac_periodic (IN/OUT same shape, OUT flags provisional).
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
  v_needs_provisional_method TEXT := NULL;
  v_value_transfer_id BIGINT;
  v_period_id        BIGINT;
BEGIN
  SELECT id INTO v_existing_id FROM inventory_adjustments WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT cost_method INTO v_cost_method FROM skus WHERE id = p_sku_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'sku % not found', p_sku_id USING ERRCODE = 'P0010'; END IF;

  -- D3: WIP-class deferred for wac_periodic AND wac_retroactive.
  IF v_cost_method IN ('wac_periodic', 'wac_retroactive') AND p_inventory_class = 'wip' THEN
    RAISE EXCEPTION
      '% adjustment on inv_value_wip class not supported in Phase 1 '
      '(see acct-p7v Phase 2 Epic J: wac across WIP pools); sku=%',
      v_cost_method, p_sku_id USING ERRCODE = 'P0006';
  END IF;

  SELECT id INTO v_qty_acct FROM accounts
   WHERE kind = 'stock_available' AND sku_id = p_sku_id AND location_id = p_location_id AND NOT is_closed;
  IF v_qty_acct IS NULL THEN
    RAISE EXCEPTION 'no open stock_available account for sku=% loc=%', p_sku_id, p_location_id USING ERRCODE = 'P0010';
  END IF;

  v_value_kind := 'inv_value_' || p_inventory_class;
  EXECUTE format('SELECT id FROM accounts WHERE kind = %L AND sku_id = $1 AND location_id = $2 AND currency = $3 AND NOT is_closed', v_value_kind)
    INTO v_val_acct USING p_sku_id, p_location_id, p_currency;
  IF v_val_acct IS NULL THEN
    RAISE EXCEPTION 'no open % account for sku=% loc=% ccy=%', v_value_kind, p_sku_id, p_location_id, p_currency USING ERRCODE = 'P0010';
  END IF;

  SELECT id INTO v_void_qty FROM accounts WHERE kind = 'creation_void' AND ledger_kind = 'qty' AND NOT is_closed;
  IF v_void_qty IS NULL THEN RAISE EXCEPTION 'no creation_void(qty) account configured' USING ERRCODE = 'P0010'; END IF;

  SELECT id INTO v_void_val FROM accounts WHERE kind = 'inv_adj_expense' AND ledger_kind = 'value' AND currency = p_currency AND NOT is_closed;
  IF v_void_val IS NULL THEN RAISE EXCEPTION 'no inv_adj_expense(value, ccy=%) account configured', p_currency USING ERRCODE = 'P0010'; END IF;

  CASE v_cost_method
  WHEN 'standard' THEN
    IF p_unit_cost IS NOT NULL THEN
      RAISE EXCEPTION 'standard SKU % has a fixed standard cost; do not pass p_unit_cost (got %)', p_sku_id, p_unit_cost USING ERRCODE = 'P0011';
    END IF;
    v_effective_uc := resolve_standard_cost_at(p_sku_id, p_business_date);

  WHEN 'wac_perpetual' THEN
    v_lock_first := LEAST(v_qty_acct, v_val_acct);
    v_lock_second := GREATEST(v_qty_acct, v_val_acct);
    PERFORM 1 FROM accounts WHERE id = v_lock_first FOR UPDATE;
    PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;
    SELECT COALESCE(SUM(CASE WHEN t.debit_account_id = v_val_acct THEN t.qty WHEN t.credit_account_id = v_val_acct THEN -t.qty END), 0)
      INTO v_qty_balance FROM transfers t
     WHERE v_val_acct IN (t.debit_account_id, t.credit_account_id) AND t.qty IS NOT NULL;
    SELECT debits_total - credits_total INTO v_val_balance FROM accounts WHERE id = v_val_acct;
    IF p_qty_delta > 0 THEN
      IF p_unit_cost IS NULL THEN
        IF v_qty_balance <= 0 THEN
          RAISE EXCEPTION 'wac_perpetual SKU % at sku=% loc=% has empty pool (qty_balance=%); caller must pass p_unit_cost on first adjustment-in to seed', p_sku_id, p_sku_id, p_location_id, v_qty_balance USING ERRCODE = 'P0011';
        END IF;
        v_effective_uc := v_val_balance / v_qty_balance;
      ELSE
        v_effective_uc := p_unit_cost;
      END IF;
    ELSE
      IF p_unit_cost IS NOT NULL THEN
        RAISE EXCEPTION 'wac_perpetual depletion does not accept asserted unit_cost (got % on sku=% loc=%); use lot cost_method (acct-8gg) for asserted-cost-per-transaction needs', p_unit_cost, p_sku_id, p_location_id USING ERRCODE = 'P0011';
      END IF;
      IF v_qty_balance <= 0 THEN
        RAISE EXCEPTION 'wac_perpetual SKU % at sku=% loc=% has empty pool; cannot deplete', p_sku_id, p_sku_id, p_location_id USING ERRCODE = 'P0010';
      END IF;
      v_effective_uc := v_val_balance / v_qty_balance;
    END IF;

  WHEN 'wac_periodic', 'wac_retroactive' THEN
    -- Same shape for both: mid-period reads pool avg from history;
    -- depletions get flagged in transfers_provisional with the SKU's
    -- cost_method. The close hook (different per method) does the
    -- recomputation.
    v_lock_first := LEAST(v_qty_acct, v_val_acct);
    v_lock_second := GREATEST(v_qty_acct, v_val_acct);
    PERFORM 1 FROM accounts WHERE id = v_lock_first FOR UPDATE;
    PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;
    SELECT COALESCE(SUM(CASE WHEN t.debit_account_id = v_val_acct THEN t.qty WHEN t.credit_account_id = v_val_acct THEN -t.qty END), 0)
      INTO v_qty_balance FROM transfers t
     WHERE v_val_acct IN (t.debit_account_id, t.credit_account_id) AND t.qty IS NOT NULL;
    SELECT debits_total - credits_total INTO v_val_balance FROM accounts WHERE id = v_val_acct;
    IF p_qty_delta > 0 THEN
      IF p_unit_cost IS NULL THEN
        IF v_qty_balance <= 0 THEN
          RAISE EXCEPTION '% SKU % at sku=% loc=% has empty pool (qty_balance=%); caller must pass p_unit_cost on first adjustment-in to seed',
                          v_cost_method, p_sku_id, p_sku_id, p_location_id, v_qty_balance USING ERRCODE = 'P0011';
        END IF;
        v_effective_uc := v_val_balance / v_qty_balance;
      ELSE
        v_effective_uc := p_unit_cost;
      END IF;
    ELSE
      IF p_unit_cost IS NOT NULL THEN
        RAISE EXCEPTION '% depletion does not accept asserted unit_cost (got % on sku=% loc=%); use lot cost_method (acct-8gg) for asserted-cost-per-transaction needs',
                        v_cost_method, p_unit_cost, p_sku_id, p_location_id USING ERRCODE = 'P0011';
      END IF;
      IF v_qty_balance <= 0 THEN
        RAISE EXCEPTION '% SKU % at sku=% loc=% has empty pool; cannot deplete', v_cost_method, p_sku_id, p_sku_id, p_location_id USING ERRCODE = 'P0010';
      END IF;
      v_effective_uc := v_val_balance / v_qty_balance;
      v_needs_provisional_method := v_cost_method::TEXT;
    END IF;

  WHEN 'fifo', 'lot' THEN
    RAISE EXCEPTION 'cost_method_not_implemented: % (sku=%); see acct-8gg', v_cost_method, p_sku_id USING ERRCODE = 'P0006';

  ELSE
    RAISE EXCEPTION 'unknown cost_method % for sku=%', v_cost_method, p_sku_id USING ERRCODE = 'P0011';
  END CASE;

  v_qty_amount := abs(p_qty_delta);
  v_val_amount := v_qty_amount * v_effective_uc;

  IF p_qty_delta > 0 THEN
    v_qty_debit := v_qty_acct; v_qty_credit := v_void_qty;
    v_val_debit := v_val_acct; v_val_credit := v_void_val;
  ELSE
    v_qty_debit := v_void_qty; v_qty_credit := v_qty_acct;
    v_val_debit := v_void_val; v_val_credit := v_val_acct;
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
    SELECT id INTO v_doc_id FROM inventory_adjustments WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  IF v_val_amount > 0 THEN
    v_batch := jsonb_build_array(
      jsonb_build_object('reason','inventory_adjustment','document_kind','inventory_adjustment','document_id',v_doc_id,'debit_account_id',v_qty_debit,'credit_account_id',v_qty_credit,'amount',v_qty_amount,'qty',v_qty_amount,'business_date',p_business_date,'idempotency_key',gen_random_uuid(),'posted_by',p_posted_by),
      jsonb_build_object('reason','inventory_adjustment','document_kind','inventory_adjustment','document_id',v_doc_id,'debit_account_id',v_val_debit,'credit_account_id',v_val_credit,'amount',v_val_amount,'qty',v_qty_amount,'business_date',p_business_date,'idempotency_key',gen_random_uuid(),'posted_by',p_posted_by)
    );
  ELSE
    v_batch := jsonb_build_array(
      jsonb_build_object('reason','inventory_adjustment','document_kind','inventory_adjustment','document_id',v_doc_id,'debit_account_id',v_qty_debit,'credit_account_id',v_qty_credit,'amount',v_qty_amount,'qty',v_qty_amount,'business_date',p_business_date,'idempotency_key',gen_random_uuid(),'posted_by',p_posted_by)
    );
  END IF;

  PERFORM post_transfers(v_batch, FALSE);

  IF v_needs_provisional_method IS NOT NULL THEN
    SELECT id INTO v_value_transfer_id FROM transfers WHERE document_id = v_doc_id AND reason = 'inventory_adjustment' AND credit_account_id = v_val_acct;
    SELECT id INTO v_period_id FROM periods WHERE opens_at <= p_business_date AND closes_at >= p_business_date;
    INSERT INTO transfers_provisional (transfer_id, period_id, cost_method, qty)
    VALUES (v_value_transfer_id, v_period_id, v_needs_provisional_method::cost_method, v_qty_amount);
  END IF;

  RETURN v_doc_id;
END;
$$;

-- ============================================================
-- wac_retroactive_close_hook: real body, chronological replay.
-- Drop the s6n 1-arg stub first; PG signature change requires DROP.
-- ============================================================

DROP FUNCTION IF EXISTS wac_retroactive_close_hook(BIGINT);

CREATE OR REPLACE FUNCTION wac_retroactive_close_hook(
  p_period_id         BIGINT,
  p_force_provisional BOOLEAN DEFAULT FALSE
) RETURNS BIGINT LANGUAGE plpgsql AS $$
DECLARE
  v_period_opens   DATE;
  v_period_closes  DATE;
  v_period_code    TEXT;
  v_count          BIGINT := 0;
  v_pool           RECORD;
  v_event          RECORD;
  v_pool_value     BIGINT;
  v_pool_qty       BIGINT;
  v_recomputed_avg BIGINT;
  v_recomputed_amt BIGINT;
  v_variance       BIGINT;
  v_var_acct       BIGINT;
  v_batch          JSONB;
  v_var_xfer_id    BIGINT;
  v_event_a        JSONB;
  v_event_b        JSONB;
  v_orig_amount    BIGINT;
BEGIN
  SELECT opens_at, closes_at, code INTO v_period_opens, v_period_closes, v_period_code
    FROM periods WHERE id = p_period_id;
  IF NOT FOUND THEN RETURN 0; END IF;

  -- Set of pools touched by un-finalized wac_retroactive provisional rows.
  -- Pool = inv_value_<class>(sku, location, currency) on the credit side
  -- of the original depletion transfer.
  FOR v_pool IN
    SELECT DISTINCT a.id AS pool_id, a.kind, a.sku_id, a.location_id, a.currency
      FROM transfers_provisional tp
      JOIN transfers t ON t.id = tp.transfer_id
      JOIN accounts a ON a.id = t.credit_account_id
     WHERE tp.period_id = p_period_id
       AND tp.cost_method = 'wac_retroactive'
       AND tp.finalized_at IS NULL
     ORDER BY a.id
  LOOP
    -- D5: pre-period running state. Sum of value/qty up to but not
    -- including the period (signed by debit/credit on the pool).
    SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = v_pool.pool_id THEN  t.amount
                             WHEN t.credit_account_id = v_pool.pool_id THEN -t.amount END), 0)
      INTO v_pool_value
      FROM transfers t
     WHERE v_pool.pool_id IN (t.debit_account_id, t.credit_account_id)
       AND t.business_date < v_period_opens;
    SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = v_pool.pool_id THEN  t.qty
                             WHEN t.credit_account_id = v_pool.pool_id THEN -t.qty END), 0)
      INTO v_pool_qty
      FROM transfers t
     WHERE v_pool.pool_id IN (t.debit_account_id, t.credit_account_id)
       AND t.business_date < v_period_opens
       AND t.qty IS NOT NULL;

    -- D1: walk in-period events ordered (business_date, posted_at, id).
    -- D2: ALL events affecting the pool participate, not just
    -- wac_retroactive-flagged ones.
    FOR v_event IN
      SELECT t.id, t.amount, t.qty, t.debit_account_id, t.credit_account_id,
             t.business_date, t.posted_at,
             tp.transfer_id IS NOT NULL AS is_prov
        FROM transfers t
        LEFT JOIN transfers_provisional tp
          ON tp.transfer_id = t.id
         AND tp.cost_method = 'wac_retroactive'
         AND tp.finalized_at IS NULL
       WHERE v_pool.pool_id IN (t.debit_account_id, t.credit_account_id)
         AND t.business_date BETWEEN v_period_opens AND v_period_closes
       ORDER BY t.business_date, t.posted_at, t.id
    LOOP
      IF v_event.debit_account_id = v_pool.pool_id THEN
        -- Inflow (receipt or variance correction).
        v_pool_value := v_pool_value + v_event.amount;
        IF v_event.qty IS NOT NULL THEN
          v_pool_qty := v_pool_qty + v_event.qty;
        END IF;
      ELSE
        -- Outflow (depletion). Recompute if qty available.
        IF v_event.qty IS NULL THEN
          -- Non-inventory credit (rare; e.g. value-only correction).
          -- Just subtract amount from running value; qty unchanged.
          v_pool_value := v_pool_value - v_event.amount;
          CONTINUE;
        END IF;

        IF v_pool_qty <= 0 THEN
          IF p_force_provisional AND v_event.is_prov THEN
            -- Skip un-processable provisional row; leaves it un-finalized
            -- for forensics. Provisional gate (P0015) catches it after.
            CONTINUE;
          END IF;
          RAISE EXCEPTION
            'wac_retroactive_replay_pool_empty: period % (id=%) pool kind=% sku=% '
            'loc=% ccy=%: running qty went non-positive at depletion of transfer %; '
            'this indicates the perpetual chain has an inconsistency (more depletions '
            'than receipts of valid age). Pass p_force_provisional=TRUE to skip this row.',
            v_period_code, p_period_id, v_pool.kind, v_pool.sku_id, v_pool.location_id,
            v_pool.currency, v_event.id
            USING ERRCODE = 'P0006';
        END IF;

        v_recomputed_avg := v_pool_value / v_pool_qty;
        v_recomputed_amt := v_event.qty * v_recomputed_avg;
        v_orig_amount    := v_event.amount;

        IF v_event.is_prov THEN
          v_variance := v_recomputed_amt - v_orig_amount;
          IF v_variance = 0 THEN
            UPDATE transfers_provisional
               SET finalized_at = clock_timestamp(),
                   variance_amount = 0,
                   variance_transfer_id = NULL
             WHERE transfer_id = v_event.id;
          ELSE
            -- Resolve variance_wac_retroactive(currency).
            SELECT id INTO v_var_acct FROM accounts
             WHERE kind = 'variance_wac_retroactive' AND ledger_kind = 'value'
               AND currency = v_pool.currency AND NOT is_closed;
            IF v_var_acct IS NULL THEN
              RAISE EXCEPTION
                'wac_retroactive_close: no variance_wac_retroactive(value, ccy=%) account configured',
                v_pool.currency USING ERRCODE = 'P0010';
            END IF;

            -- D4: 2-transfer routing through variance_wac_retroactive.
            -- Net effect: original_debit += variance, original_credit -= variance.
            IF v_variance > 0 THEN
              v_event_a := jsonb_build_object('reason','cost_restate','document_kind','wac_retroactive_close','document_id',gen_random_uuid(),'debit_account_id',v_event.debit_account_id,'credit_account_id',v_var_acct,'amount',v_variance,'business_date',v_period_closes,'idempotency_key',gen_random_uuid(),'posted_by','00000000-0000-0000-0000-000000000000');
              v_event_b := jsonb_build_object('reason','cost_restate','document_kind','wac_retroactive_close','document_id',gen_random_uuid(),'debit_account_id',v_var_acct,'credit_account_id',v_pool.pool_id,'amount',v_variance,'business_date',v_period_closes,'idempotency_key',gen_random_uuid(),'posted_by','00000000-0000-0000-0000-000000000000');
            ELSE
              v_event_a := jsonb_build_object('reason','cost_restate','document_kind','wac_retroactive_close','document_id',gen_random_uuid(),'debit_account_id',v_var_acct,'credit_account_id',v_event.debit_account_id,'amount',-v_variance,'business_date',v_period_closes,'idempotency_key',gen_random_uuid(),'posted_by','00000000-0000-0000-0000-000000000000');
              v_event_b := jsonb_build_object('reason','cost_restate','document_kind','wac_retroactive_close','document_id',gen_random_uuid(),'debit_account_id',v_pool.pool_id,'credit_account_id',v_var_acct,'amount',-v_variance,'business_date',v_period_closes,'idempotency_key',gen_random_uuid(),'posted_by','00000000-0000-0000-0000-000000000000');
            END IF;

            v_batch := jsonb_build_array(v_event_a, v_event_b);
            PERFORM post_transfers(v_batch, FALSE);
            SELECT id INTO v_var_xfer_id FROM transfers WHERE idempotency_key = (v_event_b->>'idempotency_key')::UUID;

            UPDATE transfers_provisional
               SET finalized_at = clock_timestamp(),
                   variance_amount = v_variance,
                   variance_transfer_id = v_var_xfer_id
             WHERE transfer_id = v_event.id;
          END IF;

          v_count := v_count + 1;
          -- Update running state with recomputed amount (matches post-variance pool).
          v_pool_value := v_pool_value - v_recomputed_amt;
          v_pool_qty   := v_pool_qty - v_event.qty;
        ELSE
          -- Non-provisional credit (e.g., from a different cost_method's
          -- workflow, or already-finalized from prior run). Update running
          -- state with the original amount.
          v_pool_value := v_pool_value - v_orig_amount;
          v_pool_qty   := v_pool_qty - v_event.qty;
        END IF;
      END IF;
    END LOOP;
  END LOOP;

  RETURN v_count;
END;
$$;

-- ============================================================
-- close_period: thread p_force_provisional to wac_retroactive_close_hook
-- (previously only threaded to wac_periodic).
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
    RAISE EXCEPTION 'period_close_invalid: period id=% not found', p_period_id USING ERRCODE = 'P0014';
  END IF;
  IF v_already_closed IS NOT NULL THEN
    RAISE EXCEPTION 'period_close_invalid: period % (id=%) already closed at %',
      v_period_code, p_period_id, v_already_closed USING ERRCODE = 'P0014';
  END IF;

  -- Both WAC hooks now take p_force_provisional; cost_adjust_retroactive
  -- remains the s6n 1-arg stub until Epic E (acct-og1) replaces it.
  v_wac_period_count     := wac_periodic_close_hook(p_period_id, p_force_provisional);
  v_wac_retro_count      := wac_retroactive_close_hook(p_period_id, p_force_provisional);
  v_cost_adj_retro_count := cost_adjust_retroactive_hook(p_period_id);
  v_finalized_count      := v_wac_period_count + v_wac_retro_count + v_cost_adj_retro_count;

  SELECT COUNT(*) INTO v_unfinalized_remaining
    FROM transfers_provisional
   WHERE period_id = p_period_id AND finalized_at IS NULL;

  IF v_unfinalized_remaining > 0 AND NOT p_force_provisional THEN
    RAISE EXCEPTION
      'period_close_provisional: % un-finalized provisional rows '
      'remain in period % (id=%); pass p_force_provisional=TRUE to override',
      v_unfinalized_remaining, v_period_code, p_period_id USING ERRCODE = 'P0015';
  END IF;

  v_alerts := run_daily_reconciliation();
  IF v_alerts > 0 AND NOT p_force_recon THEN
    RAISE EXCEPTION
      'period_close_reconciliation: % new reconciliation alert(s) raised '
      'while closing period % (id=%); pass p_force_recon=TRUE to override',
      v_alerts, v_period_code, p_period_id USING ERRCODE = 'P0016';
  END IF;

  v_now := clock_timestamp();
  UPDATE periods SET closed_at = v_now, closed_by = p_actor WHERE id = p_period_id;

  RETURN jsonb_build_object(
    'period_id', p_period_id, 'period_code', v_period_code,
    'closed_at', v_now, 'closed_by', p_actor,
    'finalized_count', v_finalized_count,
    'hook_results', jsonb_build_object(
      'wac_periodic', v_wac_period_count,
      'wac_retroactive', v_wac_retro_count,
      'cost_adjust_retroactive', v_cost_adj_retro_count
    ),
    'unfinalized_remaining', v_unfinalized_remaining,
    'alerts', v_alerts,
    'forced', jsonb_build_object('provisional', p_force_provisional, 'recon', p_force_recon)
  );
END;
$$;
