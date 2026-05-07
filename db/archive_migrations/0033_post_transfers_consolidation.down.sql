-- Down migration: restore post_transfers body verbatim from 0031,
-- drop the helpers introduced in 0033.

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

DROP FUNCTION IF EXISTS _post_transfers_apply_event(JSONB, INT, BIGINT, accounts, accounts, cost_method, BOOLEAN);
DROP FUNCTION IF EXISTS _post_transfers_lock_pre_scan(JSONB, BIGINT[]);
