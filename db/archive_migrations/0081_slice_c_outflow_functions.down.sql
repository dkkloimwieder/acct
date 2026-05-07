-- Down: drop the three Slice C functions and revert post_transfers
-- to mig 0033's body (debit-first SKU resolution; raises P0006 for
-- non-SKU value legs of cost-event reasons).

DROP FUNCTION IF EXISTS post_ar_payment(UUID, CHAR, BIGINT, DATE, UUID, UUID, TEXT);
DROP FUNCTION IF EXISTS post_customer_invoice(UUID, CHAR, JSONB, DATE, UUID, UUID, TEXT);
DROP FUNCTION IF EXISTS post_so_ship(UUID, JSONB, DATE, UUID, UUID, TEXT);

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
  v_idem_key      UUID;
  v_reason        transfer_reason;
  v_cost_sku      UUID;
  v_cost_method   cost_method;
  v_amount        BIGINT;
  v_amounts       BIGINT[];
  v_aux_qty_id    BIGINT;
  v_aux_qty_ids   BIGINT[] := '{}';
  v_has_wac       BOOLEAN  := FALSE;
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
        v_cost_sku := COALESCE(v_d_acct.sku_id, v_c_acct.sku_id);
        IF v_cost_sku IS NULL THEN
          RAISE EXCEPTION
            'cost_method_not_implemented: sku not resolvable for reason % at event index %',
            v_reason, v_idx USING ERRCODE = 'P0006';
        END IF;
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

  v_amounts := array_fill(NULL::BIGINT, ARRAY[v_n]);
  FOR v_idx IN 1..v_n LOOP
    v_event := p_events -> (v_idx - 1);
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
    v_cost_sku := COALESCE(v_d_acct.sku_id, v_c_acct.sku_id);
    IF v_cost_sku IS NULL THEN
      RAISE EXCEPTION
        'cost_method_not_implemented: sku not resolvable for reason % at event index %',
        v_reason, v_idx USING ERRCODE = 'P0006';
    END IF;
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
    PERFORM _post_transfers_apply_event(
      v_event, v_idx, v_amount, v_d_acct, v_c_acct,
      v_cost_method, p_override_closed_period
    );
    v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'ok');
  END LOOP;
  RETURN v_results;
END;
$$;
