-- post_transfers — W2: adds cost-method dispatch on top of W1's base path.
--
-- Phase 0 implements the 'standard' branch as a pure GATE: the caller still
-- pre-computes 'amount' (W1 base behavior); for cost-relevant reasons
-- (op_move, scrap, wo_complete, so_ship) the function looks up
-- skus.cost_method and refuses to post if it's anything other than
-- 'standard'. Non-standard methods raise P0006.
--
-- The decision to gate (rather than have the function compute amount) is
-- documented as Option A. Future work to switch to function-computed
-- amounts (required before WAC/FIFO/lot land) is tracked as acct-0ig,
-- which acct-8gg depends on.
--
-- Error codes:
--   P0001  account_closed     debit or credit account is_closed=TRUE
--   P0002  ledger_mismatch    debit.ledger_kind <> credit.ledger_kind
--   P0003  currency_mismatch  value ledger, currencies differ
--   P0004  period_missing     no period contains business_date
--   P0005  period_closed      period.closed_at IS NOT NULL and override=FALSE
--   P0006  cost_method_not_implemented
--          for reason in {op_move,scrap,wo_complete,so_ship}, the resolved
--          sku has cost_method != 'standard' (or sku not resolvable from
--          either account's sku_id).
--
-- Per-event return shape: {"index": <1-based>, "result": "ok"|"exists"}.
-- See db/README.md "Write path" for the JSONB event input schema.

CREATE OR REPLACE FUNCTION post_transfers(
  p_events                 JSONB,
  p_override_closed_period BOOLEAN DEFAULT FALSE
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
  v_results       JSONB := '[]'::JSONB;
  v_event         JSONB;
  v_idx           INT;
  v_d_acct        accounts%ROWTYPE;
  v_c_acct        accounts%ROWTYPE;
  v_period_id     BIGINT;
  v_period_closed TIMESTAMPTZ;
  v_amount        BIGINT;
  v_business_date DATE;
  v_idem_key      UUID;
  v_d_id          BIGINT;
  v_c_id          BIGINT;
  v_reason        transfer_reason;
  v_cost_sku      UUID;
  v_cost_method   cost_method;
BEGIN
  IF jsonb_array_length(p_events) = 0 THEN
    RETURN '[]'::JSONB;
  END IF;

  PERFORM 1 FROM accounts
   WHERE id IN (
     SELECT (ev->>'debit_account_id')::BIGINT  FROM jsonb_array_elements(p_events) ev
     UNION
     SELECT (ev->>'credit_account_id')::BIGINT FROM jsonb_array_elements(p_events) ev
   )
   ORDER BY id
   FOR UPDATE;

  FOR v_idx IN 1..jsonb_array_length(p_events) LOOP
    v_event    := p_events -> (v_idx - 1);
    v_idem_key := (v_event->>'idempotency_key')::UUID;

    IF EXISTS (SELECT 1 FROM transfers WHERE idempotency_key = v_idem_key) THEN
      v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'exists');
      CONTINUE;
    END IF;

    v_d_id          := (v_event->>'debit_account_id')::BIGINT;
    v_c_id          := (v_event->>'credit_account_id')::BIGINT;
    v_amount        := (v_event->>'amount')::BIGINT;
    v_business_date := (v_event->>'business_date')::DATE;
    v_reason        := (v_event->>'reason')::transfer_reason;

    SELECT * INTO v_d_acct FROM accounts WHERE id = v_d_id;
    SELECT * INTO v_c_acct FROM accounts WHERE id = v_c_id;

    -- Cost-method dispatch (Phase 0: gate only; non-standard -> P0006).
    IF v_reason IN ('op_move','scrap','wo_complete','so_ship') THEN
      v_cost_sku := COALESCE(v_d_acct.sku_id, v_c_acct.sku_id);
      IF v_cost_sku IS NULL THEN
        RAISE EXCEPTION 'cost_method_not_implemented: sku not resolvable for reason % at event index %',
                        v_reason, v_idx
          USING ERRCODE = 'P0006';
      END IF;
      SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_cost_sku;
      IF v_cost_method <> 'standard' THEN
        RAISE EXCEPTION 'cost_method_not_implemented: % for reason % at event index %',
                        v_cost_method, v_reason, v_idx
          USING ERRCODE = 'P0006';
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
    );

    v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'ok');
  END LOOP;

  RETURN v_results;
END;
$$;
