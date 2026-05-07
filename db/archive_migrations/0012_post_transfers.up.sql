-- post_transfers — canonical write path for the ledger.
--
-- Takes a JSONB array of transfer events. For each event:
--   1. Idempotency check (skip with result='exists' if idempotency_key
--      already in transfers).
--   2. Validate accounts (closed, ledger_kind match, currency match for
--      value ledgers).
--   3. Look up period by business_date; enforce closed-period lock unless
--      p_override_closed_period.
--   4. UPDATE accounts.debits_total / credits_total.
--   5. INSERT transfer row.
--
-- All accounts referenced anywhere in the batch are taken FOR UPDATE in
-- ascending id order before any event is processed. This guarantees
-- deadlock freedom under concurrent callers (no circular wait).
--
-- Multi-event batch semantics: any validation or CHECK failure raises an
-- exception that rolls back the entire surrounding transaction — the
-- whole batch is undone (linked-batch semantics for free). Idempotent
-- duplicates are the only "skip and continue" path.
--
-- Error codes:
--   P0001  account_closed     debit or credit account is_closed=TRUE
--   P0002  ledger_mismatch    debit.ledger_kind <> credit.ledger_kind
--   P0003  currency_mismatch  value ledger, currencies differ
--   P0004  period_missing     no period contains business_date
--   P0005  period_closed      period.closed_at IS NOT NULL and override=FALSE
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
BEGIN
  IF jsonb_array_length(p_events) = 0 THEN
    RETURN '[]'::JSONB;
  END IF;

  -- Lock all referenced accounts in ascending id order.
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

    -- Idempotency: short-circuit on existing key.
    IF EXISTS (SELECT 1 FROM transfers WHERE idempotency_key = v_idem_key) THEN
      v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'exists');
      CONTINUE;
    END IF;

    v_d_id          := (v_event->>'debit_account_id')::BIGINT;
    v_c_id          := (v_event->>'credit_account_id')::BIGINT;
    v_amount        := (v_event->>'amount')::BIGINT;
    v_business_date := (v_event->>'business_date')::DATE;

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
      (v_event->>'reason')::transfer_reason,
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
