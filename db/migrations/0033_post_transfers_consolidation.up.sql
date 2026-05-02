-- acct-7mg / Slice A.1 Part A — post_transfers consolidation pass.
--
-- The function evolved 0007 → 0012 → 0019 → 0021 → 0023 → 0029 → 0030
-- → 0031 (this migration's predecessor), accreting branches at each
-- step. Body is now ~1078 lines (0030) with dual-pass logic. Audit
-- (acct-ok2) flagged the duplication; fold-in of acct-q43 happens
-- here BEFORE Slice A's post_po_receipt + post_ap_bill add new
-- dispatcher branches.
--
-- Two helpers extracted:
--
--   _post_transfers_lock_pre_scan(events, aux_qty_ids)
--     — Single FOR UPDATE block. Replaces the with/without-aux branch
--     duplication. aux_qty_ids may be empty.
--
--   _post_transfers_apply_event(event, idx, amount, d_acct, c_acct,
--                               cost_method, override_closed)
--     — Validates account state (P0001 closed, P0002 ledger_mismatch,
--     P0003 currency_mismatch); resolves and validates period (P0004
--     missing, P0005 closed unless override); resolves qty_for_row
--     from the source rule (event.qty | qty-leg fallback to amount |
--     NULL); UPDATEs balances; INSERTs transfer; INSERTs
--     transfers_provisional row for wac_periodic/wac_retroactive
--     depletions. Returns the new transfer id. Replaces ~80 lines of
--     duplicated apply logic across single-pass and two-pass.
--
-- post_transfers itself goes from ~250 lines (0031) to ~120 lines
-- here. The single-pass loop and two-pass second loop both reduce to
-- "compute amount; call apply_event". Pre-scan logic (cost-event
-- detection, WAC sku collection, aux qty account collection) is
-- unchanged.
--
-- BEHAVIOR-PRESERVING. All 173 existing tests should pass unchanged.
-- Validation order is preserved: in single-pass, compute amount →
-- validate → apply (same as 0031). In two-pass, pass-1 computes
-- amounts (no validation); pass-2 validates → applies (same as 0031).
--
-- Schema digest changes (function bodies). No table/index/CHECK
-- changes. Down migration restores the 0031 post_transfers body
-- verbatim and drops the new helpers.

-- ============================================================
-- Helper 1: lock pre-scan
-- ============================================================

CREATE OR REPLACE FUNCTION _post_transfers_lock_pre_scan(
  p_events       JSONB,
  p_aux_qty_ids  BIGINT[]
) RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM 1 FROM accounts
   WHERE id IN (
     SELECT (ev->>'debit_account_id')::BIGINT
       FROM jsonb_array_elements(p_events) ev
     UNION
     SELECT (ev->>'credit_account_id')::BIGINT
       FROM jsonb_array_elements(p_events) ev
     UNION
     SELECT unnest(p_aux_qty_ids))
   ORDER BY id FOR UPDATE;
END;
$$;

COMMENT ON FUNCTION _post_transfers_lock_pre_scan(JSONB, BIGINT[]) IS
  'Acquires FOR UPDATE locks in ascending account-id order for every '
  'account referenced by the event batch (debit, credit) plus any aux '
  'qty accounts that WAC cost computation needs to read. Single helper '
  'collapses the previous with/without-aux branch duplication. Pass '
  'an empty array for non-WAC batches.';

-- ============================================================
-- Helper 2: apply a single event
-- ============================================================

CREATE OR REPLACE FUNCTION _post_transfers_apply_event(
  p_event           JSONB,
  p_idx             INT,
  p_amount          BIGINT,
  p_d_acct          accounts,
  p_c_acct          accounts,
  p_cost_method     cost_method,
  p_override_closed BOOLEAN
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_period_id     BIGINT;
  v_period_closed TIMESTAMPTZ;
  v_business_date DATE;
  v_qty_for_row   BIGINT;
  v_reason        transfer_reason;
  v_idem_key      UUID;
  v_new_id        BIGINT;
  v_event_qty     BIGINT;
BEGIN
  v_business_date := (p_event->>'business_date')::DATE;
  v_reason        := (p_event->>'reason')::transfer_reason;
  v_idem_key      := (p_event->>'idempotency_key')::UUID;

  -- Account state validation (P0001 / P0002 / P0003).
  IF p_d_acct.is_closed OR p_c_acct.is_closed THEN
    RAISE EXCEPTION 'account_closed: event index %', p_idx
      USING ERRCODE = 'P0001';
  END IF;
  IF p_d_acct.ledger_kind <> p_c_acct.ledger_kind THEN
    RAISE EXCEPTION 'ledger_mismatch: event index % (% vs %)',
                    p_idx, p_d_acct.ledger_kind, p_c_acct.ledger_kind
      USING ERRCODE = 'P0002';
  END IF;
  IF p_d_acct.ledger_kind = 'value'
     AND p_d_acct.currency <> p_c_acct.currency THEN
    RAISE EXCEPTION 'currency_mismatch: event index % (% vs %)',
                    p_idx, p_d_acct.currency, p_c_acct.currency
      USING ERRCODE = 'P0003';
  END IF;

  -- Period resolution and gate (P0004 / P0005).
  SELECT id, closed_at INTO v_period_id, v_period_closed
    FROM periods
   WHERE opens_at <= v_business_date AND closes_at >= v_business_date;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'period_missing: event index % business_date %',
                    p_idx, v_business_date USING ERRCODE = 'P0004';
  END IF;
  IF v_period_closed IS NOT NULL AND NOT p_override_closed THEN
    RAISE EXCEPTION 'period_closed: event index % business_date %',
                    p_idx, v_business_date USING ERRCODE = 'P0005';
  END IF;

  -- qty_for_row source rule (acct-1vr / migration 0030):
  --   1. caller-asserted event.qty (cost-relevant value-leg events
  --      always supply this);
  --   2. else qty-leg fallback (both sides ledger_kind='qty' → qty == amount);
  --   3. else NULL (cash, AR, AP, FX).
  v_qty_for_row := (p_event->>'qty')::BIGINT;
  IF v_qty_for_row IS NULL
     AND p_d_acct.ledger_kind = 'qty'
     AND p_c_acct.ledger_kind = 'qty' THEN
    v_qty_for_row := p_amount;
  END IF;

  -- Apply: balance update + transfer insert.
  UPDATE accounts SET debits_total  = debits_total  + p_amount
    WHERE id = p_d_acct.id;
  UPDATE accounts SET credits_total = credits_total + p_amount
    WHERE id = p_c_acct.id;
  INSERT INTO transfers (
    reason, document_kind, document_id, document_line_id,
    debit_account_id, credit_account_id, amount, qty,
    routing_op, counterparty_id, period_id, business_date,
    idempotency_key, posted_by
  ) VALUES (
    v_reason, p_event->>'document_kind', (p_event->>'document_id')::UUID,
    (p_event->>'document_line_id')::UUID, p_d_acct.id, p_c_acct.id,
    p_amount, v_qty_for_row,
    (p_event->>'routing_op')::INT, (p_event->>'counterparty_id')::UUID,
    v_period_id, v_business_date, v_idem_key,
    (p_event->>'posted_by')::UUID
  ) RETURNING id INTO v_new_id;

  -- Provisional flag for wac_periodic / wac_retroactive depletions.
  -- p_cost_method is NULL for non-cost events; NULL IN (...) is NULL,
  -- so this branch only fires for cost events with the matching method.
  IF p_cost_method IN ('wac_periodic', 'wac_retroactive')
     AND v_reason IN ('op_move','scrap','wo_complete','so_ship')
     AND p_d_acct.ledger_kind = 'value' THEN
    v_event_qty := (p_event->>'qty')::BIGINT;
    INSERT INTO transfers_provisional (transfer_id, period_id, cost_method, qty)
    VALUES (v_new_id, v_period_id, p_cost_method, v_event_qty);
  END IF;

  RETURN v_new_id;
END;
$$;

COMMENT ON FUNCTION _post_transfers_apply_event(JSONB, INT, BIGINT, accounts, accounts, cost_method, BOOLEAN) IS
  'Single-event apply step: validates account/period state (P0001-P0005), '
  'computes qty_for_row, UPDATEs account balances, INSERTs the transfer '
  'row, optionally INSERTs transfers_provisional for wac_periodic/'
  'wac_retroactive depletions. Returns the new transfer id. Centralizes '
  'the apply logic that was duplicated between post_transfers'' '
  'single-pass and two-pass branches before acct-7mg.';

-- ============================================================
-- post_transfers: refactored body using helpers.
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

  -- Cost-event scan: detect WAC, collect aux qty accounts for the
  -- lock pre-scan. Aux qty accounts let WAC cost computation see a
  -- consistent qty divisor under concurrent posters.
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

  -- Lock pre-scan (single branch; aux_qty_ids is '{}' when no WAC).
  PERFORM _post_transfers_lock_pre_scan(p_events, v_aux_qty_ids);

  IF NOT v_has_wac THEN
    -- Single-pass: per event, compute amount inline, then apply.
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

  -- Two-pass: pass-1 compute amounts; pass-2 apply.
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

COMMENT ON FUNCTION post_transfers(JSONB, BOOLEAN) IS
  'Append-only ledger primitive. Validates a JSONB array of transfer '
  'events, applies each as one row in transfers + paired UPDATE on '
  'accounts.debits_total/credits_total, returns per-event status. '
  'Single-pass for non-WAC batches; two-pass for batches that touch a '
  'WAC SKU (pre-compute amounts so depletions see a consistent '
  'pre-application pool average). Apply step extracted to '
  '_post_transfers_apply_event (acct-7mg).';
