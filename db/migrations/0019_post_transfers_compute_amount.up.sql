-- post_transfers — acct-0ig: function-computed amount on the standard path.
--
-- Phase 0 W2 (migration 0013) shipped Option A: the caller pre-computed
-- 'amount' and the function only gated on skus.cost_method (raising P0006
-- for non-standard). This migration moves the amount computation INTO
-- the function for cost-relevant value-side events (Option B). The
-- standard branch is real; wac/lot/fifo branches RAISE P0006 with TODO
-- comments referencing acct-uxu and acct-8gg.
--
-- Bifurcated input contract:
--   * Cost-relevant reasons {op_move, scrap, wo_complete, so_ship} on
--     VALUE-ledger events: caller MUST send 'qty'. Function ignores any
--     'amount' sent and computes amount = _post_transfers_compute_amount(...).
--   * Same reasons on QTY-ledger events: caller still sends 'amount'
--     (= qty by convention). The qty-side gate (P0006 if cost_method
--     <> 'standard') is preserved from migration 0013, since qty-side
--     posting still implies a costing model — even though qty itself
--     doesn't depend on cost method, allowing qty-side ops on a non-
--     standard SKU without a working value-side path leaves the ledger
--     in an inconsistent state. The gate relaxes when WAC ships
--     (acct-uxu).
--   * All other reasons: caller sends 'amount'. Unchanged.
--
-- Why standard-only here: implementing WAC requires schema decisions
-- (per-location and per-routing-op granularity on value accounts;
-- acct-5re) and a lock-set pre-scan that aren't this issue's scope.
-- The dispatcher pattern lands now; non-standard branches fill in later.

-- ============================================================
-- Helper: _post_transfers_compute_amount
-- ============================================================
--
-- Inputs:
--   p_event        the JSONB event (carries 'qty' for cost-relevant value events)
--   p_d_acct       loaded debit  account row (under FOR UPDATE)
--   p_c_acct       loaded credit account row (under FOR UPDATE)
--   p_cost_method  the SKU's cost method, already resolved by caller
--   p_idx          1-based event index for error messages
--
-- Returns: BIGINT amount to post.
--
-- Currently implements only the 'standard' branch:
--   amount = qty * skus.standard_cost
--
-- The other branches (wac, lot, fifo) raise P0006 with TODO references
-- so adding a method is a single-branch change.

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
  v_qty    BIGINT;
  v_sku    UUID;
  v_unit   BIGINT;
BEGIN
  v_qty := (p_event->>'qty')::BIGINT;
  IF v_qty IS NULL THEN
    RAISE EXCEPTION 'cost_method_not_implemented: cost-relevant value event missing qty at event index %',
                    p_idx
      USING ERRCODE = 'P0006';
  END IF;

  v_sku := COALESCE(p_d_acct.sku_id, p_c_acct.sku_id);
  IF v_sku IS NULL THEN
    -- Should be unreachable: caller already gated p_cost_method, which
    -- requires a SKU. Defensive raise.
    RAISE EXCEPTION 'cost_method_not_implemented: sku not resolvable in compute_amount at event index %',
                    p_idx
      USING ERRCODE = 'P0006';
  END IF;

  CASE p_cost_method
    WHEN 'standard' THEN
      SELECT v_qty * standard_cost INTO v_unit
        FROM skus WHERE id = v_sku;
      RETURN v_unit;

    WHEN 'wac' THEN
      -- TODO acct-uxu: implement WAC.
      -- amount = qty * (value_balance / qty_balance) for the matching
      -- (sku, location[, currency]) pool. Requires lock-set pre-scan
      -- (acct-uxu) and schema decision on value-account granularity
      -- (acct-5re).
      RAISE EXCEPTION 'cost_method_not_implemented: wac (tracked as acct-uxu) at event index %',
                      p_idx
        USING ERRCODE = 'P0006';

    WHEN 'lot' THEN
      -- TODO acct-8gg + lot infrastructure.
      -- amount = qty * lots(p_event->>'lot_id').unit_cost.
      -- Lot tracking is a feature larger than just cost (lot creation,
      -- expiry, traceability); not yet scoped.
      RAISE EXCEPTION 'cost_method_not_implemented: lot (tracked as acct-8gg + lot infrastructure) at event index %',
                      p_idx
        USING ERRCODE = 'P0006';

    WHEN 'fifo' THEN
      -- TODO acct-8gg + lot infrastructure.
      -- FIFO walks oldest-first across cost layers (or lot layers,
      -- depending on chosen design). Same external-feature dependency
      -- as 'lot'.
      RAISE EXCEPTION 'cost_method_not_implemented: fifo (tracked as acct-8gg + lot infrastructure) at event index %',
                      p_idx
        USING ERRCODE = 'P0006';
  END CASE;

  -- Defensive: every cost_method enum value handled above. If an enum
  -- value is added without a branch, fail loudly.
  RAISE EXCEPTION 'cost_method_not_implemented: unhandled cost_method % at event index %',
                  p_cost_method, p_idx
    USING ERRCODE = 'P0006';
END;
$$;

COMMENT ON FUNCTION _post_transfers_compute_amount(JSONB, accounts, accounts, cost_method, INT) IS
  'Cost-method dispatcher for post_transfers (acct-0ig). Computes value-side amount = qty * unit_cost. Standard branch is real; WAC/lot/FIFO RAISE P0006 with TODO refs.';

-- ============================================================
-- Updated post_transfers
-- ============================================================
--
-- Diff vs migration 0013:
--   * Cost-relevant value-side events: amount is computed via the
--     dispatcher helper instead of taken from the event.
--   * Cost-relevant qty-side events: caller's amount used (unchanged);
--     P0006 gate preserved.
--   * Non-cost-relevant events: caller's amount used (unchanged).
--   * Per-event ordering of checks unchanged: idempotency → cost gate
--     → P0001..P0005 → UPDATE → INSERT.

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
    v_business_date := (v_event->>'business_date')::DATE;
    v_reason        := (v_event->>'reason')::transfer_reason;

    SELECT * INTO v_d_acct FROM accounts WHERE id = v_d_id;
    SELECT * INTO v_c_acct FROM accounts WHERE id = v_c_id;

    -- ----------------------------------------------------------
    -- Cost-method dispatch (acct-0ig — Option B):
    --   * Resolve SKU + cost_method for cost-relevant reasons.
    --   * Value-side events: amount computed via dispatcher helper.
    --   * Qty-side events:   amount = caller's 'amount'; P0006 gate
    --                        preserved for non-standard cost methods.
    -- For non-cost-relevant reasons: amount = caller's 'amount'.
    -- ----------------------------------------------------------
    IF v_reason IN ('op_move','scrap','wo_complete','so_ship') THEN
      v_cost_sku := COALESCE(v_d_acct.sku_id, v_c_acct.sku_id);
      IF v_cost_sku IS NULL THEN
        RAISE EXCEPTION 'cost_method_not_implemented: sku not resolvable for reason % at event index %',
                        v_reason, v_idx
          USING ERRCODE = 'P0006';
      END IF;
      SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_cost_sku;

      IF v_d_acct.ledger_kind = 'value' THEN
        -- Value-side: dispatcher computes amount (standard works;
        -- non-standard raises P0006 inside the helper).
        v_amount := _post_transfers_compute_amount(
                      v_event, v_d_acct, v_c_acct, v_cost_method, v_idx);
      ELSE
        -- Qty-side: gate non-standard, then use caller's amount.
        IF v_cost_method <> 'standard' THEN
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
END;
$$;
