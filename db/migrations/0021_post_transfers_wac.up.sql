-- acct-uxu: WAC (weighted average cost) implementation in post_transfers.
--
-- Builds on:
--   * 0019 dispatcher scaffold (acct-0ig)
--   * 0020 per-location and per-routing-op value-account UK (acct-nfr)
--
-- What this migration does:
--   1. New helper `_post_transfers_lookup_qty_account(p_value_acct)` that
--      resolves the matching qty-side account for a value-side account
--      based on its kind/sku/location/routing_op. Returns NULL if no
--      match (caller raises P0006).
--   2. Updates `_post_transfers_compute_amount` to fill in the WAC branch:
--      unit_cost = credit_value_pool_balance / matching_qty_pool_balance,
--      amount    = qty * unit_cost.
--   3. Refactors `post_transfers` to add a lock pre-scan (so WAC's aux
--      qty account is in the FOR UPDATE set) AND a two-pass execution
--      (pass 1 computes amounts under lock with PRE-batch balances; pass
--      2 validates and applies). Two-pass is required because WAC reads
--      balances that pass-2 mutations would invalidate within a batch.
--   4. Relaxes the qty-side gate for `cost_method='wac'`: WAC qty-side
--      cost-relevant events now pass through with caller's amount (= qty).
--      Gate stays for 'lot' and 'fifo' until those branches are
--      implemented (acct-8gg).
--
-- WAC rule (per Part IV §6.2):
--   For a cost-relevant value-side event, the CREDIT account is the
--   "source pool" — what's being consumed/issued. unit_cost is the
--   weighted-average for that pool, computed PRE-batch.
--
--   so_ship      cr inv_value_fg(sku, loc, USD)   -> matching qty: stock_available(sku, loc)
--   wo_complete  cr inv_value_wip(sku, op, USD)   -> matching qty: stock_wip(sku, op)
--   op_move      cr inv_value_wip(sku, op_from, USD) -> matching qty: stock_wip(sku, op_from)
--   scrap        cr inv_value_*                    -> matching qty: stock_available or stock_wip
--
-- Phase 0 ships per-location WAC (matches doc §4.1's partition).
-- Multi-location SKUs each have an independent running average per
-- location.

-- ============================================================
-- Helper: resolve matching qty-side account for a value-side account.
-- ============================================================

CREATE OR REPLACE FUNCTION _post_transfers_lookup_qty_account(
  p_value_acct accounts
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_id BIGINT;
BEGIN
  IF p_value_acct.ledger_kind <> 'value' OR p_value_acct.sku_id IS NULL THEN
    RETURN NULL;
  END IF;

  CASE p_value_acct.kind
    WHEN 'inv_value_raw', 'inv_value_fg' THEN
      IF p_value_acct.location_id IS NULL THEN
        RETURN NULL;
      END IF;
      SELECT id INTO v_id FROM accounts
        WHERE kind        = 'stock_available'
          AND sku_id      = p_value_acct.sku_id
          AND location_id = p_value_acct.location_id
          AND NOT is_closed;
      RETURN v_id;
    WHEN 'inv_value_wip' THEN
      IF p_value_acct.routing_op IS NULL THEN
        RETURN NULL;
      END IF;
      SELECT id INTO v_id FROM accounts
        WHERE kind       = 'stock_wip'
          AND sku_id     = p_value_acct.sku_id
          AND routing_op = p_value_acct.routing_op
          AND NOT is_closed;
      RETURN v_id;
    ELSE
      RETURN NULL;
  END CASE;
END;
$$;

COMMENT ON FUNCTION _post_transfers_lookup_qty_account(accounts) IS
  'Maps a value-side account to its matching qty-side account (acct-uxu). Returns NULL if no clean match.';

-- ============================================================
-- Updated dispatcher with WAC branch.
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

    WHEN 'wac' THEN
      -- WAC: unit_cost from CREDIT-side pool's pre-batch state.
      --   value_pool = p_c_acct (already loaded under FOR UPDATE)
      --   qty_pool   = matching stock_available or stock_wip account
      --                (added to lock set by post_transfers pre-scan)
      IF p_c_acct.ledger_kind <> 'value' THEN
        RAISE EXCEPTION 'wac requires credit-side value account, got % at event index %',
                        p_c_acct.kind, p_idx
          USING ERRCODE = 'P0006';
      END IF;

      v_qty_id := _post_transfers_lookup_qty_account(p_c_acct);
      IF v_qty_id IS NULL THEN
        RAISE EXCEPTION 'wac cannot resolve matching qty account for credit-side % at event index %',
                        p_c_acct.kind, p_idx
          USING ERRCODE = 'P0006';
      END IF;

      SELECT (debits_total - credits_total) INTO v_qty_balance
        FROM accounts WHERE id = v_qty_id;

      IF v_qty_balance IS NULL OR v_qty_balance <= 0 THEN
        RAISE EXCEPTION 'wac qty balance is %, cannot divide for unit cost at event index %',
                        v_qty_balance, p_idx
          USING ERRCODE = 'P0006';
      END IF;

      -- Value pool balance: debit-normal account, so balance = debits - credits.
      v_value_balance := p_c_acct.debits_total - p_c_acct.credits_total;
      IF v_value_balance < 0 THEN
        v_value_balance := 0;
      END IF;

      -- Integer division. unit_cost truncates the fractional part.
      -- amount = qty * unit_cost. Discrepancy between caller's value
      -- expectations and exact arithmetic appears as a residual in the
      -- value pool (ok: variance accounts capture it at period close).
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
-- Updated post_transfers: lock pre-scan + two-pass when WAC present.
-- ============================================================
--
-- Diff vs migration 0019:
--   * Pass 0 (pre-scan): walks events; for each WAC value-side
--     cost-relevant event, looks up the matching qty account ID via
--     `_post_transfers_lookup_qty_account` and adds it to the lock set.
--     Sets `v_has_wac` flag.
--   * FOR UPDATE acquires base + aux qty accounts (when present) in
--     ascending id order.
--   * If NO WAC event present → single-pass execution identical to
--     migration 0019 (avoid two-pass overhead on the hot path).
--   * If WAC event present → two-pass execution:
--     - Pass 1 (compute): under lock, computes amount[i] for each
--       event. Reads-only (no mutations). For WAC events this
--       captures the pre-batch unit cost.
--     - Pass 2 (apply): validates P0001-P0005, UPDATEs accounts,
--       INSERTs transfer.
--   * Qty-side gate relaxed for cost_method='wac': WAC qty-side
--     cost-relevant events pass through with caller's amount.
--     Gate stays for 'lot' and 'fifo' (P0006).
--
-- Why the single-pass / two-pass branch:
--   The two-pass refactor is required for WAC's pre-batch-balance
--   invariant. For non-WAC workloads, two-pass adds ~16% overhead at
--   100-writer scale (extra JSONB extracts, array storage, doubled
--   per-event iterations). Branching on `v_has_wac` keeps the hot
--   path (no WAC) at migration 0019's perf.

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
  v_amounts          BIGINT[];          -- pass-1 output, indexed 1..v_n (WAC path only)
  v_aux_qty_id       BIGINT;
  v_aux_qty_ids      BIGINT[] := '{}';  -- WAC aux qty ids
  v_has_wac          BOOLEAN  := FALSE; -- set by pre-scan; gates two-pass path
  v_has_cost_event   BOOLEAN;
BEGIN
  v_n := jsonb_array_length(p_events);
  IF v_n = 0 THEN
    RETURN '[]'::JSONB;
  END IF;

  -- ============================================================
  -- Fast short-circuit: if no event is cost-relevant, the entire
  -- WAC pre-scan is unnecessary. One SQL EXISTS replaces the
  -- per-event PL/pgSQL loop, which materially helps the common
  -- non-cost-relevant workload (e.g. bin_move heavy).
  -- ============================================================
  v_has_cost_event := EXISTS (
    SELECT 1 FROM jsonb_array_elements(p_events) ev
     WHERE (ev->>'reason')::transfer_reason IN ('op_move','scrap','wo_complete','so_ship')
  );

  IF v_has_cost_event THEN
    -- Pass 0: pre-scan for WAC auxiliary qty account ids. Snapshot
    -- reads only — no locks acquired here. Loop body skips after the
    -- cheap reason check.
    FOR v_idx IN 1..v_n LOOP
      v_event  := p_events -> (v_idx - 1);
      v_reason := (v_event->>'reason')::transfer_reason;
      IF v_reason NOT IN ('op_move','scrap','wo_complete','so_ship') THEN
        CONTINUE;
      END IF;
      v_c_id := (v_event->>'credit_account_id')::BIGINT;
      SELECT * INTO v_c_acct FROM accounts WHERE id = v_c_id;
      IF v_c_acct.ledger_kind <> 'value' THEN
        CONTINUE;  -- qty-side cost-relevant; no aux lookup needed
      END IF;
      v_cost_sku := v_c_acct.sku_id;
      IF v_cost_sku IS NULL THEN
        v_d_id := (v_event->>'debit_account_id')::BIGINT;
        SELECT * INTO v_d_acct FROM accounts WHERE id = v_d_id;
        v_cost_sku := v_d_acct.sku_id;
      END IF;
      IF v_cost_sku IS NULL THEN
        CONTINUE;  -- compute pass will P0006
      END IF;
      SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_cost_sku;
      IF v_cost_method = 'wac' THEN
        v_has_wac := TRUE;
        v_aux_qty_id := _post_transfers_lookup_qty_account(v_c_acct);
        IF v_aux_qty_id IS NOT NULL THEN
          v_aux_qty_ids := array_append(v_aux_qty_ids, v_aux_qty_id);
        END IF;
      END IF;
    END LOOP;
  END IF;

  -- ============================================================
  -- Acquire FOR UPDATE on base accounts (+ WAC aux when present).
  -- Branch the SELECT to avoid the unnest planner overhead when no
  -- WAC events are in the batch (the dominant case).
  -- ============================================================
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

  -- ============================================================
  -- Two execution paths:
  --   * Single-pass (no WAC events): identical to migration 0019.
  --     The hot path. Avoids two-pass overhead.
  --   * Two-pass (WAC events present): pass 1 computes amounts
  --     against PRE-batch balances; pass 2 validates and applies.
  -- ============================================================
  IF NOT v_has_wac THEN
    -- ----------- SINGLE-PASS (identical to migration 0019) -----------
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
          -- Qty-side: gate non-implemented methods. WAC qty-side passes
          -- through (qty doesn't depend on cost method) — but we won't
          -- hit this branch with v_has_wac=FALSE, so 'wac' here is
          -- impossible by construction. Kept as defensive code.
          IF v_cost_method NOT IN ('standard', 'wac') THEN
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

  -- ----------- TWO-PASS (WAC present in batch) -----------
  -- Pass 1: compute amount[i] for every event under lock, with
  -- pre-batch balances. Non-cost-relevant events skip account loads.
  v_amounts := array_fill(NULL::BIGINT, ARRAY[v_n]);
  FOR v_idx IN 1..v_n LOOP
    v_event    := p_events -> (v_idx - 1);
    v_reason   := (v_event->>'reason')::transfer_reason;

    IF v_reason NOT IN ('op_move','scrap','wo_complete','so_ship') THEN
      v_amounts[v_idx] := (v_event->>'amount')::BIGINT;
      CONTINUE;
    END IF;

    -- Skip pre-existing duplicates from compute (saves dispatcher work).
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
      IF v_cost_method NOT IN ('standard', 'wac') THEN
        RAISE EXCEPTION 'cost_method_not_implemented: % for reason % at event index %',
                        v_cost_method, v_reason, v_idx
          USING ERRCODE = 'P0006';
      END IF;
      v_amounts[v_idx] := (v_event->>'amount')::BIGINT;
    END IF;
  END LOOP;

  -- Pass 2: validate + apply.
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
