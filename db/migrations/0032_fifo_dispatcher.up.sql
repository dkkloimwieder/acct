-- ============================================================
-- Phase E1 E1.2 — FIFO dispatcher + apply_event layer writes
-- (acct-wb3f, sub-issue of acct-cbss).
--
-- Two halves working together:
--
--   (a) the cost-method dispatcher half (lookup + EXECUTE-format
--       per-strategy fn returning BIGINT amount). Registered in
--       cost_method_strategies as ('fifo','outbound',
--       '_compute_amount_fifo_outbound', FALSE).
--
--   (b) the apply_event D-block extension that, after posting_line
--       INSERT, writes cost_layers rows (for FIFO receipts) and
--       cost_layer_depletions rows (for FIFO issues) and stamps
--       posting_line_inventory.cost_layer_id with the first consumed
--       layer.
--
-- Design call resolved (per saved memory plan-phase-e1-fifo, Option A):
-- the dispatcher returns BIGINT (preserves the registry contract);
-- a separate `_fifo_write_depletions` helper called from apply_event
-- writes the depletion rows. Both reuse `_fifo_walk_layers` which
-- holds FOR UPDATE on cost_layers ORDER BY receipt_date ASC. The
-- two walks happen in the same transaction with locks held throughout,
-- so they produce identical allocations — SUM(depletions cost_amount)
-- equals posting_line.amount EXACTLY by construction.
--
-- Receipt-side layer creation: gated on debit-side `inv_value_raw`
-- (raw materials FIFO; FG-FIFO and WIP-FIFO out of scope at MVP).
-- Issue-side depletions: gated on credit-side `inv_value_raw`
-- (component issues, returns to vendor, etc.). Both gates require
-- the SKU's cost_method = 'fifo'.
--
-- The D-block's existing cost_method gate is widened from the WAC+
-- standard list to include 'fifo' so the inventory_movements row
-- continues to be written for FIFO posts (subledger consistency
-- with the rest of the cost-method matrix).
-- ============================================================

-- ============================================================
-- Shared FIFO walk. Returns one row per layer consumed, each with
-- the allocation taken and the resulting cost_amount (per-layer
-- ROUND to BIGINT cents). Walks ORDER BY receipt_date ASC, layer_id
-- ASC FOR UPDATE so concurrent calls serialize on the layer rows.
-- P0006 raised if layers exhausted before p_qty is satisfied.
--
-- Both _compute_amount_fifo_outbound (sums cost_amount → BIGINT
-- total) and _fifo_write_depletions (writes one row per allocation)
-- iterate this set. Locks held by the first call survive into the
-- second within the same transaction, so the two walks see identical
-- residuals.
-- ============================================================

CREATE OR REPLACE FUNCTION _fifo_walk_layers(
  p_product_id   UUID,
  p_location_id  UUID,
  p_cost_book_id SMALLINT,
  p_qty          NUMERIC
) RETURNS TABLE (
  layer_id     BIGINT,
  receipt_date DATE,
  allocation   NUMERIC,
  unit_cost    NUMERIC,
  cost_amount  BIGINT
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_remaining NUMERIC := p_qty;
  v_layer     RECORD;
  v_alloc     NUMERIC;
BEGIN
  IF p_qty IS NULL OR p_qty <= 0 THEN
    RAISE EXCEPTION 'fifo_walk_invalid_qty: % must be positive', p_qty
      USING ERRCODE = 'P0006';
  END IF;

  FOR v_layer IN
    SELECT
      cl.layer_id     AS lid,
      cl.receipt_date AS rd,
      cl.unit_cost    AS uc,
      cl.original_quantity - COALESCE(
        (SELECT SUM(d.depleted_quantity)
           FROM cost_layer_depletions d
          WHERE d.layer_id = cl.layer_id
            AND d.layer_receipt_date = cl.receipt_date),
        0
      )                AS residual
      FROM cost_layers cl
     WHERE cl.product_id   = p_product_id
       AND cl.location_id  = p_location_id
       AND cl.cost_book_id = p_cost_book_id
     ORDER BY cl.receipt_date ASC, cl.layer_id ASC
       FOR UPDATE
  LOOP
    IF v_remaining <= 0 THEN EXIT; END IF;
    IF v_layer.residual <= 0 THEN CONTINUE; END IF;

    v_alloc := LEAST(v_layer.residual, v_remaining);

    layer_id     := v_layer.lid;
    receipt_date := v_layer.rd;
    allocation   := v_alloc;
    unit_cost    := v_layer.uc;
    cost_amount  := ROUND(v_alloc * v_layer.uc)::BIGINT;
    RETURN NEXT;

    v_remaining := v_remaining - v_alloc;
  END LOOP;

  IF v_remaining > 0 THEN
    RAISE EXCEPTION
      'fifo_layers_exhausted: product=% location=% requested=% short=%',
      p_product_id, p_location_id, p_qty, v_remaining
      USING ERRCODE = 'P0006';
  END IF;
END;
$$;

-- ============================================================
-- FIFO outbound dispatcher. Same signature as other strategies.
-- Walks layers FOR UPDATE; sums per-layer cost_amount; returns BIGINT.
-- ============================================================

CREATE OR REPLACE FUNCTION _compute_amount_fifo_outbound(
  p_event   JSONB,
  p_d_acct  accounts,
  p_c_acct  accounts,
  p_idx     INT
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_qty          NUMERIC;
  v_product_id   UUID;
  v_location_id  UUID;
  v_total_amount BIGINT := 0;
  v_walk         RECORD;
BEGIN
  v_qty := ABS((p_event->>'qty')::NUMERIC);

  IF p_c_acct.ledger_kind <> 'value' THEN
    RAISE EXCEPTION
      'fifo requires credit-side value account, got % at event index %',
      p_c_acct.kind, p_idx
      USING ERRCODE = 'P0006';
  END IF;

  -- R2: credit-first SKU resolution.
  v_product_id  := COALESCE(p_c_acct.sku_id, p_d_acct.sku_id);
  v_location_id := COALESCE(p_c_acct.location_id, p_d_acct.location_id);
  IF v_product_id IS NULL OR v_location_id IS NULL THEN
    RAISE EXCEPTION
      'fifo dispatch requires resolved (sku, location) at event index %',
      p_idx USING ERRCODE = 'P0006';
  END IF;

  FOR v_walk IN
    SELECT * FROM _fifo_walk_layers(v_product_id, v_location_id, 1::SMALLINT, v_qty)
  LOOP
    v_total_amount := v_total_amount + v_walk.cost_amount;
  END LOOP;

  RETURN v_total_amount;
END;
$$;

-- ============================================================
-- _fifo_write_depletions — called from apply_event D-block AFTER
-- posting_line INSERT, on FIFO outbound posts. Re-walks the layers
-- (locks held from the dispatcher's earlier walk; same allocations);
-- INSERTs one cost_layer_depletions row per consumed layer; returns
-- the FIRST consumed layer_id so apply_event can stamp it on
-- posting_line_inventory.cost_layer_id.
--
-- The append-only trigger on cost_layer_depletions only blocks
-- UPDATE/DELETE — INSERTs are fine.
-- ============================================================

CREATE OR REPLACE FUNCTION _fifo_write_depletions(
  p_posting_line_id BIGINT,
  p_product_id      UUID,
  p_location_id     UUID,
  p_cost_book_id    SMALLINT,
  p_qty             NUMERIC,
  p_issue_date      DATE
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_walk           RECORD;
  v_first_layer_id BIGINT := NULL;
BEGIN
  FOR v_walk IN
    SELECT * FROM _fifo_walk_layers(p_product_id, p_location_id, p_cost_book_id, p_qty)
  LOOP
    IF v_first_layer_id IS NULL THEN
      v_first_layer_id := v_walk.layer_id;
    END IF;
    INSERT INTO cost_layer_depletions (
      layer_id, layer_receipt_date, issue_date,
      depleted_quantity, unit_cost, cost_amount, posting_line_id
    ) VALUES (
      v_walk.layer_id, v_walk.receipt_date, p_issue_date,
      v_walk.allocation, v_walk.unit_cost, v_walk.cost_amount,
      p_posting_line_id
    );
  END LOOP;
  RETURN v_first_layer_id;
END;
$$;

-- ============================================================
-- Strategy registry seed for FIFO.
-- ============================================================

INSERT INTO cost_method_strategies
  (cost_method, event_kind, compute_fn_name,                     flag_provisional)
VALUES
  ('fifo',      'outbound', '_compute_amount_fifo_outbound',     FALSE);

-- ============================================================
-- post_posting_lines — qty-leg cost_method gate widened to
-- include 'fifo' alongside the WAC family + standard. Body
-- otherwise identical to mig 0014. The qty-leg gate fires when
-- a cost-event reason (op_move / scrap / wo_complete / so_ship)
-- has a qty-side posting; today it allows the four cost methods
-- and rejects FIFO + lot at P0006. After E1.2, FIFO joins the
-- allowed list (lot still rejected pending acct-uze). FIFO does
-- NOT trigger the two-pass WAC-style flow — the v_has_wac probe
-- (line 389) only matches WAC variants, and FIFO's FOR UPDATE
-- locks live inside _fifo_walk_layers per layer-row.
-- ============================================================

CREATE OR REPLACE FUNCTION post_posting_lines(
  p_events                JSONB,
  p_override_closed_period BOOLEAN DEFAULT FALSE
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
  v_n              INT;
  v_idx            INT;
  v_event          JSONB;
  v_d_acct         accounts;
  v_c_acct         accounts;
  v_results        JSONB := '[]'::JSONB;
  v_d_id           BIGINT;
  v_c_id           BIGINT;
  v_idem_key       UUID;
  v_reason         posting_line_reason;
  v_cost_sku       UUID;
  v_cost_method    cost_method;
  v_amount         BIGINT;
  v_amounts        BIGINT[];
  v_aux_qty_id     BIGINT;
  v_aux_qty_ids    BIGINT[] := '{}';
  v_has_wac        BOOLEAN  := FALSE;
  v_has_cost_event BOOLEAN;
BEGIN
  v_n := jsonb_array_length(p_events);
  IF v_n = 0 THEN RETURN '[]'::JSONB; END IF;

  v_has_cost_event := EXISTS (
    SELECT 1 FROM jsonb_array_elements(p_events) ev
     WHERE (ev->>'reason')::posting_line_reason
           IN ('op_move','scrap','wo_complete','so_ship')
  );

  IF v_has_cost_event THEN
    FOR v_idx IN 1..v_n LOOP
      v_event  := p_events -> (v_idx - 1);
      v_reason := (v_event->>'reason')::posting_line_reason;
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
        v_aux_qty_id := _post_posting_lines_lookup_qty_account(v_c_acct);
        IF v_aux_qty_id IS NOT NULL THEN
          v_aux_qty_ids := array_append(v_aux_qty_ids, v_aux_qty_id);
        END IF;
      END IF;
    END LOOP;
  END IF;

  PERFORM _post_posting_lines_lock_pre_scan(p_events, v_aux_qty_ids);

  IF NOT v_has_wac THEN
    FOR v_idx IN 1..v_n LOOP
      v_event    := p_events -> (v_idx - 1);
      v_idem_key := (v_event->>'idempotency_key')::UUID;
      IF EXISTS (SELECT 1 FROM posting_lines WHERE idempotency_key = v_idem_key) THEN
        v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'exists');
        CONTINUE;
      END IF;
      v_d_id := (v_event->>'debit_account_id')::BIGINT;
      v_c_id := (v_event->>'credit_account_id')::BIGINT;
      v_reason := (v_event->>'reason')::posting_line_reason;
      v_cost_method := NULL;
      SELECT * INTO v_d_acct FROM accounts WHERE id = v_d_id;
      SELECT * INTO v_c_acct FROM accounts WHERE id = v_c_id;
      IF v_reason IN ('op_move','scrap','wo_complete','so_ship') THEN
        v_cost_sku := COALESCE(v_c_acct.sku_id, v_d_acct.sku_id);
        IF v_cost_sku IS NULL THEN
          IF v_reason = 'so_ship' AND v_d_acct.ledger_kind = 'value' THEN
            v_amount := (v_event->>'amount')::BIGINT;
          ELSE
            RAISE EXCEPTION
              'cost_method_not_implemented: sku not resolvable for reason % at event index %',
              v_reason, v_idx USING ERRCODE = 'P0006';
          END IF;
        ELSE
          SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_cost_sku;
          IF v_d_acct.ledger_kind = 'value' THEN
            v_amount := _post_posting_lines_compute_amount(
              v_event, v_d_acct, v_c_acct, v_cost_method, v_idx
            );
          ELSE
            IF v_cost_method NOT IN
               ('standard', 'wac_perpetual', 'wac_periodic', 'wac_retroactive', 'fifo') THEN
              RAISE EXCEPTION
                'cost_method_not_implemented: % for reason % at event index %',
                v_cost_method, v_reason, v_idx USING ERRCODE = 'P0006';
            END IF;
            v_amount := (v_event->>'amount')::BIGINT;
          END IF;
        END IF;
      ELSE
        v_amount := (v_event->>'amount')::BIGINT;
      END IF;
      PERFORM _post_posting_lines_apply_event(
        v_event, v_idx, v_amount, v_d_acct, v_c_acct,
        v_cost_method, p_override_closed_period
      );
      v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'ok');
    END LOOP;
    RETURN v_results;
  END IF;

  v_amounts := array_fill(NULL::BIGINT, ARRAY[v_n]);
  FOR v_idx IN 1..v_n LOOP
    v_event  := p_events -> (v_idx - 1);
    v_reason := (v_event->>'reason')::posting_line_reason;
    IF v_reason NOT IN ('op_move','scrap','wo_complete','so_ship') THEN
      v_amounts[v_idx] := (v_event->>'amount')::BIGINT;
      CONTINUE;
    END IF;
    v_idem_key := (v_event->>'idempotency_key')::UUID;
    IF EXISTS (SELECT 1 FROM posting_lines WHERE idempotency_key = v_idem_key) THEN
      CONTINUE;
    END IF;
    v_d_id := (v_event->>'debit_account_id')::BIGINT;
    v_c_id := (v_event->>'credit_account_id')::BIGINT;
    SELECT * INTO v_d_acct FROM accounts WHERE id = v_d_id;
    SELECT * INTO v_c_acct FROM accounts WHERE id = v_c_id;
    v_cost_sku := COALESCE(v_c_acct.sku_id, v_d_acct.sku_id);
    IF v_cost_sku IS NULL THEN
      IF v_reason = 'so_ship' AND v_d_acct.ledger_kind = 'value' THEN
        v_amounts[v_idx] := (v_event->>'amount')::BIGINT;
      ELSE
        RAISE EXCEPTION
          'cost_method_not_implemented: sku not resolvable for reason % at event index %',
          v_reason, v_idx USING ERRCODE = 'P0006';
      END IF;
    ELSE
      SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_cost_sku;
      IF v_d_acct.ledger_kind = 'value' THEN
        v_amounts[v_idx] := _post_posting_lines_compute_amount(
          v_event, v_d_acct, v_c_acct, v_cost_method, v_idx
        );
      ELSE
        IF v_cost_method NOT IN
           ('standard', 'wac_perpetual', 'wac_periodic', 'wac_retroactive', 'fifo') THEN
          RAISE EXCEPTION
            'cost_method_not_implemented: % for reason % at event index %',
            v_cost_method, v_reason, v_idx USING ERRCODE = 'P0006';
        END IF;
        v_amounts[v_idx] := (v_event->>'amount')::BIGINT;
      END IF;
    END IF;
  END LOOP;

  FOR v_idx IN 1..v_n LOOP
    v_event    := p_events -> (v_idx - 1);
    v_idem_key := (v_event->>'idempotency_key')::UUID;
    IF EXISTS (SELECT 1 FROM posting_lines WHERE idempotency_key = v_idem_key) THEN
      v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'exists');
      CONTINUE;
    END IF;
    v_d_id := (v_event->>'debit_account_id')::BIGINT;
    v_c_id := (v_event->>'credit_account_id')::BIGINT;
    v_reason := (v_event->>'reason')::posting_line_reason;
    v_amount := v_amounts[v_idx];
    v_cost_method := NULL;
    SELECT * INTO v_d_acct FROM accounts WHERE id = v_d_id;
    SELECT * INTO v_c_acct FROM accounts WHERE id = v_c_id;
    IF v_reason IN ('op_move','scrap','wo_complete','so_ship') THEN
      v_cost_sku := COALESCE(v_c_acct.sku_id, v_d_acct.sku_id);
      IF v_cost_sku IS NOT NULL THEN
        SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_cost_sku;
      END IF;
    END IF;
    PERFORM _post_posting_lines_apply_event(
      v_event, v_idx, v_amount, v_d_acct, v_c_acct,
      v_cost_method, p_override_closed_period
    );
    v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'ok');
  END LOOP;
  RETURN v_results;
END;
$$;

-- ============================================================
-- _post_posting_lines_apply_event — extended with E1 block.
--
-- Body identical to mig 0029 EXCEPT:
--   1. cost_method gate widened from 4-element list to include 'fifo'
--      so the existing D-block writes inventory_movements rows for
--      FIFO posts (subledger consistency).
--   2. New E1 block AFTER the D-block: writes cost_layers (FIFO
--      receipts) or cost_layer_depletions (FIFO issues), and
--      updates posting_line_inventory.cost_layer_id.
-- ============================================================

CREATE OR REPLACE FUNCTION _post_posting_lines_apply_event(
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
  v_period_id          BIGINT;
  v_period_closed      TIMESTAMPTZ;
  v_business_date      DATE;
  v_qty_for_row        BIGINT;
  v_reason             posting_line_reason;
  v_idem_key           UUID;
  v_new_id             BIGINT;
  v_event_qty          BIGINT;
  v_resolved_cm        cost_method;
  v_cost_sku           UUID;
  v_reverses_id        BIGINT;
  v_parent_doc         UUID;
  v_ic_pair            UUID;
  v_proc               VARCHAR;
  v_functional_ccy     CHAR(3);
  v_fx_rate            NUMERIC(20, 10);
  v_dim_sku            UUID;
  v_dim_loc            UUID;
  v_dim_routing_op     INT;
  v_event_cp           UUID;
  v_dim_cp             UUID;
  v_dim_cp_type        SMALLINT;
  v_inv_unit_cost      NUMERIC(19, 4);
  v_inv_cost_method    cost_method;
  v_im_event_type      SMALLINT;
  v_im_std_unit_cost   NUMERIC(19, 4);
  v_fifo_first_layer   BIGINT;
BEGIN
  v_business_date := (p_event->>'business_date')::DATE;
  v_reason        := (p_event->>'reason')::posting_line_reason;
  v_idem_key      := (p_event->>'idempotency_key')::UUID;

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

  v_qty_for_row := (p_event->>'qty')::BIGINT;
  IF v_qty_for_row IS NULL
     AND p_d_acct.ledger_kind = 'qty'
     AND p_c_acct.ledger_kind = 'qty' THEN
    v_qty_for_row := p_amount;
  END IF;

  UPDATE accounts SET debits_total  = debits_total  + p_amount
    WHERE id = p_d_acct.id;
  UPDATE accounts SET credits_total = credits_total + p_amount
    WHERE id = p_c_acct.id;
  INSERT INTO posting_lines (
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
  IF v_reason IN ('op_move','scrap','wo_complete','so_ship',
                  'op_move_v','scrap_v','wo_complete_v',
                  'rm_issue_to_wo')
     AND p_d_acct.ledger_kind = 'value' THEN
    v_resolved_cm := p_cost_method;
    IF v_resolved_cm IS NULL THEN
      v_cost_sku := COALESCE(p_c_acct.sku_id, p_d_acct.sku_id);
      IF v_cost_sku IS NOT NULL THEN
        SELECT cost_method INTO v_resolved_cm FROM skus WHERE id = v_cost_sku;
      END IF;
    END IF;
    IF v_resolved_cm IN ('wac_periodic', 'wac_retroactive') THEN
      v_event_qty := (p_event->>'qty')::BIGINT;
      INSERT INTO posting_lines_provisional (posting_line_id, period_id, cost_method, qty)
      VALUES (v_new_id, v_period_id, v_resolved_cm, v_event_qty);
    END IF;
  END IF;

  -- B1 extension write.
  v_reverses_id := (p_event->>'reverses_posting_line_id')::BIGINT;
  v_parent_doc  := (p_event->>'parent_document_id')::UUID;
  v_ic_pair     := (p_event->>'intercompany_pair_id')::UUID;
  v_proc        := p_event->>'created_by_process';
  IF v_reverses_id IS NOT NULL
     OR v_parent_doc  IS NOT NULL
     OR v_ic_pair     IS NOT NULL
     OR v_proc        IS NOT NULL THEN
    INSERT INTO posting_line_sources (
      posting_line_id, reverses_posting_line_id, parent_document_id,
      intercompany_pair_id, created_by_process
    ) VALUES (
      v_new_id, v_reverses_id, v_parent_doc, v_ic_pair, v_proc
    );
  END IF;

  -- B2 extension write.
  IF p_c_acct.ledger_kind = 'value' THEN
    SELECT functional_currency INTO v_functional_ccy
      FROM legal_entities WHERE id = p_c_acct.legal_entity_id;

    IF v_functional_ccy IS NOT NULL
       AND p_c_acct.currency <> v_functional_ccy THEN
      SELECT rate INTO v_fx_rate
        FROM fx_rates
       WHERE from_currency = p_c_acct.currency
         AND to_currency   = v_functional_ccy
         AND effective_at::DATE <= v_business_date
       ORDER BY effective_at DESC LIMIT 1;
      IF v_fx_rate IS NULL THEN
        RAISE EXCEPTION
          'missing_fx_rate: no fx_rates row found for % → % effective_at <= %',
          p_c_acct.currency, v_functional_ccy, v_business_date
          USING ERRCODE = 'P0050';
      END IF;

      INSERT INTO posting_line_currencies (
        posting_line_id, amount_transaction, currency_transaction,
        fx_rate_to_functional
      ) VALUES (
        v_new_id, p_amount, p_c_acct.currency, v_fx_rate
      );
    END IF;
  END IF;

  -- B3 extension writes. Credit-first composition resolution per R2.
  v_dim_sku := COALESCE(p_c_acct.sku_id, p_d_acct.sku_id);
  IF v_dim_sku IS NOT NULL THEN
    INSERT INTO posting_line_dimensions
      (posting_line_id, dimension_type, dimension_value_uuid)
      VALUES (v_new_id, 3, v_dim_sku);
  END IF;

  v_dim_loc := COALESCE(p_c_acct.location_id, p_d_acct.location_id);
  IF v_dim_loc IS NOT NULL THEN
    INSERT INTO posting_line_dimensions
      (posting_line_id, dimension_type, dimension_value_uuid)
      VALUES (v_new_id, 4, v_dim_loc);
  END IF;

  v_dim_routing_op := COALESCE(
    (p_event->>'routing_op')::INT,
    p_c_acct.routing_op,
    p_d_acct.routing_op
  );
  IF v_dim_routing_op IS NOT NULL THEN
    INSERT INTO posting_line_dimensions
      (posting_line_id, dimension_type, dimension_value)
      VALUES (v_new_id, 5, v_dim_routing_op::BIGINT);
  END IF;

  v_event_cp := (p_event->>'counterparty_id')::UUID;
  v_dim_cp := COALESCE(v_event_cp, p_c_acct.counterparty_id, p_d_acct.counterparty_id);
  IF v_dim_cp IS NOT NULL THEN
    IF p_c_acct.kind IN ('ar','ar_unsettled','customer_pool')
       OR p_d_acct.kind IN ('ar','ar_unsettled','customer_pool') THEN
      v_dim_cp_type := 1;
    ELSIF p_c_acct.kind IN ('ap','ap_unsettled','vendor_pool','accrued_disposal_liability')
       OR p_d_acct.kind IN ('ap','ap_unsettled','vendor_pool','accrued_disposal_liability') THEN
      v_dim_cp_type := 2;
    ELSE
      v_dim_cp_type := NULL;
    END IF;
    IF v_dim_cp_type IS NOT NULL THEN
      INSERT INTO posting_line_dimensions
        (posting_line_id, dimension_type, dimension_value_uuid)
        VALUES (v_new_id, v_dim_cp_type, v_dim_cp);
    END IF;
  END IF;

  -- C extension write. One row per qty-bearing inventory posting_line.
  IF v_qty_for_row IS NOT NULL
     AND COALESCE(p_c_acct.sku_id, p_d_acct.sku_id) IS NOT NULL THEN
    IF p_d_acct.ledger_kind = 'value' AND v_qty_for_row <> 0 THEN
      v_inv_unit_cost := p_amount::NUMERIC / ABS(v_qty_for_row)::NUMERIC;
    ELSE
      v_inv_unit_cost := NULL;
    END IF;

    SELECT cost_method INTO v_inv_cost_method
      FROM skus
     WHERE id = COALESCE(p_c_acct.sku_id, p_d_acct.sku_id);

    INSERT INTO posting_line_inventory (
      posting_line_id, product_id, quantity, qty_uom,
      unit_cost, cost_method_at_event
    ) VALUES (
      v_new_id,
      COALESCE(p_c_acct.sku_id, p_d_acct.sku_id),
      ABS(v_qty_for_row)::NUMERIC,
      'EA',
      v_inv_unit_cost,
      v_inv_cost_method
    );

    -- D extension writes — TWO-ROW per-leg attribution. Now includes
    -- 'fifo' alongside the WAC family + standard, so FIFO posts also
    -- write inventory_movements rows for subledger consistency.
    IF v_inv_cost_method IN ('standard', 'wac_perpetual',
                             'wac_periodic', 'wac_retroactive', 'fifo')
       AND p_d_acct.ledger_kind = 'value'
       AND v_qty_for_row <> 0 THEN

      -- DR side
      IF p_d_acct.kind::TEXT LIKE 'inv_value_%'
         AND p_d_acct.sku_id IS NOT NULL
         AND p_d_acct.location_id IS NOT NULL THEN

        v_im_event_type := _inventory_movement_event_type(
          v_reason, ABS(v_qty_for_row)::NUMERIC);
        IF v_im_event_type IS NOT NULL THEN
          SELECT cost::NUMERIC INTO v_im_std_unit_cost
            FROM standard_costs
           WHERE sku_id = p_d_acct.sku_id
             AND effective_at <= v_business_date
           ORDER BY effective_at DESC LIMIT 1;

          INSERT INTO inventory_movements (
            product_id, legal_entity_id, location_id,
            event_type, movement_date, quantity,
            standard_unit_cost, actual_unit_cost,
            cost_currency, posting_line_id
          ) VALUES (
            p_d_acct.sku_id,
            p_d_acct.legal_entity_id,
            p_d_acct.location_id,
            v_im_event_type,
            v_business_date,
            ABS(v_qty_for_row)::NUMERIC,
            v_im_std_unit_cost,
            v_inv_unit_cost,
            p_d_acct.currency,
            v_new_id
          );
        END IF;
      END IF;

      -- CR side
      IF p_c_acct.kind::TEXT LIKE 'inv_value_%'
         AND p_c_acct.sku_id IS NOT NULL
         AND p_c_acct.location_id IS NOT NULL THEN

        v_im_event_type := _inventory_movement_event_type(
          v_reason, -ABS(v_qty_for_row)::NUMERIC);
        IF v_im_event_type IS NOT NULL THEN
          SELECT cost::NUMERIC INTO v_im_std_unit_cost
            FROM standard_costs
           WHERE sku_id = p_c_acct.sku_id
             AND effective_at <= v_business_date
           ORDER BY effective_at DESC LIMIT 1;

          INSERT INTO inventory_movements (
            product_id, legal_entity_id, location_id,
            event_type, movement_date, quantity,
            standard_unit_cost, actual_unit_cost,
            cost_currency, posting_line_id
          ) VALUES (
            p_c_acct.sku_id,
            p_c_acct.legal_entity_id,
            p_c_acct.location_id,
            v_im_event_type,
            v_business_date,
            -ABS(v_qty_for_row)::NUMERIC,
            v_im_std_unit_cost,
            v_inv_unit_cost,
            p_c_acct.currency,
            v_new_id
          );
        END IF;
      END IF;
    END IF;

    -- E1 extension writes — FIFO layer state.
    --
    -- Receipts (DR side inv_value_raw, FIFO SKU): create one
    -- cost_layers row at this receipt's unit_cost.
    --
    -- Issues (CR side inv_value_raw, FIFO SKU): walk layers
    -- via _fifo_write_depletions, write per-layer depletions,
    -- stamp first consumed layer on posting_line_inventory.
    IF v_inv_cost_method = 'fifo' AND v_qty_for_row <> 0 THEN

      -- Receipt-side layer creation.
      IF p_d_acct.kind = 'inv_value_raw'
         AND p_d_acct.sku_id IS NOT NULL
         AND p_d_acct.location_id IS NOT NULL THEN
        INSERT INTO cost_layers (
          product_id, legal_entity_id, location_id,
          receipt_posting_line_id, receipt_date,
          original_quantity, unit_cost, cost_currency
        ) VALUES (
          p_d_acct.sku_id,
          p_d_acct.legal_entity_id,
          p_d_acct.location_id,
          v_new_id,
          v_business_date,
          ABS(v_qty_for_row)::NUMERIC,
          v_inv_unit_cost,
          p_d_acct.currency
        );
      END IF;

      -- Issue-side depletion writeback.
      IF p_c_acct.kind = 'inv_value_raw'
         AND p_c_acct.sku_id IS NOT NULL
         AND p_c_acct.location_id IS NOT NULL THEN
        v_fifo_first_layer := _fifo_write_depletions(
          v_new_id,
          p_c_acct.sku_id,
          p_c_acct.location_id,
          1::SMALLINT,
          ABS(v_qty_for_row)::NUMERIC,
          v_business_date
        );
        UPDATE posting_line_inventory
           SET cost_layer_id = v_fifo_first_layer
         WHERE posting_line_id = v_new_id;
      END IF;
    END IF;
  END IF;

  RETURN v_new_id;
END;
$$;
