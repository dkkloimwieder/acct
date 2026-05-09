-- ============================================================
-- Phase E2 E2.2 — lot_fifo dispatcher + apply_event lot writes
-- (claimed against acct-uze; sub-issue to be filed at completion).
--
-- Three halves working together (mirrors E1.2's FIFO pattern):
--
--   (a) the cost-method dispatcher: '_compute_amount_lot_fifo_outbound'
--       registered in cost_method_strategies as
--       ('lot_fifo','outbound', '_compute_amount_lot_fifo_outbound', FALSE).
--       Returns BIGINT amount via _lot_walk_layers (specific lot if
--       event JSON carries `lot_id`, else FIFO-by-receipt-date walk).
--
--   (b) the apply_event D-block extension that, after posting_line
--       INSERT, writes inventory_lot_events 'issue' rows for
--       outflows and inventory_lots rows for inflows, plus stamps
--       posting_line_inventory.lot_id with the first consumed
--       (or just-created) lot_id.
--
--   (c) post_posting_lines qty-leg cost_method gate widened to
--       include 'lot_fifo' alongside the WAC family + standard +
--       fifo, so qty-leg posts on lot-tracked SKUs don't raise P0006.
--
-- Schema extensions:
--
--   * posting_line_inventory.lot_id BIGINT NULL — per-leg lot stamp
--     (soft pointer; composite PK on inventory_lots forbids a single-
--     column FK; mirrors cost_layer_id pattern from mig 0024).
--   * inventory_movements.lot_id BIGINT NULL — per-leg lot stamp on
--     the subledger row. Recon invariant E2.4 will join on this.
--
-- Receipt-side lot creation: gated on (debit-side inv_value_raw or
-- inv_value_fg) AND cost_method='lot_fifo'. Reads lot metadata
-- (lot_code REQUIRED, expiration_date / manufacture_date /
-- supplier_lot_number / quality_status / attributes optional) from
-- the event JSON; INSERTs inventory_lots; stamps
-- posting_line_inventory.lot_id.
--
-- Issue-side depletion: gated on (credit-side inv_value_raw or
-- inv_value_fg) AND cost_method='lot_fifo'. event JSON's `lot_id`
-- (optional) selects a specific lot; otherwise FIFO walks by
-- receipt_date. Empty lot pool → P0006.
--
-- Wrapper integration (E2.3) and recon invariant (E2.4) are
-- separate sub-issues. Until E2.3 ships, lot_fifo only flows via
-- direct post_posting_lines calls (test fixtures + the t1 probe).
-- ============================================================

-- (cost_method enum extension 'lot_fifo' was added in mig 0045 —
--  PG forbids using a newly-added enum value in the same transaction
--  as the ALTER TYPE that added it; the registry INSERT below
--  consumes that value, so the enum lift runs in its own mig.)

-- ---------- 1. Stamping columns on subledger ----------
-- posting_line_inventory.lot_id was pre-declared in mig 0024 alongside
-- cost_layer_id / unit_id (partial index already present). Only
-- inventory_movements needs the lot_id column added here.

ALTER TABLE inventory_movements    ADD COLUMN lot_id BIGINT;
CREATE INDEX inventory_movements_lot
  ON inventory_movements (lot_id) WHERE lot_id IS NOT NULL;

-- ---------- 3. _lot_walk_layers ----------

-- Walks lots for (product, location, cost_book) under FOR UPDATE,
-- returning per-lot allocations whose unit_cost+cost_amount sum to
-- the depletion's amount. p_specific_lot_id NOT NULL pins to that
-- single lot (raises P0006 if its residual is short); NULL FIFOs
-- ORDER BY receipt_date ASC, lot_id ASC. Mirrors _fifo_walk_layers.

CREATE OR REPLACE FUNCTION _lot_walk_layers(
  p_product_id      UUID,
  p_location_id     UUID,
  p_cost_book_id    SMALLINT,
  p_qty             NUMERIC,
  p_specific_lot_id BIGINT DEFAULT NULL
) RETURNS TABLE (
  lot_id       BIGINT,
  receipt_date DATE,
  allocation   NUMERIC,
  unit_cost    NUMERIC,
  cost_amount  BIGINT
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_remaining NUMERIC := p_qty;
  v_lot       RECORD;
  v_alloc     NUMERIC;
BEGIN
  IF p_qty IS NULL OR p_qty <= 0 THEN
    RAISE EXCEPTION 'lot_walk_invalid_qty: % must be positive', p_qty
      USING ERRCODE = 'P0006';
  END IF;

  FOR v_lot IN
    SELECT
      il.lot_id        AS lid,
      il.receipt_date  AS rd,
      il.unit_cost     AS uc,
      il.original_quantity + COALESCE(
        (SELECT SUM(e.quantity_change)
           FROM inventory_lot_events e
          WHERE e.lot_id = il.lot_id
            AND e.lot_receipt_date = il.receipt_date),
        0
      )                AS residual
      FROM inventory_lots il
     WHERE il.product_id   = p_product_id
       AND il.location_id  = p_location_id
       AND il.cost_book_id = p_cost_book_id
       AND (p_specific_lot_id IS NULL OR il.lot_id = p_specific_lot_id)
     ORDER BY il.receipt_date ASC, il.lot_id ASC
       FOR UPDATE
  LOOP
    IF v_remaining <= 0 THEN EXIT; END IF;
    IF v_lot.residual <= 0 THEN CONTINUE; END IF;

    v_alloc := LEAST(v_lot.residual, v_remaining);

    lot_id       := v_lot.lid;
    receipt_date := v_lot.rd;
    allocation   := v_alloc;
    unit_cost    := v_lot.uc;
    cost_amount  := ROUND(v_alloc * v_lot.uc)::BIGINT;
    RETURN NEXT;

    v_remaining := v_remaining - v_alloc;
  END LOOP;

  IF v_remaining > 0 THEN
    IF p_specific_lot_id IS NOT NULL THEN
      RAISE EXCEPTION
        'lot_residual_short: lot=% requested=% short=%',
        p_specific_lot_id, p_qty, v_remaining
        USING ERRCODE = 'P0006';
    ELSE
      RAISE EXCEPTION
        'lot_layers_exhausted: product=% location=% requested=% short=%',
        p_product_id, p_location_id, p_qty, v_remaining
        USING ERRCODE = 'P0006';
    END IF;
  END IF;
END;
$$;

-- ---------- 4. _compute_amount_lot_fifo_outbound ----------

CREATE OR REPLACE FUNCTION _compute_amount_lot_fifo_outbound(
  p_event   JSONB,
  p_d_acct  accounts,
  p_c_acct  accounts,
  p_idx     INT
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_qty             NUMERIC;
  v_product_id      UUID;
  v_location_id     UUID;
  v_specific_lot_id BIGINT;
  v_total_amount    BIGINT := 0;
  v_walk            RECORD;
BEGIN
  v_qty := ABS((p_event->>'qty')::NUMERIC);

  IF p_c_acct.ledger_kind <> 'value' THEN
    RAISE EXCEPTION
      'lot_fifo requires credit-side value account, got % at event index %',
      p_c_acct.kind, p_idx
      USING ERRCODE = 'P0006';
  END IF;

  -- R2: credit-first SKU resolution.
  v_product_id  := COALESCE(p_c_acct.sku_id, p_d_acct.sku_id);
  v_location_id := COALESCE(p_c_acct.location_id, p_d_acct.location_id);
  IF v_product_id IS NULL OR v_location_id IS NULL THEN
    RAISE EXCEPTION
      'lot_fifo dispatch requires resolved (sku, location) at event index %',
      p_idx USING ERRCODE = 'P0006';
  END IF;

  v_specific_lot_id := (p_event->>'lot_id')::BIGINT;

  FOR v_walk IN
    SELECT * FROM _lot_walk_layers(
      v_product_id, v_location_id, 1::SMALLINT, v_qty, v_specific_lot_id
    )
  LOOP
    v_total_amount := v_total_amount + v_walk.cost_amount;
  END LOOP;

  RETURN v_total_amount;
END;
$$;

-- ---------- 5. _lot_write_issues ----------

-- Called from apply_event after posting_line INSERT for lot
-- outflows. Re-walks (locks held from the dispatcher's earlier
-- walk; same allocations); INSERTs one inventory_lot_events 'issue'
-- (event_type=1) row per consumed lot; returns the FIRST consumed
-- lot_id so apply_event can stamp it on posting_line_inventory.lot_id.

CREATE OR REPLACE FUNCTION _lot_write_issues(
  p_posting_line_id BIGINT,
  p_product_id      UUID,
  p_location_id     UUID,
  p_cost_book_id    SMALLINT,
  p_qty             NUMERIC,
  p_business_date   DATE,
  p_specific_lot_id BIGINT DEFAULT NULL
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_walk        RECORD;
  v_first_lot   BIGINT := NULL;
BEGIN
  FOR v_walk IN
    SELECT * FROM _lot_walk_layers(
      p_product_id, p_location_id, p_cost_book_id, p_qty, p_specific_lot_id
    )
  LOOP
    IF v_first_lot IS NULL THEN
      v_first_lot := v_walk.lot_id;
    END IF;
    INSERT INTO inventory_lot_events (
      lot_id, lot_receipt_date, event_date, event_type,
      quantity_change, posting_line_id
    ) VALUES (
      v_walk.lot_id, v_walk.receipt_date, p_business_date, 1,
      -v_walk.allocation, p_posting_line_id
    );
  END LOOP;
  RETURN v_first_lot;
END;
$$;

-- ---------- 6. _lot_create_from_event ----------

-- Called from apply_event for lot inflows. Reads metadata from
-- event JSON (lot_code REQUIRED; rest optional). INSERTs an
-- inventory_lots row keyed at (product, location) of the DR side.
-- Returns the newly-allocated lot_id.

CREATE OR REPLACE FUNCTION _lot_create_from_event(
  p_event           JSONB,
  p_posting_line_id BIGINT,
  p_d_acct          accounts,
  p_qty             NUMERIC,
  p_unit_cost       NUMERIC,
  p_business_date   DATE,
  p_idx             INT
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_lot_code   TEXT := p_event->>'lot_code';
  v_new_id     BIGINT;
BEGIN
  IF v_lot_code IS NULL OR length(v_lot_code) = 0 THEN
    RAISE EXCEPTION
      'lot_fifo inflow requires lot_code in event payload at event index %',
      p_idx USING ERRCODE = 'P0006';
  END IF;

  INSERT INTO inventory_lots (
    product_id, legal_entity_id, location_id, lot_code,
    receipt_posting_line_id, receipt_date,
    original_quantity, unit_cost, cost_currency,
    manufacture_date, expiration_date, supplier_lot_number,
    quality_status, attributes
  ) VALUES (
    p_d_acct.sku_id,
    p_d_acct.legal_entity_id,
    p_d_acct.location_id,
    v_lot_code,
    p_posting_line_id,
    p_business_date,
    p_qty,
    p_unit_cost,
    p_d_acct.currency,
    (p_event->>'manufacture_date')::DATE,
    (p_event->>'expiration_date')::DATE,
    p_event->>'supplier_lot_number',
    COALESCE(p_event->>'quality_status', 'approved'),
    (p_event->'attributes')::JSONB
  ) RETURNING lot_id INTO v_new_id;

  RETURN v_new_id;
END;
$$;

-- ---------- 7. Strategy registry seed ----------

INSERT INTO cost_method_strategies
  (cost_method, event_kind, compute_fn_name,                   flag_provisional)
VALUES
  ('lot_fifo',  'outbound', '_compute_amount_lot_fifo_outbound', FALSE);

-- ---------- 8. post_posting_lines — widen qty-leg gates ----------

-- Body is identical to mig 0032's CREATE OR REPLACE except the two
-- qty-leg gates at lines 332 and 385 are widened to admit 'lot_fifo'
-- alongside the WAC family + standard + fifo. Same single-pass
-- vs two-pass dispatch (lot_fifo does NOT trigger v_has_wac; locks
-- live inside _lot_walk_layers per-row).

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
            -- E2.2: 'lot_fifo' joins the allowed list.
            IF v_cost_method NOT IN
               ('standard', 'wac_perpetual', 'wac_periodic',
                'wac_retroactive', 'fifo', 'lot_fifo') THEN
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
        -- E2.2: 'lot_fifo' joins the allowed list.
        IF v_cost_method NOT IN
           ('standard', 'wac_perpetual', 'wac_periodic',
            'wac_retroactive', 'fifo', 'lot_fifo') THEN
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

-- ---------- 9. _post_posting_lines_apply_event — extended with E2 block ----------

-- Body is identical to mig 0037's CREATE OR REPLACE EXCEPT:
--   1. cost_method gate at line 248 widened from
--      ('standard','wac_perpetual','wac_periodic','wac_retroactive','fifo')
--      to also include 'lot_fifo' so the D-block writes inventory_movements
--      rows for lot posts (subledger consistency).
--   2. NEW E2 block AFTER the E1 block: writes inventory_lots (lot_fifo
--      receipts) or inventory_lot_events 'issue' rows (lot_fifo issues),
--      and updates posting_line_inventory.lot_id.

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
  v_lot_first          BIGINT;
  v_specific_lot_id    BIGINT;
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

    IF v_inv_cost_method IN ('standard', 'wac_perpetual',
                             'wac_periodic', 'wac_retroactive',
                             'fifo', 'lot_fifo')
       AND p_d_acct.ledger_kind = 'value'
       AND v_qty_for_row <> 0 THEN

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

    -- E1 block — FIFO layer state.
    IF v_inv_cost_method = 'fifo' AND v_qty_for_row <> 0 THEN

      IF p_d_acct.kind IN ('inv_value_raw', 'inv_value_fg')
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

      IF p_c_acct.kind IN ('inv_value_raw', 'inv_value_fg')
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

    -- E2 block — lot subledger writes.
    --
    -- Receipt-side (DR inv_value_raw/_fg, lot_fifo SKU):
    --   create one inventory_lots row from event JSON metadata.
    -- Issue-side (CR inv_value_raw/_fg, lot_fifo SKU):
    --   walk lots, INSERT inventory_lot_events 'issue' rows.
    -- A single posting that has BOTH sides on inv_value_*+lot_fifo
    -- (i.e., a same-SKU/same-cost-method lot transfer) is rejected
    -- — multi-lot transfer semantics are E2 follow-up.
    IF v_inv_cost_method = 'lot_fifo' AND v_qty_for_row <> 0 THEN

      IF p_d_acct.kind IN ('inv_value_raw', 'inv_value_fg')
         AND p_c_acct.kind IN ('inv_value_raw', 'inv_value_fg')
         AND p_d_acct.sku_id IS NOT NULL
         AND p_c_acct.sku_id IS NOT NULL THEN
        RAISE EXCEPTION
          'lot_transfer_not_implemented: posting touches both inv_value_* '
          'sides for lot_fifo SKU at event index %', p_idx
          USING ERRCODE = 'P0006';
      END IF;

      v_specific_lot_id := (p_event->>'lot_id')::BIGINT;

      -- Receipt: inflow on DR inv_value_*.
      IF p_d_acct.kind IN ('inv_value_raw', 'inv_value_fg')
         AND p_d_acct.sku_id IS NOT NULL
         AND p_d_acct.location_id IS NOT NULL THEN
        v_lot_first := _lot_create_from_event(
          p_event, v_new_id, p_d_acct,
          ABS(v_qty_for_row)::NUMERIC, v_inv_unit_cost,
          v_business_date, p_idx
        );
        UPDATE posting_line_inventory
           SET lot_id = v_lot_first
         WHERE posting_line_id = v_new_id;
        UPDATE inventory_movements
           SET lot_id = v_lot_first
         WHERE posting_line_id = v_new_id
           AND product_id = p_d_acct.sku_id
           AND location_id = p_d_acct.location_id;
      END IF;

      -- Issue: outflow on CR inv_value_*.
      IF p_c_acct.kind IN ('inv_value_raw', 'inv_value_fg')
         AND p_c_acct.sku_id IS NOT NULL
         AND p_c_acct.location_id IS NOT NULL THEN
        v_lot_first := _lot_write_issues(
          v_new_id,
          p_c_acct.sku_id,
          p_c_acct.location_id,
          1::SMALLINT,
          ABS(v_qty_for_row)::NUMERIC,
          v_business_date,
          v_specific_lot_id
        );
        UPDATE posting_line_inventory
           SET lot_id = v_lot_first
         WHERE posting_line_id = v_new_id;
        UPDATE inventory_movements
           SET lot_id = v_lot_first
         WHERE posting_line_id = v_new_id
           AND product_id = p_c_acct.sku_id
           AND location_id = p_c_acct.location_id;
      END IF;
    END IF;
  END IF;

  RETURN v_new_id;
END;
$$;
