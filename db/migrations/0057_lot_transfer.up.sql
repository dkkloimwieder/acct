-- ============================================================
-- Phase E2 lot follow-up — post_lot_transfer wrapper (acct-fzzw).
--
-- First-class document for moving qty + value of a lot_fifo SKU
-- between two locations. Wrapper-driven: walks source lots under
-- FOR UPDATE (via _lot_walk_layers, respecting skus.allocation_strategy),
-- builds qty + value postings per consumed source lot, calls
-- post_posting_lines, then writes the lot subledger trail itself.
--
-- Pieces:
--   1. _inventory_movement_event_type extended: 'lot_transfer'
--      maps to 3 (out, signed_qty<0) / 4 (in, signed_qty>0) so
--      the D-block writes inventory_movements rows for both legs.
--   2. lot_transfers + lot_transfer_lines schema (document tables).
--   3. _post_posting_lines_apply_event extended: E2 block now
--      gated on v_reason <> 'lot_transfer' so the wrapper, not
--      apply_event, owns lot subledger writeback for transfers.
--      The bilateral-rejection at the head of E2 (the gate the
--      whole epic was waiting on) is naturally bypassed when
--      reason='lot_transfer' since the entire E2 block is skipped.
--   4. post_lot_transfer wrapper.
--
-- Lot-event semantics: source-side drain uses event_type=8
-- 'adjust_out' (qty_change<0). event_type=2 'transfer' is a
-- status-only marker (qty_change=0) and not used at MVP — the
-- adjust_out + posting_line_id linkage carries the audit trail.
--
-- Multi-lot walks produce N dest rows in inventory_lots (one per
-- consumed source lot, copying source metadata: lot_code,
-- expiration_date, manufacture_date, supplier_lot_number,
-- quality_status, attributes, unit_cost). Each consumed source
-- lot also gets one adjust_out event. Cross-currency rejected
-- (FROM and TO inv_value_* must share currency).
--
-- inventory_movements lot_id stamping: DR-side (TO) movement
-- gets the new dest lot_id; CR-side (FROM) movement gets the
-- consumed source lot_id. posting_line_inventory.lot_id stamps
-- the source lot (mirrors mig 0046's issue-side convention).
--
-- Cost-method dispatch + posting_lines_provisional flagging:
-- 'lot_transfer' is NOT a cost event (not in op_move/scrap/
-- wo_complete/so_ship/op_move_v/scrap_v/wo_complete_v/
-- rm_issue_to_wo lists), so amount is caller-supplied per leg
-- and no provisional row is flagged. The dispatcher path is
-- bypassed entirely.
-- ============================================================

-- ---------- 1. _inventory_movement_event_type — add 'lot_transfer' ----------

CREATE OR REPLACE FUNCTION _inventory_movement_event_type(
  p_reason     posting_line_reason,
  p_signed_qty NUMERIC
) RETURNS SMALLINT
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
  RETURN CASE p_reason::TEXT
    WHEN 'po_receipt'              THEN 1
    WHEN 'po_receipt_provisional'  THEN 1
    WHEN 'so_ship'                 THEN 2
    WHEN 'to_release'              THEN 3
    WHEN 'osp_ship'                THEN 3
    WHEN 'to_receipt'              THEN 4
    WHEN 'osp_receive'             THEN 4
    WHEN 'cycle_count_adj'
      THEN CASE WHEN p_signed_qty > 0 THEN 5 ELSE 6 END
    WHEN 'inventory_adjustment'
      THEN CASE WHEN p_signed_qty > 0 THEN 5 ELSE 6 END
    WHEN 'scrap'                   THEN 7
    WHEN 'scrap_v'                 THEN 7
    WHEN 'rm_issue_to_wo'          THEN 8
    WHEN 'wo_complete'             THEN 9
    WHEN 'wo_complete_v'           THEN 9
    WHEN 'op_move'                 THEN 11
    WHEN 'op_move_v'               THEN 11
    WHEN 'customer_return'         THEN 12
    WHEN 'po_return_to_vendor'     THEN 13
    WHEN 'standard_cost_roll'      THEN 14
    WHEN 'cost_adjustment'         THEN 16
    WHEN 'cost_restate'            THEN 16
    WHEN 'wo_close_v'              THEN 16
    -- E2 lot follow-up (acct-fzzw):
    WHEN 'lot_transfer'
      THEN CASE WHEN p_signed_qty > 0 THEN 4 ELSE 3 END
    ELSE NULL
  END;
END;
$$;

-- ---------- 2. lot_transfers + lot_transfer_lines ----------

CREATE TABLE lot_transfers (
  id                 UUID         NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  from_location_id   UUID         NOT NULL REFERENCES locations(id),
  to_location_id     UUID         NOT NULL REFERENCES locations(id),
  business_date      DATE         NOT NULL,
  posted_at          TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
  posted_by          UUID         NOT NULL,
  idempotency_key    UUID         NOT NULL UNIQUE,
  notes              TEXT,
  created_at         TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
  CHECK (from_location_id <> to_location_id)
);

CREATE INDEX lot_transfers_from_loc ON lot_transfers (from_location_id);
CREATE INDEX lot_transfers_to_loc   ON lot_transfers (to_location_id);
CREATE INDEX lot_transfers_business_date ON lot_transfers (business_date);

COMMENT ON TABLE lot_transfers IS
  'Document table for lot-tracked SKU movement between locations '
  '(acct-fzzw). One row per transfer; multi-line via lot_transfer_lines. '
  'Wrapper post_lot_transfer walks source lots under FOR UPDATE and '
  'creates dest inventory_lots rows + adjust_out events on source.';

CREATE TABLE lot_transfer_lines (
  id              UUID         NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  transfer_id     UUID         NOT NULL REFERENCES lot_transfers(id) ON DELETE CASCADE,
  line_no         INT          NOT NULL,
  sku_id          UUID         NOT NULL REFERENCES skus(id),
  qty             NUMERIC(19, 6) NOT NULL CHECK (qty > 0),
  -- pinned source lot (caller-specified). NULL means walk by
  -- skus.allocation_strategy (fifo/fefo).
  lot_id          BIGINT,
  -- Audit snapshot (set by wrapper post-walk). For multi-lot walks
  -- unit_cost is the weighted (total_amount / qty); per-lot truth
  -- lives in inventory_lot_events + inventory_lots dest rows.
  unit_cost       NUMERIC(19, 4),
  total_amount    BIGINT,
  created_at      TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (transfer_id, line_no)
);

CREATE INDEX lot_transfer_lines_transfer ON lot_transfer_lines (transfer_id);
CREATE INDEX lot_transfer_lines_sku ON lot_transfer_lines (sku_id);
CREATE INDEX lot_transfer_lines_lot ON lot_transfer_lines (lot_id) WHERE lot_id IS NOT NULL;

COMMENT ON COLUMN lot_transfer_lines.lot_id IS
  'NULL = unpinned (walk via skus.allocation_strategy). NOT NULL = '
  'pinned to a specific source lot (raises P0006 lot_residual_short '
  'if pinned residual is insufficient).';

COMMENT ON COLUMN lot_transfer_lines.unit_cost IS
  'Audit snapshot weighted unit_cost (total_amount / qty). For '
  'multi-lot walks may NOT equal any individual source lot unit_cost; '
  'per-lot truth in inventory_lot_events + inventory_lots dest rows.';

-- ---------- 3. _post_posting_lines_apply_event — gate E2 on reason ----------

-- Body identical to mig 0046's CREATE OR REPLACE except:
--   - E2 block (lines 864ff) is now gated on v_reason <> 'lot_transfer'
--     so the entire lot subledger writeback is skipped for transfers.
--     The wrapper post_lot_transfer owns lot subledger writes for
--     transfer documents (multi-lot walks need different metadata
--     per consumed source lot than the receipt-from-event JSON
--     pattern can express).

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
    -- acct-fzzw: gate on v_reason <> 'lot_transfer'. The
    -- post_lot_transfer wrapper owns lot subledger writes for
    -- transfers (multi-lot walk needs to copy per-source-lot
    -- metadata to per-dest-lot rows; the receipt-from-event JSON
    -- pattern below can't express that). Skipping the entire E2
    -- block also bypasses the bilateral-rejection that previously
    -- raised P0006 'lot_transfer_not_implemented' for transfer
    -- postings.
    --
    -- Receipt-side (DR inv_value_raw/_fg, lot_fifo SKU):
    --   create one inventory_lots row from event JSON metadata.
    -- Issue-side (CR inv_value_raw/_fg, lot_fifo SKU):
    --   walk lots, INSERT inventory_lot_events 'issue' rows.
    -- A bilateral posting for any reason OTHER than 'lot_transfer'
    -- (i.e., a same-SKU/same-cost-method posting that touches both
    -- inv_value_* sides outside of the transfer wrapper path) is
    -- still rejected — the wrapper is the only sanctioned bilateral
    -- path.
    IF v_inv_cost_method = 'lot_fifo'
       AND v_qty_for_row <> 0
       AND v_reason <> 'lot_transfer' THEN

      IF p_d_acct.kind IN ('inv_value_raw', 'inv_value_fg')
         AND p_c_acct.kind IN ('inv_value_raw', 'inv_value_fg')
         AND p_d_acct.sku_id IS NOT NULL
         AND p_c_acct.sku_id IS NOT NULL THEN
        RAISE EXCEPTION
          'lot_transfer_not_implemented: posting touches both inv_value_* '
          'sides for lot_fifo SKU at event index % outside post_lot_transfer',
          p_idx
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

-- ---------- 4. post_lot_transfer wrapper ----------

-- p_lines is a JSONB array of objects:
--   { "sku_id": UUID, "qty": NUMERIC, "lot_id": BIGINT? }
-- lot_id NULL = unpinned (walk by skus.allocation_strategy).
-- lot_id NOT NULL = pinned (raises P0006 if residual short).
--
-- Idempotent on p_idempotency_key — replay returns same UUID
-- without writing anything new.

CREATE OR REPLACE FUNCTION post_lot_transfer(
  p_from_location_id UUID,
  p_to_location_id   UUID,
  p_lines            JSONB,
  p_business_date    DATE,
  p_posted_by        UUID,
  p_idempotency_key  UUID,
  p_notes            TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_doc_id          UUID;
  v_existing        UUID;
  v_line            JSONB;
  v_idx             INT;
  v_n_lines         INT;
  v_sku             UUID;
  v_qty             NUMERIC;
  v_specific_lot    BIGINT;
  v_cost_method     cost_method;
  v_tracked_by      inventory_tracking;
  v_value_kind      account_kind;
  v_currency        CHAR(3);
  v_qty_from        BIGINT;
  v_qty_to          BIGINT;
  v_val_from        BIGINT;
  v_val_to          BIGINT;
  v_walk            RECORD;
  v_batch           JSONB := '[]'::JSONB;
  v_walks           JSONB := '[]'::JSONB;
  v_idem_value      UUID;
  v_idem_qty        UUID;
  v_lot_meta        inventory_lots;
  v_walk_meta       JSONB;
  v_value_pl_id     BIGINT;
  v_dest_lot_id     BIGINT;
  v_line_id         UUID;
BEGIN
  -- Idempotency replay.
  SELECT id INTO v_existing FROM lot_transfers WHERE idempotency_key = p_idempotency_key;
  IF v_existing IS NOT NULL THEN
    RETURN v_existing;
  END IF;

  IF p_from_location_id IS NULL OR p_to_location_id IS NULL THEN
    RAISE EXCEPTION 'lot_transfer_invalid: from/to location required'
      USING ERRCODE = 'P0006';
  END IF;
  IF p_from_location_id = p_to_location_id THEN
    RAISE EXCEPTION 'lot_transfer_invalid: from and to locations must differ'
      USING ERRCODE = 'P0006';
  END IF;

  v_n_lines := jsonb_array_length(p_lines);
  IF v_n_lines = 0 THEN
    RAISE EXCEPTION 'lot_transfer_invalid: at least one line required'
      USING ERRCODE = 'P0006';
  END IF;

  INSERT INTO lot_transfers (
    from_location_id, to_location_id, business_date, posted_by,
    idempotency_key, notes
  ) VALUES (
    p_from_location_id, p_to_location_id, p_business_date, p_posted_by,
    p_idempotency_key, p_notes
  ) RETURNING id INTO v_doc_id;

  FOR v_idx IN 0..v_n_lines - 1 LOOP
    v_line := p_lines -> v_idx;
    v_sku := (v_line->>'sku_id')::UUID;
    v_qty := (v_line->>'qty')::NUMERIC;
    v_specific_lot := (v_line->>'lot_id')::BIGINT;

    IF v_sku IS NULL OR v_qty IS NULL OR v_qty <= 0 THEN
      RAISE EXCEPTION
        'lot_transfer_invalid: line % requires sku_id and qty>0',
        v_idx + 1 USING ERRCODE = 'P0006';
    END IF;

    SELECT cost_method, tracked_by INTO v_cost_method, v_tracked_by
      FROM skus WHERE id = v_sku;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'lot_transfer_invalid: line % unknown sku=%',
        v_idx + 1, v_sku USING ERRCODE = 'P0006';
    END IF;
    IF v_cost_method <> 'lot_fifo' THEN
      RAISE EXCEPTION
        'lot_transfer_invalid: line % sku=% cost_method=% (lot_fifo required)',
        v_idx + 1, v_sku, v_cost_method USING ERRCODE = 'P0006';
    END IF;
    IF v_tracked_by NOT IN ('lot', 'lot_and_serial') THEN
      RAISE EXCEPTION
        'lot_transfer_invalid: line % sku=% tracked_by=% (lot or lot_and_serial required)',
        v_idx + 1, v_sku, v_tracked_by USING ERRCODE = 'P0006';
    END IF;

    -- Resolve FROM value account (auto-detect inv_value_raw vs inv_value_fg).
    SELECT id, kind, currency INTO v_val_from, v_value_kind, v_currency
      FROM accounts
     WHERE sku_id = v_sku AND location_id = p_from_location_id
       AND ledger_kind = 'value'
       AND kind IN ('inv_value_raw','inv_value_fg')
       AND lot_id IS NULL AND NOT is_closed
     ORDER BY id LIMIT 1;
    IF v_val_from IS NULL THEN
      RAISE EXCEPTION
        'lot_transfer_invalid: line % no open value account at FROM (sku=% loc=%)',
        v_idx + 1, v_sku, p_from_location_id USING ERRCODE = 'P0010';
    END IF;

    -- Resolve TO value account (same kind, same currency).
    SELECT id INTO v_val_to FROM accounts
     WHERE sku_id = v_sku AND location_id = p_to_location_id
       AND ledger_kind = 'value' AND kind = v_value_kind
       AND currency = v_currency AND lot_id IS NULL AND NOT is_closed
     LIMIT 1;
    IF v_val_to IS NULL THEN
      RAISE EXCEPTION
        'lot_transfer_invalid: line % no open value account at TO (sku=% loc=% kind=% ccy=%)',
        v_idx + 1, v_sku, p_to_location_id, v_value_kind, v_currency
        USING ERRCODE = 'P0010';
    END IF;

    -- Resolve qty accounts.
    SELECT id INTO v_qty_from FROM accounts
     WHERE sku_id = v_sku AND location_id = p_from_location_id
       AND kind = 'stock_available' AND lot_id IS NULL AND NOT is_closed
     LIMIT 1;
    IF v_qty_from IS NULL THEN
      RAISE EXCEPTION
        'lot_transfer_invalid: line % no open stock_available at FROM (sku=% loc=%)',
        v_idx + 1, v_sku, p_from_location_id USING ERRCODE = 'P0010';
    END IF;
    SELECT id INTO v_qty_to FROM accounts
     WHERE sku_id = v_sku AND location_id = p_to_location_id
       AND kind = 'stock_available' AND lot_id IS NULL AND NOT is_closed
     LIMIT 1;
    IF v_qty_to IS NULL THEN
      RAISE EXCEPTION
        'lot_transfer_invalid: line % no open stock_available at TO (sku=% loc=%)',
        v_idx + 1, v_sku, p_to_location_id USING ERRCODE = 'P0010';
    END IF;

    INSERT INTO lot_transfer_lines (transfer_id, line_no, sku_id, qty, lot_id)
      VALUES (v_doc_id, v_idx + 1, v_sku, v_qty, v_specific_lot)
      RETURNING id INTO v_line_id;

    -- Walk source lots under FOR UPDATE.
    FOR v_walk IN
      SELECT * FROM _lot_walk_layers(
        v_sku, p_from_location_id, 1::SMALLINT, v_qty, v_specific_lot
      )
    LOOP
      v_idem_qty   := gen_random_uuid();
      v_idem_value := gen_random_uuid();

      -- qty leg: stock_available@TO ↔ stock_available@FROM.
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',           'lot_transfer',
        'document_kind',    'lot_transfer',
        'document_id',      v_doc_id,
        'document_line_id', v_line_id,
        'debit_account_id', v_qty_to,
        'credit_account_id',v_qty_from,
        'amount',           v_walk.allocation::BIGINT,
        'qty',              v_walk.allocation::BIGINT,
        'business_date',    p_business_date,
        'idempotency_key',  v_idem_qty,
        'posted_by',        p_posted_by
      ));

      -- value leg: inv_value_*@TO ↔ inv_value_*@FROM.
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',           'lot_transfer',
        'document_kind',    'lot_transfer',
        'document_id',      v_doc_id,
        'document_line_id', v_line_id,
        'debit_account_id', v_val_to,
        'credit_account_id',v_val_from,
        'amount',           v_walk.cost_amount,
        'qty',              v_walk.allocation::BIGINT,
        'business_date',    p_business_date,
        'idempotency_key',  v_idem_value,
        'posted_by',        p_posted_by
      ));

      -- Track per-walk metadata for post-posting subledger writeback.
      v_walks := v_walks || jsonb_build_array(jsonb_build_object(
        'value_idem_key',      v_idem_value,
        'source_lot_id',       v_walk.lot_id,
        'source_receipt_date', v_walk.receipt_date,
        'allocation',          v_walk.allocation,
        'cost_amount',         v_walk.cost_amount,
        'sku_id',              v_sku,
        'to_location_id',      p_to_location_id,
        'from_location_id',    p_from_location_id
      ));
    END LOOP;
  END LOOP;

  -- Post all legs (qty + value) in one batch.
  PERFORM post_posting_lines(v_batch, FALSE);

  -- Lot subledger writeback: per consumed source lot, INSERT new
  -- lot row at TO + adjust_out event on source + lot_id stamps.
  FOR v_idx IN 0..jsonb_array_length(v_walks) - 1 LOOP
    v_walk_meta := v_walks -> v_idx;

    SELECT id INTO v_value_pl_id FROM posting_lines
     WHERE idempotency_key = (v_walk_meta->>'value_idem_key')::UUID;

    SELECT * INTO v_lot_meta FROM inventory_lots
     WHERE lot_id       = (v_walk_meta->>'source_lot_id')::BIGINT
       AND receipt_date = (v_walk_meta->>'source_receipt_date')::DATE;

    -- Create dest lot row, copying source metadata.
    INSERT INTO inventory_lots (
      product_id, legal_entity_id, cost_book_id, location_id, lot_code,
      receipt_posting_line_id, receipt_date,
      original_quantity, unit_cost, cost_currency,
      manufacture_date, expiration_date, supplier_lot_number,
      quality_status, attributes
    ) VALUES (
      v_lot_meta.product_id,
      v_lot_meta.legal_entity_id,
      v_lot_meta.cost_book_id,
      (v_walk_meta->>'to_location_id')::UUID,
      v_lot_meta.lot_code,
      v_value_pl_id,
      p_business_date,
      (v_walk_meta->>'allocation')::NUMERIC,
      v_lot_meta.unit_cost,
      v_lot_meta.cost_currency,
      v_lot_meta.manufacture_date,
      v_lot_meta.expiration_date,
      v_lot_meta.supplier_lot_number,
      v_lot_meta.quality_status,
      v_lot_meta.attributes
    ) RETURNING lot_id INTO v_dest_lot_id;

    -- Drain event on source lot (event_type=8 'adjust_out').
    INSERT INTO inventory_lot_events (
      lot_id, lot_receipt_date, event_date, event_type,
      quantity_change, posting_line_id,
      location_id_from, location_id_to, notes
    ) VALUES (
      v_lot_meta.lot_id,
      v_lot_meta.receipt_date,
      p_business_date,
      8,
      -(v_walk_meta->>'allocation')::NUMERIC,
      v_value_pl_id,
      v_lot_meta.location_id,
      (v_walk_meta->>'to_location_id')::UUID,
      'lot_transfer'
    );

    -- Stamp posting_line_inventory with source lot (mirrors mig 0046's
    -- issue-side convention: pli.lot_id is the consumed/source lot).
    UPDATE posting_line_inventory
       SET lot_id = v_lot_meta.lot_id
     WHERE posting_line_id = v_value_pl_id;

    -- Stamp inventory_movements lot_id per leg:
    --   DR-side movement (TO location, +qty) gets dest lot.
    --   CR-side movement (FROM location, -qty) gets source lot.
    UPDATE inventory_movements
       SET lot_id = v_dest_lot_id
     WHERE posting_line_id = v_value_pl_id
       AND product_id = (v_walk_meta->>'sku_id')::UUID
       AND location_id = (v_walk_meta->>'to_location_id')::UUID;
    UPDATE inventory_movements
       SET lot_id = v_lot_meta.lot_id
     WHERE posting_line_id = v_value_pl_id
       AND product_id = (v_walk_meta->>'sku_id')::UUID
       AND location_id = v_lot_meta.location_id;
  END LOOP;

  -- Update audit fields on lot_transfer_lines from the posted value legs.
  UPDATE lot_transfer_lines ltl
     SET total_amount = sub.total_amount,
         unit_cost    = ROUND(sub.total_amount::NUMERIC / sub.total_qty, 4)
    FROM (
      SELECT pl.document_line_id AS line_id,
             SUM(pl.amount)::BIGINT AS total_amount,
             SUM(ABS(pl.qty))::NUMERIC AS total_qty
        FROM posting_lines pl
        JOIN accounts a ON a.id = pl.debit_account_id
       WHERE pl.document_id = v_doc_id
         AND pl.reason = 'lot_transfer'
         AND a.ledger_kind = 'value'
       GROUP BY pl.document_line_id
    ) sub
   WHERE ltl.id = sub.line_id;

  RETURN v_doc_id;
END;
$$;

COMMENT ON FUNCTION post_lot_transfer(UUID, UUID, JSONB, DATE, UUID, UUID, TEXT) IS
  'Lot-tracked SKU transfer between locations (acct-fzzw). Walks '
  'source lots under FOR UPDATE via _lot_walk_layers (FIFO/FEFO via '
  'skus.allocation_strategy; specific_lot pin overrides). Per consumed '
  'source lot: posts qty + value legs (reason=lot_transfer), creates '
  'a new inventory_lots row at TO copying source metadata, writes '
  'event_type=8 adjust_out on source. inventory_movements rows write '
  'per leg via apply_event D-block; lot_id stamped post-hoc by wrapper '
  '(DR side gets dest lot, CR side gets source lot). Cross-currency '
  'rejected (FROM/TO must share inv_value_* currency). Idempotent on '
  'p_idempotency_key.';
