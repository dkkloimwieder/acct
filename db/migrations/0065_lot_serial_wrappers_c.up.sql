-- ============================================================
-- acct-sxl2.5 — Wrappers C: post_lot_transfer + rm_issue_to_wo
-- per-unit lifecycle for tracked_by='lot_and_serial'.
--
-- WHAT:
--   1. lot_transfer_lines.unit_ids audit column.
--   2. New table wo_unit_consumption (per-unit mirror of
--      wo_lot_consumption — one row per consumed unit per
--      rm_issue value-leg posting).
--   3. post_lot_transfer: accept per-line unit_ids/unit_serials
--      arrays (REQUIRED for lot_and_serial SKUs); pin specific
--      source lot from the units; flip current_location_id +
--      lot_id + lot_receipt_date on each transferred unit;
--      emit type=3 unit events.
--   4. _wo_emit_bom_lines: when a lot_fifo component is also
--      tracked_by='lot_and_serial', auto-allocate units of each
--      consumed source lot (in serial_no order) and stash them
--      on the per-walk metadata. Auto-allocation is the MVP
--      (caller-supplied component_unit_ids deferred).
--   5. _wo_write_lot_consumption: extended to also flip unit
--      status to 'consumed', stamp work_order_id, emit type=2
--      unit events, and INSERT wo_unit_consumption rows when
--      the walk row carries 'unit_ids'. Helper name retained
--      for callsite stability (post_wo_start / post_op_move
--      both call it post-PERFORM; the unit writeback rides
--      along on the same walks JSONB).
--
-- WHY:
--   Last wrapper layer of the sxl2 epic before recon (sxl2.6).
--   sxl2.3 covered po_receipt + inventory_adjustment paths;
--   sxl2.4 covered so_ship + wo_complete paths. sxl2.5
--   completes the lifecycle by extending lot_transfer (the
--   only cross-location identity-preserving move) and
--   rm_issue_to_wo (component consumption into WIP).
--
-- DESIGN CALLS:
--   - AUTO-ALLOCATE for rm_issue components (Q3 lean: pinned
--     mode is MVP-deferred; auto-allocation by serial_no order
--     is enough for the cost-flow guarantees). Caller-supplied
--     component_unit_ids tracked as sxl2.5-followup if a
--     real need surfaces.
--   - PINNED-ONLY for lot_transfer (Q5 design call): caller
--     MUST supply unit_ids (or unit_serials) per line; no
--     auto-allocation across lots. Single-lot per line
--     constraint mirrors the so_ship pattern from sxl2.4.
--   - Unit status during transfer stays 'available' so the
--     partial UNIQUE (product_id, serial_no) WHERE status IN
--     active continues to cover the unit through the chain
--     (A → B → A is legal).
--   - wo_unit_consumption uses composite PK (wo_event_id,
--     unit_id) — one event consumes a given unit at most once;
--     replay is prevented at the PK layer.
--   - Append-only via the shared block_inventory_lot_modifications
--     trigger fn (mig 0044).
--   - _wo_write_lot_consumption is extended (not split) to
--     avoid having to re-verbatim-copy post_wo_start and
--     post_op_move (~460 lines combined). The unit writeback
--     is conditional on walk-row 'unit_ids' presence; lot-only
--     walks emit only wo_lot_consumption rows as before.
--
-- NOT IN SCOPE (sxl2.5-followup if needed):
--   - Caller-supplied component_unit_ids in rm_issue (would
--     allow operators to override the FIFO/serial pick order).
--   - Backflush component-issue paths
--     (consumption_policy='backflush_at_op' /
--     'backflush_at_complete'). Pre-sxl2 these raise P0035 at
--     WO start; that gate is preserved.
--   - Cross-tenant unit transfers, hold-window checks at
--     transfer time, expiration-based rejection.
--   - wo_unit_consumption recon (deferred to sxl2.6 — its
--     check #14 covers cross-table integrity).
-- ============================================================


-- ---------- 1. lot_transfer_lines.unit_ids audit column ----------

ALTER TABLE lot_transfer_lines
  ADD COLUMN unit_ids BIGINT[];

CREATE INDEX lot_transfer_lines_unit_ids
  ON lot_transfer_lines USING GIN (unit_ids)
  WHERE unit_ids IS NOT NULL;

COMMENT ON COLUMN lot_transfer_lines.unit_ids IS
  'Per-line unit identity audit for tracked_by=''lot_and_serial'' '
  'SKUs (sxl2.5). NULL for plain lot_fifo (no per-unit identity). '
  'Stamped post-PERFORM by post_lot_transfer.';


-- ---------- 2. wo_unit_consumption table ----------

CREATE TABLE wo_unit_consumption (
  wo_event_id       UUID         NOT NULL REFERENCES wo_events(id),
  unit_id           BIGINT       NOT NULL REFERENCES inventory_units(unit_id),
  wo_id             UUID         NOT NULL REFERENCES work_orders(id),
  routing_op        INT          NOT NULL,
  component_sku_id  UUID         NOT NULL REFERENCES skus(id),
  lot_id            BIGINT       NOT NULL,
  lot_receipt_date  DATE         NOT NULL,
  posting_line_id   BIGINT       NOT NULL REFERENCES posting_lines(id),
  posted_at         TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (wo_event_id, unit_id),
  FOREIGN KEY (lot_id, lot_receipt_date)
    REFERENCES inventory_lots (lot_id, receipt_date)
);

CREATE INDEX wo_unit_consumption_wo
  ON wo_unit_consumption (wo_id, routing_op);

CREATE INDEX wo_unit_consumption_unit
  ON wo_unit_consumption (unit_id);

CREATE INDEX wo_unit_consumption_pl
  ON wo_unit_consumption (posting_line_id);

CREATE INDEX wo_unit_consumption_component
  ON wo_unit_consumption (component_sku_id);

COMMENT ON TABLE wo_unit_consumption IS
  'Per-unit mirror of wo_lot_consumption (sxl2.5). One row per '
  'consumed inventory_unit per rm_issue_to_wo value-leg posting. '
  'Composite PK (wo_event_id, unit_id) prevents replay of the '
  'same unit by the same event. Append-only.';


-- ---------- 3. Append-only trigger (reuses shared fn) ----------

CREATE TRIGGER trg_wo_unit_consumption_append_only
  BEFORE UPDATE OR DELETE ON wo_unit_consumption
  FOR EACH ROW EXECUTE FUNCTION block_inventory_lot_modifications();


-- ---------- 4. _wo_write_lot_consumption — extended for units ----------

-- Verbatim from mig 0058 + per-walk unit handling:
--   When a walk row carries 'unit_ids' (BIGINT array), flip each
--   unit to 'consumed', stamp work_order_id, emit a type=2 issue
--   event, and INSERT a wo_unit_consumption row per unit. Single
--   pass over walks; lot-only walks behave exactly as before.

CREATE OR REPLACE FUNCTION _wo_write_lot_consumption(
  p_walks JSONB
) RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
  v_walk         JSONB;
  v_pl_id        BIGINT;
  v_idem         UUID;
  v_unit_ids     BIGINT[];
  v_business_date DATE;
  v_loc_from     UUID;
  v_wo_id        UUID;
  v_wo_event_id  UUID;
  v_routing_op   INT;
  v_comp_sku     UUID;
  v_lot_id       BIGINT;
  v_lot_date     DATE;
BEGIN
  IF p_walks IS NULL OR jsonb_array_length(p_walks) = 0 THEN
    RETURN;
  END IF;

  FOR v_walk IN SELECT * FROM jsonb_array_elements(p_walks) LOOP
    v_idem := (v_walk->>'value_idem_key')::UUID;

    SELECT id INTO v_pl_id
      FROM posting_lines
     WHERE idempotency_key = v_idem;
    IF v_pl_id IS NULL THEN
      RAISE EXCEPTION
        'wo_lot_consumption_writeback: no posting_line for idem=%',
        v_idem USING ERRCODE = 'P0010';
    END IF;

    v_wo_id       := (v_walk->>'wo_id')::UUID;
    v_wo_event_id := (v_walk->>'wo_event_id')::UUID;
    v_routing_op  := (v_walk->>'routing_op')::INT;
    v_comp_sku    := (v_walk->>'component_sku_id')::UUID;
    v_lot_id      := (v_walk->>'lot_id')::BIGINT;
    v_lot_date    := (v_walk->>'lot_receipt_date')::DATE;

    INSERT INTO wo_lot_consumption (
      wo_id, wo_event_id, routing_op, component_sku_id,
      lot_id, lot_receipt_date, qty, posting_line_id
    ) VALUES (
      v_wo_id,
      v_wo_event_id,
      v_routing_op,
      v_comp_sku,
      v_lot_id,
      v_lot_date,
      (v_walk->>'qty')::NUMERIC,
      v_pl_id
    );

    -- sxl2.5: per-unit writeback when this walk carries unit_ids.
    IF (v_walk->'unit_ids') IS NOT NULL
       AND jsonb_typeof(v_walk->'unit_ids') = 'array'
       AND jsonb_array_length(v_walk->'unit_ids') > 0 THEN
      SELECT array_agg((x)::BIGINT ORDER BY ord)
        INTO v_unit_ids
        FROM jsonb_array_elements_text(v_walk->'unit_ids')
             WITH ORDINALITY AS t(x, ord);

      v_business_date := (v_walk->>'business_date')::DATE;
      v_loc_from      := (v_walk->>'component_loc_id')::UUID;

      UPDATE inventory_units
         SET status = 'consumed',
             work_order_id = v_wo_id,
             updated_at = clock_timestamp()
       WHERE unit_id = ANY(v_unit_ids);

      INSERT INTO inventory_unit_events (
        unit_id, event_date, event_type,
        posting_line_id, new_status,
        location_id_from, work_order_id, notes
      )
      SELECT u, v_business_date, 2,
             v_pl_id, 'consumed',
             v_loc_from, v_wo_id, 'rm_issue_to_wo'
        FROM unnest(v_unit_ids) AS u;

      INSERT INTO wo_unit_consumption (
        wo_event_id, unit_id, wo_id, routing_op,
        component_sku_id, lot_id, lot_receipt_date, posting_line_id
      )
      SELECT v_wo_event_id, u, v_wo_id, v_routing_op,
             v_comp_sku, v_lot_id, v_lot_date, v_pl_id
        FROM unnest(v_unit_ids) AS u;
    END IF;
  END LOOP;
END;
$$;

COMMENT ON FUNCTION _wo_write_lot_consumption(JSONB) IS
  'Walks p_walks JSONB and INSERTs wo_lot_consumption rows; '
  'when a walk row carries unit_ids (sxl2.5), also UPDATEs '
  'inventory_units status=consumed + work_order_id, INSERTs '
  'type=2 unit events, and INSERTs wo_unit_consumption rows. '
  'Called post-PERFORM by post_wo_start and post_op_move; the '
  'value-leg posting_lines.id is resolved via idempotency_key.';


-- ---------- 5. _wo_emit_bom_lines — auto-allocate units ----------

-- Verbatim from mig 0058 + lot_and_serial auto-allocation block
-- inside the lot_fifo branch. Signature unchanged.
-- Each lot_fifo walk now also picks N units of the consumed
-- lot at the component location (status='available', ORDER BY
-- serial_no, FOR UPDATE) and stashes the unit_ids on the walk.

CREATE OR REPLACE FUNCTION _wo_emit_bom_lines(
  p_wo_id              UUID,
  p_bom_id             BIGINT,
  p_routing_op         INT,
  p_qty                BIGINT,
  p_filter             JSONB,
  p_event_id           UUID,
  p_business_date      DATE,
  p_posted_by          UUID,
  p_document_kind      TEXT,
  p_component_lot_pins JSONB DEFAULT NULL
) RETURNS TABLE (batch JSONB, walks JSONB)
LANGUAGE plpgsql
AS $$
DECLARE
  v_wo                    work_orders%ROWTYPE;
  v_val_acct_wip          BIGINT;
  v_batch                 JSONB := '[]'::JSONB;
  v_walks                 JSONB := '[]'::JSONB;
  v_line                  RECORD;
  v_filter_kind           TEXT;
  v_filter_basis          TEXT;
  v_filter_fire_at        TEXT;
  v_filter_applies_at_op  INT;
  v_adj_qty               BIGINT;
  v_value                 BIGINT;
  v_amount                BIGINT;
  v_reason                posting_line_reason;
  v_comp_consumed         BIGINT;
  v_comp_qty_acct         BIGINT;
  v_comp_val_acct         BIGINT;
  v_applied_kind          account_kind;
  v_applied_acct          BIGINT;
  v_comp_std_cost         BIGINT;
  v_comp_cost_method      cost_method;
  v_comp_tracked_by       inventory_tracking;
  v_pool_qty              BIGINT;
  v_pool_value            BIGINT;
  v_unit                  BIGINT;
  v_specific_lot_id       BIGINT;
  v_value_event           JSONB;
  v_value_idem            UUID;
  v_lot_walk              RECORD;
  v_unit_ids              BIGINT[];
  v_picked_count          INT;
BEGIN
  IF p_qty IS NULL OR p_qty <= 0 THEN
    RAISE EXCEPTION 'wo_invalid: _wo_emit_bom_lines requires positive p_qty (got %)',
                    p_qty USING ERRCODE = 'P0026';
  END IF;
  IF p_bom_id IS NULL THEN
    RAISE EXCEPTION 'wo_invalid: _wo_emit_bom_lines requires non-NULL p_bom_id'
      USING ERRCODE = 'P0026';
  END IF;

  SELECT * INTO v_wo FROM work_orders WHERE id = p_wo_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: WO % not found', p_wo_id USING ERRCODE = 'P0026';
  END IF;

  SELECT id INTO v_val_acct_wip FROM accounts
   WHERE kind='inv_value_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_routing_op AND currency=v_wo.currency
     AND NOT is_closed;
  IF v_val_acct_wip IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_wip account for sku=% op=% ccy=%',
                    v_wo.parent_sku_id, p_routing_op, v_wo.currency
      USING ERRCODE = 'P0010';
  END IF;

  v_filter_kind          := p_filter->>'kind';
  v_filter_basis         := p_filter->>'basis';
  v_filter_fire_at       := p_filter->>'fire_at';
  v_filter_applies_at_op := NULLIF(p_filter->>'applies_at_op', '')::INT;

  FOR v_line IN
    SELECT exp.*
      FROM _wo_explode_bom(p_bom_id, p_business_date) exp
     WHERE (v_filter_kind          IS NULL OR exp.kind          = v_filter_kind)
       AND (v_filter_basis         IS NULL OR exp.basis         = v_filter_basis)
       AND (v_filter_fire_at       IS NULL OR exp.fire_at       = v_filter_fire_at)
       AND (v_filter_applies_at_op IS NULL OR exp.applies_at_op = v_filter_applies_at_op)
     ORDER BY exp.source_bom_id, exp.source_line_no, exp.depth
  LOOP
    IF v_line.kind = 'item' THEN
      v_adj_qty := p_qty * v_line.qty_per_parent;
      v_specific_lot_id := NULL;

      SELECT id INTO v_comp_consumed FROM accounts
       WHERE kind='stock_consumed' AND sku_id=v_line.component_sku_id
         AND ledger_kind='qty' AND NOT is_closed;
      IF v_comp_consumed IS NULL THEN
        RAISE EXCEPTION 'no open stock_consumed account for sku=%',
                        v_line.component_sku_id USING ERRCODE = 'P0010';
      END IF;

      SELECT id INTO v_comp_qty_acct FROM accounts
       WHERE kind='stock_available' AND sku_id=v_line.component_sku_id
         AND location_id=v_line.component_loc_id AND NOT is_closed;
      IF v_comp_qty_acct IS NULL THEN
        RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                        v_line.component_sku_id, v_line.component_loc_id
          USING ERRCODE = 'P0010';
      END IF;

      SELECT id INTO v_comp_val_acct FROM accounts
       WHERE kind='inv_value_raw' AND sku_id=v_line.component_sku_id
         AND location_id=v_line.component_loc_id AND currency=v_wo.currency
         AND NOT is_closed;
      IF v_comp_val_acct IS NULL THEN
        RAISE EXCEPTION 'no open inv_value_raw account for sku=% loc=% ccy=%',
                        v_line.component_sku_id, v_line.component_loc_id, v_wo.currency
          USING ERRCODE = 'P0010';
      END IF;

      SELECT cost_method, tracked_by
        INTO v_comp_cost_method, v_comp_tracked_by
        FROM skus WHERE id = v_line.component_sku_id;

      CASE v_comp_cost_method
        WHEN 'standard' THEN
          v_comp_std_cost := _resolve_standard_cost_at(
            v_line.component_sku_id, p_business_date);
          v_value := v_adj_qty * v_comp_std_cost;
          v_unit  := v_comp_std_cost;

        WHEN 'wac_perpetual', 'wac_periodic', 'wac_retroactive' THEN
          SELECT COALESCE(SUM(qty), 0) INTO v_pool_qty
            FROM posting_lines pl
            JOIN accounts a ON a.id = pl.debit_account_id
           WHERE pl.debit_account_id = v_comp_val_acct
              OR pl.credit_account_id = v_comp_val_acct;
          SELECT COALESCE(
                   SUM(CASE WHEN pl.debit_account_id  = v_comp_val_acct THEN  pl.qty
                            WHEN pl.credit_account_id = v_comp_val_acct THEN -pl.qty END),
                 0)
            INTO v_pool_qty
            FROM posting_lines pl
           WHERE (pl.debit_account_id = v_comp_val_acct
                  OR pl.credit_account_id = v_comp_val_acct)
             AND pl.qty IS NOT NULL;
          IF v_pool_qty <= 0 THEN
            RAISE EXCEPTION
              'wac_pool_qty_zero: cannot price rm_issue from empty pool sku=% loc=%',
              v_line.component_sku_id, v_line.component_loc_id
              USING ERRCODE = 'P0010';
          END IF;

          SELECT (debits_total - credits_total) INTO v_pool_value
            FROM accounts WHERE id = v_comp_val_acct;
          v_unit  := GREATEST(COALESCE(v_pool_value, 0), 0) / v_pool_qty;
          v_value := v_adj_qty * v_unit;

        WHEN 'fifo' THEN
          SELECT COALESCE(SUM(cost_amount), 0)::BIGINT
            INTO v_value
            FROM _fifo_walk_layers(v_line.component_sku_id,
                                   v_line.component_loc_id,
                                   1::SMALLINT,
                                   v_adj_qty::NUMERIC);
          v_unit := v_value / v_adj_qty;

        WHEN 'lot_fifo' THEN
          -- vohc: walk per-lot, accumulate value AND capture per-walk
          -- metadata in v_walks. Generate v_value_idem ONCE so the
          -- value-leg event and every walk row share the lookup key.
          IF p_component_lot_pins IS NOT NULL THEN
            v_specific_lot_id := (p_component_lot_pins
              ->>(v_line.component_sku_id::TEXT))::BIGINT;
          END IF;
          v_value := 0;
          v_value_idem := gen_random_uuid();
          FOR v_lot_walk IN
            SELECT * FROM _lot_walk_layers(v_line.component_sku_id,
                                           v_line.component_loc_id,
                                           1::SMALLINT,
                                           v_adj_qty::NUMERIC,
                                           v_specific_lot_id)
          LOOP
            v_value := v_value + v_lot_walk.cost_amount;

            -- sxl2.5: auto-allocate units for lot_and_serial components.
            -- Pick N units of this consumed lot at the component
            -- location in serial_no order, FOR UPDATE, where N is the
            -- walk's allocation. The recon (sxl2.6 check #14) guards
            -- against unit_count drift from lot residual.
            v_unit_ids := NULL;
            IF v_comp_tracked_by = 'lot_and_serial' THEN
              WITH picked AS (
                SELECT unit_id
                  FROM inventory_units
                 WHERE product_id          = v_line.component_sku_id
                   AND current_location_id = v_line.component_loc_id
                   AND lot_id              = v_lot_walk.lot_id
                   AND lot_receipt_date    = v_lot_walk.receipt_date
                   AND status              = 'available'
                 ORDER BY serial_no
                 LIMIT v_lot_walk.allocation::INT
                   FOR UPDATE
              )
              SELECT array_agg(unit_id ORDER BY unit_id),
                     COUNT(*)::INT
                INTO v_unit_ids, v_picked_count
                FROM picked;

              IF v_picked_count < v_lot_walk.allocation::INT THEN
                RAISE EXCEPTION
                  'lot_and_serial component allocation: only %/%'
                  ' available units found for sku=% loc=% lot=%',
                  v_picked_count, v_lot_walk.allocation::INT,
                  v_line.component_sku_id, v_line.component_loc_id,
                  v_lot_walk.lot_id USING ERRCODE = 'P0006';
              END IF;
            END IF;

            v_walks := v_walks || jsonb_build_array(jsonb_build_object(
              'value_idem_key',   v_value_idem,
              'wo_id',            p_wo_id,
              'wo_event_id',      p_event_id,
              'routing_op',       p_routing_op,
              'component_sku_id', v_line.component_sku_id,
              'component_loc_id', v_line.component_loc_id,
              'lot_id',           v_lot_walk.lot_id,
              'lot_receipt_date', v_lot_walk.receipt_date,
              'qty',              v_lot_walk.allocation,
              'business_date',    p_business_date,
              'unit_ids',         CASE WHEN v_unit_ids IS NULL
                                       THEN NULL
                                       ELSE to_jsonb(v_unit_ids) END
            ));
          END LOOP;
          v_unit := CASE WHEN v_adj_qty > 0 THEN v_value / v_adj_qty ELSE 0 END;

        WHEN 'lot' THEN
          RAISE EXCEPTION
            'cost_method_not_implemented: % for component % (acct-uze)',
            v_comp_cost_method, v_line.component_sku_id
            USING ERRCODE = 'P0006';

        ELSE
          RAISE EXCEPTION
            'unknown cost_method % for component %',
            v_comp_cost_method, v_line.component_sku_id
            USING ERRCODE = 'P0011';
      END CASE;

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'rm_issue_to_wo',
        'document_kind',     p_document_kind,
        'document_id',       p_event_id,
        'debit_account_id',  v_comp_consumed,
        'credit_account_id', v_comp_qty_acct,
        'amount',            v_adj_qty,
        'qty',               v_adj_qty,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      ));

      IF v_value > 0 THEN
        -- For lot_fifo, reuse v_value_idem so walks rows and the
        -- emitted value-leg event share the same idempotency_key
        -- (lookup handle for _wo_write_lot_consumption).
        v_value_event := jsonb_build_object(
          'reason',            'rm_issue_to_wo',
          'document_kind',     p_document_kind,
          'document_id',       p_event_id,
          'debit_account_id',  v_val_acct_wip,
          'credit_account_id', v_comp_val_acct,
          'amount',            v_value,
          'qty',               v_adj_qty,
          'business_date',     p_business_date,
          'idempotency_key',   CASE WHEN v_comp_cost_method = 'lot_fifo'
                                    THEN v_value_idem
                                    ELSE gen_random_uuid() END,
          'posted_by',         p_posted_by
        );

        -- Forward lot_id key for lot_fifo so apply_event's E2 block
        -- writes inventory_lot_events against the named lot
        -- (or FIFO-walks when v_specific_lot_id is NULL).
        IF v_comp_cost_method = 'lot_fifo' THEN
          v_value_event := v_value_event || jsonb_build_object(
            'lot_id', v_specific_lot_id
          );
        END IF;

        v_batch := v_batch || jsonb_build_array(v_value_event);
      END IF;

    ELSIF v_line.kind IN ('service', 'charge') THEN
      IF v_line.basis = 'per_unit' THEN
        v_amount := p_qty * v_line.std_amount;
      ELSE
        v_amount := v_line.std_amount;
      END IF;
      IF v_amount <= 0 THEN
        CONTINUE;
      END IF;

      v_reason := _wo_apply_reason_for(v_line.absorption_class_id, v_line.basis);

      SELECT applied_account_kind INTO v_applied_kind FROM absorption_classes
       WHERE id = v_line.absorption_class_id;
      IF v_applied_kind IS NULL THEN
        RAISE EXCEPTION 'wo_invalid: absorption_class id=% not found',
                        v_line.absorption_class_id USING ERRCODE = 'P0026';
      END IF;

      SELECT id INTO v_applied_acct FROM accounts
       WHERE kind = v_applied_kind AND ledger_kind='value'
         AND currency = v_wo.currency AND NOT is_closed
       LIMIT 1;
      IF v_applied_acct IS NULL THEN
        RAISE EXCEPTION 'no open % account for ccy=%',
                        v_applied_kind, v_wo.currency
          USING ERRCODE = 'P0010';
      END IF;

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            v_reason,
        'document_kind',     p_document_kind,
        'document_id',       p_event_id,
        'debit_account_id',  v_val_acct_wip,
        'credit_account_id', v_applied_acct,
        'amount',            v_amount,
        'qty',               NULL,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      ));
    END IF;
  END LOOP;

  batch := v_batch;
  walks := v_walks;
  RETURN NEXT;
END;
$$;


-- ---------- 6. post_lot_transfer — accept unit_ids per line ----------

-- Verbatim from mig 0057 + sxl2.5 hooks:
--   - Each line accepts 'unit_ids' BIGINT[] or 'unit_serials'
--     TEXT[] (XOR). REQUIRED for tracked_by='lot_and_serial';
--     rejected for non-lot_and_serial SKUs.
--   - Wrapper validates units (count = qty, all active at FROM,
--     all same lot, all same product), derives v_specific_lot
--     from the units, then proceeds through the existing walk.
--   - Post-PERFORM: for each walk that carries unit_ids, UPDATE
--     units (current_location_id=TO, lot_id=dest, lot_receipt_date=
--     dest), emit type=3 transfer events, and stamp
--     lot_transfer_lines.unit_ids.

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
  v_unit_ids_json   JSONB;
  v_unit_serials    JSONB;
  v_unit_ids        BIGINT[];
  v_unit_serials_arr TEXT[];
  v_resolved_unit_ids BIGINT[];
  v_unit_count      INT;
  v_qty_int         INT;
  v_unit_check_lot      BIGINT;
  v_unit_check_lot_max  BIGINT;
  v_unit_check_date     DATE;
  v_unit_check_date_max DATE;
  v_unit_matched_count  INT;
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
    v_unit_ids_json := v_line->'unit_ids';
    v_unit_serials  := v_line->'unit_serials';
    v_resolved_unit_ids := NULL;

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

    -- sxl2.5: per-unit identification.
    --   - lot_and_serial: REQUIRED to pass unit_ids OR unit_serials
    --     (XOR; both rejected). Wrapper validates + pins specific_lot.
    --   - lot (no serial): reject unit_ids/unit_serials if supplied
    --     (no per-unit identity to bind).
    IF v_unit_ids_json IS NOT NULL AND v_unit_serials IS NOT NULL THEN
      RAISE EXCEPTION
        'lot_transfer_invalid: line % cannot supply both unit_ids and unit_serials',
        v_idx + 1 USING ERRCODE = 'P0006';
    END IF;
    IF v_tracked_by <> 'lot_and_serial' THEN
      IF v_unit_ids_json IS NOT NULL OR v_unit_serials IS NOT NULL THEN
        RAISE EXCEPTION
          'lot_transfer_invalid: line % sku=% tracked_by=% does not accept '
          'unit_ids/unit_serials (only ''lot_and_serial'' SKUs)',
          v_idx + 1, v_sku, v_tracked_by USING ERRCODE = 'P0006';
      END IF;
    ELSE
      -- lot_and_serial: identifiers required.
      IF v_unit_ids_json IS NULL AND v_unit_serials IS NULL THEN
        RAISE EXCEPTION
          'lot_transfer_invalid: line % sku=% tracked_by=''lot_and_serial'' '
          'requires unit_ids or unit_serials',
          v_idx + 1, v_sku USING ERRCODE = 'P0006';
      END IF;

      v_qty_int := v_qty::INT;
      IF v_qty_int::NUMERIC <> v_qty THEN
        RAISE EXCEPTION
          'lot_transfer_invalid: line % sku=% qty=% must be integer for lot_and_serial',
          v_idx + 1, v_sku, v_qty USING ERRCODE = 'P0006';
      END IF;

      IF v_unit_ids_json IS NOT NULL THEN
        SELECT array_agg((x)::BIGINT ORDER BY ord)
          INTO v_unit_ids
          FROM jsonb_array_elements_text(v_unit_ids_json)
               WITH ORDINALITY AS t(x, ord);
        IF COALESCE(array_length(v_unit_ids, 1), 0) <> v_qty_int THEN
          RAISE EXCEPTION
            'lot_transfer_invalid: line % unit_ids length % does not match qty %',
            v_idx + 1, COALESCE(array_length(v_unit_ids, 1), 0), v_qty_int
            USING ERRCODE = 'P0006';
        END IF;
        v_resolved_unit_ids := v_unit_ids;
      ELSE
        SELECT array_agg(s ORDER BY ord)
          INTO v_unit_serials_arr
          FROM jsonb_array_elements_text(v_unit_serials)
               WITH ORDINALITY AS t(s, ord);
        IF COALESCE(array_length(v_unit_serials_arr, 1), 0) <> v_qty_int THEN
          RAISE EXCEPTION
            'lot_transfer_invalid: line % unit_serials length % does not match qty %',
            v_idx + 1, COALESCE(array_length(v_unit_serials_arr, 1), 0), v_qty_int
            USING ERRCODE = 'P0006';
        END IF;
        SELECT array_agg(iu.unit_id ORDER BY arr.ord)
          INTO v_resolved_unit_ids
          FROM unnest(v_unit_serials_arr) WITH ORDINALITY AS arr(s, ord)
          JOIN inventory_units iu
            ON iu.product_id = v_sku
           AND iu.serial_no  = arr.s
           AND iu.status IN ('available', 'reserved', 'allocated',
                             'on_hold', 'returned');
        IF COALESCE(array_length(v_resolved_unit_ids, 1), 0) <> v_qty_int THEN
          RAISE EXCEPTION
            'lot_transfer_invalid: line % one or more unit_serials did not '
            'resolve to an active unit for sku=% (resolved %/%)',
            v_idx + 1, v_sku,
            COALESCE(array_length(v_resolved_unit_ids, 1), 0), v_qty_int
            USING ERRCODE = 'P0006';
        END IF;
      END IF;

      -- Lock + validate units share lot and are at FROM.
      PERFORM 1 FROM inventory_units
       WHERE unit_id = ANY(v_resolved_unit_ids)
       ORDER BY unit_id
         FOR UPDATE;

      SELECT MIN(lot_id), MAX(lot_id),
             MIN(lot_receipt_date), MAX(lot_receipt_date),
             COUNT(*)
        INTO v_unit_check_lot, v_unit_check_lot_max,
             v_unit_check_date, v_unit_check_date_max,
             v_unit_matched_count
        FROM inventory_units
       WHERE unit_id = ANY(v_resolved_unit_ids)
         AND product_id = v_sku
         AND current_location_id = p_from_location_id
         AND status IN ('available', 'reserved', 'allocated',
                        'on_hold', 'returned');

      IF v_unit_matched_count <> COALESCE(array_length(v_resolved_unit_ids, 1), 0) THEN
        RAISE EXCEPTION
          'lot_transfer_invalid: line % one or more units are not active / '
          'not at sku=% / not at FROM loc=% (matched %/%)',
          v_idx + 1, v_sku, p_from_location_id,
          v_unit_matched_count,
          COALESCE(array_length(v_resolved_unit_ids, 1), 0)
          USING ERRCODE = 'P0006';
      END IF;
      IF v_unit_check_lot <> v_unit_check_lot_max
         OR v_unit_check_date <> v_unit_check_date_max THEN
        RAISE EXCEPTION
          'lot_transfer_invalid: line % units span multiple lots '
          '(% to %); one line must transfer units from a single lot',
          v_idx + 1, v_unit_check_lot, v_unit_check_lot_max
          USING ERRCODE = 'P0006';
      END IF;

      -- Override caller-supplied lot_id with the units' shared lot.
      v_specific_lot := v_unit_check_lot;
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

    INSERT INTO lot_transfer_lines (transfer_id, line_no, sku_id, qty, lot_id, unit_ids)
      VALUES (v_doc_id, v_idx + 1, v_sku, v_qty, v_specific_lot, v_resolved_unit_ids)
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
        'from_location_id',    p_from_location_id,
        'unit_ids',            CASE WHEN v_resolved_unit_ids IS NULL
                                    THEN NULL
                                    ELSE to_jsonb(v_resolved_unit_ids) END
      ));
    END LOOP;
  END LOOP;

  -- Post all legs (qty + value) in one batch.
  PERFORM post_posting_lines(v_batch, FALSE);

  -- Lot subledger writeback: per consumed source lot, INSERT new
  -- lot row at TO + adjust_out event on source + lot_id stamps.
  -- Plus sxl2.5: per-unit UPDATE + type=3 events when units present.
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

    -- sxl2.5: per-unit writeback when this walk carries unit_ids.
    IF (v_walk_meta->'unit_ids') IS NOT NULL
       AND jsonb_typeof(v_walk_meta->'unit_ids') = 'array'
       AND jsonb_array_length(v_walk_meta->'unit_ids') > 0 THEN
      SELECT array_agg((x)::BIGINT ORDER BY ord)
        INTO v_unit_ids
        FROM jsonb_array_elements_text(v_walk_meta->'unit_ids')
             WITH ORDINALITY AS t(x, ord);

      -- Move units to dest lot+location; status stays 'available'.
      UPDATE inventory_units
         SET current_location_id = (v_walk_meta->>'to_location_id')::UUID,
             lot_id              = v_dest_lot_id,
             lot_receipt_date    = p_business_date,
             updated_at          = clock_timestamp()
       WHERE unit_id = ANY(v_unit_ids);

      -- One type=3 transfer event per unit.
      INSERT INTO inventory_unit_events (
        unit_id, event_date, event_type,
        posting_line_id, new_status,
        location_id_from, location_id_to,
        new_lot_id, new_lot_receipt_date, notes
      )
      SELECT u, p_business_date, 3,
             v_value_pl_id, 'available',
             v_lot_meta.location_id,
             (v_walk_meta->>'to_location_id')::UUID,
             v_dest_lot_id, p_business_date,
             'lot_transfer'
        FROM unnest(v_unit_ids) AS u;
    END IF;
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
  'Lot-tracked SKU transfer between locations (acct-fzzw + sxl2.5). '
  'Walks source lots under FOR UPDATE via _lot_walk_layers; per '
  'consumed source lot posts qty + value legs, creates a new '
  'inventory_lots row at TO copying source metadata, writes '
  'event_type=8 adjust_out on source. For tracked_by=''lot_and_serial'' '
  'SKUs the caller MUST supply unit_ids or unit_serials per line; '
  'the wrapper pins specific_lot from the units, flips '
  'current_location_id + lot_id + lot_receipt_date on each unit '
  '(status stays available), and emits a type=3 transfer event per '
  'unit. Cross-currency rejected. Idempotent on p_idempotency_key.';
