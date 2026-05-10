-- ============================================================
-- acct-vohc — op_arrival lot pinning persistence in WO routing.
--
-- WHAT:
--   New table wo_lot_consumption persists per-routing-op lot
--   lineage when a lot_fifo component is consumed via
--   rm_issue_to_wo (post_wo_start at first-op fire, post_op_move
--   at subsequent op_arrival). One row per consumed lot per
--   rm_issue value-leg posting (multi-lot unpinned walks produce
--   multiple rows tied to the SAME posting_line_id).
--
-- WHY:
--   Pre-vohc, lot lineage was recoverable only via a multi-table
--   join (inventory_lot_events.posting_line_id -> posting_lines
--   -> filter document_kind LIKE 'wo_%' -> document_id ->
--   wo_events). That works but is operationally painful for
--   per-routing-op cost-flow analysis and lays no foundation
--   for downstream lot-genealogy work (acct-3j3z) which needs
--   to tie consumed lots to FG-side child lots.
--
-- HOW (Option A from design discussion):
--   _wo_emit_bom_lines signature changes from RETURNS JSONB to
--   RETURNS TABLE(batch JSONB, walks JSONB). The lot_fifo branch
--   replaces SUM(cost_amount) with a per-walk FOR loop, captures
--   each walk in the walks JSONB array, stamps the value-leg
--   idempotency_key on every walk row. Callers (post_wo_start,
--   post_op_move) merge the helper's batch with their own events,
--   PERFORM post_posting_lines once, then PERFORM
--   _wo_write_lot_consumption(walks) to look up posting_line.id
--   by idempotency_key and INSERT wo_lot_consumption rows.
--
-- DESIGN CALLS:
--   - lot_fifo only. Other cost methods (standard/wac/fifo) leave
--     walks empty; only lot_fifo populates the persistence path.
--   - One row per consumed lot. Pinned single-lot consumption
--     produces one row; unpinned multi-lot walks produce N rows
--     (Q1 design ruling: "yes to each consumed lot").
--   - posting_line_id stamps the rm_issue VALUE-leg posting line
--     (where the lot identity matters; the qty-leg has no lot
--     dimension).
--   - wo_event_id is NOT NULL — both call sites have v_event_id
--     by the time _wo_write_lot_consumption fires.
--   - Append-only via shared block_inventory_lot_modifications
--     trigger fn (raises P9999 on UPDATE/DELETE).
--   - No partitioning. wo_lot_consumption volume is bounded by
--     WO count × routing_op count × lot count per consumption
--     event, much smaller than inventory_lot_events.
--   - Recon check #11 (lot_consumption_orphan + inverse) added
--     to run_daily_reconciliation. Both directions per Q4 ruling.
--
-- NOT IN SCOPE (acct-vohc-followup if needed):
--   - Backfill helper for existing closed WOs (Phase 0/1, no
--     production data per Q3 ruling).
--   - Lot genealogy parent-child links (acct-3j3z).
--   - Per-lot value-level subledger<->GL recon (acct-20y0).
-- ============================================================

-- ---------- 1. wo_lot_consumption table ----------

CREATE TABLE wo_lot_consumption (
  id                BIGSERIAL PRIMARY KEY,
  wo_id             UUID         NOT NULL REFERENCES work_orders(id),
  wo_event_id       UUID         NOT NULL REFERENCES wo_events(id),
  routing_op        INT          NOT NULL,
  component_sku_id  UUID         NOT NULL REFERENCES skus(id),
  lot_id            BIGINT       NOT NULL,
  lot_receipt_date  DATE         NOT NULL,
  qty               NUMERIC(19, 6) NOT NULL CHECK (qty > 0),
  posting_line_id   BIGINT       NOT NULL REFERENCES posting_lines(id),
  posted_at         TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
  FOREIGN KEY (lot_id, lot_receipt_date)
    REFERENCES inventory_lots (lot_id, receipt_date)
);

CREATE INDEX wo_lot_consumption_wo
  ON wo_lot_consumption (wo_id, routing_op);

CREATE INDEX wo_lot_consumption_lot
  ON wo_lot_consumption (lot_id);

CREATE INDEX wo_lot_consumption_pl
  ON wo_lot_consumption (posting_line_id);

CREATE INDEX wo_lot_consumption_component
  ON wo_lot_consumption (component_sku_id);

-- ---------- 2. Append-only trigger (reuses lot table fn) ----------

CREATE TRIGGER trg_wo_lot_consumption_append_only
  BEFORE UPDATE OR DELETE ON wo_lot_consumption
  FOR EACH ROW EXECUTE FUNCTION block_inventory_lot_modifications();

-- ---------- 3. _wo_write_lot_consumption helper ----------

-- Iterates walks JSONB (each row carries value_idem_key +
-- per-walk metadata), looks up posting_line.id by
-- idempotency_key, INSERTs one wo_lot_consumption row per walk.
-- Caller invokes after PERFORM post_posting_lines completes;
-- the value-leg postings are committed (in-txn) and their ids
-- discoverable via the idempotency_key UNIQUE index.

CREATE OR REPLACE FUNCTION _wo_write_lot_consumption(
  p_walks JSONB
) RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
  v_walk     JSONB;
  v_pl_id    BIGINT;
  v_idem     UUID;
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

    INSERT INTO wo_lot_consumption (
      wo_id, wo_event_id, routing_op, component_sku_id,
      lot_id, lot_receipt_date, qty, posting_line_id
    ) VALUES (
      (v_walk->>'wo_id')::UUID,
      (v_walk->>'wo_event_id')::UUID,
      (v_walk->>'routing_op')::INT,
      (v_walk->>'component_sku_id')::UUID,
      (v_walk->>'lot_id')::BIGINT,
      (v_walk->>'lot_receipt_date')::DATE,
      (v_walk->>'qty')::NUMERIC,
      v_pl_id
    );
  END LOOP;
END;
$$;

-- ---------- 4. _wo_emit_bom_lines — signature change ----------

-- DROP FUNCTION required: return type changes from JSONB to
-- TABLE(batch JSONB, walks JSONB). PG forbids RETURN-type
-- changes via CREATE OR REPLACE.

DROP FUNCTION IF EXISTS _wo_emit_bom_lines(
  UUID, BIGINT, INT, BIGINT, JSONB, UUID, DATE, UUID, TEXT, JSONB
);

CREATE FUNCTION _wo_emit_bom_lines(
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
  v_wo                   work_orders%ROWTYPE;
  v_val_acct_wip         BIGINT;
  v_batch                JSONB := '[]'::JSONB;
  v_walks                JSONB := '[]'::JSONB;
  v_line                 RECORD;
  v_filter_kind          TEXT;
  v_filter_basis         TEXT;
  v_filter_fire_at       TEXT;
  v_filter_applies_at_op INT;
  v_adj_qty              BIGINT;
  v_value                BIGINT;
  v_amount               BIGINT;
  v_reason               posting_line_reason;
  v_comp_consumed        BIGINT;
  v_comp_qty_acct        BIGINT;
  v_comp_val_acct        BIGINT;
  v_applied_kind         account_kind;
  v_applied_acct         BIGINT;
  v_comp_std_cost        BIGINT;
  v_comp_cost_method     cost_method;
  v_pool_qty             BIGINT;
  v_pool_value           BIGINT;
  v_unit                 BIGINT;
  v_specific_lot_id      BIGINT;
  v_value_event          JSONB;
  v_value_idem           UUID;
  v_lot_walk             RECORD;
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

      SELECT cost_method INTO v_comp_cost_method
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
            v_walks := v_walks || jsonb_build_array(jsonb_build_object(
              'value_idem_key',   v_value_idem,
              'wo_id',            p_wo_id,
              'wo_event_id',      p_event_id,
              'routing_op',       p_routing_op,
              'component_sku_id', v_line.component_sku_id,
              'lot_id',           v_lot_walk.lot_id,
              'lot_receipt_date', v_lot_walk.receipt_date,
              'qty',              v_lot_walk.allocation
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

-- ---------- 5. post_wo_start — accumulate v_walks; write after post ----------

CREATE OR REPLACE FUNCTION post_wo_start(
  p_wo_id              UUID,
  p_business_date      DATE,
  p_posted_by          UUID,
  p_idempotency_key    UUID,
  p_notes              TEXT DEFAULT NULL,
  p_component_lot_pins JSONB DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id     UUID;
  v_event_id        UUID;
  v_wo              work_orders%ROWTYPE;
  v_first_op        INT;
  v_op_count        INT;
  v_cost_method     cost_method;
  v_qty_acct_wip    BIGINT;
  v_void_qty        BIGINT;
  v_val_acct_wip    BIGINT;
  v_bom             bom_headers%ROWTYPE;
  v_bad_op          INT;
  v_alloc_sum       NUMERIC;
  v_batch           JSONB := '[]'::JSONB;
  v_walks           JSONB := '[]'::JSONB;
  v_emit_batch      JSONB;
  v_emit_walks      JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  SELECT * INTO v_wo FROM work_orders WHERE id = p_wo_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: WO % not found', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;

  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF v_wo.status <> 'draft' THEN
    RAISE EXCEPTION 'wo_invalid: WO % status=% not draft (already started)',
                    p_wo_id, v_wo.status USING ERRCODE = 'P0026';
  END IF;

  SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_wo.parent_sku_id;
  IF v_cost_method NOT IN ('standard', 'wac_perpetual', 'wac_periodic',
                           'wac_retroactive', 'fifo', 'lot_fifo') THEN
    RAISE EXCEPTION
      'wo_invalid: parent_sku % has cost_method=% which post_wo_start does not handle',
      v_wo.parent_sku_id, v_cost_method USING ERRCODE = 'P0026';
  END IF;

  SELECT MIN(routing_op), COUNT(*) INTO v_first_op, v_op_count
    FROM wo_routings WHERE wo_id = p_wo_id;
  IF v_op_count = 0 THEN
    RAISE EXCEPTION 'wo_invalid: WO % has no routing operations', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;

  SELECT id INTO v_qty_acct_wip FROM accounts
   WHERE kind='stock_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=v_first_op AND NOT is_closed;
  IF v_qty_acct_wip IS NULL THEN
    RAISE EXCEPTION 'no open stock_wip account for sku=% op=%',
                    v_wo.parent_sku_id, v_first_op USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_void_qty FROM accounts
   WHERE kind='creation_void' AND ledger_kind='qty' AND NOT is_closed;
  IF v_void_qty IS NULL THEN
    RAISE EXCEPTION 'no creation_void(qty) account configured'
      USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_val_acct_wip FROM accounts
   WHERE kind='inv_value_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=v_first_op AND currency=v_wo.currency
     AND NOT is_closed;
  IF v_val_acct_wip IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_wip account for sku=% op=% ccy=%',
                    v_wo.parent_sku_id, v_first_op, v_wo.currency
      USING ERRCODE = 'P0010';
  END IF;

  v_bom := _wo_resolve_bom_for(p_wo_id, p_business_date);

  SELECT exp.applies_at_op INTO v_bad_op
    FROM _wo_explode_bom(v_bom.id, p_business_date) exp
   WHERE NOT EXISTS (
     SELECT 1 FROM wo_routings wr
      WHERE wr.wo_id = p_wo_id AND wr.routing_op = exp.applies_at_op
   )
   LIMIT 1;
  IF v_bad_op IS NOT NULL THEN
    RAISE EXCEPTION
      'wo_start_op_mismatch: bom_lines reference applies_at_op=% '
      'which is not in wo_routings(wo=%)',
      v_bad_op, p_wo_id USING ERRCODE = 'P0028';
  END IF;

  PERFORM 1 FROM wo_outputs WHERE wo_id = p_wo_id LIMIT 1;
  IF NOT FOUND THEN
    INSERT INTO wo_outputs (
      wo_id, output_no, output_sku_id, fg_location_id, qty,
      allocation_method, allocation_pct
    ) VALUES (
      p_wo_id, 1, v_wo.parent_sku_id, v_wo.fg_location_id, v_wo.qty_target,
      'primary', 100
    );
  ELSE
    SELECT COALESCE(SUM(allocation_pct), 0)
      INTO v_alloc_sum
      FROM wo_outputs WHERE wo_id = p_wo_id;
    IF v_alloc_sum <> 100 THEN
      RAISE EXCEPTION
        'output_allocation_invalid: wo_outputs(wo=%) allocation_pct sums to % (expected 100)',
        p_wo_id, v_alloc_sum USING ERRCODE = 'P0033';
    END IF;
  END IF;

  PERFORM 1 FROM wo_by_products WHERE wo_id = p_wo_id LIMIT 1;
  IF NOT FOUND THEN
    INSERT INTO wo_by_products (
      wo_id, by_product_no, output_sku_id, fg_location_id,
      planned_qty, actual_qty, unit_value, treatment,
      disposal_basis, disposal_vendor_id, disposal_expense_account_kind
    )
    SELECT
      p_wo_id,
      bbp.by_product_no,
      bbp.output_sku_id,
      bbp.fg_location_id,
      ROUND(bbp.qty_per_parent * v_wo.qty_target)::BIGINT AS planned_qty,
      ROUND(bbp.qty_per_parent * v_wo.qty_target)::BIGINT AS actual_qty,
      bbp.unit_value,
      bbp.treatment,
      bbp.disposal_basis,
      bbp.disposal_vendor_id,
      bbp.disposal_expense_account_kind
    FROM bom_by_products bbp
   WHERE bbp.bom_id = v_bom.id
     AND ROUND(bbp.qty_per_parent * v_wo.qty_target) >= 1;
  END IF;

  INSERT INTO wo_events (
    wo_id, event_kind, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_wo_id, 'start', p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_event_id;
  IF v_event_id IS NULL THEN RETURN p_wo_id; END IF;

  v_batch := v_batch || jsonb_build_array(jsonb_build_object(
    'reason',            'wo_start',
    'document_kind',     'wo_start',
    'document_id',       v_event_id,
    'debit_account_id',  v_qty_acct_wip,
    'credit_account_id', v_void_qty,
    'amount',            v_wo.qty_target,
    'qty',               v_wo.qty_target,
    'business_date',     p_business_date,
    'idempotency_key',   gen_random_uuid(),
    'posted_by',         p_posted_by
  ));

  SELECT b.batch, b.walks INTO v_emit_batch, v_emit_walks
    FROM _wo_emit_bom_lines(
      p_wo_id, v_bom.id, v_first_op, v_wo.qty_target,
      jsonb_build_object('fire_at', 'wo_start'),
      v_event_id, p_business_date, p_posted_by, 'wo_start',
      p_component_lot_pins
    ) b;
  v_batch := v_batch || v_emit_batch;
  v_walks := v_walks || v_emit_walks;

  SELECT b.batch, b.walks INTO v_emit_batch, v_emit_walks
    FROM _wo_emit_bom_lines(
      p_wo_id, v_bom.id, v_first_op, v_wo.qty_target,
      jsonb_build_object('fire_at', 'op_arrival', 'applies_at_op', v_first_op),
      v_event_id, p_business_date, p_posted_by, 'wo_start',
      p_component_lot_pins
    ) b;
  v_batch := v_batch || v_emit_batch;
  v_walks := v_walks || v_emit_walks;

  PERFORM post_posting_lines(v_batch, FALSE);
  PERFORM _wo_write_lot_consumption(v_walks);

  UPDATE work_orders SET status = 'released' WHERE id = p_wo_id;
  RETURN p_wo_id;
END;
$$;

-- ---------- 6. post_op_move — accumulate v_walks; write after post ----------

CREATE OR REPLACE FUNCTION post_op_move(
  p_wo_id           UUID,
  p_from_op         INT,
  p_to_op           INT,
  p_qty             BIGINT,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id      UUID;
  v_event_id         UUID;
  v_wo               work_orders%ROWTYPE;
  v_from_count       INT;
  v_to_count         INT;
  v_qty_from         BIGINT;
  v_qty_to           BIGINT;
  v_val_from         BIGINT;
  v_val_to           BIGINT;
  v_value_amount     BIGINT;
  v_batch            JSONB := '[]'::JSONB;
  v_walks            JSONB := '[]'::JSONB;
  v_emit_batch       JSONB;
  v_emit_walks       JSONB;
  v_bom              bom_headers%ROWTYPE;
  v_first_op         INT;
  v_default_lot_size BIGINT;
  v_per_unit_cum     BIGINT;
  v_per_lot_cum      BIGINT;
  v_first_arrival    BOOLEAN;
  v_cost_method      cost_method;
  v_pool_value       BIGINT;
  v_pool_qty         BIGINT;
  v_unit             BIGINT;
  v_lock_first       BIGINT;
  v_lock_second      BIGINT;
BEGIN
  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF p_qty IS NULL OR p_qty <= 0 THEN
    RAISE EXCEPTION 'wo_invalid: op_move qty must be > 0 (got %)', p_qty
      USING ERRCODE = 'P0026';
  END IF;
  IF p_from_op = p_to_op THEN
    RAISE EXCEPTION 'routing_op_invalid: from_op (%) = to_op (%)',
                    p_from_op, p_to_op USING ERRCODE = 'P0028';
  END IF;

  SELECT * INTO v_wo FROM work_orders WHERE id = p_wo_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: WO % not found', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;

  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF v_wo.status <> 'released' THEN
    RAISE EXCEPTION 'wo_invalid: WO % status=% not released',
                    p_wo_id, v_wo.status USING ERRCODE = 'P0026';
  END IF;

  SELECT COUNT(*) INTO v_from_count FROM wo_routings
   WHERE wo_id = p_wo_id AND routing_op = p_from_op;
  IF v_from_count = 0 THEN
    RAISE EXCEPTION 'routing_op_invalid: from_op % not in WO % routing',
                    p_from_op, p_wo_id USING ERRCODE = 'P0028';
  END IF;
  SELECT COUNT(*) INTO v_to_count FROM wo_routings
   WHERE wo_id = p_wo_id AND routing_op = p_to_op;
  IF v_to_count = 0 THEN
    RAISE EXCEPTION 'routing_op_invalid: to_op % not in WO % routing',
                    p_to_op, p_wo_id USING ERRCODE = 'P0028';
  END IF;

  SELECT id INTO v_qty_from FROM accounts
   WHERE kind='stock_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_from_op AND NOT is_closed;
  IF v_qty_from IS NULL THEN
    RAISE EXCEPTION 'no open stock_wip account for sku=% op=%',
                    v_wo.parent_sku_id, p_from_op USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_qty_to FROM accounts
   WHERE kind='stock_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_to_op AND NOT is_closed;
  IF v_qty_to IS NULL THEN
    RAISE EXCEPTION 'no open stock_wip account for sku=% op=%',
                    v_wo.parent_sku_id, p_to_op USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_val_from FROM accounts
   WHERE kind='inv_value_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_from_op AND currency=v_wo.currency AND NOT is_closed;
  IF v_val_from IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_wip account for sku=% op=% ccy=%',
                    v_wo.parent_sku_id, p_from_op, v_wo.currency
      USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_val_to FROM accounts
   WHERE kind='inv_value_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_to_op AND currency=v_wo.currency AND NOT is_closed;
  IF v_val_to IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_wip account for sku=% op=% ccy=%',
                    v_wo.parent_sku_id, p_to_op, v_wo.currency
      USING ERRCODE = 'P0010';
  END IF;

  v_bom := _wo_resolve_bom_for(p_wo_id, p_business_date);
  SELECT cost_method, default_lot_size INTO v_cost_method, v_default_lot_size
    FROM skus WHERE id = v_wo.parent_sku_id;
  SELECT MIN(routing_op) INTO v_first_op
    FROM wo_routings WHERE wo_id = p_wo_id;

  IF v_cost_method = 'standard' THEN
    SELECT COALESCE(SUM(
      CASE
        WHEN exp.kind = 'item' THEN
          (exp.qty_per_parent
            * _resolve_standard_cost_at(exp.component_sku_id, p_business_date))
        WHEN exp.kind = 'service' AND exp.basis = 'per_unit' THEN exp.std_amount
        ELSE 0
      END
    ), 0) INTO v_per_unit_cum
      FROM _wo_explode_bom(v_bom.id, p_business_date) exp
     WHERE exp.basis = 'per_unit'
       AND exp.applies_at_op <= p_from_op;

    SELECT COALESCE(SUM(exp.std_amount), 0) / v_default_lot_size
      INTO v_per_lot_cum
      FROM _wo_explode_bom(v_bom.id, p_business_date) exp
     WHERE exp.basis = 'per_lot'
       AND (
         exp.fire_at = 'wo_start'
         OR (exp.fire_at = 'op_arrival' AND exp.applies_at_op <= p_from_op)
       );

    v_value_amount := p_qty * (v_per_unit_cum + v_per_lot_cum);

  -- L4: 'lot_fifo' joins WAC family for op_move value calc.
  ELSIF v_cost_method IN ('wac_perpetual', 'wac_periodic',
                          'wac_retroactive', 'fifo', 'lot_fifo') THEN
    -- WIP pool is single-pool (lot/FIFO at WIP is post-MVP).
    v_lock_first  := LEAST(v_qty_from, v_val_from);
    v_lock_second := GREATEST(v_qty_from, v_val_from);
    PERFORM 1 FROM accounts WHERE id = v_lock_first  FOR UPDATE;
    PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;
    SELECT (debits_total - credits_total) INTO v_pool_value
      FROM accounts WHERE id = v_val_from;
    SELECT (debits_total - credits_total) INTO v_pool_qty
      FROM accounts WHERE id = v_qty_from;

    IF v_pool_qty IS NULL OR v_pool_qty <= 0 THEN
      v_value_amount := 0;
    ELSE
      v_unit := GREATEST(COALESCE(v_pool_value, 0), 0) / v_pool_qty;
      v_value_amount := p_qty * v_unit;
    END IF;

  ELSE
    RAISE EXCEPTION
      'wo_invalid: parent_sku % has cost_method=% which post_op_move does not handle',
      v_wo.parent_sku_id, v_cost_method USING ERRCODE = 'P0026';
  END IF;

  v_first_arrival := NOT EXISTS (
    SELECT 1 FROM wo_events
     WHERE wo_id = p_wo_id
       AND (
         (event_kind = 'op_move' AND routing_op_to = p_to_op)
         OR (event_kind = 'start' AND p_to_op = v_first_op)
       )
  );

  INSERT INTO wo_events (
    wo_id, event_kind, routing_op_from, routing_op_to, qty,
    business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_wo_id, 'op_move', p_from_op, p_to_op, p_qty,
    p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_event_id;
  IF v_event_id IS NULL THEN RETURN p_wo_id; END IF;

  v_batch := v_batch || jsonb_build_array(jsonb_build_object(
    'reason',            'op_move',
    'document_kind',     'op_move',
    'document_id',       v_event_id,
    'debit_account_id',  v_qty_to,
    'credit_account_id', v_qty_from,
    'amount',            p_qty,
    'qty',               p_qty,
    'business_date',     p_business_date,
    'idempotency_key',   gen_random_uuid(),
    'posted_by',         p_posted_by
  ));

  IF v_value_amount > 0 THEN
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'op_move_v',
      'document_kind',     'op_move',
      'document_id',       v_event_id,
      'debit_account_id',  v_val_to,
      'credit_account_id', v_val_from,
      'amount',            v_value_amount,
      'qty',               p_qty,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'posted_by',         p_posted_by
    ));
  END IF;

  IF v_first_arrival THEN
    SELECT b.batch, b.walks INTO v_emit_batch, v_emit_walks
      FROM _wo_emit_bom_lines(
        p_wo_id, v_bom.id, p_to_op, p_qty,
        jsonb_build_object('fire_at', 'op_arrival', 'applies_at_op', p_to_op),
        v_event_id, p_business_date, p_posted_by, 'op_move'
      ) b;
  ELSE
    SELECT b.batch, b.walks INTO v_emit_batch, v_emit_walks
      FROM _wo_emit_bom_lines(
        p_wo_id, v_bom.id, p_to_op, p_qty,
        jsonb_build_object('fire_at',        'op_arrival',
                           'applies_at_op',  p_to_op,
                           'basis',          'per_unit',
                           'kind',           'service'),
        v_event_id, p_business_date, p_posted_by, 'op_move'
      ) b;
  END IF;
  v_batch := v_batch || v_emit_batch;
  v_walks := v_walks || v_emit_walks;

  PERFORM post_posting_lines(v_batch, FALSE);
  PERFORM _wo_write_lot_consumption(v_walks);

  RETURN p_wo_id;
END;
$$;

-- ---------- 7. run_daily_reconciliation — add check #11 ----------

-- Check 11: wo_lot_consumption ↔ inventory_lot_events agreement.
--
-- Both directions per acct-vohc Q4 ruling:
--   (a) Forward — every rm_issue_to_wo value-leg posting on a
--       lot_fifo component MUST have a matching wo_lot_consumption
--       row(s). Orphan side: posting exists, no wo_lot_consumption.
--   (b) Inverse — every wo_lot_consumption row MUST have a matching
--       inventory_lot_events row (event_type=1 issue, same
--       posting_line_id, same lot_id+receipt_date, same qty).
--
-- The walks captured by _wo_emit_bom_lines and the writebacks done
-- by apply_event's E2 block are independent walks under FOR UPDATE
-- on the same lot rows in the same txn — they MUST agree by
-- construction. This check is a defensive net for future code
-- evolution that might decouple them.

CREATE OR REPLACE FUNCTION run_daily_reconciliation() RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
  v_total INT := 0;
  v_step  INT;
BEGIN
  -- Check 1: per-ledger double-entry.
  WITH imbalances AS (
    SELECT ledger_kind, currency,
           SUM(debits_total)::BIGINT  AS dr,
           SUM(credits_total)::BIGINT AS cr
      FROM accounts
     GROUP BY ledger_kind, currency
    HAVING SUM(debits_total) <> SUM(credits_total)
  )
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'double_entry_imbalance',
         jsonb_build_object(
           'ledger_kind', ledger_kind,
           'currency',    currency,
           'debits',      dr,
           'credits',     cr,
           'imbalance',   dr - cr
         )
    FROM imbalances;
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 2: reservation over-promise.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'reservation_over_promise',
         jsonb_build_object(
           'sku_id',      a.sku_id,
           'location_id', a.location_id,
           'on_hand',     (a.debits_total - a.credits_total),
           'reserved',    COALESCE(r.total, 0),
           'deficit',     (a.debits_total - a.credits_total) - COALESCE(r.total, 0)
         )
    FROM accounts a
    LEFT JOIN (
      SELECT sku_id, location_id, SUM(qty)::BIGINT AS total
        FROM inventory_reservations
       WHERE status = 'active'
       GROUP BY sku_id, location_id
    ) r ON r.sku_id = a.sku_id AND r.location_id = a.location_id
   WHERE a.kind = 'stock_available'
     AND NOT a.is_closed
     AND (a.debits_total - a.credits_total) < COALESCE(r.total, 0);
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 3: B2 currency extension amount consistency.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'currency_extension_amount_mismatch',
         jsonb_build_object(
           'posting_line_id',      plc.posting_line_id,
           'amount_transaction',   plc.amount_transaction,
           'amount',               pl.amount,
           'currency_transaction', plc.currency_transaction,
           'fx_rate_to_functional',plc.fx_rate_to_functional::TEXT
         )
    FROM posting_line_currencies plc
    JOIN posting_lines pl ON pl.id = plc.posting_line_id
   WHERE plc.amount_transaction <> pl.amount;
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 4: B3 dimensions — product coverage.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'dimension_product_missing',
         jsonb_build_object(
           'posting_line_id',  pl.id,
           'expected_sku_id',  COALESCE(c.sku_id, d.sku_id)
         )
    FROM posting_lines pl
    JOIN accounts c ON c.id = pl.credit_account_id
    JOIN accounts d ON d.id = pl.debit_account_id
    LEFT JOIN posting_line_dimensions pld
      ON pld.posting_line_id = pl.id AND pld.dimension_type = 3
   WHERE COALESCE(c.sku_id, d.sku_id) IS NOT NULL
     AND pld.posting_line_id IS NULL;
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 5: B3 dimensions — counterparty coverage.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'dimension_counterparty_missing',
         jsonb_build_object(
           'posting_line_id', pl.id,
           'expected_kind',
              CASE
                WHEN c.kind IN ('ar','ar_unsettled','customer_pool')
                  OR d.kind IN ('ar','ar_unsettled','customer_pool')
                  THEN 'customer'
                ELSE 'vendor'
              END
         )
    FROM posting_lines pl
    JOIN accounts c ON c.id = pl.credit_account_id
    JOIN accounts d ON d.id = pl.debit_account_id
    LEFT JOIN posting_line_dimensions pld
      ON pld.posting_line_id = pl.id AND pld.dimension_type IN (1, 2)
   WHERE COALESCE(pl.counterparty_id, c.counterparty_id, d.counterparty_id) IS NOT NULL
     AND (c.kind IN ('ar','ar_unsettled','customer_pool',
                     'ap','ap_unsettled','vendor_pool','accrued_disposal_liability')
       OR d.kind IN ('ar','ar_unsettled','customer_pool',
                     'ap','ap_unsettled','vendor_pool','accrued_disposal_liability'))
     AND pld.posting_line_id IS NULL;
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 6: C inventory extension count match.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'inventory_extension_missing',
         jsonb_build_object(
           'posting_line_id', pl.id,
           'reason',          pl.reason::TEXT,
           'qty',             pl.qty
         )
    FROM posting_lines pl
    JOIN accounts c ON c.id = pl.credit_account_id
    JOIN accounts d ON d.id = pl.debit_account_id
    LEFT JOIN posting_line_inventory pli ON pli.posting_line_id = pl.id
   WHERE pl.qty IS NOT NULL
     AND COALESCE(c.sku_id, d.sku_id) IS NOT NULL
     AND pli.posting_line_id IS NULL;
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 7: D subledger ↔ GL divergence per (product, location, period).
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  WITH gl_inv AS (
    SELECT
      d.sku_id        AS product_id,
      d.location_id   AS location_id,
      pl.period_id    AS period_id,
      pl.amount::NUMERIC AS gl_net
      FROM posting_lines pl
      JOIN accounts d ON d.id = pl.debit_account_id
     WHERE d.kind::TEXT LIKE 'inv_value_%'
       AND d.sku_id IS NOT NULL
       AND d.location_id IS NOT NULL
       AND pl.qty IS NOT NULL
    UNION ALL
    SELECT
      c.sku_id,
      c.location_id,
      pl.period_id,
      -pl.amount::NUMERIC
      FROM posting_lines pl
      JOIN accounts c ON c.id = pl.credit_account_id
     WHERE c.kind::TEXT LIKE 'inv_value_%'
       AND c.sku_id IS NOT NULL
       AND c.location_id IS NOT NULL
       AND pl.qty IS NOT NULL
  ),
  gl AS (
    SELECT product_id, location_id, period_id,
           SUM(gl_net) AS gl_net
      FROM gl_inv
     GROUP BY 1, 2, 3
  ),
  sub AS (
    SELECT
      im.product_id,
      im.location_id,
      p.id AS period_id,
      SUM(im.quantity * im.actual_unit_cost)::NUMERIC AS sub_net
      FROM inventory_movements im
      JOIN periods p
        ON p.opens_at  <= im.movement_date
       AND p.closes_at >= im.movement_date
     GROUP BY 1, 2, 3
  )
  SELECT 'subledger_gl_divergence',
         jsonb_build_object(
           'product_id',  COALESCE(g.product_id, s.product_id),
           'location_id', COALESCE(g.location_id, s.location_id),
           'period_id',   COALESCE(g.period_id, s.period_id),
           'gl_net',      COALESCE(g.gl_net, 0)::TEXT,
           'sub_net',     COALESCE(s.sub_net, 0)::TEXT,
           'diff',        (COALESCE(g.gl_net, 0) - COALESCE(s.sub_net, 0))::TEXT
         )
    FROM gl g
    FULL OUTER JOIN sub s
      ON g.product_id  = s.product_id
     AND g.location_id = s.location_id
     AND g.period_id   = s.period_id
   WHERE ABS(COALESCE(g.gl_net, 0) - COALESCE(s.sub_net, 0)) > 1;
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 8: FIFO layer residual ↔ on-hand qty.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  WITH layers AS (
    SELECT cl.product_id,
           cl.location_id,
           SUM(cl.original_quantity) AS layer_total
      FROM cost_layers cl
      JOIN skus s ON s.id = cl.product_id
     WHERE s.cost_method = 'fifo'
     GROUP BY 1, 2
  ),
  depletions AS (
    SELECT cl.product_id,
           cl.location_id,
           COALESCE(SUM(d.depleted_quantity), 0) AS depleted_total
      FROM cost_layers cl
      JOIN skus s ON s.id = cl.product_id
      LEFT JOIN cost_layer_depletions d
        ON d.layer_id = cl.layer_id
       AND d.layer_receipt_date = cl.receipt_date
     WHERE s.cost_method = 'fifo'
     GROUP BY 1, 2
  ),
  layer_residual AS (
    SELECT l.product_id,
           l.location_id,
           l.layer_total - d.depleted_total AS residual
      FROM layers l
      JOIN depletions d
        ON d.product_id  = l.product_id
       AND d.location_id = l.location_id
  ),
  on_hand AS (
    SELECT a.sku_id      AS product_id,
           a.location_id AS location_id,
           (a.debits_total - a.credits_total)::NUMERIC AS qty
      FROM accounts a
      JOIN skus s ON s.id = a.sku_id
     WHERE a.kind = 'stock_available'
       AND s.cost_method = 'fifo'
       AND NOT a.is_closed
  )
  SELECT 'fifo_layer_residual_mismatch',
         jsonb_build_object(
           'product_id',     COALESCE(lr.product_id, oh.product_id),
           'location_id',    COALESCE(lr.location_id, oh.location_id),
           'layer_residual', COALESCE(lr.residual, 0)::TEXT,
           'on_hand',        COALESCE(oh.qty, 0)::TEXT,
           'diff',           (COALESCE(lr.residual, 0) - COALESCE(oh.qty, 0))::TEXT
         )
    FROM layer_residual lr
    FULL OUTER JOIN on_hand oh
      ON oh.product_id  = lr.product_id
     AND oh.location_id = lr.location_id
   WHERE COALESCE(lr.residual, 0) <> COALESCE(oh.qty, 0);
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 9: lot residual ↔ on-hand qty (lot_fifo SKUs).
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  WITH lot_aggr AS (
    SELECT il.product_id,
           il.location_id,
           SUM(il.original_quantity) AS lot_total
      FROM inventory_lots il
      JOIN skus s ON s.id = il.product_id
     WHERE s.cost_method = 'lot_fifo'
     GROUP BY 1, 2
  ),
  event_aggr AS (
    SELECT il.product_id,
           il.location_id,
           COALESCE(SUM(ev.quantity_change), 0) AS event_total
      FROM inventory_lots il
      JOIN skus s ON s.id = il.product_id
      LEFT JOIN inventory_lot_events ev
        ON ev.lot_id           = il.lot_id
       AND ev.lot_receipt_date = il.receipt_date
     WHERE s.cost_method = 'lot_fifo'
     GROUP BY 1, 2
  ),
  lot_residual AS (
    SELECT l.product_id,
           l.location_id,
           l.lot_total + e.event_total AS residual
      FROM lot_aggr l
      JOIN event_aggr e
        ON e.product_id  = l.product_id
       AND e.location_id = l.location_id
  ),
  on_hand AS (
    SELECT a.sku_id      AS product_id,
           a.location_id AS location_id,
           SUM(a.debits_total - a.credits_total)::NUMERIC AS qty
      FROM accounts a
      JOIN skus s ON s.id = a.sku_id
     WHERE a.kind = 'stock_available'
       AND s.cost_method = 'lot_fifo'
       AND NOT a.is_closed
     GROUP BY 1, 2
  )
  SELECT 'lot_residual_mismatch',
         jsonb_build_object(
           'product_id',   COALESCE(lr.product_id, oh.product_id),
           'location_id',  COALESCE(lr.location_id, oh.location_id),
           'lot_residual', COALESCE(lr.residual, 0)::TEXT,
           'on_hand',      COALESCE(oh.qty, 0)::TEXT,
           'diff',         (COALESCE(lr.residual, 0) - COALESCE(oh.qty, 0))::TEXT
         )
    FROM lot_residual lr
    FULL OUTER JOIN on_hand oh
      ON oh.product_id  = lr.product_id
     AND oh.location_id = lr.location_id
   WHERE COALESCE(lr.residual, 0) <> COALESCE(oh.qty, 0);
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 10: per-lot negative residual sentinel (lot_fifo SKUs).
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'lot_negative_residual',
         jsonb_build_object(
           'lot_id',            il.lot_id,
           'lot_receipt_date',  il.receipt_date::TEXT,
           'product_id',        il.product_id,
           'location_id',       il.location_id,
           'lot_code',          il.lot_code,
           'original_quantity', il.original_quantity::TEXT,
           'event_total',       COALESCE(ev_sum.total, 0)::TEXT,
           'residual',          (il.original_quantity + COALESCE(ev_sum.total, 0))::TEXT
         )
    FROM inventory_lots il
    JOIN skus s ON s.id = il.product_id
    LEFT JOIN (
      SELECT lot_id, lot_receipt_date,
             SUM(quantity_change) AS total
        FROM inventory_lot_events
       GROUP BY lot_id, lot_receipt_date
    ) ev_sum
      ON ev_sum.lot_id           = il.lot_id
     AND ev_sum.lot_receipt_date = il.receipt_date
   WHERE s.cost_method = 'lot_fifo'
     AND (il.original_quantity + COALESCE(ev_sum.total, 0)) < 0;
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 11a: forward — orphan rm_issue_to_wo on lot_fifo component
  -- lacking wo_lot_consumption coverage.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'wo_lot_consumption_orphan_forward',
         jsonb_build_object(
           'posting_line_id',  pl.id,
           'document_id',      pl.document_id,
           'component_sku_id', a_c.sku_id
         )
    FROM posting_lines pl
    JOIN accounts a_d ON a_d.id = pl.debit_account_id
    JOIN accounts a_c ON a_c.id = pl.credit_account_id
    JOIN skus       s ON s.id   = a_c.sku_id
   WHERE pl.reason = 'rm_issue_to_wo'
     AND a_d.kind  = 'inv_value_wip'
     AND a_c.kind  = 'inv_value_raw'
     AND s.cost_method = 'lot_fifo'
     AND NOT EXISTS (
       SELECT 1 FROM wo_lot_consumption wlc
        WHERE wlc.posting_line_id = pl.id
     );
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 11b: inverse — wo_lot_consumption row lacks matching
  -- inventory_lot_events type=1 issue with same posting_line_id +
  -- lot identity + qty.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'wo_lot_consumption_orphan_inverse',
         jsonb_build_object(
           'wo_lot_consumption_id', wlc.id,
           'posting_line_id',       wlc.posting_line_id,
           'lot_id',                wlc.lot_id,
           'lot_receipt_date',      wlc.lot_receipt_date::TEXT,
           'qty',                   wlc.qty::TEXT
         )
    FROM wo_lot_consumption wlc
   WHERE NOT EXISTS (
     SELECT 1 FROM inventory_lot_events ev
      WHERE ev.posting_line_id  = wlc.posting_line_id
        AND ev.lot_id           = wlc.lot_id
        AND ev.lot_receipt_date = wlc.lot_receipt_date
        AND ev.event_type       = 1
        AND ev.quantity_change  = -wlc.qty
   );
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  RETURN v_total;
END;
$$;
