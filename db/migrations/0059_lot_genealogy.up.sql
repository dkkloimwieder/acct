-- ============================================================
-- acct-3j3z — lot genealogy (parent-child lineage).
--
-- WHAT:
--   New table lot_genealogy persists parent-child lot lineage
--   when a lot_fifo WO produces FG lots from raw-component lots.
--   Each post_wo_complete that drains lot_fifo WIP -> FG creates
--   a new inventory_lots row at the FG side via apply_event's
--   E2 receipt block; this migration ties THAT FG lot back to
--   the component lots consumed earlier in the WO via the
--   wo_lot_consumption persistence built in acct-vohc.
--
-- WHY:
--   Pre-3j3z, FG-lot ↔ raw-lot lineage was reconstructible via a
--   multi-table join (inventory_lots -> posting_lines on
--   receipt_posting_line_id, then JOIN inventory_lot_events on
--   the same wo_complete document_id, then GROUP BY raw lot).
--   That works for ad-hoc queries but doesn't support the recall
--   workflow ('vendor recalls lot V; what FG lots used it?')
--   without N-level recursive joins. Mainstream-ERP convention
--   (SAP Batch Where-Used / Oracle WMS Genealogy / NetSuite Lot
--   Tracking) maintains an explicit parent-child graph.
--
-- HOW:
--   1. Schema (lot_genealogy table + indexes + append-only trigger
--      reusing block_inventory_lot_modifications).
--   2. Recursive views v_lot_lineage_upstream and
--      v_lot_lineage_downstream walk the graph with cycle guards.
--   3. New helper _wo_write_lot_genealogy looks up the FG lot via
--      receipt_posting_line_id from inventory_lots and INSERTs one
--      genealogy row per consumed parent lot per output.
--   4. post_wo_complete (CREATE OR REPLACE — verbatim copy from
--      mig 0055 with surgical additions): hoists the per-output
--      value-leg idempotency_key into v_output_value_idem and
--      tracks (output_sku_id, fg_location_id, allocation_pct,
--      value_idem_key) tuples in v_output_recs. After the PERFORM
--      post_posting_lines completes, calls _wo_write_lot_genealogy
--      with the recs and the partial qty_share.
--   5. run_daily_reconciliation (CREATE OR REPLACE — verbatim
--      copy from mig 0058 + check #12 for genealogy qty
--      overshoot, loose <= bound to admit yield-loss / scrap).
--
-- DESIGN CALLS (from saved plan-3j3z-2026-05-10):
--   Q1 — multi-output allocation: divide qty_consumed by
--        allocation_pct (Option (b)). Matches how WIP value
--        drains across outputs at line 556 of post_wo_complete.
--   Q2 — partial wo_complete: attribute consumption proportionally
--        to the partial's qty share (Option (b), qty/qty_target).
--        Each partial creates ONE FG lot per output; the same
--        wo_lot_consumption rows are visible to every partial,
--        so rather than watermark we scale by the partial's share.
--   Q3 — phantom FG SKUs naturally skip: no inventory_lots row
--        is created for is_phantom outputs (apply_event's E2
--        receipt block is gated on lot_fifo+lot_code in the JSONB
--        which only fires for tracked outputs); the receipt-line
--        lookup returns NULL → CONTINUE.
--   Q4 — yield-loss: NOT recorded as a separate column at MVP.
--        qty_consumed reflects the actual consumed parent qty
--        scaled by allocation × partial-share; the FG-output qty
--        differs implicitly. Recon check #12 (loose <= bound)
--        admits the gap.
--   Q5 — scrap/disposal events without FG output: contribute to
--        inventory_lot_events.quantity_change but not to
--        lot_genealogy. The loose <= bound on check #12 covers
--        this case (genealogy_total < events_total when scrap).
--
-- CRITICAL: aggregate wo_lot_consumption by (lot_id,
-- lot_receipt_date) BEFORE iterating outputs. Multi-routing-op
-- consumption of the same parent_lot can produce N rows in
-- wo_lot_consumption; without aggregation the same (parent, child,
-- wo_id) tuple would attempt N inserts (UNIQUE constraint blocks;
-- ON CONFLICT DO NOTHING swallows but masks the rounding error).
--
-- NOT IN SCOPE (file as 3j3z-followup if needed):
--   - Per-genealogy-row yield_loss attribution column.
--   - Per-event lot_genealogy (mid-routing emission); only
--     post_wo_complete writes today.
--   - Cross-WO transitive cost rollup (the recursive views are
--     read-only; cost flowing through phantom intermediate WOs
--     traverses the graph but doesn't aggregate cost).
-- ============================================================

-- ---------- 1. lot_genealogy table ----------

CREATE TABLE lot_genealogy (
  id                    BIGSERIAL PRIMARY KEY,
  parent_lot_id         BIGINT       NOT NULL,
  parent_receipt_date   DATE         NOT NULL,
  child_lot_id          BIGINT       NOT NULL,
  child_receipt_date    DATE         NOT NULL,
  qty_consumed          NUMERIC(19, 6) NOT NULL CHECK (qty_consumed > 0),
  wo_id                 UUID         NOT NULL REFERENCES work_orders(id),
  posting_line_id       BIGINT       NOT NULL REFERENCES posting_lines(id),
  recorded_at           TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
  FOREIGN KEY (parent_lot_id, parent_receipt_date)
    REFERENCES inventory_lots (lot_id, receipt_date),
  FOREIGN KEY (child_lot_id, child_receipt_date)
    REFERENCES inventory_lots (lot_id, receipt_date),
  UNIQUE (parent_lot_id, child_lot_id, wo_id)
);

CREATE INDEX lot_genealogy_parent ON lot_genealogy (parent_lot_id);
CREATE INDEX lot_genealogy_child  ON lot_genealogy (child_lot_id);
CREATE INDEX lot_genealogy_wo     ON lot_genealogy (wo_id);
CREATE INDEX lot_genealogy_pl     ON lot_genealogy (posting_line_id);

-- ---------- 2. Append-only trigger ----------

CREATE TRIGGER trg_lot_genealogy_append_only
  BEFORE UPDATE OR DELETE ON lot_genealogy
  FOR EACH ROW EXECUTE FUNCTION block_inventory_lot_modifications();

-- ---------- 3. Recursive lineage views ----------

-- Upstream: from a FG lot, walk backward to find all ancestor
-- raw-component lots (transitively, through multi-WO chains
-- where an intermediate FG lot is consumed by a downstream WO).
CREATE VIEW v_lot_lineage_upstream AS
WITH RECURSIVE up AS (
  SELECT child_lot_id, child_receipt_date,
         parent_lot_id, parent_receipt_date,
         qty_consumed, wo_id,
         1 AS depth,
         ARRAY[child_lot_id] AS path
    FROM lot_genealogy
  UNION ALL
  SELECT u.child_lot_id, u.child_receipt_date,
         g.parent_lot_id, g.parent_receipt_date,
         g.qty_consumed, g.wo_id,
         u.depth + 1,
         u.path || g.child_lot_id
    FROM up u
    JOIN lot_genealogy g
      ON g.child_lot_id       = u.parent_lot_id
     AND g.child_receipt_date = u.parent_receipt_date
   WHERE NOT g.child_lot_id = ANY (u.path)
     AND u.depth < 32
)
SELECT child_lot_id, child_receipt_date,
       parent_lot_id AS ancestor_lot_id,
       parent_receipt_date AS ancestor_receipt_date,
       qty_consumed, wo_id, depth, path
  FROM up;

-- Downstream: from a raw-component lot, walk forward to find all
-- descendant FG lots (transitively).
CREATE VIEW v_lot_lineage_downstream AS
WITH RECURSIVE down AS (
  SELECT parent_lot_id, parent_receipt_date,
         child_lot_id, child_receipt_date,
         qty_consumed, wo_id,
         1 AS depth,
         ARRAY[parent_lot_id] AS path
    FROM lot_genealogy
  UNION ALL
  SELECT d.parent_lot_id, d.parent_receipt_date,
         g.child_lot_id, g.child_receipt_date,
         g.qty_consumed, g.wo_id,
         d.depth + 1,
         d.path || g.parent_lot_id
    FROM down d
    JOIN lot_genealogy g
      ON g.parent_lot_id       = d.child_lot_id
     AND g.parent_receipt_date = d.child_receipt_date
   WHERE NOT g.parent_lot_id = ANY (d.path)
     AND d.depth < 32
)
SELECT parent_lot_id AS root_lot_id,
       parent_receipt_date AS root_receipt_date,
       child_lot_id AS descendant_lot_id,
       child_receipt_date AS descendant_receipt_date,
       qty_consumed, wo_id, depth, path
  FROM down;

-- ---------- 4. _wo_write_lot_genealogy helper ----------

-- Iterates p_output_recs (one entry per output with non-NULL
-- lot_code), looks up the FG inventory_lots row by
-- receipt_posting_line_id, and INSERTs one genealogy row per
-- (output × distinct parent_lot in wo_lot_consumption).
--
-- p_qty_share is this partial wo_complete's fraction of total
-- (p_qty / wo.qty_target as NUMERIC). For full-completion WOs
-- this is 1.0; for partial it splits consumption proportionally
-- across the multiple FG lots that emerge.
--
-- qty_consumed = aggregated_parent_qty
--                × (output.allocation_pct / 100)
--                × p_qty_share
--
-- Aggregation collapses multi-routing-op consumption rows of the
-- same parent_lot into one row (otherwise the UNIQUE constraint
-- on (parent_lot_id, child_lot_id, wo_id) would mask later rows
-- via ON CONFLICT DO NOTHING and silently lose the qty).

CREATE OR REPLACE FUNCTION _wo_write_lot_genealogy(
  p_wo_id        UUID,
  p_output_recs  JSONB,
  p_qty_share    NUMERIC
) RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
  v_output       JSONB;
  v_pl_id        BIGINT;
  v_child_lot    BIGINT;
  v_child_rd     DATE;
  v_consumption  RECORD;
  v_alloc_pct    NUMERIC;
  v_qty_consumed NUMERIC;
BEGIN
  IF p_output_recs IS NULL OR jsonb_array_length(p_output_recs) = 0 THEN
    RETURN;
  END IF;

  FOR v_output IN SELECT * FROM jsonb_array_elements(p_output_recs) LOOP
    v_alloc_pct := (v_output->>'allocation_pct')::NUMERIC;

    -- Resolve the wo_complete_v posting_line_id by idempotency_key.
    SELECT id INTO v_pl_id FROM posting_lines
     WHERE idempotency_key = (v_output->>'value_idem_key')::UUID;
    IF v_pl_id IS NULL THEN CONTINUE; END IF;

    -- Resolve the FG inventory_lots row created by apply_event's
    -- E2 receipt block. It stamps receipt_posting_line_id = the
    -- value-leg posting_line.id. Phantom outputs (no lot row)
    -- naturally skip via the IF NULL guard.
    SELECT lot_id, receipt_date INTO v_child_lot, v_child_rd
      FROM inventory_lots
     WHERE receipt_posting_line_id = v_pl_id;
    IF v_child_lot IS NULL THEN CONTINUE; END IF;

    -- Aggregate wo_lot_consumption by parent lot identity to
    -- collapse multi-op consumption of the same lot into one
    -- genealogy row per (parent, child, wo_id) tuple.
    FOR v_consumption IN
      SELECT lot_id, lot_receipt_date, SUM(qty) AS total_qty
        FROM wo_lot_consumption
       WHERE wo_id = p_wo_id
       GROUP BY lot_id, lot_receipt_date
    LOOP
      v_qty_consumed := v_consumption.total_qty
                        * v_alloc_pct
                        / 100.0
                        * p_qty_share;

      IF v_qty_consumed <= 0 THEN CONTINUE; END IF;

      INSERT INTO lot_genealogy (
        parent_lot_id, parent_receipt_date,
        child_lot_id,  child_receipt_date,
        qty_consumed, wo_id, posting_line_id
      ) VALUES (
        v_consumption.lot_id, v_consumption.lot_receipt_date,
        v_child_lot, v_child_rd,
        v_qty_consumed, p_wo_id, v_pl_id
      ) ON CONFLICT (parent_lot_id, child_lot_id, wo_id) DO NOTHING;
    END LOOP;
  END LOOP;
END;
$$;

-- ---------- 5. post_wo_complete CREATE OR REPLACE ----------
-- Verbatim copy from mig 0055 with these surgical additions:
--   * v_output_value_idem and v_output_recs and v_qty_share added
--     to DECLARE.
--   * Per-output value-leg event hoists gen_random_uuid() into
--     v_output_value_idem so the post-PERFORM lookup can find the
--     posting_line.id by idempotency_key.
--   * Tracks v_output_recs for lot_fifo outputs (gated by
--     v_lot_code IS NOT NULL, the same gate that adds 'lot_code'
--     to the event JSON for apply_event's lot-creation path).
--   * After PERFORM post_posting_lines completes, computes
--     v_qty_share := p_qty::NUMERIC / v_wo.qty_target::NUMERIC
--     and PERFORMs _wo_write_lot_genealogy. The helper's
--     IS NULL OR jsonb_array_length=0 guard makes this a no-op
--     for non-lot_fifo parents (where v_output_recs stays empty).

CREATE OR REPLACE FUNCTION post_wo_complete(
  p_wo_id           UUID,
  p_qty             BIGINT,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id    UUID;
  v_event_id       UUID;
  v_wo             work_orders%ROWTYPE;
  v_last_op        INT;
  v_qty_from       BIGINT;
  v_qty_fg         BIGINT;
  v_val_from       BIGINT;
  v_val_fg         BIGINT;
  v_var_close      BIGINT;
  v_will_close     BOOLEAN;
  v_residual       BIGINT;
  v_batch          JSONB := '[]'::JSONB;
  v_event_obj      JSONB;
  v_alloc_sum      NUMERIC;
  v_outputs_n      INT;
  v_output         RECORD;
  v_output_idx     INT;
  v_parent_std     BIGINT;
  v_total_drain    BIGINT;
  v_qty_used       BIGINT := 0;
  v_val_used       BIGINT := 0;
  v_q_share        BIGINT;
  v_v_share        BIGINT;
  v_op_residual    RECORD;
  v_pool_at_last   BIGINT;
  v_prebalance     BIGINT;
  v_cost_method    cost_method;
  v_pool_qty       BIGINT;
  v_unit           BIGINT;
  v_pool_qty_pre   BIGINT;
  v_op_qty_acct    BIGINT;
  v_op_qty         BIGINT;
  v_solo_at_last   BOOLEAN;
  v_lock_first     BIGINT;
  v_lock_second    BIGINT;
  v_bp             wo_by_products%ROWTYPE;
  v_bp_qty_acct    BIGINT;
  v_bp_val_acct    BIGINT;
  v_void_qty       BIGINT;
  v_byproduct_drain BIGINT := 0;
  v_disp_total       BIGINT;
  v_disp_liability   BIGINT;
  v_disp_exp_acct    BIGINT;
  v_disp_exp_kind    account_kind;
  v_disp_share       BIGINT;
  v_disp_used        BIGINT;
  v_disp_output      RECORD;
  v_disp_output_idx  INT;
  v_yield_var_acct   BIGINT;
  v_yield_qty_delta  BIGINT;
  v_yield_amount     BIGINT;
  v_lot_code         TEXT;
  -- acct-3j3z additions:
  v_output_value_idem UUID;
  v_output_recs       JSONB := '[]'::JSONB;
  v_qty_share         NUMERIC;
BEGIN
  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF p_qty IS NULL OR p_qty <= 0 THEN
    RAISE EXCEPTION 'wo_invalid: wo_complete qty must be > 0 (got %)', p_qty
      USING ERRCODE = 'P0026';
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

  IF v_wo.qty_completed + v_wo.qty_scrapped + p_qty > v_wo.qty_target THEN
    RAISE EXCEPTION
      'wo_qty_overflow: WO % completed=% scrapped=% + this=% > target=%',
      p_wo_id, v_wo.qty_completed, v_wo.qty_scrapped, p_qty, v_wo.qty_target
      USING ERRCODE = 'P0027';
  END IF;

  v_will_close :=
    (v_wo.qty_completed + v_wo.qty_scrapped + p_qty) = v_wo.qty_target;

  SELECT MAX(routing_op) INTO v_last_op FROM wo_routings WHERE wo_id = p_wo_id;
  IF v_last_op IS NULL THEN
    RAISE EXCEPTION 'wo_invalid: WO % has no routing operations', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;

  SELECT id INTO v_qty_from FROM accounts
   WHERE kind='stock_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=v_last_op AND NOT is_closed;
  IF v_qty_from IS NULL THEN
    RAISE EXCEPTION 'no open stock_wip account for sku=% op=%',
                    v_wo.parent_sku_id, v_last_op USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_val_from FROM accounts
   WHERE kind='inv_value_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=v_last_op AND currency=v_wo.currency AND NOT is_closed;
  IF v_val_from IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_wip account for sku=% op=% ccy=%',
                    v_wo.parent_sku_id, v_last_op, v_wo.currency
      USING ERRCODE = 'P0010';
  END IF;

  SELECT COUNT(*), COALESCE(SUM(allocation_pct), 0)
    INTO v_outputs_n, v_alloc_sum
    FROM wo_outputs WHERE wo_id = p_wo_id;
  IF v_outputs_n = 0 THEN
    RAISE EXCEPTION 'wo_invalid: WO % has no wo_outputs rows', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;
  IF v_alloc_sum <> 100 THEN
    RAISE EXCEPTION
      'output_allocation_invalid: wo_outputs(wo=%) allocation_pct sums to % (expected 100)',
      p_wo_id, v_alloc_sum USING ERRCODE = 'P0033';
  END IF;

  v_lock_first  := LEAST(v_qty_from, v_val_from);
  v_lock_second := GREATEST(v_qty_from, v_val_from);
  PERFORM 1 FROM accounts WHERE id = v_lock_first  FOR UPDATE;
  PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;

  SELECT (debits_total - credits_total) INTO v_pool_qty_pre
    FROM accounts WHERE id = v_qty_from;
  v_solo_at_last := COALESCE(v_pool_qty_pre, 0) = p_qty;

  SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_wo.parent_sku_id;

  IF v_cost_method = 'standard' THEN
    v_parent_std  := _resolve_standard_cost_at(v_wo.parent_sku_id, p_business_date);
    v_total_drain := p_qty * v_parent_std;

  ELSIF v_cost_method IN ('wac_perpetual', 'wac_periodic',
                          'wac_retroactive', 'fifo', 'lot_fifo') THEN
    SELECT (debits_total - credits_total) INTO v_pool_at_last
      FROM accounts WHERE id = v_val_from;
    SELECT (debits_total - credits_total) INTO v_pool_qty
      FROM accounts WHERE id = v_qty_from;

    IF v_pool_qty IS NULL OR v_pool_qty <= 0 THEN
      v_unit := 0;
    ELSE
      v_unit := GREATEST(COALESCE(v_pool_at_last, 0), 0) / v_pool_qty;
    END IF;
    v_total_drain := p_qty * v_unit;

  ELSE
    RAISE EXCEPTION
      'wo_invalid: parent_sku % has cost_method=% which post_wo_complete does not handle',
      v_wo.parent_sku_id, v_cost_method USING ERRCODE = 'P0026';
  END IF;

  INSERT INTO wo_events (
    wo_id, event_kind, routing_op_from, qty,
    business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_wo_id, 'wo_complete', v_last_op, p_qty,
    p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_event_id;
  IF v_event_id IS NULL THEN RETURN p_wo_id; END IF;

  IF v_will_close AND v_solo_at_last THEN
    IF v_cost_method = 'standard' THEN
      SELECT (debits_total - credits_total) INTO v_pool_at_last
        FROM accounts WHERE id = v_val_from;
    END IF;
    v_prebalance := v_total_drain - COALESCE(v_pool_at_last, 0);

    IF v_prebalance <> 0 THEN
      SELECT id INTO v_var_close FROM accounts
       WHERE kind='variance_wo_close' AND ledger_kind='value'
         AND currency=v_wo.currency AND NOT is_closed;
      IF v_var_close IS NULL THEN
        RAISE EXCEPTION 'no open variance_wo_close account for ccy=%',
                        v_wo.currency USING ERRCODE = 'P0010';
      END IF;

      IF v_prebalance > 0 THEN
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'wo_close_v',
          'document_kind',     'wo_complete',
          'document_id',       v_event_id,
          'debit_account_id',  v_val_from,
          'credit_account_id', v_var_close,
          'amount',            v_prebalance,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        ));
      ELSE
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'wo_close_v',
          'document_kind',     'wo_complete',
          'document_id',       v_event_id,
          'debit_account_id',  v_var_close,
          'credit_account_id', v_val_from,
          'amount',            -v_prebalance,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        ));
      END IF;
    END IF;
  END IF;

  -- By-products pre-pass on closing call.
  IF v_will_close AND v_cost_method IN
       ('standard', 'wac_perpetual', 'wac_periodic',
        'wac_retroactive', 'lot_fifo') THEN
    SELECT id INTO v_void_qty FROM accounts
     WHERE kind='creation_void' AND ledger_kind='qty' AND NOT is_closed;

    FOR v_bp IN
      SELECT * FROM wo_by_products WHERE wo_id = p_wo_id
       ORDER BY by_product_no
    LOOP
      v_yield_qty_delta := v_bp.actual_qty - v_bp.planned_qty;

      IF v_bp.actual_qty > 0 THEN
        IF v_void_qty IS NULL THEN
          RAISE EXCEPTION 'no creation_void(qty) account configured'
            USING ERRCODE = 'P0010';
        END IF;
        SELECT id INTO v_bp_qty_acct FROM accounts
         WHERE kind='stock_available' AND sku_id=v_bp.output_sku_id
           AND location_id=v_bp.fg_location_id AND NOT is_closed;
        IF v_bp_qty_acct IS NULL THEN
          RAISE EXCEPTION
            'no open stock_available account for by-product sku=% loc=%',
            v_bp.output_sku_id, v_bp.fg_location_id USING ERRCODE = 'P0010';
        END IF;

        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'wo_complete',
          'document_kind',     'wo_complete',
          'document_id',       v_event_id,
          'debit_account_id',  v_bp_qty_acct,
          'credit_account_id', v_void_qty,
          'amount',            v_bp.actual_qty,
          'qty',               v_bp.actual_qty,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        ));
      END IF;

      IF v_bp.treatment = 'nrv_credit' THEN
        SELECT id INTO v_bp_val_acct FROM accounts
         WHERE kind='inv_value_fg' AND sku_id=v_bp.output_sku_id
           AND location_id=v_bp.fg_location_id AND currency=v_wo.currency
           AND NOT is_closed;
        IF v_bp_val_acct IS NULL THEN
          RAISE EXCEPTION
            'no open inv_value_fg account for by-product sku=% loc=% ccy=%',
            v_bp.output_sku_id, v_bp.fg_location_id, v_wo.currency
            USING ERRCODE = 'P0010';
        END IF;

        v_byproduct_drain := v_byproduct_drain + v_bp.unit_value * v_bp.planned_qty;

        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'wo_byproduct_credit',
          'document_kind',     'wo_complete',
          'document_id',       v_event_id,
          'debit_account_id',  v_bp_val_acct,
          'credit_account_id', v_val_from,
          'amount',            v_bp.unit_value * v_bp.planned_qty,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        ));

        IF v_yield_qty_delta <> 0 THEN
          SELECT id INTO v_yield_var_acct FROM accounts
           WHERE kind='variance_yield_byproduct' AND ledger_kind='value'
             AND currency=v_wo.currency AND NOT is_closed;
          IF v_yield_var_acct IS NULL THEN
            RAISE EXCEPTION
              'no open variance_yield_byproduct account for ccy=%',
              v_wo.currency USING ERRCODE = 'P0010';
          END IF;

          v_yield_amount := v_yield_qty_delta * v_bp.unit_value;
          IF v_yield_amount > 0 THEN
            v_batch := v_batch || jsonb_build_array(jsonb_build_object(
              'reason',            'wo_byproduct_credit',
              'document_kind',     'wo_complete',
              'document_id',       v_event_id,
              'debit_account_id',  v_bp_val_acct,
              'credit_account_id', v_yield_var_acct,
              'amount',            v_yield_amount,
              'qty',               v_yield_qty_delta,
              'business_date',     p_business_date,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         p_posted_by
            ));
          ELSE
            v_batch := v_batch || jsonb_build_array(jsonb_build_object(
              'reason',            'wo_byproduct_credit',
              'document_kind',     'wo_complete',
              'document_id',       v_event_id,
              'debit_account_id',  v_yield_var_acct,
              'credit_account_id', v_bp_val_acct,
              'amount',            -v_yield_amount,
              'qty',               -v_yield_qty_delta,
              'business_date',     p_business_date,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         p_posted_by
            ));
          END IF;
        END IF;

      ELSIF v_bp.treatment = 'disposal_cost' THEN
        IF v_cost_method = 'lot_fifo'
           AND v_bp.disposal_basis = 'inventoriable' THEN
          RAISE EXCEPTION
            'cost_method_not_implemented: lot_fifo parent + '
            'disposal_cost(inventoriable) at wo_complete (wo=%, '
            'by_product_no=%); period basis is supported, '
            'inventoriable basis requires per-lot revaluation '
            'infrastructure not yet built',
            p_wo_id, v_bp.by_product_no USING ERRCODE = 'P0006';
        END IF;

        SELECT id INTO v_disp_liability FROM accounts
         WHERE kind = 'accrued_disposal_liability'
           AND counterparty_id = v_bp.disposal_vendor_id
           AND currency = v_wo.currency
           AND NOT is_closed;
        IF v_disp_liability IS NULL THEN
          RAISE EXCEPTION
            'no open accrued_disposal_liability account for vendor=% ccy=%',
            v_bp.disposal_vendor_id, v_wo.currency
            USING ERRCODE = 'P0010';
        END IF;

        v_disp_total := ABS(v_bp.unit_value) * v_bp.planned_qty;

        IF v_bp.disposal_basis = 'period' THEN
          v_disp_exp_kind := COALESCE(
            v_bp.disposal_expense_account_kind,
            'disposal_expense'::account_kind
          );
          SELECT id INTO v_disp_exp_acct FROM accounts
           WHERE kind = v_disp_exp_kind
             AND ledger_kind = 'value'
             AND currency = v_wo.currency
             AND NOT is_closed;
          IF v_disp_exp_acct IS NULL THEN
            RAISE EXCEPTION
              'no open % account for ccy=%',
              v_disp_exp_kind, v_wo.currency USING ERRCODE = 'P0010';
          END IF;

          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'wo_byproduct_credit',
            'document_kind',     'wo_complete',
            'document_id',       v_event_id,
            'debit_account_id',  v_disp_exp_acct,
            'credit_account_id', v_disp_liability,
            'amount',            v_disp_total,
            'qty',               v_bp.planned_qty,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'posted_by',         p_posted_by
          ));

        ELSIF v_bp.disposal_basis = 'inventoriable' THEN
          v_disp_used := 0;
          v_disp_output_idx := 0;
          FOR v_disp_output IN
            SELECT * FROM wo_outputs WHERE wo_id = p_wo_id
             ORDER BY output_no
          LOOP
            v_disp_output_idx := v_disp_output_idx + 1;
            IF v_disp_output_idx = v_outputs_n THEN
              v_disp_share := v_disp_total - v_disp_used;
            ELSE
              v_disp_share := (v_disp_total * v_disp_output.allocation_pct)::BIGINT / 100;
            END IF;
            v_disp_used := v_disp_used + v_disp_share;

            IF v_disp_share = 0 THEN
              CONTINUE;
            END IF;

            SELECT id INTO v_val_fg FROM accounts
             WHERE kind = 'inv_value_fg'
               AND sku_id = v_disp_output.output_sku_id
               AND location_id = v_disp_output.fg_location_id
               AND currency = v_wo.currency
               AND NOT is_closed;
            IF v_val_fg IS NULL THEN
              RAISE EXCEPTION
                'no open inv_value_fg account for sku=% loc=% ccy=%',
                v_disp_output.output_sku_id, v_disp_output.fg_location_id, v_wo.currency
                USING ERRCODE = 'P0010';
            END IF;

            v_batch := v_batch || jsonb_build_array(jsonb_build_object(
              'reason',            'wo_byproduct_credit',
              'document_kind',     'wo_complete',
              'document_id',       v_event_id,
              'debit_account_id',  v_val_fg,
              'credit_account_id', v_disp_liability,
              'amount',            v_disp_share,
              'business_date',     p_business_date,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         p_posted_by
            ));
          END LOOP;
        END IF;

        IF v_yield_qty_delta <> 0 THEN
          SELECT id INTO v_yield_var_acct FROM accounts
           WHERE kind='variance_yield_byproduct' AND ledger_kind='value'
             AND currency=v_wo.currency AND NOT is_closed;
          IF v_yield_var_acct IS NULL THEN
            RAISE EXCEPTION
              'no open variance_yield_byproduct account for ccy=%',
              v_wo.currency USING ERRCODE = 'P0010';
          END IF;

          v_yield_amount := v_yield_qty_delta * ABS(v_bp.unit_value);
          IF v_yield_amount > 0 THEN
            v_batch := v_batch || jsonb_build_array(jsonb_build_object(
              'reason',            'wo_byproduct_credit',
              'document_kind',     'wo_complete',
              'document_id',       v_event_id,
              'debit_account_id',  v_yield_var_acct,
              'credit_account_id', v_disp_liability,
              'amount',            v_yield_amount,
              'qty',               v_yield_qty_delta,
              'business_date',     p_business_date,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         p_posted_by
            ));
          ELSE
            v_batch := v_batch || jsonb_build_array(jsonb_build_object(
              'reason',            'wo_byproduct_credit',
              'document_kind',     'wo_complete',
              'document_id',       v_event_id,
              'debit_account_id',  v_disp_liability,
              'credit_account_id', v_yield_var_acct,
              'amount',            -v_yield_amount,
              'qty',               -v_yield_qty_delta,
              'business_date',     p_business_date,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         p_posted_by
            ));
          END IF;
        END IF;
      END IF;
    END LOOP;

    v_total_drain := v_total_drain - v_byproduct_drain;
  END IF;

  v_output_idx := 0;
  FOR v_output IN
    SELECT * FROM wo_outputs WHERE wo_id = p_wo_id
     ORDER BY output_no
  LOOP
    v_output_idx := v_output_idx + 1;
    IF v_output_idx = v_outputs_n THEN
      v_q_share := p_qty - v_qty_used;
    ELSE
      v_q_share := (v_output.qty * p_qty) / v_wo.qty_target;
    END IF;
    v_qty_used := v_qty_used + v_q_share;

    IF v_output_idx = v_outputs_n THEN
      v_v_share := v_total_drain - v_val_used;
    ELSE
      v_v_share := (v_total_drain * v_output.allocation_pct)::BIGINT / 100;
    END IF;
    v_val_used := v_val_used + v_v_share;

    SELECT id INTO v_qty_fg FROM accounts
     WHERE kind='stock_available' AND sku_id=v_output.output_sku_id
       AND location_id=v_output.fg_location_id AND NOT is_closed;
    IF v_qty_fg IS NULL THEN
      RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                      v_output.output_sku_id, v_output.fg_location_id
        USING ERRCODE = 'P0010';
    END IF;
    SELECT id INTO v_val_fg FROM accounts
     WHERE kind='inv_value_fg' AND sku_id=v_output.output_sku_id
       AND location_id=v_output.fg_location_id AND currency=v_wo.currency
       AND NOT is_closed;
    IF v_val_fg IS NULL THEN
      RAISE EXCEPTION 'no open inv_value_fg account for sku=% loc=% ccy=%',
                      v_output.output_sku_id, v_output.fg_location_id, v_wo.currency
        USING ERRCODE = 'P0010';
    END IF;

    IF v_cost_method = 'lot_fifo' THEN
      v_lot_code := v_output.lot_code;
      IF v_lot_code IS NULL OR length(v_lot_code) = 0 THEN
        v_lot_code := 'WO-' || substr(v_event_id::TEXT, 1, 8) || '-' || v_output.output_no;
      END IF;
    ELSE
      v_lot_code := NULL;
    END IF;

    IF v_q_share > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'wo_complete',
        'document_kind',     'wo_complete',
        'document_id',       v_event_id,
        'debit_account_id',  v_qty_fg,
        'credit_account_id', v_qty_from,
        'amount',            v_q_share,
        'qty',               v_q_share,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      ));
    END IF;

    IF v_v_share > 0 THEN
      -- acct-3j3z: hoist the value-leg idempotency_key so the
      -- post-PERFORM helper can resolve posting_line.id.
      v_output_value_idem := gen_random_uuid();
      v_event_obj := jsonb_build_object(
        'reason',            'wo_complete_v',
        'document_kind',     'wo_complete',
        'document_id',       v_event_id,
        'debit_account_id',  v_val_fg,
        'credit_account_id', v_val_from,
        'amount',            v_v_share,
        'qty',               v_q_share,
        'business_date',     p_business_date,
        'idempotency_key',   v_output_value_idem,
        'posted_by',         p_posted_by
      );
      IF v_lot_code IS NOT NULL THEN
        v_event_obj := v_event_obj || jsonb_build_object('lot_code', v_lot_code);
        -- acct-3j3z: capture per-output rec for genealogy writeback.
        -- Gated by v_lot_code IS NOT NULL — same gate that adds
        -- 'lot_code' to the event JSON for apply_event's lot
        -- creation, so v_output_recs only populates for lot_fifo
        -- outputs that actually create FG lots.
        v_output_recs := v_output_recs || jsonb_build_array(jsonb_build_object(
          'output_sku_id',    v_output.output_sku_id,
          'fg_location_id',   v_output.fg_location_id,
          'allocation_pct',   v_output.allocation_pct,
          'value_idem_key',   v_output_value_idem
        ));
      END IF;
      v_batch := v_batch || jsonb_build_array(v_event_obj);
    END IF;
  END LOOP;

  PERFORM post_posting_lines(v_batch, FALSE);

  -- acct-3j3z: write lot_genealogy for lot_fifo parents. The
  -- helper's IS NULL OR jsonb_array_length=0 guard makes this a
  -- no-op for non-lot_fifo parents (v_output_recs stays empty).
  -- v_qty_share lets multi-partial wo_complete attribute
  -- consumption proportionally across the FG lots that emerge.
  v_qty_share := p_qty::NUMERIC / v_wo.qty_target::NUMERIC;
  PERFORM _wo_write_lot_genealogy(p_wo_id, v_output_recs, v_qty_share);

  UPDATE work_orders SET qty_completed = qty_completed + p_qty
   WHERE id = p_wo_id;

  IF v_will_close THEN
    FOR v_op_residual IN
      SELECT a.id AS acct_id,
             a.routing_op AS rop,
             (a.debits_total - a.credits_total) AS balance
        FROM accounts a
       WHERE a.kind = 'inv_value_wip'
         AND a.sku_id = v_wo.parent_sku_id
         AND a.currency = v_wo.currency
         AND a.routing_op IN (
           SELECT routing_op FROM wo_routings WHERE wo_id = p_wo_id
         )
         AND NOT a.is_closed
       ORDER BY a.routing_op
    LOOP
      SELECT id INTO v_op_qty_acct FROM accounts
       WHERE kind = 'stock_wip' AND sku_id = v_wo.parent_sku_id
         AND routing_op = v_op_residual.rop AND NOT is_closed;
      IF v_op_qty_acct IS NULL THEN
        v_op_qty := 0;
      ELSE
        v_lock_first  := LEAST(v_op_qty_acct, v_op_residual.acct_id);
        v_lock_second := GREATEST(v_op_qty_acct, v_op_residual.acct_id);
        PERFORM 1 FROM accounts WHERE id = v_lock_first  FOR UPDATE;
        PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;

        SELECT (debits_total - credits_total) INTO v_op_qty
          FROM accounts WHERE id = v_op_qty_acct;
      END IF;
      IF COALESCE(v_op_qty, 0) <> 0 THEN
        CONTINUE;
      END IF;

      SELECT (debits_total - credits_total) INTO v_residual
        FROM accounts WHERE id = v_op_residual.acct_id;
      IF v_residual = 0 OR v_residual IS NULL THEN CONTINUE; END IF;

      SELECT id INTO v_var_close FROM accounts
       WHERE kind='variance_wo_close' AND ledger_kind='value'
         AND currency=v_wo.currency AND NOT is_closed;
      IF v_var_close IS NULL THEN
        RAISE EXCEPTION 'no open variance_wo_close account for ccy=%',
                        v_wo.currency USING ERRCODE = 'P0010';
      END IF;

      IF v_residual > 0 THEN
        PERFORM post_posting_lines(jsonb_build_array(jsonb_build_object(
          'reason',            'wo_close_v',
          'document_kind',     'wo_complete',
          'document_id',       v_event_id,
          'debit_account_id',  v_var_close,
          'credit_account_id', v_op_residual.acct_id,
          'amount',            v_residual,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        )), FALSE);
      ELSE
        PERFORM post_posting_lines(jsonb_build_array(jsonb_build_object(
          'reason',            'wo_close_v',
          'document_kind',     'wo_complete',
          'document_id',       v_event_id,
          'debit_account_id',  v_op_residual.acct_id,
          'credit_account_id', v_var_close,
          'amount',            -v_residual,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        )), FALSE);
      END IF;
    END LOOP;

    UPDATE work_orders SET status = 'closed' WHERE id = p_wo_id;
  END IF;

  RETURN p_wo_id;
END;
$$;

-- ---------- 6. run_daily_reconciliation CREATE OR REPLACE ----------
-- Verbatim copy from mig 0058 with check #12 inserted between 11b
-- and the RETURN. Check #12 fires when SUM(lot_genealogy.qty_consumed)
-- per parent_lot exceeds ABS(SUM(inventory_lot_events.quantity_change))
-- for issue/scrap-style events (event_type IN (1, 5, 8)). Loose <=
-- bound: yield-loss and scrap-only consumption legitimately produce
-- gaps where genealogy_total < events_total. Equality would over-fire.

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

  -- Check 12: lot_genealogy qty_consumed overshoot per parent lot.
  -- Loose <= bound: SUM(genealogy.qty_consumed) per parent must not
  -- exceed ABS(SUM(inventory_lot_events.quantity_change)) for
  -- consumption-style events (event_type IN (1, 5, 8)). Inequality
  -- (genealogy < events) is permitted: yield-loss (raw consumed >
  -- FG produced), scrap-only consumption, and disposal events
  -- legitimately produce that gap.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  WITH genealogy_per_lot AS (
    SELECT parent_lot_id, parent_receipt_date,
           SUM(qty_consumed) AS g_total
      FROM lot_genealogy
     GROUP BY 1, 2
  ),
  events_per_lot AS (
    SELECT lot_id AS parent_lot_id,
           lot_receipt_date AS parent_receipt_date,
           ABS(SUM(quantity_change)) AS e_total
      FROM inventory_lot_events
     WHERE event_type IN (1, 5, 8)
     GROUP BY 1, 2
  )
  SELECT 'lot_genealogy_qty_overshoot',
         jsonb_build_object(
           'parent_lot_id',       g.parent_lot_id,
           'parent_receipt_date', g.parent_receipt_date::TEXT,
           'genealogy_total',     g.g_total::TEXT,
           'events_total',        COALESCE(e.e_total, 0)::TEXT,
           'overshoot',           (g.g_total - COALESCE(e.e_total, 0))::TEXT
         )
    FROM genealogy_per_lot g
    LEFT JOIN events_per_lot e
      ON e.parent_lot_id       = g.parent_lot_id
     AND e.parent_receipt_date = g.parent_receipt_date
   WHERE g.g_total > COALESCE(e.e_total, 0);
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  RETURN v_total;
END;
$$;
