-- acct-b82 / Slice B.1 — Work-order lifecycle functions.
--
-- Adds the wo_events document table (one row per WO lifecycle call —
-- start, op_move, wo_complete, scrap) and the four document-layer
-- wrapper functions. See migration 0037 header for the full design
-- model (BOM + per-op burdens, MVP cost-method restriction, op_move_v
-- reason rationale).
--
-- ============================================================
-- Per-op burden timing semantics
-- ============================================================
--
--   post_wo_start(wo_id)
--     Charges WIP@first_op with:
--       qty_target × Σ_comp (bom.qty_per_parent × comp_std_cost)
--       qty_target × Σ_kind wo_routing_burdens(first_op).std_amount
--     by emitting:
--       1 × wo_start (qty leg, stock_wip@first_op DR / creation_void CR)
--       N × rm_issue_to_wo (qty + value pair per BOM component)
--       M × <burden>_apply (one per first-op burden kind, e.g.
--           labor_apply, oh_apply) — value leg only, qty implied
--     Flips work_orders.status 'draft' → 'released'.
--
--   post_op_move(wo_id, from_op, to_op, qty)
--     Moves qty units from from_op to to_op, then applies to_op
--     burdens. Emits:
--       1 × op_move    (qty leg, stock_wip@to DR / stock_wip@from CR)
--       1 × op_move_v  (value leg, inv_value_wip@to DR /
--                       inv_value_wip@from CR, amount =
--                       qty × std_cum_at_from_op)
--       M × <burden>_apply (one per to_op burden kind)
--     std_cum_at_from_op = (Σ_comp bom.qty_per_parent × comp_std_cost)
--                        + (Σ_kind wo_routing_burdens for ops ≤ from_op)
--     i.e. all upstream costs accumulated by the time qty arrived at
--     from_op. After this call, WIP@to_op holds qty × std_cum_at_to_op.
--     op_move BACKWARDS (rework) re-applies the destination op's
--     burdens — realistic ERP semantics for rework labor.
--
--   post_wo_complete(wo_id, qty)
--     Completes qty units from the highest routing_op into FG. Emits:
--       1 × wo_complete (qty leg)
--       1 × wo_complete (value leg, dispatcher prices via standard
--                        branch = qty × parent_std_cost; CORRECT at
--                        last op because std_cum_at_last_op = full
--                        parent_std_cost)
--     If qty_completed + qty_scrapped reaches qty_target, reads
--     inv_value_wip@last_op residual under FOR UPDATE and emits a
--     wo_close_v leg for any nonzero balance, then sets
--     status='closed'.
--
--   post_scrap(wo_id, op, qty)
--     Reads inv_value_wip + stock_wip@op pools FOR UPDATE to compute
--     accumulated unit cost (= std_cum_at_op for that WO under MVP).
--     Emits:
--       1 × scrap   (qty leg, stock_scrap DR / stock_wip@op CR)
--       1 × scrap_v (value leg, variance_scrap DR /
--                    inv_value_wip@op CR, amount = unit_cost × qty)
--     Updates work_orders.qty_scrapped.
--
-- ============================================================
-- applied_account_kind → transfer_reason mapping
-- ============================================================
--
-- Each row in wo_routing_burdens carries an applied_account_kind
-- (e.g. labor_applied). The corresponding transfer_reason is derived
-- via _wo_apply_reason_for(applied_account_kind). MVP supports
-- labor_applied → labor_apply and oh_applied → oh_apply. Adding a
-- new burden type requires (a) ALTER TYPE account_kind ADD VALUE,
-- (b) ALTER TYPE transfer_reason ADD VALUE, (c) extending this
-- helper, (d) per-currency account scaffolded.
--
-- ============================================================
-- New error codes
-- ============================================================
--
--   P0026 — wo_invalid: WO not found, wrong status, parent ≠ standard,
--           empty wo_routings, scrap qty > pool balance,
--           applied_account_kind has no reason mapping.
--   P0027 — wo_qty_overflow: qty_completed + qty_scrapped + this qty
--           > qty_target.
--   P0028 — routing_op_invalid: from_op or to_op (or scrap op) not in
--           wo_routings; from_op = to_op.
--   P0029 — bom_missing: parent_sku has zero BOM rows at WO start.
--
-- Phase 2 deferrals: wac_perpetual / wac_periodic / wac_retroactive on
-- WO parent (acct-p7v); per-op MUV/LV/OHV variance grain (Q3); orphan
-- WIP at period close (Q4).

-- ============================================================
-- wo_events
-- ============================================================

CREATE TABLE wo_events (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  wo_id           UUID NOT NULL REFERENCES work_orders(id),
  event_kind      TEXT NOT NULL
                    CHECK (event_kind IN ('start', 'op_move', 'wo_complete', 'scrap')),
  routing_op_from INT,
  routing_op_to   INT,
  qty             BIGINT CHECK (qty IS NULL OR qty > 0),
  business_date   DATE NOT NULL,
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID NOT NULL UNIQUE,
  notes           TEXT,
  CHECK (
    (event_kind = 'start'
     AND routing_op_from IS NULL AND routing_op_to IS NULL AND qty IS NULL)
    OR
    (event_kind = 'op_move'
     AND routing_op_from IS NOT NULL AND routing_op_to IS NOT NULL
     AND qty IS NOT NULL)
    OR
    (event_kind = 'wo_complete'
     AND routing_op_from IS NOT NULL AND routing_op_to IS NULL
     AND qty IS NOT NULL)
    OR
    (event_kind = 'scrap'
     AND routing_op_from IS NOT NULL AND routing_op_to IS NULL
     AND qty IS NOT NULL)
  )
);

CREATE INDEX wo_events_wo ON wo_events (wo_id);
CREATE INDEX wo_events_posted_at ON wo_events (posted_at);

COMMENT ON TABLE wo_events IS
  'WO-lifecycle audit log. One row per post_wo_start / post_op_move / '
  'post_wo_complete / post_scrap call. routing_op_from = source op '
  'for op_move / wo_complete (last op) / scrap. routing_op_to = '
  'destination op (op_move only).';

-- ============================================================
-- Helper: applied_account_kind → transfer_reason
-- ============================================================

CREATE OR REPLACE FUNCTION _wo_apply_reason_for(p_applied_account_kind account_kind)
RETURNS transfer_reason
LANGUAGE plpgsql IMMUTABLE
AS $$
BEGIN
  CASE p_applied_account_kind
    WHEN 'labor_applied' THEN RETURN 'labor_apply';
    WHEN 'oh_applied'    THEN RETURN 'oh_apply';
    -- Future: outside_processing_applied → outside_proc_apply,
    --         setup_applied → setup_apply, tooling_applied → tooling_apply
    ELSE
      RAISE EXCEPTION
        'wo_invalid: applied_account_kind % has no transfer_reason '
        'mapping; extend _wo_apply_reason_for after adding the '
        'corresponding ALTER TYPE transfer_reason ADD VALUE',
        p_applied_account_kind USING ERRCODE = 'P0026';
  END CASE;
END;
$$;

COMMENT ON FUNCTION _wo_apply_reason_for(account_kind) IS
  'Maps an absorption account_kind (X_applied) to its companion '
  'transfer_reason (X_apply). Single source of truth for the burden-'
  'apply reason. Adding a new burden type requires extending the '
  'CASE here AND adding the matching enum values.';

-- ============================================================
-- Helper: emit per-op burden-apply events
-- ============================================================
--
-- Returns the JSONB array of apply events for one op of a WO, batched
-- across all wo_routing_burdens rows for that op. Caller appends the
-- result to its own batch.

CREATE OR REPLACE FUNCTION _wo_burden_events_for_op(
  p_wo_id           UUID,
  p_routing_op      INT,
  p_qty             BIGINT,
  p_wip_value_acct  BIGINT,
  p_currency        CHAR(3),
  p_business_date   DATE,
  p_event_id        UUID,
  p_posted_by       UUID
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
  v_batch    JSONB := '[]'::JSONB;
  v_burden   RECORD;
  v_acct     BIGINT;
  v_amount   BIGINT;
  v_reason   transfer_reason;
BEGIN
  FOR v_burden IN
    SELECT applied_account_kind, std_amount
      FROM wo_routing_burdens
     WHERE wo_id = p_wo_id AND routing_op = p_routing_op
     ORDER BY applied_account_kind
  LOOP
    v_amount := p_qty * v_burden.std_amount;
    IF v_amount <= 0 THEN CONTINUE; END IF;

    v_reason := _wo_apply_reason_for(v_burden.applied_account_kind);

    SELECT id INTO v_acct FROM accounts
     WHERE kind = v_burden.applied_account_kind
       AND ledger_kind = 'value'
       AND currency = p_currency AND NOT is_closed;
    IF v_acct IS NULL THEN
      RAISE EXCEPTION 'no open % account for ccy=%',
                      v_burden.applied_account_kind, p_currency
        USING ERRCODE = 'P0010';
    END IF;

    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            v_reason,
      'document_kind',     'wo_event',
      'document_id',       p_event_id,
      'debit_account_id',  p_wip_value_acct,
      'credit_account_id', v_acct,
      'amount',            v_amount,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'posted_by',         p_posted_by
    ));
  END LOOP;
  RETURN v_batch;
END;
$$;

COMMENT ON FUNCTION _wo_burden_events_for_op(UUID, INT, BIGINT, BIGINT, CHAR, DATE, UUID, UUID) IS
  'Builds the per-op burden-apply event batch. Returns one JSONB '
  'event per wo_routing_burdens row for the (wo_id, routing_op), '
  'each charging WIP at p_wip_value_acct against the absorption '
  'account looked up by applied_account_kind in p_currency.';

-- ============================================================
-- post_wo_start
-- ============================================================

CREATE OR REPLACE FUNCTION post_wo_start(
  p_wo_id           UUID,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
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
  v_bom_count       INT;
  v_bom             RECORD;
  v_comp_qty        BIGINT;
  v_comp_std_cost   BIGINT;
  v_comp_value      BIGINT;
  v_comp_qty_acct   BIGINT;
  v_comp_consumed   BIGINT;
  v_comp_val_acct   BIGINT;
  v_batch           JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  SELECT * INTO v_wo FROM work_orders WHERE id = p_wo_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: WO % not found', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;
  IF v_wo.status <> 'draft' THEN
    RAISE EXCEPTION 'wo_invalid: WO % status=% not draft (already started)',
                    p_wo_id, v_wo.status USING ERRCODE = 'P0026';
  END IF;

  -- MVP gate: parent_sku must be standard-cost.
  SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_wo.parent_sku_id;
  IF v_cost_method <> 'standard' THEN
    RAISE EXCEPTION
      'wo_invalid: parent_sku % has cost_method=%, only ''standard'' '
      'supported in Slice B MVP (Phase 2 acct-p7v)',
      v_wo.parent_sku_id, v_cost_method USING ERRCODE = 'P0026';
  END IF;

  -- Routing must be non-empty.
  SELECT MIN(routing_op), COUNT(*) INTO v_first_op, v_op_count
    FROM wo_routings WHERE wo_id = p_wo_id;
  IF v_op_count = 0 THEN
    RAISE EXCEPTION 'wo_invalid: WO % has no routing operations', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;

  -- BOM must be non-empty.
  SELECT COUNT(*) INTO v_bom_count FROM boms
   WHERE parent_sku_id = v_wo.parent_sku_id;
  IF v_bom_count = 0 THEN
    RAISE EXCEPTION 'bom_missing: parent_sku % has no BOM rows',
                    v_wo.parent_sku_id USING ERRCODE = 'P0029';
  END IF;

  -- Resolve parent qty/void/value accounts at first_op.
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

  INSERT INTO wo_events (
    wo_id, event_kind, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_wo_id, 'start', p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_event_id;
  IF v_event_id IS NULL THEN RETURN p_wo_id; END IF;

  -- Event 1: wo_start qty leg.
  v_batch := v_batch || jsonb_build_array(jsonb_build_object(
    'reason',            'wo_start',
    'document_kind',     'wo_event',
    'document_id',       v_event_id,
    'debit_account_id',  v_qty_acct_wip,
    'credit_account_id', v_void_qty,
    'amount',            v_wo.qty_target,
    'qty',               v_wo.qty_target,
    'business_date',     p_business_date,
    'idempotency_key',   gen_random_uuid(),
    'posted_by',         p_posted_by
  ));

  -- Per-component BOM expansion: rm_issue_to_wo qty + value pair.
  FOR v_bom IN
    SELECT b.component_sku_id, b.component_loc_id, b.qty_per_parent
      FROM boms b WHERE b.parent_sku_id = v_wo.parent_sku_id
      ORDER BY b.component_sku_id
  LOOP
    v_comp_qty      := v_wo.qty_target * v_bom.qty_per_parent;
    v_comp_std_cost := resolve_standard_cost_at(v_bom.component_sku_id, p_business_date);
    v_comp_value    := v_comp_qty * v_comp_std_cost;

    SELECT id INTO v_comp_consumed FROM accounts
     WHERE kind='stock_consumed' AND sku_id=v_bom.component_sku_id
       AND ledger_kind='qty' AND NOT is_closed;
    IF v_comp_consumed IS NULL THEN
      RAISE EXCEPTION 'no open stock_consumed account for sku=%',
                      v_bom.component_sku_id USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_comp_qty_acct FROM accounts
     WHERE kind='stock_available' AND sku_id=v_bom.component_sku_id
       AND location_id=v_bom.component_loc_id AND NOT is_closed;
    IF v_comp_qty_acct IS NULL THEN
      RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                      v_bom.component_sku_id, v_bom.component_loc_id
        USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_comp_val_acct FROM accounts
     WHERE kind='inv_value_raw' AND sku_id=v_bom.component_sku_id
       AND location_id=v_bom.component_loc_id AND currency=v_wo.currency
       AND NOT is_closed;
    IF v_comp_val_acct IS NULL THEN
      RAISE EXCEPTION 'no open inv_value_raw account for sku=% loc=% ccy=%',
                      v_bom.component_sku_id, v_bom.component_loc_id, v_wo.currency
        USING ERRCODE = 'P0010';
    END IF;

    -- rm_issue_to_wo qty leg.
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'rm_issue_to_wo',
      'document_kind',     'wo_event',
      'document_id',       v_event_id,
      'debit_account_id',  v_comp_consumed,
      'credit_account_id', v_comp_qty_acct,
      'amount',            v_comp_qty,
      'qty',               v_comp_qty,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'posted_by',         p_posted_by
    ));

    -- rm_issue_to_wo value leg.
    IF v_comp_value > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'rm_issue_to_wo',
        'document_kind',     'wo_event',
        'document_id',       v_event_id,
        'debit_account_id',  v_val_acct_wip,
        'credit_account_id', v_comp_val_acct,
        'amount',            v_comp_value,
        'qty',               v_comp_qty,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      ));
    END IF;
  END LOOP;

  -- First-op burdens (labor, oh, etc. — whatever wo_routing_burdens
  -- declares for this op).
  v_batch := v_batch || _wo_burden_events_for_op(
    p_wo_id, v_first_op, v_wo.qty_target,
    v_val_acct_wip, v_wo.currency, p_business_date,
    v_event_id, p_posted_by
  );

  PERFORM post_transfers(v_batch, FALSE);

  UPDATE work_orders SET status = 'released' WHERE id = p_wo_id;

  RETURN p_wo_id;
END;
$$;

COMMENT ON FUNCTION post_wo_start(UUID, DATE, UUID, UUID, TEXT) IS
  'Releases a draft WO. Charges WIP@first_op with RM (per BOM '
  'component, valued at component standard cost) plus first-op '
  'burdens (per wo_routing_burdens for first_op). Subsequent ops'' '
  'burdens apply at op_move arrival into those ops.';

-- ============================================================
-- post_op_move
-- ============================================================

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
  v_existing_id    UUID;
  v_event_id       UUID;
  v_wo             work_orders%ROWTYPE;
  v_from_count     INT;
  v_to_count       INT;
  v_qty_from       BIGINT;
  v_qty_to         BIGINT;
  v_val_from       BIGINT;
  v_val_to         BIGINT;
  v_rm_per_unit    BIGINT;
  v_burden_at_from BIGINT;
  v_std_cum_at_from BIGINT;
  v_value_amount   BIGINT;
  v_batch          JSONB := '[]'::JSONB;
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
  IF v_wo.status <> 'released' THEN
    RAISE EXCEPTION 'wo_invalid: WO % status=% not released',
                    p_wo_id, v_wo.status USING ERRCODE = 'P0026';
  END IF;

  -- Both ops must be in the routing.
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

  -- Resolve from/to qty + value WIP accounts.
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

  -- Compute std_cum_at_from_op (per-unit). RM is constant across ops;
  -- burdens accumulate up through (and including) from_op.
  SELECT COALESCE(SUM(b.qty_per_parent
                      * resolve_standard_cost_at(b.component_sku_id, p_business_date)), 0)
    INTO v_rm_per_unit
    FROM boms b WHERE b.parent_sku_id = v_wo.parent_sku_id;
  SELECT COALESCE(SUM(std_amount), 0) INTO v_burden_at_from
    FROM wo_routing_burdens
   WHERE wo_id = p_wo_id AND routing_op <= p_from_op;
  v_std_cum_at_from := v_rm_per_unit + v_burden_at_from;
  v_value_amount    := p_qty * v_std_cum_at_from;

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

  -- Qty leg (op_move): caller-supplied amount=qty, dispatcher skips
  -- (qty class).
  v_batch := v_batch || jsonb_build_array(jsonb_build_object(
    'reason',            'op_move',
    'document_kind',     'wo_event',
    'document_id',       v_event_id,
    'debit_account_id',  v_qty_to,
    'credit_account_id', v_qty_from,
    'amount',            p_qty,
    'qty',               p_qty,
    'business_date',     p_business_date,
    'idempotency_key',   gen_random_uuid(),
    'posted_by',         p_posted_by
  ));

  -- Value leg (op_move_v): caller-supplied amount stands; reason is
  -- NOT in dispatcher cost-event list, so no auto-pricing.
  IF v_value_amount > 0 THEN
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'op_move_v',
      'document_kind',     'wo_event',
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

  -- to_op burdens applied against the moved qty.
  v_batch := v_batch || _wo_burden_events_for_op(
    p_wo_id, p_to_op, p_qty,
    v_val_to, v_wo.currency, p_business_date,
    v_event_id, p_posted_by
  );

  PERFORM post_transfers(v_batch, FALSE);

  RETURN p_wo_id;
END;
$$;

COMMENT ON FUNCTION post_op_move(UUID, INT, INT, BIGINT, DATE, UUID, UUID, TEXT) IS
  'Moves p_qty units from p_from_op to p_to_op for p_wo_id, then '
  'applies p_to_op burdens. Value-leg amount = qty × std_cum_at_from_op '
  '(RM + burdens for ops ≤ from_op). Reason op_move_v bypasses '
  'dispatcher auto-pricing. Rework moves (to_op < from_op) are '
  'allowed and re-apply destination-op burdens.';

-- ============================================================
-- post_wo_complete
-- ============================================================

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
  v_val_balance    BIGINT;
  v_residual       BIGINT;
  v_batch          JSONB := '[]'::JSONB;
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
  SELECT id INTO v_qty_fg FROM accounts
   WHERE kind='stock_available' AND sku_id=v_wo.parent_sku_id
     AND location_id=v_wo.fg_location_id AND NOT is_closed;
  IF v_qty_fg IS NULL THEN
    RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                    v_wo.parent_sku_id, v_wo.fg_location_id
      USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_val_from FROM accounts
   WHERE kind='inv_value_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=v_last_op AND currency=v_wo.currency AND NOT is_closed;
  IF v_val_from IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_wip account for sku=% op=% ccy=%',
                    v_wo.parent_sku_id, v_last_op, v_wo.currency
      USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_val_fg FROM accounts
   WHERE kind='inv_value_fg' AND sku_id=v_wo.parent_sku_id
     AND location_id=v_wo.fg_location_id AND currency=v_wo.currency
     AND NOT is_closed;
  IF v_val_fg IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_fg account for sku=% loc=% ccy=%',
                    v_wo.parent_sku_id, v_wo.fg_location_id, v_wo.currency
      USING ERRCODE = 'P0010';
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

  -- Qty leg.
  v_batch := v_batch || jsonb_build_array(jsonb_build_object(
    'reason',            'wo_complete',
    'document_kind',     'wo_event',
    'document_id',       v_event_id,
    'debit_account_id',  v_qty_fg,
    'credit_account_id', v_qty_from,
    'amount',            p_qty,
    'qty',               p_qty,
    'business_date',     p_business_date,
    'idempotency_key',   gen_random_uuid(),
    'posted_by',         p_posted_by
  ));

  -- Value leg — dispatcher prices via standard branch
  -- (qty × parent_std_cost). Correct at last op because
  -- std_cum_at_last_op = full parent_std_cost.
  v_batch := v_batch || jsonb_build_array(jsonb_build_object(
    'reason',            'wo_complete',
    'document_kind',     'wo_event',
    'document_id',       v_event_id,
    'debit_account_id',  v_val_fg,
    'credit_account_id', v_val_from,
    'qty',               p_qty,
    'business_date',     p_business_date,
    'idempotency_key',   gen_random_uuid(),
    'posted_by',         p_posted_by
  ));

  PERFORM post_transfers(v_batch, FALSE);

  UPDATE work_orders
     SET qty_completed = qty_completed + p_qty
   WHERE id = p_wo_id;

  -- On final completion: settle WIP@last_op residual + close.
  IF v_will_close THEN
    PERFORM 1 FROM accounts WHERE id = v_val_from FOR UPDATE;
    SELECT (debits_total - credits_total) INTO v_val_balance
      FROM accounts WHERE id = v_val_from;
    v_residual := COALESCE(v_val_balance, 0);

    IF v_residual <> 0 THEN
      SELECT id INTO v_var_close FROM accounts
       WHERE kind='variance_wo_close' AND ledger_kind='value'
         AND currency=v_wo.currency AND NOT is_closed;
      IF v_var_close IS NULL THEN
        RAISE EXCEPTION 'no open variance_wo_close account for ccy=%',
                        v_wo.currency USING ERRCODE = 'P0010';
      END IF;

      IF v_residual > 0 THEN
        PERFORM post_transfers(jsonb_build_array(jsonb_build_object(
          'reason',            'wo_close_v',
          'document_kind',     'wo_event',
          'document_id',       v_event_id,
          'debit_account_id',  v_var_close,
          'credit_account_id', v_val_from,
          'amount',            v_residual,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        )), FALSE);
      ELSE
        PERFORM post_transfers(jsonb_build_array(jsonb_build_object(
          'reason',            'wo_close_v',
          'document_kind',     'wo_event',
          'document_id',       v_event_id,
          'debit_account_id',  v_val_from,
          'credit_account_id', v_var_close,
          'amount',            -v_residual,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        )), FALSE);
      END IF;
    END IF;

    UPDATE work_orders SET status = 'closed' WHERE id = p_wo_id;
  END IF;

  RETURN p_wo_id;
END;
$$;

COMMENT ON FUNCTION post_wo_complete(UUID, BIGINT, DATE, UUID, UUID, TEXT) IS
  'Completes p_qty units from the highest routing_op into FG. Value-'
  'leg dispatcher-priced as qty × parent_std_cost (correct at last '
  'op). On final completion, reads inv_value_wip@last_op residual '
  'FOR UPDATE and emits wo_close_v leg for any nonzero balance, then '
  'sets status=''closed''. P0027 on cumulative overflow.';

-- ============================================================
-- post_scrap
-- ============================================================

CREATE OR REPLACE FUNCTION post_scrap(
  p_wo_id           UUID,
  p_routing_op      INT,
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
  v_op_count       INT;
  v_qty_from       BIGINT;
  v_qty_scrap      BIGINT;
  v_val_from       BIGINT;
  v_var_scrap      BIGINT;
  v_qty_balance    BIGINT;
  v_val_balance    BIGINT;
  v_unit_cost      BIGINT;
  v_scrap_value    BIGINT;
  v_batch          JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF p_qty IS NULL OR p_qty <= 0 THEN
    RAISE EXCEPTION 'wo_invalid: scrap qty must be > 0 (got %)', p_qty
      USING ERRCODE = 'P0026';
  END IF;

  SELECT * INTO v_wo FROM work_orders WHERE id = p_wo_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: WO % not found', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;
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

  SELECT COUNT(*) INTO v_op_count FROM wo_routings
   WHERE wo_id = p_wo_id AND routing_op = p_routing_op;
  IF v_op_count = 0 THEN
    RAISE EXCEPTION 'routing_op_invalid: op % not in WO % routing',
                    p_routing_op, p_wo_id USING ERRCODE = 'P0028';
  END IF;

  SELECT id INTO v_qty_from FROM accounts
   WHERE kind='stock_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_routing_op AND NOT is_closed;
  IF v_qty_from IS NULL THEN
    RAISE EXCEPTION 'no open stock_wip account for sku=% op=%',
                    v_wo.parent_sku_id, p_routing_op USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_qty_scrap FROM accounts
   WHERE kind='stock_scrap' AND sku_id=v_wo.parent_sku_id
     AND ledger_kind='qty' AND NOT is_closed;
  IF v_qty_scrap IS NULL THEN
    RAISE EXCEPTION 'no open stock_scrap account for sku=%',
                    v_wo.parent_sku_id USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_val_from FROM accounts
   WHERE kind='inv_value_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_routing_op AND currency=v_wo.currency AND NOT is_closed;
  IF v_val_from IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_wip account for sku=% op=% ccy=%',
                    v_wo.parent_sku_id, p_routing_op, v_wo.currency
      USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_var_scrap FROM accounts
   WHERE kind='variance_scrap' AND ledger_kind='value'
     AND currency=v_wo.currency AND NOT is_closed;
  IF v_var_scrap IS NULL THEN
    RAISE EXCEPTION 'no open variance_scrap account for ccy=%',
                    v_wo.currency USING ERRCODE = 'P0010';
  END IF;

  -- Lock pool then read accumulated unit cost.
  PERFORM 1 FROM accounts WHERE id IN (v_val_from, v_qty_from)
   ORDER BY id FOR UPDATE;
  SELECT (debits_total - credits_total) INTO v_qty_balance
    FROM accounts WHERE id = v_qty_from;
  SELECT (debits_total - credits_total) INTO v_val_balance
    FROM accounts WHERE id = v_val_from;

  IF v_qty_balance IS NULL OR v_qty_balance <= 0 THEN
    RAISE EXCEPTION
      'wo_invalid: stock_wip(sku=%, op=%) balance=%, cannot scrap',
      v_wo.parent_sku_id, p_routing_op, v_qty_balance USING ERRCODE = 'P0026';
  END IF;
  IF p_qty > v_qty_balance THEN
    RAISE EXCEPTION
      'wo_invalid: scrap qty=% > stock_wip balance=% at op=%',
      p_qty, v_qty_balance, p_routing_op USING ERRCODE = 'P0026';
  END IF;
  v_unit_cost   := COALESCE(v_val_balance, 0) / v_qty_balance;
  v_scrap_value := v_unit_cost * p_qty;

  INSERT INTO wo_events (
    wo_id, event_kind, routing_op_from, qty,
    business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_wo_id, 'scrap', p_routing_op, p_qty,
    p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_event_id;
  IF v_event_id IS NULL THEN RETURN p_wo_id; END IF;

  -- Qty leg: stock_scrap DR / stock_wip CR.
  v_batch := v_batch || jsonb_build_array(jsonb_build_object(
    'reason',            'scrap',
    'document_kind',     'wo_event',
    'document_id',       v_event_id,
    'debit_account_id',  v_qty_scrap,
    'credit_account_id', v_qty_from,
    'amount',            p_qty,
    'qty',               p_qty,
    'business_date',     p_business_date,
    'idempotency_key',   gen_random_uuid(),
    'posted_by',         p_posted_by
  ));

  -- Value leg: variance_scrap DR / inv_value_wip CR (NOT a cost-event;
  -- caller-supplied amount = unit_cost × qty).
  IF v_scrap_value > 0 THEN
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'scrap_v',
      'document_kind',     'wo_event',
      'document_id',       v_event_id,
      'debit_account_id',  v_var_scrap,
      'credit_account_id', v_val_from,
      'amount',            v_scrap_value,
      'qty',               p_qty,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'posted_by',         p_posted_by
    ));
  END IF;

  PERFORM post_transfers(v_batch, FALSE);

  UPDATE work_orders SET qty_scrapped = qty_scrapped + p_qty WHERE id = p_wo_id;

  RETURN p_wo_id;
END;
$$;

COMMENT ON FUNCTION post_scrap(UUID, INT, BIGINT, DATE, UUID, UUID, TEXT) IS
  'Scraps p_qty units from WIP at p_routing_op. Reads inv_value_wip + '
  'stock_wip pools FOR UPDATE to compute accumulated unit cost (= '
  'std_cum_at_op for that WO). Emits scrap qty leg + scrap_v value '
  'leg crediting variance_scrap.';
