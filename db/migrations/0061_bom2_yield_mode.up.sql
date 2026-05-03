-- acct-6jq / BOM2 — yield_mode dual-mode + drop emission/per-op gross-up.
--
-- Original C5b implementation grossed up rm_issue qty + value at WO_start
-- emission AND grossed up per_unit_cum in post_op_move. Both were wrong:
-- build cost in WIP must come from ACTUAL usage, not planned-grossed-up.
-- Symptom: op_move_v over-drains pool@from_op into negative territory and
-- hits the inv_value_wip CHECK (debit-normal: credits ≤ debits).
--
-- Dual-mode design:
--   bom_lines.scrap_pct stays as-is (planning metadata).
--   New flag skus.yield_mode ∈ {'plan_only','absorbed'}:
--     plan_only (default): scrap_pct is recorded but does NOT affect the
--                          parent's standard cost rollup. Variance at
--                          wo_close captures any actual scrap as a clean
--                          unfavorable variance.
--     absorbed:            the (future) BOM rollup tool factors scrap into
--                          parent's standard_costs entry. variance at
--                          wo_close captures the planned-vs-actual gap
--                          (favorable when actual < plan).
--
--   In BOTH modes, pool flow is literal (qty_per × p_qty × comp_std).
--   The mode flag affects only the rollup tool's output and caller
--   intent; current cost-flow functions never branch on it.
--
-- Replaces (CREATE OR REPLACE) _wo_emit_bom_lines (mig 0051) and
-- post_op_move (mig 0053). Schema-additive on skus.

ALTER TABLE skus ADD COLUMN yield_mode TEXT NOT NULL DEFAULT 'plan_only'
  CHECK (yield_mode IN ('plan_only', 'absorbed'));

COMMENT ON COLUMN skus.yield_mode IS
  'Whether bom_lines.scrap_pct factors into the parent''s standard cost '
  'rollup. ''plan_only'' (default): scrap_pct is planning metadata, std '
  'cost is literal. ''absorbed'': the rollup tool inflates parent std '
  'cost by 1/(1-scrap_pct/100) per line. Pool flow (emission, op_move) '
  'uses literal cost in both modes. acct-6jq.';

-- ============================================================
-- _wo_emit_bom_lines: drop CEIL gross-up at item emission.
-- ============================================================

CREATE OR REPLACE FUNCTION _wo_emit_bom_lines(
  p_wo_id          UUID,
  p_bom_id         BIGINT,
  p_routing_op     INT,
  p_qty            BIGINT,
  p_filter         JSONB,
  p_event_id       UUID,
  p_business_date  DATE,
  p_posted_by      UUID
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
  v_wo                   work_orders%ROWTYPE;
  v_val_acct_wip         BIGINT;
  v_batch                JSONB := '[]'::JSONB;
  v_line                 RECORD;
  v_filter_kind          TEXT;
  v_filter_basis         TEXT;
  v_filter_fire_at       TEXT;
  v_filter_applies_at_op INT;
  v_issue_qty            BIGINT;
  v_value                BIGINT;
  v_amount               BIGINT;
  v_reason               transfer_reason;
  v_comp_consumed        BIGINT;
  v_comp_qty_acct        BIGINT;
  v_comp_val_acct        BIGINT;
  v_applied_kind         account_kind;
  v_applied_acct         BIGINT;
  v_comp_std_cost        BIGINT;
BEGIN
  IF p_qty IS NULL OR p_qty <= 0 THEN
    RAISE EXCEPTION 'wo_invalid: _wo_emit_bom_lines requires positive p_qty (got %)', p_qty
      USING ERRCODE = 'P0026';
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
      -- LITERAL emission: qty_per × p_qty. scrap_pct is planning metadata
      -- and does NOT affect the build-cost flow. If the floor consumes
      -- more than this, the excess is a separate post_transfers call
      -- (an inventory adjustment or follow-up rm_issue_to_wo).
      v_issue_qty     := p_qty * v_line.qty_per_parent;
      v_comp_std_cost := resolve_standard_cost_at(v_line.component_sku_id, p_business_date);
      v_value         := v_issue_qty * v_comp_std_cost;

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

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'rm_issue_to_wo',
        'document_kind',     'wo_event',
        'document_id',       p_event_id,
        'debit_account_id',  v_comp_consumed,
        'credit_account_id', v_comp_qty_acct,
        'amount',            v_issue_qty,
        'qty',               v_issue_qty,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      ));

      IF v_value > 0 THEN
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'rm_issue_to_wo',
          'document_kind',     'wo_event',
          'document_id',       p_event_id,
          'debit_account_id',  v_val_acct_wip,
          'credit_account_id', v_comp_val_acct,
          'amount',            v_value,
          'qty',               v_issue_qty,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        ));
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
        'document_kind',     'wo_event',
        'document_id',       p_event_id,
        'debit_account_id',  v_val_acct_wip,
        'credit_account_id', v_applied_acct,
        'amount',            v_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      ));
    END IF;
  END LOOP;

  RETURN v_batch;
END;
$$;

COMMENT ON FUNCTION _wo_emit_bom_lines(UUID, BIGINT, INT, BIGINT, JSONB, UUID, DATE, UUID) IS
  'BOM-line event emitter. Walks _wo_explode_bom, filters by '
  '{kind, basis, fire_at, applies_at_op} subset. Emission is LITERAL: '
  'qty_per × p_qty for items, p_qty × std_amount for per-unit absorbtion, '
  'std_amount for per-lot. bom_lines.scrap_pct is planning metadata and '
  'does NOT affect the build-cost flow. acct-5ba (rev acct-6jq).';

-- ============================================================
-- post_op_move: drop scrap_pct gross-up in per_unit_cum.
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
  v_existing_id      UUID;
  v_event_id         UUID;
  v_wo               work_orders%ROWTYPE;
  v_from_count       INT;
  v_to_count         INT;
  v_qty_from         BIGINT;
  v_qty_to           BIGINT;
  v_val_from         BIGINT;
  v_val_to           BIGINT;
  v_std_cum_at_from  BIGINT;
  v_value_amount     BIGINT;
  v_batch            JSONB := '[]'::JSONB;
  v_use_new          BOOLEAN;
  v_bom              bom_headers%ROWTYPE;
  v_first_op         INT;
  v_default_lot_size BIGINT;
  v_per_unit_cum     BIGINT;
  v_per_lot_cum      BIGINT;
  v_first_arrival    BOOLEAN;
  v_rm_per_unit      BIGINT;
  v_burden_at_from   BIGINT;
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

  v_use_new := v_wo.bom_id IS NOT NULL OR EXISTS (
    SELECT 1 FROM bom_headers bh
     WHERE bh.parent_sku_id = v_wo.parent_sku_id
       AND bh.is_primary AND bh.status='active'
       AND bh.effective_at <= (p_business_date::TIMESTAMPTZ + INTERVAL '1 day')
       AND bh.obsolete_at  >  p_business_date::TIMESTAMPTZ
  );

  IF v_use_new THEN
    v_bom := _wo_resolve_bom_for(p_wo_id, p_business_date);
    SELECT default_lot_size INTO v_default_lot_size
      FROM skus WHERE id = v_wo.parent_sku_id;
    SELECT MIN(routing_op) INTO v_first_op
      FROM wo_routings WHERE wo_id = p_wo_id;

    -- Per-unit contribution at applies_at_op ≤ from_op. LITERAL —
    -- bom_lines.scrap_pct is planning metadata and does NOT affect
    -- per-op cost flow (acct-6jq). The future BOM rollup tool reads
    -- skus.yield_mode to decide whether scrap inflates the parent's
    -- standard_costs entry; per-op flow stays literal in both modes.
    SELECT COALESCE(SUM(
      CASE
        WHEN exp.kind = 'item' THEN
          (exp.qty_per_parent
            * resolve_standard_cost_at(exp.component_sku_id, p_business_date))
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

    v_std_cum_at_from := v_per_unit_cum + v_per_lot_cum;
    v_value_amount    := p_qty * v_std_cum_at_from;

    v_first_arrival := NOT EXISTS (
      SELECT 1 FROM wo_events
       WHERE wo_id = p_wo_id
         AND (
           (event_kind = 'op_move' AND routing_op_to = p_to_op)
           OR (event_kind = 'start' AND p_to_op = v_first_op)
         )
    );
  ELSE
    SELECT COALESCE(SUM(b.qty_per_parent
                        * resolve_standard_cost_at(b.component_sku_id, p_business_date)), 0)
      INTO v_rm_per_unit
      FROM boms b WHERE b.parent_sku_id = v_wo.parent_sku_id;
    SELECT COALESCE(SUM(std_amount), 0) INTO v_burden_at_from
      FROM wo_routing_burdens
     WHERE wo_id = p_wo_id AND routing_op <= p_from_op;
    v_std_cum_at_from := v_rm_per_unit + v_burden_at_from;
    v_value_amount    := p_qty * v_std_cum_at_from;
  END IF;

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

  IF v_use_new THEN
    IF v_first_arrival THEN
      v_batch := v_batch || _wo_emit_bom_lines(
        p_wo_id, v_bom.id, p_to_op, p_qty,
        jsonb_build_object('fire_at', 'op_arrival', 'applies_at_op', p_to_op),
        v_event_id, p_business_date, p_posted_by
      );
    ELSE
      v_batch := v_batch || _wo_emit_bom_lines(
        p_wo_id, v_bom.id, p_to_op, p_qty,
        jsonb_build_object('fire_at', 'op_arrival', 'applies_at_op', p_to_op,
                           'basis', 'per_unit'),
        v_event_id, p_business_date, p_posted_by
      );
    END IF;
  ELSE
    v_batch := v_batch || _wo_burden_events_for_op(
      p_wo_id, p_to_op, p_qty,
      v_val_to, v_wo.currency, p_business_date,
      v_event_id, p_posted_by
    );
  END IF;

  PERFORM post_transfers(v_batch, FALSE);

  RETURN p_wo_id;
END;
$$;

COMMENT ON FUNCTION post_op_move(UUID, INT, INT, BIGINT, DATE, UUID, UUID, TEXT) IS
  'Moves p_qty units from p_from_op to p_to_op. NEW path: per_unit_cum + '
  'per_lot_cum from bom_lines, both LITERAL (acct-6jq dropped scrap_pct '
  'gross-up so pool flow matches actual issuance). first-arrival detection '
  'drives per_lot firing at to_op. acct-j3r (rev acct-6jq).';

-- ============================================================
-- post_wo_complete: pre-balance pool@last_op before per-output drain.
-- ============================================================
-- When the rollup is scrap-aware (yield_mode='absorbed') and actual
-- pool < parent_std × qty, the per-output wo_complete_v over-drains
-- pool@last_op into negative (debit-normal CHECK violation). When
-- actual > parent_std × qty (unfavorable), the post-loop residual
-- sweep handles it but we may as well unify both directions in one
-- pre-balance step at FINAL close.
--
-- Strategy: at v_will_close, compare pool@last_op vs v_total_drain.
-- Pre-emit a wo_close_v to align them (favorable: DR pool / CR var;
-- unfavorable: DR var / CR pool). Then per-output drain works clean.
-- The post-loop walk-all sweep then only catches non-last-op residue
-- (op_move integer truncation etc.).

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
  v_use_new        BOOLEAN;
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

  v_use_new := v_wo.bom_id IS NOT NULL OR EXISTS (
    SELECT 1 FROM bom_headers bh
     WHERE bh.parent_sku_id = v_wo.parent_sku_id
       AND bh.is_primary AND bh.status='active'
       AND bh.effective_at <= (p_business_date::TIMESTAMPTZ + INTERVAL '1 day')
       AND bh.obsolete_at  >  p_business_date::TIMESTAMPTZ
  );

  IF v_use_new THEN
    SELECT COUNT(*), COALESCE(SUM(allocation_pct), 0)
      INTO v_outputs_n, v_alloc_sum
      FROM wo_outputs WHERE wo_id = p_wo_id;
    IF v_outputs_n = 0 THEN
      RAISE EXCEPTION 'wo_invalid: WO % has no wo_outputs rows (new path)',
                      p_wo_id USING ERRCODE = 'P0026';
    END IF;
    IF v_alloc_sum <> 100 THEN
      RAISE EXCEPTION
        'output_allocation_invalid: wo_outputs(wo=%) allocation_pct sums to % (expected 100)',
        p_wo_id, v_alloc_sum USING ERRCODE = 'P0033';
    END IF;

    v_parent_std := resolve_standard_cost_at(v_wo.parent_sku_id, p_business_date);
    v_total_drain := p_qty * v_parent_std;

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

    -- Pre-balance pool@last_op so per-output wo_complete_v drains clean
    -- (avoids debit-normal CHECK violation when v_total_drain > pool).
    -- Only at final close — partial completes drain proportionally and
    -- shouldn't trigger close-style variance.
    IF v_will_close THEN
      PERFORM 1 FROM accounts WHERE id = v_val_from FOR UPDATE;
      SELECT (debits_total - credits_total) INTO v_pool_at_last
        FROM accounts WHERE id = v_val_from;
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
          -- pool < drain: favorable variance. Inflate pool, credit variance.
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'wo_close_v',
            'document_kind',     'wo_event',
            'document_id',       v_event_id,
            'debit_account_id',  v_val_from,
            'credit_account_id', v_var_close,
            'amount',            v_prebalance,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'posted_by',         p_posted_by
          ));
        ELSE
          -- pool > drain: unfavorable variance. Deflate pool, debit variance.
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'wo_close_v',
            'document_kind',     'wo_event',
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

      IF v_q_share > 0 THEN
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'wo_complete',
          'document_kind',     'wo_event',
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
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'wo_complete_v',
          'document_kind',     'wo_event',
          'document_id',       v_event_id,
          'debit_account_id',  v_val_fg,
          'credit_account_id', v_val_from,
          'amount',            v_v_share,
          'qty',               v_q_share,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        ));
      END IF;
    END LOOP;

    PERFORM post_transfers(v_batch, FALSE);

    UPDATE work_orders SET qty_completed = qty_completed + p_qty
     WHERE id = p_wo_id;

    IF v_will_close THEN
      -- last_op was pre-balanced above; this sweep catches non-last-op
      -- residue (op_move integer truncation, etc.).
      FOR v_op_residual IN
        SELECT a.id AS acct_id,
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
        PERFORM 1 FROM accounts WHERE id = v_op_residual.acct_id FOR UPDATE;
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
          PERFORM post_transfers(jsonb_build_array(jsonb_build_object(
            'reason',            'wo_close_v',
            'document_kind',     'wo_event',
            'document_id',       v_event_id,
            'debit_account_id',  v_var_close,
            'credit_account_id', v_op_residual.acct_id,
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
  END IF;

  -- OLD PATH (Slice B) — preserved verbatim from migration 0055.
  SELECT id INTO v_qty_fg FROM accounts
   WHERE kind='stock_available' AND sku_id=v_wo.parent_sku_id
     AND location_id=v_wo.fg_location_id AND NOT is_closed;
  IF v_qty_fg IS NULL THEN
    RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                    v_wo.parent_sku_id, v_wo.fg_location_id
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

  UPDATE work_orders SET qty_completed = qty_completed + p_qty
   WHERE id = p_wo_id;

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
  'Closes a WO (final or partial). NEW path: drives multi-output via '
  'wo_outputs; at FINAL close pre-balances pool@last_op vs parent_std × '
  'qty_target via a wo_close_v before per-output drain (avoids debit-'
  'normal CHECK violation when actual pool diverges from rolled-up std). '
  'Walk-all sweep then catches non-last-op residue. acct-n7p (rev acct-6jq).';
