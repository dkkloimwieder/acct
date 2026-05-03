-- acct-58w / BOM2 Phase B6 — post_wo_start rewrite (additive, dispatch-by-existence).
--
-- Strategy: keep the old (Slice B) `boms` + `wo_routing_burdens` path so
-- existing tests stay green. Detect "new model in use" by checking if
-- work_orders.bom_id is set OR a primary active bom_header exists for
-- the parent SKU at p_business_date. When new model is used, drive the
-- emission via _wo_resolve_bom_for + _wo_emit_bom_lines and initialize
-- wo_outputs.
--
-- Final cleanup migration (filed during C3) will:
--   1. switch all tests to the new model,
--   2. drop the old `boms` and `wo_routing_burdens` tables,
--   3. delete the old-path branch from this function.
--
-- New-path semantics:
--   1. Resolve bom_id via _wo_resolve_bom_for (P0033 if mis-resolved).
--   2. Validate every applies_at_op (after phantom explosion) is in
--      wo_routings — P0028 'wo_start_op_mismatch' otherwise.
--   3. Validate or initialize wo_outputs:
--        empty       → INSERT default single primary row (parent_sku,
--                      fg_location_id, qty_target, allocation_pct=100);
--        non-empty   → require Σ allocation_pct = 100, else P0033
--                      'output_allocation_invalid'.
--   4. Emit wo_start qty leg as before (stock_wip@first_op DR /
--      creation_void CR, qty=qty_target).
--   5. Emit lines via _wo_emit_bom_lines:
--        a) filter {fire_at:'wo_start'}                   → all wo_start
--           lines, charging WIP@first_op.
--        b) filter {fire_at:'op_arrival', applies_at_op:first_op} →
--           per-unit / per-lot lines that apply at the first op,
--           charging WIP@first_op.
--   6. Status → 'released'.
--
-- Idempotency-after-FOR-UPDATE replay pattern (from migration 0039) is
-- preserved verbatim in both paths.

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
  v_use_new         BOOLEAN;
  v_bom             bom_headers%ROWTYPE;
  v_bad_op          INT;
  v_alloc_sum       NUMERIC;
  v_batch           JSONB := '[]'::JSONB;
  -- old-path locals
  v_bom_count       INT;
  v_bom_old         RECORD;
  v_comp_qty        BIGINT;
  v_comp_std_cost   BIGINT;
  v_comp_value      BIGINT;
  v_comp_qty_acct   BIGINT;
  v_comp_consumed   BIGINT;
  v_comp_val_acct   BIGINT;
BEGIN
  -- Fast-path replay check (no lock).
  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  -- Lock work_orders row.
  SELECT * INTO v_wo FROM work_orders WHERE id = p_wo_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: WO % not found', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;

  -- Race-safe replay check AFTER lock (acct-69p / migration 0039).
  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF v_wo.status <> 'draft' THEN
    RAISE EXCEPTION 'wo_invalid: WO % status=% not draft (already started)',
                    p_wo_id, v_wo.status USING ERRCODE = 'P0026';
  END IF;

  -- MVP cost-method gate.
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

  -- Parent qty/void/value accounts at first_op.
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

  -- Decide path: new (bom_headers) vs old (boms).
  v_use_new := v_wo.bom_id IS NOT NULL OR EXISTS (
    SELECT 1 FROM bom_headers bh
     WHERE bh.parent_sku_id = v_wo.parent_sku_id
       AND bh.is_primary AND bh.status='active'
       AND bh.effective_at <= (p_business_date::TIMESTAMPTZ + INTERVAL '1 day')
       AND bh.obsolete_at  >  p_business_date::TIMESTAMPTZ
  );

  IF v_use_new THEN
    -- ============================================================
    -- NEW PATH: bom_headers / bom_lines / wo_outputs
    -- ============================================================

    v_bom := _wo_resolve_bom_for(p_wo_id, p_business_date);

    -- Validate all applies_at_op (post-explosion) appear in wo_routings.
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

    -- wo_outputs: initialize default if empty, else validate sum of pct.
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

    -- All fire_at='wo_start' lines (any applies_at_op), charging WIP@first_op.
    v_batch := v_batch || _wo_emit_bom_lines(
      p_wo_id, v_bom.id, v_first_op, v_wo.qty_target,
      jsonb_build_object('fire_at', 'wo_start'),
      v_event_id, p_business_date, p_posted_by
    );

    -- fire_at='op_arrival' lines at first_op, charging WIP@first_op.
    v_batch := v_batch || _wo_emit_bom_lines(
      p_wo_id, v_bom.id, v_first_op, v_wo.qty_target,
      jsonb_build_object('fire_at', 'op_arrival', 'applies_at_op', v_first_op),
      v_event_id, p_business_date, p_posted_by
    );

    PERFORM post_transfers(v_batch, FALSE);

    UPDATE work_orders SET status = 'released' WHERE id = p_wo_id;

    RETURN p_wo_id;
  END IF;

  -- ============================================================
  -- OLD PATH (Slice B): `boms` + `wo_routing_burdens`. Preserved
  -- verbatim from migration 0039 so existing tests stay green.
  -- Will be removed in the C3 cleanup migration.
  -- ============================================================

  SELECT COUNT(*) INTO v_bom_count FROM boms
   WHERE parent_sku_id = v_wo.parent_sku_id;
  IF v_bom_count = 0 THEN
    RAISE EXCEPTION 'bom_missing: parent_sku % has no BOM rows',
                    v_wo.parent_sku_id USING ERRCODE = 'P0029';
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

  FOR v_bom_old IN
    SELECT b.component_sku_id, b.component_loc_id, b.qty_per_parent
      FROM boms b WHERE b.parent_sku_id = v_wo.parent_sku_id
      ORDER BY b.component_sku_id
  LOOP
    v_comp_qty      := v_wo.qty_target * v_bom_old.qty_per_parent;
    v_comp_std_cost := resolve_standard_cost_at(v_bom_old.component_sku_id, p_business_date);
    v_comp_value    := v_comp_qty * v_comp_std_cost;

    SELECT id INTO v_comp_consumed FROM accounts
     WHERE kind='stock_consumed' AND sku_id=v_bom_old.component_sku_id
       AND ledger_kind='qty' AND NOT is_closed;
    IF v_comp_consumed IS NULL THEN
      RAISE EXCEPTION 'no open stock_consumed account for sku=%',
                      v_bom_old.component_sku_id USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_comp_qty_acct FROM accounts
     WHERE kind='stock_available' AND sku_id=v_bom_old.component_sku_id
       AND location_id=v_bom_old.component_loc_id AND NOT is_closed;
    IF v_comp_qty_acct IS NULL THEN
      RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                      v_bom_old.component_sku_id, v_bom_old.component_loc_id
        USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_comp_val_acct FROM accounts
     WHERE kind='inv_value_raw' AND sku_id=v_bom_old.component_sku_id
       AND location_id=v_bom_old.component_loc_id AND currency=v_wo.currency
       AND NOT is_closed;
    IF v_comp_val_acct IS NULL THEN
      RAISE EXCEPTION 'no open inv_value_raw account for sku=% loc=% ccy=%',
                      v_bom_old.component_sku_id, v_bom_old.component_loc_id, v_wo.currency
        USING ERRCODE = 'P0010';
    END IF;

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
  'Releases a draft WO. Dispatches on whether the new bom_headers model '
  'is in use for this parent_sku (or work_orders.bom_id is set): NEW '
  'path drives via _wo_resolve_bom_for + _wo_emit_bom_lines + wo_outputs '
  'init; OLD path keeps the Slice B `boms` + `wo_routing_burdens` flow. '
  'New error codes: P0028 wo_start_op_mismatch, P0033 output_allocation_'
  'invalid. acct-58w (BOM2 B6).';
