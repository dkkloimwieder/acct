-- acct-rso — Tier 3 of acct-8in (and acct-rgb): wac_retroactive on WIP.
-- Lifts the wac_retroactive parent / component gates on the WO path and
-- refactors wac_retroactive_close_hook for topological per-pool walk +
-- per-event chronological replay with merged value/qty streams for
-- inv_value_wip pools.
--
-- WHAT CHANGES.
--
-- 1. post_wo_start: cost_method gate accepts wac_retroactive parents
--    alongside standard / wac_perpetual / wac_periodic. The close hook
--    handles per-event chronological replay across the WIP pool chain.
--
-- 2. post_op_move: extends the wac branch from {wac_perpetual,
--    wac_periodic} to also include wac_retroactive. Mid-period math is
--    identical: read source pool running avg under FOR UPDATE; the
--    op_move_v transfer is flagged into transfers_provisional via
--    _post_transfers_apply_event (tier 1 / mig 0064 already covers
--    wac_retroactive in the flagging list).
--
-- 3. _wo_emit_bom_lines: lifts the wac_retroactive component gate from
--    tier 2 (mig 0067). Mid-period math is identical to wac_perpetual /
--    wac_periodic — read component value pool's running avg at issue
--    time. SCOPE GATE: parent SKU MUST also be wac_retroactive. Mixed
--    parent/component cost methods (e.g., standard parent + wac_retroactive
--    component) raise P0026 → acct-7eo (same rationale as tier 2's
--    wac_periodic mixed gate).
--
-- 4. wac_retroactive_close_hook: full refactor.
--
--    a. TOPOLOGICAL POOL WALK. Build pool set + edge set from in-period
--       wac_retroactive flagged provisionals. Pool set = credit accounts
--       of all flagged depletions UNION debit accounts of internal-chain
--       flagged rows (op_move_v / rm_issue_to_wo). Edge set =
--       (credit, debit) tuples of internal-chain rows. Kahn's algorithm
--       sorts pools; cycles raise P0036 ('wac_retroactive_pool_cycle';
--       deferred to acct-p7v-rework).
--
--    b. PER-POOL CHRONOLOGICAL REPLAY. For each pool in topological order:
--       walk in-period events affecting the pool ordered by
--       (business_date, posted_at, sub_priority, id) where sub_priority
--       handles the qty-leg vs value-leg ordering for inv_value_wip pools
--       (see merged-stream subkey below). For each value-leg event:
--         * Inflow (debit on pool): pool_value += t.amount + cache;
--           pool_qty += t.qty for raw/fg pools (per-class pattern), or
--           handled by paired qty-leg event for inv_value_wip pools.
--         * Outflow (credit on pool): recompute amount at running avg;
--           variance = recompute - t.amount. For internal-chain reasons
--           (op_move_v, rm_issue_to_wo): record variance only,
--           variance_transfer_id = NULL. For leaf reasons on inv_value_wip
--           source: single-leg variance posting (tier 1 pattern). For
--           leaf reasons on raw/fg source: 2-leg wash (mig 0031 pattern).
--           Update pool_value -= recomputed_amt; pool_qty -= t.qty for
--           raw/fg.
--
--    c. MERGED VALUE/QTY STREAM FOR inv_value_wip POOLS. inv_value_wip
--       pool_qty must be sourced from stock_wip(parent, op) because
--       rm_issue_to_wo value-leg's transfers.qty stores component qty
--       (qty_per_parent × p_qty), not parent qty (acct-1vr / mig 0030
--       per-class pattern is ambiguous on WIP — addressed by tier 1
--       acct-bol mig 0064's _wac_close_pool_qty_in helper). Per-event
--       replay can't use a period-aggregate helper; instead, merge
--       value-leg events on the value pool with qty-leg events on the
--       paired stock_wip account into one chronological stream. Sort
--       sub-priority places:
--         * INFLOW qty-leg (debit on stock_wip) BEFORE INFLOW value-leg
--           (debit on inv_value_wip) — qty arrives before value averages.
--         * OUTFLOW value-leg (credit on inv_value_wip) BEFORE OUTFLOW
--           qty-leg (credit on stock_wip) — recompute uses pool_qty
--           BEFORE the qty-leg decrement.
--         Sub-priority: 0 = qty inflow, 1 = value (any direction),
--                       2 = qty outflow.
--       This yields the right semantics: at any value depletion the
--       running pool_qty reflects state before that event's paired
--       qty-leg fires.
--
--    d. UPSTREAM VARIANCE CACHE. For inflow value-leg events, augment
--       t.amount with COALESCE(transfers_provisional.variance_amount, 0)
--       filtered to (finalized_at IS NOT NULL AND variance_transfer_id
--       IS NULL). Topological order guarantees upstream pools are
--       finalized BEFORE this pool's replay; their internal-chain
--       variance_amount values flow into this pool's inflow values.
--       The variance_transfer_id IS NULL filter prevents double-counting
--       leaf-posted variance (the variance transfer is already a
--       separate t.amount summed in).
--
-- WHY rm_issue_to_wo IS INTERNAL-CHAIN ON WIP CHAIN. Same as tier 2
-- (acct-7py) reasoning. The rm_issue value-leg's destination pool is
-- inv_value_wip on a wac_retroactive parent, itself flagged via
-- op_move_v / wo_complete_v provisionals. Posting variance at the
-- rm_issue level would either push WIP destination negative on
-- net-negative variance (debit-normal CHECK), or break the cache by
-- double-counting. Internal-chain is correct: record variance,
-- propagate via cache to leaf wo_complete_v.
--
-- DEFENSIVE: post_inventory_adjustment / _post_transfers_compute_amount
-- still raise P0006 when wac_retroactive depletes from inv_value_wip
-- via canonical reasons (op_move/scrap/wo_complete/so_ship). The WO
-- path uses BOM2 *_v reasons (op_move_v/scrap_v/wo_complete_v) which
-- bypass the dispatcher's auto-cost branch entirely — caller-supplied
-- amounts plus the apply-step's flagging list (mig 0064/0067) cover
-- the WO path. The dispatcher gates remain dead code on WO flows;
-- they protect direct post_transfers callers and post_inventory_adjustment.
--
-- TESTS expected: tests/wac_retroactive_wip.rs (single-op + multi-op
-- chronological replay; drift via late-arriving receipt; mixed cost
-- methods raise P0026).

-- ============================================================
-- post_wo_start: lift the wac_retroactive parent gate.
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
  v_bom             bom_headers%ROWTYPE;
  v_bad_op          INT;
  v_alloc_sum       NUMERIC;
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

  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF v_wo.status <> 'draft' THEN
    RAISE EXCEPTION 'wo_invalid: WO % status=% not draft (already started)',
                    p_wo_id, v_wo.status USING ERRCODE = 'P0026';
  END IF;

  SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_wo.parent_sku_id;
  -- Tier 3 (acct-rso): wac_retroactive parents accepted; close hook
  -- chronologically replays the WIP pool chain in topological order.
  IF v_cost_method NOT IN ('standard', 'wac_perpetual', 'wac_periodic', 'wac_retroactive') THEN
    RAISE EXCEPTION
      'wo_invalid: parent_sku % has cost_method=% which post_wo_start '
      'does not handle',
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

  v_batch := v_batch || _wo_emit_bom_lines(
    p_wo_id, v_bom.id, v_first_op, v_wo.qty_target,
    jsonb_build_object('fire_at', 'wo_start'),
    v_event_id, p_business_date, p_posted_by
  );

  v_batch := v_batch || _wo_emit_bom_lines(
    p_wo_id, v_bom.id, v_first_op, v_wo.qty_target,
    jsonb_build_object('fire_at', 'op_arrival', 'applies_at_op', v_first_op),
    v_event_id, p_business_date, p_posted_by
  );

  PERFORM post_transfers(v_batch, FALSE);
  UPDATE work_orders SET status = 'released' WHERE id = p_wo_id;
  RETURN p_wo_id;
END;
$$;

COMMENT ON FUNCTION post_wo_start(UUID, DATE, UUID, UUID, TEXT) IS
  'WO lifecycle entry. Validates idempotency / status / BOM / routing / '
  'cost_method ∈ {standard, wac_perpetual, wac_periodic, wac_retroactive}. '
  'wac_retroactive parents accepted via tier 3 (acct-rso) topological + '
  'chronological replay close hook.';

-- ============================================================
-- post_op_move: extend wac branch to include wac_retroactive.
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
  v_value_amount     BIGINT;
  v_batch            JSONB := '[]'::JSONB;
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

    v_value_amount := p_qty * (v_per_unit_cum + v_per_lot_cum);

  ELSIF v_cost_method IN ('wac_perpetual', 'wac_periodic', 'wac_retroactive') THEN
    -- Lock the source value pool before reading its balance for the
    -- running avg. Tier 3 (acct-rso): wac_retroactive uses the same
    -- mid-period math as wac_perpetual / wac_periodic; the close hook
    -- chronologically replays the per-event chain at period close.
    PERFORM 1 FROM accounts WHERE id = v_val_from FOR UPDATE;
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
      'wo_invalid: parent_sku % has cost_method=% which post_op_move '
      'does not handle',
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

  IF v_first_arrival THEN
    v_batch := v_batch || _wo_emit_bom_lines(
      p_wo_id, v_bom.id, p_to_op, p_qty,
      jsonb_build_object('fire_at', 'op_arrival', 'applies_at_op', p_to_op),
      v_event_id, p_business_date, p_posted_by
    );
  ELSE
    v_batch := v_batch || _wo_emit_bom_lines(
      p_wo_id, v_bom.id, p_to_op, p_qty,
      jsonb_build_object('fire_at',        'op_arrival',
                         'applies_at_op',  p_to_op,
                         'basis',          'per_unit',
                         'kind',           'service'),
      v_event_id, p_business_date, p_posted_by
    );
  END IF;

  PERFORM post_transfers(v_batch, FALSE);
  RETURN p_wo_id;
END;
$$;

COMMENT ON FUNCTION post_op_move(UUID, INT, INT, BIGINT, DATE, UUID, UUID, TEXT) IS
  'Moves p_qty between routing ops. Dispatches on parent_sku.cost_method: '
  'standard → bom_lines std_cum (literal); wac_perpetual / wac_periodic / '
  'wac_retroactive → source pool running avg (FOR UPDATE on src val pool). '
  'wac_periodic op_move_v transfers re-amounted at close via topological '
  'per-pool recompute (acct-smn). wac_retroactive op_move_v re-amounted '
  'via per-event chronological replay (acct-rso).';

-- ============================================================
-- _wo_emit_bom_lines: lift wac_retroactive component gate.
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
  v_adj_qty              BIGINT;
  v_value                BIGINT;
  v_amount               BIGINT;
  v_reason               transfer_reason;
  v_comp_consumed        BIGINT;
  v_comp_qty_acct        BIGINT;
  v_comp_val_acct        BIGINT;
  v_applied_kind         account_kind;
  v_applied_acct         BIGINT;
  v_comp_std_cost        BIGINT;
  v_comp_cost_method     cost_method;
  v_parent_cost_method   cost_method;
  v_pool_qty             BIGINT;
  v_pool_value           BIGINT;
  v_unit                 BIGINT;
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

  SELECT cost_method INTO v_parent_cost_method
    FROM skus WHERE id = v_wo.parent_sku_id;

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
          v_comp_std_cost := resolve_standard_cost_at(
            v_line.component_sku_id, p_business_date
          );
          v_value := v_adj_qty * v_comp_std_cost;

        WHEN 'wac_perpetual' THEN
          PERFORM 1 FROM accounts WHERE id = v_comp_val_acct FOR UPDATE;
          SELECT COALESCE(SUM(
            CASE
              WHEN t.debit_account_id  = v_comp_val_acct THEN  t.qty
              WHEN t.credit_account_id = v_comp_val_acct THEN -t.qty
            END
          ), 0)
            INTO v_pool_qty
            FROM transfers t
           WHERE v_comp_val_acct IN (t.debit_account_id, t.credit_account_id)
             AND t.qty IS NOT NULL;

          IF v_pool_qty <= 0 THEN
            RAISE EXCEPTION
              'rm_issue_empty_pool: wac_perpetual component % at sku=% loc=% '
              'has empty inv_value_raw pool (per-class qty=%); cannot issue '
              '% units to WO %',
              v_line.component_sku_id, v_line.component_sku_id,
              v_line.component_loc_id, v_pool_qty, v_adj_qty, p_wo_id
              USING ERRCODE = 'P0010';
          END IF;

          SELECT (debits_total - credits_total) INTO v_pool_value
            FROM accounts WHERE id = v_comp_val_acct;
          v_unit  := GREATEST(COALESCE(v_pool_value, 0), 0) / v_pool_qty;
          v_value := v_adj_qty * v_unit;

        WHEN 'wac_periodic' THEN
          IF v_parent_cost_method <> 'wac_periodic' THEN
            RAISE EXCEPTION
              'rm_issue_mixed_cost_method: wac_periodic component % at '
              'sku=% requires wac_periodic parent (parent % has '
              'cost_method=%). Mixed parent/component cost methods '
              'deferred to acct-7eo.',
              v_line.component_sku_id, v_line.component_sku_id,
              v_wo.parent_sku_id, v_parent_cost_method
              USING ERRCODE = 'P0026';
          END IF;

          PERFORM 1 FROM accounts WHERE id = v_comp_val_acct FOR UPDATE;
          SELECT COALESCE(SUM(
            CASE
              WHEN t.debit_account_id  = v_comp_val_acct THEN  t.qty
              WHEN t.credit_account_id = v_comp_val_acct THEN -t.qty
            END
          ), 0)
            INTO v_pool_qty
            FROM transfers t
           WHERE v_comp_val_acct IN (t.debit_account_id, t.credit_account_id)
             AND t.qty IS NOT NULL;

          IF v_pool_qty <= 0 THEN
            RAISE EXCEPTION
              'rm_issue_empty_pool: wac_periodic component % at sku=% loc=% '
              'has empty inv_value_raw pool (per-class qty=%); cannot issue '
              '% units to WO %',
              v_line.component_sku_id, v_line.component_sku_id,
              v_line.component_loc_id, v_pool_qty, v_adj_qty, p_wo_id
              USING ERRCODE = 'P0010';
          END IF;

          SELECT (debits_total - credits_total) INTO v_pool_value
            FROM accounts WHERE id = v_comp_val_acct;
          v_unit  := GREATEST(COALESCE(v_pool_value, 0), 0) / v_pool_qty;
          v_value := v_adj_qty * v_unit;

        WHEN 'wac_retroactive' THEN
          -- Tier 3 (acct-rso): supported only when parent is also
          -- wac_retroactive. Mixed cost methods raise P0026; deferred
          -- to acct-7eo because the close-hook's chronological replay
          -- on rm_issue cannot propagate cleanly into a non-
          -- wac_retroactive destination WIP pool.
          IF v_parent_cost_method <> 'wac_retroactive' THEN
            RAISE EXCEPTION
              'rm_issue_mixed_cost_method: wac_retroactive component % at '
              'sku=% requires wac_retroactive parent (parent % has '
              'cost_method=%). Mixed parent/component cost methods '
              'deferred to acct-7eo.',
              v_line.component_sku_id, v_line.component_sku_id,
              v_wo.parent_sku_id, v_parent_cost_method
              USING ERRCODE = 'P0026';
          END IF;

          PERFORM 1 FROM accounts WHERE id = v_comp_val_acct FOR UPDATE;
          SELECT COALESCE(SUM(
            CASE
              WHEN t.debit_account_id  = v_comp_val_acct THEN  t.qty
              WHEN t.credit_account_id = v_comp_val_acct THEN -t.qty
            END
          ), 0)
            INTO v_pool_qty
            FROM transfers t
           WHERE v_comp_val_acct IN (t.debit_account_id, t.credit_account_id)
             AND t.qty IS NOT NULL;

          IF v_pool_qty <= 0 THEN
            RAISE EXCEPTION
              'rm_issue_empty_pool: wac_retroactive component % at sku=% loc=% '
              'has empty inv_value_raw pool (per-class qty=%); cannot issue '
              '% units to WO %',
              v_line.component_sku_id, v_line.component_sku_id,
              v_line.component_loc_id, v_pool_qty, v_adj_qty, p_wo_id
              USING ERRCODE = 'P0010';
          END IF;

          SELECT (debits_total - credits_total) INTO v_pool_value
            FROM accounts WHERE id = v_comp_val_acct;
          v_unit  := GREATEST(COALESCE(v_pool_value, 0), 0) / v_pool_qty;
          v_value := v_adj_qty * v_unit;

        WHEN 'fifo', 'lot' THEN
          RAISE EXCEPTION
            'cost_method_not_implemented: % for component % (acct-8gg)',
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
        'document_kind',     'wo_event',
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
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'rm_issue_to_wo',
          'document_kind',     'wo_event',
          'document_id',       p_event_id,
          'debit_account_id',  v_val_acct_wip,
          'credit_account_id', v_comp_val_acct,
          'amount',            v_value,
          'qty',               v_adj_qty,
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
  'Generic BOM-line emitter. Dispatches item value-leg cost on the '
  'COMPONENT''s cost_method. tier 1 (acct-24b): standard / wac_perpetual. '
  'tier 2 (acct-7py): wac_periodic — same running-avg math as wac_perpetual '
  'but only when parent is also wac_periodic. tier 3 (acct-rso): '
  'wac_retroactive — same shape, requires wac_retroactive parent. Mixed '
  'cost methods raise P0026 → acct-7eo.';

-- ============================================================
-- wac_retroactive_close_hook — topological + chronological + WIP.
-- ============================================================

CREATE OR REPLACE FUNCTION wac_retroactive_close_hook(
  p_period_id         BIGINT,
  p_force_provisional BOOLEAN DEFAULT FALSE
) RETURNS BIGINT LANGUAGE plpgsql AS $$
DECLARE
  v_period_opens   DATE;
  v_period_closes  DATE;
  v_period_code    TEXT;
  v_count          BIGINT := 0;
  v_pool_id        BIGINT;
  v_processed      BIGINT[] := ARRAY[]::BIGINT[];
  v_remaining      INT;
  v_progress       INT;
  v_cycle_pools    TEXT;
  v_pool_acct      accounts%ROWTYPE;
  v_qty_pool_id    BIGINT;
  v_pool_value     BIGINT;
  v_pool_qty       BIGINT;
  v_event          RECORD;
  v_recomputed_avg BIGINT;
  v_recomputed_amt BIGINT;
  v_variance       BIGINT;
  v_var_acct       BIGINT;
  v_batch          JSONB;
  v_var_xfer_id    BIGINT;
  v_event_a        JSONB;
  v_event_b        JSONB;
  v_orig_reason    transfer_reason;
BEGIN
  SELECT opens_at, closes_at, code
    INTO v_period_opens, v_period_closes, v_period_code
    FROM periods WHERE id = p_period_id;
  IF NOT FOUND THEN
    RETURN 0;
  END IF;

  -- Build pool set + edge set from in-period wac_retroactive flagged
  -- provisional rows. Same shape as wac_periodic tier 2 (acct-smn / acct-7py).

  CREATE TEMP TABLE _wac_retro_pools (
    pool_id BIGINT PRIMARY KEY
  ) ON COMMIT DROP;

  CREATE TEMP TABLE _wac_retro_edges (
    predecessor BIGINT,
    successor   BIGINT,
    PRIMARY KEY (predecessor, successor)
  ) ON COMMIT DROP;

  -- Pool set: credit_account_id of every wac_retroactive provisional row,
  -- plus debit_account_id of every internal-chain provisional
  -- (op_move_v + rm_issue_to_wo) so successor pools that have no own
  -- depletion-out still get processed.
  INSERT INTO _wac_retro_pools (pool_id)
  SELECT DISTINCT t.credit_account_id
    FROM transfers_provisional p
    JOIN transfers t ON t.id = p.transfer_id
   WHERE p.period_id = p_period_id
     AND p.cost_method = 'wac_retroactive'
     AND p.finalized_at IS NULL
  UNION
  SELECT DISTINCT t.debit_account_id
    FROM transfers_provisional p
    JOIN transfers t ON t.id = p.transfer_id
   WHERE p.period_id = p_period_id
     AND p.cost_method = 'wac_retroactive'
     AND p.finalized_at IS NULL
     AND t.reason IN ('op_move_v', 'rm_issue_to_wo')
  ON CONFLICT DO NOTHING;

  -- Edge set: op_move_v + rm_issue_to_wo transfers in-period drive the
  -- predecessor → successor relationships.
  INSERT INTO _wac_retro_edges (predecessor, successor)
  SELECT DISTINCT t.credit_account_id, t.debit_account_id
    FROM transfers_provisional p
    JOIN transfers t ON t.id = p.transfer_id
   WHERE p.period_id = p_period_id
     AND p.cost_method = 'wac_retroactive'
     AND p.finalized_at IS NULL
     AND t.reason IN ('op_move_v', 'rm_issue_to_wo')
  ON CONFLICT DO NOTHING;

  -- Iterative Kahn's algorithm. At each step, pick a pool with no
  -- unprocessed predecessor; process it; record processed; remove
  -- edges that now have a finished tail. If no pool has zero unprocessed
  -- predecessors and pools remain → cycle.
  LOOP
    SELECT COUNT(*) INTO v_remaining FROM _wac_retro_pools;
    EXIT WHEN v_remaining = 0;

    v_progress := 0;
    FOR v_pool_id IN
      SELECT wp.pool_id
        FROM _wac_retro_pools wp
       WHERE NOT EXISTS (
         SELECT 1 FROM _wac_retro_edges e
          WHERE e.successor = wp.pool_id
            AND e.predecessor IN (SELECT pool_id FROM _wac_retro_pools)
       )
    LOOP
      v_progress := v_progress + 1;

      SELECT * INTO v_pool_acct FROM accounts WHERE id = v_pool_id;

      -- Resolve qty pool: stock_wip(parent, op) for inv_value_wip,
      -- pool itself (per-class pattern) for raw / fg.
      IF v_pool_acct.kind = 'inv_value_wip' THEN
        v_qty_pool_id := _post_transfers_lookup_qty_account(v_pool_acct);
        IF v_qty_pool_id IS NULL THEN
          RAISE EXCEPTION
            'wac_retroactive_close: cannot resolve stock_wip qty account '
            'for inv_value_wip pool % (sku=% op=%)',
            v_pool_id, v_pool_acct.sku_id, v_pool_acct.routing_op
            USING ERRCODE = 'P0010';
        END IF;
      ELSE
        v_qty_pool_id := v_pool_id;
      END IF;

      -- Pre-period state. Value: signed sum on value pool (no cache —
      -- prior-period variances are already settled as separate transfers).
      -- Qty: signed sum on qty pool (stock_wip for WIP, value pool for raw/fg).
      SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = v_pool_id THEN  t.amount
                               WHEN t.credit_account_id = v_pool_id THEN -t.amount END), 0)
        INTO v_pool_value
        FROM transfers t
       WHERE v_pool_id IN (t.debit_account_id, t.credit_account_id)
         AND t.business_date < v_period_opens;

      SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = v_qty_pool_id THEN  t.qty
                               WHEN t.credit_account_id = v_qty_pool_id THEN -t.qty END), 0)
        INTO v_pool_qty
        FROM transfers t
       WHERE v_qty_pool_id IN (t.debit_account_id, t.credit_account_id)
         AND t.qty IS NOT NULL
         AND t.business_date < v_period_opens;

      -- In-period chronological replay. For inv_value_wip pools, merge
      -- value-leg events (on this pool) with qty-leg events (on the paired
      -- stock_wip account) into one stream. Sort by
      -- (business_date, posted_at, sub_priority, id) where sub_priority:
      --   * 0 = qty inflow  (debit on stock_wip)  — fires before value
      --   * 1 = value (any direction)             — uses pre-decrement qty
      --   * 2 = qty outflow (credit on stock_wip) — fires after value
      --
      -- For raw/fg pools, only value-leg events; sub_priority always 1
      -- since per-class pattern uses t.qty in same row.
      --
      -- Upstream variance cache: for inflow value-leg events, augment
      -- t.amount with COALESCE(p_cache.variance_amount, 0) filtered to
      -- (finalized AND variance_transfer_id IS NULL) — internal-chain
      -- variance from upstream pools processed earlier in topo order.
      -- The IS NULL filter excludes leaf-posted variance (already a
      -- separate t.amount summed in).

      FOR v_event IN
        WITH value_events AS (
          SELECT t.id,
                 CASE
                   WHEN t.debit_account_id = v_pool_id
                        THEN t.amount + COALESCE(p_cache.variance_amount, 0)
                   ELSE t.amount
                 END AS adj_amount,
                 t.amount AS orig_amount,
                 t.qty,
                 t.debit_account_id,
                 t.credit_account_id,
                 t.business_date,
                 t.posted_at,
                 t.document_id,
                 t.reason,
                 (p_my.transfer_id IS NOT NULL) AS is_prov,
                 1 AS sub_priority,
                 'value'::TEXT AS leg
            FROM transfers t
            LEFT JOIN transfers_provisional p_cache
                   ON p_cache.transfer_id = t.id
                  AND p_cache.finalized_at IS NOT NULL
                  AND p_cache.variance_transfer_id IS NULL
            LEFT JOIN transfers_provisional p_my
                   ON p_my.transfer_id = t.id
                  AND p_my.cost_method = 'wac_retroactive'
                  AND p_my.finalized_at IS NULL
                  AND t.credit_account_id = v_pool_id
           WHERE v_pool_id IN (t.debit_account_id, t.credit_account_id)
             AND t.business_date BETWEEN v_period_opens AND v_period_closes
        ),
        qty_events AS (
          SELECT t.id,
                 t.amount AS adj_amount,
                 t.amount AS orig_amount,
                 t.qty,
                 t.debit_account_id,
                 t.credit_account_id,
                 t.business_date,
                 t.posted_at,
                 t.document_id,
                 t.reason,
                 FALSE AS is_prov,
                 CASE WHEN t.debit_account_id = v_qty_pool_id THEN 0 ELSE 2 END AS sub_priority,
                 'qty'::TEXT AS leg
            FROM transfers t
           WHERE v_pool_acct.kind = 'inv_value_wip'
             AND v_qty_pool_id <> v_pool_id
             AND v_qty_pool_id IN (t.debit_account_id, t.credit_account_id)
             AND t.qty IS NOT NULL
             AND t.business_date BETWEEN v_period_opens AND v_period_closes
        ),
        merged AS (
          SELECT * FROM value_events
          UNION ALL
          SELECT * FROM qty_events
        ),
        ordered AS (
          -- doc_chrono = MIN(posted_at) within document_id (= the wo_event
          -- UUID for WO flows) groups the qty-leg and value-leg of one
          -- logical event under a single chronological anchor. Inside a
          -- group, sub_priority + id determine processing order:
          --   sub_priority 0 (qty inflow on stock_wip)  before
          --   sub_priority 1 (value-leg, any direction) before
          --   sub_priority 2 (qty outflow on stock_wip).
          -- This handles the wo_complete case where the qty-leg and value-leg
          -- are inserted as separate INSERTs with sequential clock_timestamp()
          -- and would otherwise sort by their individual posted_at, putting
          -- the qty-leg outflow ahead of the value-leg outflow and zeroing
          -- pool_qty before recompute.
          SELECT *,
                 MIN(posted_at) OVER (PARTITION BY document_id) AS doc_chrono
            FROM merged
        )
        SELECT * FROM ordered
        ORDER BY business_date, doc_chrono, document_id, sub_priority, id
      LOOP
        IF v_event.leg = 'qty' THEN
          -- WIP-only qty-leg event (stock_wip touched, not the value pool).
          IF v_event.debit_account_id = v_qty_pool_id THEN
            v_pool_qty := v_pool_qty + v_event.qty;
          ELSE
            v_pool_qty := v_pool_qty - v_event.qty;
          END IF;
          CONTINUE;
        END IF;

        -- Value-leg event.
        IF v_event.debit_account_id = v_pool_id THEN
          -- Inflow: receipt or upstream variance correction.
          v_pool_value := v_pool_value + v_event.adj_amount;
          -- For raw/fg pools (non-WIP), pool_qty tracked via per-class
          -- pattern on this same row's t.qty. WIP pools have qty handled
          -- by separate qty-leg events above.
          IF v_pool_acct.kind <> 'inv_value_wip' AND v_event.qty IS NOT NULL THEN
            v_pool_qty := v_pool_qty + v_event.qty;
          END IF;
          CONTINUE;
        END IF;

        -- Outflow: depletion. Recompute at running avg.
        IF v_event.qty IS NULL THEN
          v_pool_value := v_pool_value - v_event.orig_amount;
          CONTINUE;
        END IF;

        IF v_pool_qty <= 0 THEN
          IF p_force_provisional AND v_event.is_prov THEN
            CONTINUE;
          END IF;
          RAISE EXCEPTION
            'wac_retroactive_replay_pool_empty: period % (id=%) pool kind=% sku=% '
            'loc=% op=% ccy=%: running qty went non-positive at depletion of transfer %; '
            'this indicates the perpetual chain has an inconsistency (more depletions '
            'than receipts of valid age). Pass p_force_provisional=TRUE to skip this row.',
            v_period_code, p_period_id, v_pool_acct.kind, v_pool_acct.sku_id,
            v_pool_acct.location_id, v_pool_acct.routing_op, v_pool_acct.currency,
            v_event.id
            USING ERRCODE = 'P0006';
        END IF;

        v_recomputed_avg := v_pool_value / v_pool_qty;
        v_recomputed_amt := v_event.qty * v_recomputed_avg;

        IF v_event.is_prov THEN
          v_variance    := v_recomputed_amt - v_event.orig_amount;
          v_orig_reason := v_event.reason;

          -- Internal-chain (op_move_v / rm_issue_to_wo): record variance,
          -- DO NOT post a variance transfer. Cost shift propagates via
          -- the LEFT JOIN cache when downstream pools compute their
          -- inflow value.
          IF v_orig_reason IN ('op_move_v', 'rm_issue_to_wo') THEN
            UPDATE transfers_provisional
               SET finalized_at = clock_timestamp(),
                   variance_amount = v_variance,
                   variance_transfer_id = NULL
             WHERE transfer_id = v_event.id;
            v_count := v_count + 1;
          ELSIF v_variance = 0 THEN
            UPDATE transfers_provisional
               SET finalized_at = clock_timestamp(),
                   variance_amount = 0,
                   variance_transfer_id = NULL
             WHERE transfer_id = v_event.id;
            v_count := v_count + 1;
          ELSE
            -- Leaf depletion (wo_complete_v / scrap_v / so_ship etc.).
            -- Single-leg variance for inv_value_wip source (tier 1 pattern);
            -- 2-leg wash for raw/fg source (mig 0031 pattern).
            SELECT id INTO v_var_acct FROM accounts
             WHERE kind = 'variance_wac_retroactive' AND ledger_kind = 'value'
               AND currency = v_pool_acct.currency AND NOT is_closed;
            IF v_var_acct IS NULL THEN
              RAISE EXCEPTION
                'wac_retroactive_close: no variance_wac_retroactive(value, ccy=%) account configured',
                v_pool_acct.currency USING ERRCODE = 'P0010';
            END IF;

            IF v_pool_acct.kind = 'inv_value_wip' THEN
              IF v_variance > 0 THEN
                v_event_a := jsonb_build_object(
                  'reason',            'cost_restate',
                  'document_kind',     'wac_retroactive_close',
                  'document_id',       gen_random_uuid(),
                  'debit_account_id',  v_event.debit_account_id,
                  'credit_account_id', v_var_acct,
                  'amount',            v_variance,
                  'business_date',     v_period_closes,
                  'idempotency_key',   gen_random_uuid(),
                  'posted_by',         '00000000-0000-0000-0000-000000000000'
                );
              ELSE
                v_event_a := jsonb_build_object(
                  'reason',            'cost_restate',
                  'document_kind',     'wac_retroactive_close',
                  'document_id',       gen_random_uuid(),
                  'debit_account_id',  v_var_acct,
                  'credit_account_id', v_event.debit_account_id,
                  'amount',            -v_variance,
                  'business_date',     v_period_closes,
                  'idempotency_key',   gen_random_uuid(),
                  'posted_by',         '00000000-0000-0000-0000-000000000000'
                );
              END IF;
              v_batch := jsonb_build_array(v_event_a);
            ELSE
              IF v_variance > 0 THEN
                v_event_a := jsonb_build_object(
                  'reason',            'cost_restate',
                  'document_kind',     'wac_retroactive_close',
                  'document_id',       gen_random_uuid(),
                  'debit_account_id',  v_event.debit_account_id,
                  'credit_account_id', v_var_acct,
                  'amount',            v_variance,
                  'business_date',     v_period_closes,
                  'idempotency_key',   gen_random_uuid(),
                  'posted_by',         '00000000-0000-0000-0000-000000000000'
                );
                v_event_b := jsonb_build_object(
                  'reason',            'cost_restate',
                  'document_kind',     'wac_retroactive_close',
                  'document_id',       gen_random_uuid(),
                  'debit_account_id',  v_var_acct,
                  'credit_account_id', v_pool_id,
                  'amount',            v_variance,
                  'business_date',     v_period_closes,
                  'idempotency_key',   gen_random_uuid(),
                  'posted_by',         '00000000-0000-0000-0000-000000000000'
                );
              ELSE
                v_event_a := jsonb_build_object(
                  'reason',            'cost_restate',
                  'document_kind',     'wac_retroactive_close',
                  'document_id',       gen_random_uuid(),
                  'debit_account_id',  v_var_acct,
                  'credit_account_id', v_event.debit_account_id,
                  'amount',            -v_variance,
                  'business_date',     v_period_closes,
                  'idempotency_key',   gen_random_uuid(),
                  'posted_by',         '00000000-0000-0000-0000-000000000000'
                );
                v_event_b := jsonb_build_object(
                  'reason',            'cost_restate',
                  'document_kind',     'wac_retroactive_close',
                  'document_id',       gen_random_uuid(),
                  'debit_account_id',  v_pool_id,
                  'credit_account_id', v_var_acct,
                  'amount',            -v_variance,
                  'business_date',     v_period_closes,
                  'idempotency_key',   gen_random_uuid(),
                  'posted_by',         '00000000-0000-0000-0000-000000000000'
                );
              END IF;
              v_batch := jsonb_build_array(v_event_a, v_event_b);
            END IF;

            PERFORM post_transfers(v_batch, TRUE);

            SELECT id INTO v_var_xfer_id
              FROM transfers
             WHERE idempotency_key = (v_event_a->>'idempotency_key')::UUID;

            UPDATE transfers_provisional
               SET finalized_at = clock_timestamp(),
                   variance_amount = v_variance,
                   variance_transfer_id = v_var_xfer_id
             WHERE transfer_id = v_event.id;
            v_count := v_count + 1;
          END IF;

          -- Update running with recomputed amount.
          v_pool_value := v_pool_value - v_recomputed_amt;
          IF v_pool_acct.kind <> 'inv_value_wip' THEN
            v_pool_qty := v_pool_qty - v_event.qty;
          END IF;
        ELSE
          -- Non-provisional credit on this pool (e.g., from a different
          -- cost_method's flagged depletion, or a finalized prior-pass row
          -- that physically posted a variance transfer counted via the
          -- cache's variance_transfer_id IS NOT NULL — actually those are
          -- excluded, so this is a non-flagged credit). Update running
          -- state with original amount.
          v_pool_value := v_pool_value - v_event.orig_amount;
          IF v_pool_acct.kind <> 'inv_value_wip' THEN
            v_pool_qty := v_pool_qty - v_event.qty;
          END IF;
        END IF;
      END LOOP;

      DELETE FROM _wac_retro_pools WHERE pool_id = v_pool_id;
      v_processed := array_append(v_processed, v_pool_id);
    END LOOP;

    IF v_progress = 0 THEN
      SELECT string_agg(pool_id::TEXT, ', ' ORDER BY pool_id)
        INTO v_cycle_pools
        FROM _wac_retro_pools;
      RAISE EXCEPTION
        'wac_retroactive_pool_cycle: period % (id=%) has rework cycles in '
        'wac_retroactive op_move_v / rm_issue_to_wo flow involving pools [%]; '
        'iterative-fixed-point handling deferred to acct-p7v-rework.',
        v_period_code, p_period_id, v_cycle_pools
        USING ERRCODE = 'P0036';
    END IF;
  END LOOP;

  RETURN v_count;
END;
$$;

COMMENT ON FUNCTION wac_retroactive_close_hook(BIGINT, BOOLEAN) IS
  'wac_retroactive period-close recompute. Tier 3 (acct-rso): topological '
  'pool walk over op_move_v / rm_issue_to_wo edges + per-pool chronological '
  'event replay merging value-leg events (on the value pool) with qty-leg '
  'events (on paired stock_wip for inv_value_wip pools) into one stream '
  'sorted by (business_date, posted_at, sub_priority, id) where sub_priority '
  'orders qty inflows BEFORE value events BEFORE qty outflows. Upstream '
  'variances flow via LEFT JOIN cache (variance_transfer_id IS NULL filter). '
  'Internal-chain reasons (op_move_v / rm_issue_to_wo) record variance only; '
  'leaf depletions on inv_value_wip source post single-leg variance, on '
  'raw/fg source post 2-leg wash. Rework cycles raise P0036.';
