-- acct-bol — Tier 1 of acct-8in: enable wac_periodic + wac_retroactive
-- on single-routing-op WO parents. Multi-op routing on these methods
-- still raises P0026 (deferred to acct-smn / acct-rso for the recursive
-- cost-chain piece).
--
-- WHAT CHANGES.
--
-- 1. _post_transfers_apply_event: flagging condition extends to BOM2
--    *_v reasons (op_move_v / wo_complete_v / scrap_v) and self-resolves
--    cost_method when post_transfers passes NULL. The reason: BOM2
--    entry points use *_v reasons to bypass the dispatcher (caller-
--    supplied amount stands), so post_transfers' two-pass detection
--    skips them and cost_method never reaches apply_event. The close
--    hook needs transfers_provisional rows on these depletions to
--    recompute at period close.
--
-- 2. post_wo_start: accepts wac_periodic / wac_retroactive ONLY when
--    wo_routings has exactly 1 op. Multi-op routings on these methods
--    raise P0026 with hint to acct-smn (tier 2) / acct-rso (tier 3).
--    Single-op = leaf-receipts only (no upstream WIP); existing
--    wac_periodic_close_hook / wac_retroactive_close_hook per-pool
--    walks work unchanged.
--
-- 3. post_wo_complete: cost_method dispatch extends to wac_periodic +
--    wac_retroactive — same code as the wac_perpetual branch added in
--    0063. Read source-pool running avg for total_drain.
--
-- DEFENSIVE: post_op_move's wac_periodic / wac_retroactive branch is
-- left raising P0026 (tier 1's single-op gate prevents op_move from
-- being called validly anyway).
--
-- NO DISPATCHER CHANGES. _post_transfers_compute_amount's
-- wac_periodic + wac_retroactive guards on inv_value_wip stay (tier 2
-- lifts them when canonical reasons need to dispatch on inv_value_wip).
-- BOM2 *_v reasons are not in the dispatcher's reason list; the guard
-- never fires for the BOM2 path.

-- ============================================================
-- _wac_close_pool_qty_in — pool_qty_in dispatch on pool.kind
-- ============================================================
-- For inv_value_wip pools, the per-class qty pattern from migration
-- 0030 (acct-1vr) breaks: rm_issue_to_wo value-leg stores qty =
-- qty_per_parent × p_qty (component qty consumed), not parent qty
-- received. So SUM(transfers.qty) on debits to inv_value_wip(parent,
-- op) returns component qty, which doesn't match the depletion's
-- parent qty divisor and produces wrong final_avg.
--
-- For raw / fg pools the per-class pattern stays correct (each
-- transfer's qty is class-tagged for the value pool). For WIP pools,
-- read parent qty inflows from the matching stock_wip account
-- (stock_wip(parent, op) is per-(sku, op), no class sharing).

CREATE OR REPLACE FUNCTION _wac_close_pool_qty_in(
  p_pool_acct      accounts,
  p_period_opens   DATE,
  p_period_closes  DATE
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_qty_acct_id  BIGINT;
  v_qty_in       BIGINT;
BEGIN
  IF p_pool_acct.kind = 'inv_value_wip' THEN
    v_qty_acct_id := _post_transfers_lookup_qty_account(p_pool_acct);
    IF v_qty_acct_id IS NULL THEN
      RETURN NULL;
    END IF;
    SELECT COALESCE(SUM(t.qty), 0) INTO v_qty_in
      FROM transfers t
     WHERE t.debit_account_id = v_qty_acct_id
       AND t.business_date BETWEEN p_period_opens AND p_period_closes
       AND t.qty IS NOT NULL;
    RETURN v_qty_in;
  ELSE
    SELECT COALESCE(SUM(t.qty), 0) INTO v_qty_in
      FROM transfers t
     WHERE t.debit_account_id = p_pool_acct.id
       AND t.business_date BETWEEN p_period_opens AND p_period_closes
       AND t.qty IS NOT NULL;
    RETURN v_qty_in;
  END IF;
END;
$$;

COMMENT ON FUNCTION _wac_close_pool_qty_in(accounts, DATE, DATE) IS
  'Returns Σ(qty) of in-period inflows to a WAC value pool. For '
  'inv_value_wip pools reads from the matching stock_wip account '
  '(parent qty); for raw/fg pools uses the per-class pattern from '
  'acct-1vr (transfers.qty on the value pool itself).';

-- ============================================================
-- wac_periodic_close_hook — use _wac_close_pool_qty_in
-- ============================================================
-- Replaces only the qty_in inline SELECT with the helper call. Rest
-- of the body unchanged from migration 0030.

CREATE OR REPLACE FUNCTION wac_periodic_close_hook(
  p_period_id         BIGINT,
  p_force_provisional BOOLEAN DEFAULT FALSE
) RETURNS BIGINT LANGUAGE plpgsql AS $$
DECLARE
  v_period_opens   DATE;
  v_period_closes  DATE;
  v_period_code    TEXT;
  v_count          BIGINT := 0;
  v_row            RECORD;
  v_orig           RECORD;
  v_pool_acct      accounts%ROWTYPE;
  v_pool_value_in  BIGINT;
  v_pool_qty_in    BIGINT;
  v_final_avg      BIGINT;
  v_provisional    BIGINT;
  v_variance       BIGINT;
  v_var_acct       BIGINT;
  v_batch          JSONB;
  v_var_xfer_id    BIGINT;
  v_orig_debit     BIGINT;
  v_orig_credit    BIGINT;
  v_event_a        JSONB;
  v_event_b        JSONB;
BEGIN
  SELECT opens_at, closes_at, code
    INTO v_period_opens, v_period_closes, v_period_code
    FROM periods WHERE id = p_period_id;
  IF NOT FOUND THEN
    RETURN 0;
  END IF;

  FOR v_row IN
    SELECT *
      FROM transfers_provisional
     WHERE period_id = p_period_id
       AND cost_method = 'wac_periodic'
       AND finalized_at IS NULL
     ORDER BY transfer_id
       FOR UPDATE
  LOOP
    SELECT * INTO v_orig FROM transfers WHERE id = v_row.transfer_id;
    SELECT * INTO v_pool_acct
      FROM accounts WHERE id = v_orig.credit_account_id;

    SELECT COALESCE(SUM(amount), 0) INTO v_pool_value_in
      FROM transfers
     WHERE debit_account_id = v_pool_acct.id
       AND business_date BETWEEN v_period_opens AND v_period_closes;

    v_pool_qty_in := _wac_close_pool_qty_in(
      v_pool_acct, v_period_opens, v_period_closes
    );
    IF v_pool_qty_in IS NULL THEN
      RAISE EXCEPTION
        'wac_periodic_close: cannot resolve qty account for value pool %',
        v_pool_acct.id USING ERRCODE = 'P0010';
    END IF;

    IF v_pool_qty_in = 0 THEN
      IF p_force_provisional THEN
        CONTINUE;
      END IF;
      RAISE EXCEPTION
        'wac_periodic_close_no_receipts: period % (id=%) has provisional '
        'depletions on pool kind=% sku=% loc=% ccy=% but zero receipts in '
        'period; post receipts and retry the close, or close with '
        'p_force_provisional=TRUE.',
        v_period_code, p_period_id, v_pool_acct.kind, v_pool_acct.sku_id,
        v_pool_acct.location_id, v_pool_acct.currency
        USING ERRCODE = 'P0020';
    END IF;

    v_final_avg := v_pool_value_in / v_pool_qty_in;
    v_provisional := v_orig.amount / v_row.qty;
    v_variance    := (v_final_avg - v_provisional) * v_row.qty;

    IF v_variance = 0 THEN
      UPDATE transfers_provisional
         SET finalized_at = clock_timestamp(),
             variance_amount = 0,
             variance_transfer_id = NULL
       WHERE transfer_id = v_row.transfer_id;
      v_count := v_count + 1;
      CONTINUE;
    END IF;

    SELECT id INTO v_var_acct
      FROM accounts
     WHERE kind = 'variance_wac_period'
       AND ledger_kind = 'value'
       AND currency = v_pool_acct.currency
       AND NOT is_closed;
    IF v_var_acct IS NULL THEN
      RAISE EXCEPTION
        'wac_periodic_close: no variance_wac_period(value, ccy=%) account configured',
        v_pool_acct.currency USING ERRCODE = 'P0010';
    END IF;

    v_orig_debit  := v_orig.debit_account_id;
    v_orig_credit := v_orig.credit_account_id;

    -- For inv_value_wip source pools, skip the second wash leg
    -- (cr/dr orig_credit). The WIP pool is typically fully drained at
    -- WO close — washing variance through it would take the pool
    -- negative (debit-normal CHECK violation) when multi-WO drift
    -- produces signed-mixed variances. Variance routes directly between
    -- orig_debit (output FG) and variance_wac_period; ledger balanced
    -- per posting; variance_wac_period accumulates the period-level
    -- WIP costing variance.
    IF v_pool_acct.kind = 'inv_value_wip' THEN
      IF v_variance > 0 THEN
        v_event_a := jsonb_build_object(
          'reason',            'cost_restate',
          'document_kind',     'wac_periodic_close',
          'document_id',       gen_random_uuid(),
          'debit_account_id',  v_orig_debit,
          'credit_account_id', v_var_acct,
          'amount',            v_variance,
          'business_date',     v_period_closes,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         '00000000-0000-0000-0000-000000000000'
        );
      ELSE
        v_event_a := jsonb_build_object(
          'reason',            'cost_restate',
          'document_kind',     'wac_periodic_close',
          'document_id',       gen_random_uuid(),
          'debit_account_id',  v_var_acct,
          'credit_account_id', v_orig_debit,
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
          'document_kind',     'wac_periodic_close',
          'document_id',       gen_random_uuid(),
          'debit_account_id',  v_orig_debit,
          'credit_account_id', v_var_acct,
          'amount',            v_variance,
          'business_date',     v_period_closes,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         '00000000-0000-0000-0000-000000000000'
        );
        v_event_b := jsonb_build_object(
          'reason',            'cost_restate',
          'document_kind',     'wac_periodic_close',
          'document_id',       gen_random_uuid(),
          'debit_account_id',  v_var_acct,
          'credit_account_id', v_orig_credit,
          'amount',            v_variance,
          'business_date',     v_period_closes,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         '00000000-0000-0000-0000-000000000000'
        );
      ELSE
        v_event_a := jsonb_build_object(
          'reason',            'cost_restate',
          'document_kind',     'wac_periodic_close',
          'document_id',       gen_random_uuid(),
          'debit_account_id',  v_var_acct,
          'credit_account_id', v_orig_debit,
          'amount',            -v_variance,
          'business_date',     v_period_closes,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         '00000000-0000-0000-0000-000000000000'
        );
        v_event_b := jsonb_build_object(
          'reason',            'cost_restate',
          'document_kind',     'wac_periodic_close',
          'document_id',       gen_random_uuid(),
          'debit_account_id',  v_orig_credit,
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
     WHERE transfer_id = v_row.transfer_id;
    v_count := v_count + 1;
  END LOOP;

  RETURN v_count;
END;
$$;

-- ============================================================
-- _post_transfers_apply_event — extended flagging
-- ============================================================

CREATE OR REPLACE FUNCTION _post_transfers_apply_event(
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
  v_period_id     BIGINT;
  v_period_closed TIMESTAMPTZ;
  v_business_date DATE;
  v_qty_for_row   BIGINT;
  v_reason        transfer_reason;
  v_idem_key      UUID;
  v_new_id        BIGINT;
  v_event_qty     BIGINT;
  v_resolved_cm   cost_method;
  v_cost_sku      UUID;
BEGIN
  v_business_date := (p_event->>'business_date')::DATE;
  v_reason        := (p_event->>'reason')::transfer_reason;
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
  INSERT INTO transfers (
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
  -- Extended (acct-bol) to include BOM2 *_v reasons; resolves cost_method
  -- on-demand when caller (post_transfers) passes NULL — which it does
  -- for *_v events since they bypass the dispatcher's reason list.
  IF v_reason IN ('op_move','scrap','wo_complete','so_ship',
                  'op_move_v','scrap_v','wo_complete_v')
     AND p_d_acct.ledger_kind = 'value' THEN
    v_resolved_cm := p_cost_method;
    IF v_resolved_cm IS NULL THEN
      v_cost_sku := COALESCE(p_d_acct.sku_id, p_c_acct.sku_id);
      IF v_cost_sku IS NOT NULL THEN
        SELECT cost_method INTO v_resolved_cm FROM skus WHERE id = v_cost_sku;
      END IF;
    END IF;
    IF v_resolved_cm IN ('wac_periodic', 'wac_retroactive') THEN
      v_event_qty := (p_event->>'qty')::BIGINT;
      INSERT INTO transfers_provisional (transfer_id, period_id, cost_method, qty)
      VALUES (v_new_id, v_period_id, v_resolved_cm, v_event_qty);
    END IF;
  END IF;

  RETURN v_new_id;
END;
$$;

COMMENT ON FUNCTION _post_transfers_apply_event(JSONB, INT, BIGINT, accounts, accounts, cost_method, BOOLEAN) IS
  'Single-event apply step. Extended (acct-bol) to flag BOM2 *_v '
  'depletions (op_move_v / wo_complete_v / scrap_v) into '
  'transfers_provisional when source SKU is wac_periodic / '
  'wac_retroactive. Resolves cost_method on-demand when the caller '
  'passes NULL (which post_transfers does for *_v reasons since they '
  'are not in the dispatcher''s cost-event list).';

-- ============================================================
-- post_wo_start — gate wac_periodic / wac_retroactive to single-op
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
  IF v_cost_method NOT IN ('standard', 'wac_perpetual', 'wac_periodic') THEN
    -- wac_retroactive on WIP defers to acct-rso (tier 3): its per-event
    -- chronological replay reads transfers.qty from value-pool events,
    -- which carries component qty (qty_per_parent × p_qty) for
    -- rm_issue_to_wo — wrong for the parent-qty divisor. Fixing
    -- requires merging qty-side events into the replay walk; that's
    -- tier 3 surgery alongside the multi-op recursive cost-chain.
    RAISE EXCEPTION
      'wo_invalid: parent_sku % has cost_method=%; ''wac_retroactive'' '
      'parents are not supported on WO. wac_retroactive on WIP requires '
      'the chronological-replay refactor (acct-rso, tier 3 of acct-8in).',
      v_wo.parent_sku_id, v_cost_method USING ERRCODE = 'P0026';
  END IF;

  SELECT MIN(routing_op), COUNT(*) INTO v_first_op, v_op_count
    FROM wo_routings WHERE wo_id = p_wo_id;
  IF v_op_count = 0 THEN
    RAISE EXCEPTION 'wo_invalid: WO % has no routing operations', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;

  -- Tier 1 (acct-bol): wac_periodic parents require single-op routings
  -- until the topological per-pool close hook recompute lands
  -- (acct-smn, tier 2). Multi-op = recursive cost-chain across pools.
  IF v_cost_method = 'wac_periodic' AND v_op_count > 1 THEN
    RAISE EXCEPTION
      'wo_invalid: parent_sku % has cost_method=% with %-op routing; '
      'multi-op routing on wac_periodic parents requires the topological '
      'per-pool close hook recompute (acct-smn, tier 2 of acct-8in). '
      'Use single-op routing for tier 1 (acct-bol).',
      v_wo.parent_sku_id, v_cost_method, v_op_count
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
  'Starts a WO. Accepts parent_sku.cost_method ∈ {standard, wac_perpetual, '
  'wac_periodic, wac_retroactive}. wac_periodic / wac_retroactive parents '
  'require single-op routings (acct-bol tier 1); multi-op on these methods '
  'raises P0026 (deferred to acct-smn / acct-rso).';

-- ============================================================
-- post_wo_complete — extend dispatch to wac_periodic / wac_retroactive
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
  v_residual       BIGINT;
  v_batch          JSONB := '[]'::JSONB;
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

  SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_wo.parent_sku_id;

  IF v_cost_method = 'standard' THEN
    v_parent_std  := resolve_standard_cost_at(v_wo.parent_sku_id, p_business_date);
    v_total_drain := p_qty * v_parent_std;

  ELSIF v_cost_method IN ('wac_perpetual', 'wac_periodic', 'wac_retroactive') THEN
    -- Lock the last-op WIP value pool; running avg drives the drain.
    -- For wac_periodic / wac_retroactive the running avg here is
    -- provisional — close hook recomputes at period close.
    PERFORM 1 FROM accounts WHERE id = v_val_from FOR UPDATE;
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
      'wo_invalid: parent_sku % has cost_method=% which post_wo_complete '
      'does not handle',
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

  IF v_will_close THEN
    IF v_cost_method = 'standard' THEN
      PERFORM 1 FROM accounts WHERE id = v_val_from FOR UPDATE;
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
END;
$$;

COMMENT ON FUNCTION post_wo_complete(UUID, BIGINT, DATE, UUID, UUID, TEXT) IS
  'Closes a WO. Dispatches total_drain on parent_sku.cost_method: '
  'standard → qty × resolve_standard_cost_at(parent); wac_perpetual / '
  'wac_periodic / wac_retroactive → qty × pool running avg at last_op. '
  'For wac_periodic / wac_retroactive the running avg is provisional; '
  'close hook (wac_periodic_close_hook / wac_retroactive_close_hook) '
  'recomputes from in-period receipts at period close and posts variance.';
