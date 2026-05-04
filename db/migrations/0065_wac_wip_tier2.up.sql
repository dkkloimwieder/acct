-- acct-smn — Tier 2 of acct-8in: enable wac_periodic on multi-op WO
-- routings via topological per-pool close-hook recompute.
--
-- WHAT CHANGES.
--
-- 1. post_wo_start: drops the single-op gate for wac_periodic. Multi-op
--    routings on wac_periodic parents are now accepted; the close hook
--    handles the recursive cost-chain across pools.
--
-- 2. post_op_move: adds a wac_periodic branch. Identical math to the
--    wac_perpetual branch (read source pool running avg under FOR
--    UPDATE), but the resulting op_move_v transfer is flagged into
--    transfers_provisional (already handled by _post_transfers_apply_event
--    since tier 1 / mig 0064). At close, the close hook re-amounts each
--    op_move_v by walking pools in topological order and accumulating
--    upstream variance into downstream final_avg.
--
-- 3. wac_periodic_close_hook: full rewrite. Topological pool walk:
--      a. Build edge set: predecessor → successor for each in-period
--         op_move_v transfer flagged on a wac_periodic parent.
--      b. Build pool set: union of predecessor / successor / depletion-
--         credit accounts on flagged rows.
--      c. Iterative Kahn's algorithm to topologically sort. Cycle → P0036
--         (rework loop; deferred to acct-p7v-rework).
--      d. Walk pools in topological order. For each pool P:
--           - corrected_value_in = SUM over debit transfers t to P:
--               t.amount + COALESCE(p.variance_amount, 0)
--             where p is the matching transfers_provisional row (NULL if
--             not flagged or not yet finalized). Topological order
--             guarantees upstream provisionals are finalized first, so
--             COALESCE picks up the upstream variance correction.
--           - qty_in via _wac_close_pool_qty_in.
--           - final_avg = corrected_value_in / qty_in.
--           - For each provisional row with credit_account = P:
--               * variance = (final_avg − provisional_unit) × qty.
--               * If reason='op_move_v' (internal-chain): record
--                 variance_amount on transfers_provisional, finalize,
--                 DO NOT post a variance transfer. The cost shift
--                 propagates to downstream pools via the LEFT JOIN cache.
--               * Else (leaf depletion: wo_complete_v / scrap_v / so_ship):
--                 single-leg variance posting via tier 1 pattern (variance
--                 routes between v_orig_debit and variance_wac_period;
--                 source WIP pool untouched). Raw/fg sources keep the 2-leg
--                 wash from migration 0030 / 0064.
--
-- WHY NOT POST op_move_v VARIANCE.
-- For an op_move_v row, both source and destination accounts are
-- inv_value_wip pools, both debit-normal, both typically drained at WO
-- close. Posting a variance correction:
--   * 2-leg wash through source: pushes drained source negative on
--     signed-mixed multi-WO drift (debit-normal CHECK violation).
--   * Single-leg into destination: pushes drained destination negative
--     on net-negative variance (same problem).
-- The cleanest fix is to skip the posting and let the cost shift flow
-- via the LEFT JOIN on transfers_provisional.variance_amount when
-- computing downstream pool's corrected_value_in. The leaf depletion
-- (wo_complete_v) lands on FG (which retains balance) and captures the
-- entire chain's cumulative variance via final_avg(last_op).
--
-- LEDGER INVARIANCE.
-- inv_value_wip pools: each is debited at provisional amount and credited
-- at provisional amount; all op_move_v / rm_issue_to_wo / wo_complete_v
-- transfers stay at their original amounts. Pool balance unchanged by
-- the close hook.
-- FG: corrected by leaf variance postings (single-leg).
-- variance_wac_period: accumulates the period-level WAC adjustment plus
-- integer truncation residue.
--
-- REWORK CYCLES.
-- A wac_periodic WO with op_move(op20 → op10) creates a cycle in the
-- pool DAG. Topological sort detects this; raises P0036
-- ('wac_periodic_pool_cycle') and aborts the close. Iterative-fixed-
-- point handling deferred to acct-p7v-rework. Standard / wac_perpetual
-- WOs doing rework are unaffected (their op_moves don't appear in
-- transfers_provisional).

-- ============================================================
-- transfers_provisional CHECK: allow finalized rows with
-- variance_amount != 0 AND variance_transfer_id IS NULL.
-- ============================================================
-- Tier 2's internal-chain op_move_v finalization records variance for
-- the cache but DOES NOT post a variance transfer (debit-normal pools
-- on both source and destination preclude single-leg or 2-leg routing).
-- The original constraint required transfer_id when variance != 0; we
-- relax it. The audit is preserved: variance_amount is still recorded;
-- callers consume it via the topological cache.

ALTER TABLE transfers_provisional
  DROP CONSTRAINT transfers_provisional_check;

ALTER TABLE transfers_provisional
  ADD CONSTRAINT transfers_provisional_check CHECK (
    CASE
      WHEN finalized_at IS NULL THEN
        variance_amount IS NULL AND variance_transfer_id IS NULL
      WHEN variance_amount IS NULL THEN
        FALSE
      WHEN variance_amount = 0 THEN
        variance_transfer_id IS NULL
      ELSE
        TRUE
    END
  );

-- ============================================================
-- post_wo_start: lift the wac_periodic multi-op gate.
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
    -- wac_retroactive on WIP defers to acct-rso (tier 3): per-event
    -- chronological replay reads transfers.qty from value-pool events
    -- which carries component qty for rm_issue_to_wo. tier 3 surgery.
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

  -- Tier 2 (acct-smn): wac_periodic multi-op routings now supported via
  -- topological per-pool close-hook recompute. No cost_method × op_count
  -- gate.

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
  'cost_method ∈ {standard, wac_perpetual, wac_periodic}. Multi-op routings '
  'on wac_periodic parents accepted (acct-smn, tier 2 of acct-8in). '
  'wac_retroactive parents raise P0026 → acct-rso (tier 3).';

-- ============================================================
-- post_op_move: add wac_periodic branch (running pool avg).
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
    -- Per-unit accumulation at applies_at_op ≤ from_op. LITERAL —
    -- bom_lines.scrap_pct is planning metadata (acct-6jq).
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

  ELSIF v_cost_method IN ('wac_perpetual', 'wac_periodic') THEN
    -- Lock the source value pool before reading its balance for the
    -- running avg. Tier 2 (acct-smn): wac_periodic uses identical
    -- mid-period math; the close hook re-amounts at predecessor's
    -- final_avg via topological pool walk.
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
    -- post_wo_start gates this; defensive only.
    RAISE EXCEPTION
      'wo_invalid: parent_sku % has cost_method=% which post_op_move '
      'does not handle (deferred to acct-rso, tier 3)',
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
  'standard → bom_lines std_cum (literal); wac_perpetual / wac_periodic → '
  'source pool running avg (FOR UPDATE on src val pool). wac_periodic '
  'op_move_v transfers are flagged into transfers_provisional and '
  're-amounted at close via topological per-pool recompute (acct-smn). '
  'wac_retroactive defers to acct-rso (tier 3 of acct-8in).';

-- ============================================================
-- wac_periodic_close_hook — topological per-pool recompute.
-- ============================================================

CREATE OR REPLACE FUNCTION wac_periodic_close_hook(
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
  v_orig_reason    transfer_reason;
BEGIN
  SELECT opens_at, closes_at, code
    INTO v_period_opens, v_period_closes, v_period_code
    FROM periods WHERE id = p_period_id;
  IF NOT FOUND THEN
    RETURN 0;
  END IF;

  -- Build a transient working set: the pools (credit accounts on flagged
  -- depletions) and the edge set (op_move_v predecessor → successor).
  -- Use TEMP TABLES with ON COMMIT DROP since the function runs inside
  -- a close_period transaction.

  CREATE TEMP TABLE _wac_pools (
    pool_id BIGINT PRIMARY KEY
  ) ON COMMIT DROP;

  CREATE TEMP TABLE _wac_edges (
    predecessor BIGINT,
    successor   BIGINT,
    PRIMARY KEY (predecessor, successor)
  ) ON COMMIT DROP;

  -- Pool set: credit_account_id of every wac_periodic provisional row,
  -- plus debit_account_id of every op_move_v provisional (so successor
  -- pools that have no own depletion-out still get processed).
  INSERT INTO _wac_pools (pool_id)
  SELECT DISTINCT t.credit_account_id
    FROM transfers_provisional p
    JOIN transfers t ON t.id = p.transfer_id
   WHERE p.period_id = p_period_id
     AND p.cost_method = 'wac_periodic'
     AND p.finalized_at IS NULL
  UNION
  SELECT DISTINCT t.debit_account_id
    FROM transfers_provisional p
    JOIN transfers t ON t.id = p.transfer_id
   WHERE p.period_id = p_period_id
     AND p.cost_method = 'wac_periodic'
     AND p.finalized_at IS NULL
     AND t.reason = 'op_move_v'
  ON CONFLICT DO NOTHING;

  -- Edge set: op_move_v transfers in-period drive the predecessor →
  -- successor relationships.
  INSERT INTO _wac_edges (predecessor, successor)
  SELECT DISTINCT t.credit_account_id, t.debit_account_id
    FROM transfers_provisional p
    JOIN transfers t ON t.id = p.transfer_id
   WHERE p.period_id = p_period_id
     AND p.cost_method = 'wac_periodic'
     AND p.finalized_at IS NULL
     AND t.reason = 'op_move_v'
  ON CONFLICT DO NOTHING;

  -- Iterative Kahn's algorithm. At each step, pick a pool with no
  -- unprocessed predecessor; process it; record processed; remove
  -- edges that now have a finished tail. If no pool has zero unprocessed
  -- predecessors and pools remain → cycle.
  LOOP
    SELECT COUNT(*) INTO v_remaining FROM _wac_pools;
    EXIT WHEN v_remaining = 0;

    v_progress := 0;
    FOR v_pool_id IN
      SELECT wp.pool_id
        FROM _wac_pools wp
       WHERE NOT EXISTS (
         SELECT 1 FROM _wac_edges e
          WHERE e.successor = wp.pool_id
            AND e.predecessor IN (SELECT pool_id FROM _wac_pools)
       )
    LOOP
      v_progress := v_progress + 1;

      SELECT * INTO v_pool_acct FROM accounts WHERE id = v_pool_id;

      -- corrected_value_in: SUM over debit transfers t to this pool;
      -- t.amount adjusted by any matching transfers_provisional.variance_amount
      -- (which is non-NULL only after upstream finalization in this hook).
      SELECT COALESCE(SUM(
        t.amount + COALESCE(p.variance_amount, 0)
      ), 0) INTO v_pool_value_in
        FROM transfers t
        LEFT JOIN transfers_provisional p
               ON p.transfer_id = t.id
              AND p.finalized_at IS NOT NULL
       WHERE t.debit_account_id = v_pool_id
         AND t.business_date BETWEEN v_period_opens AND v_period_closes;

      v_pool_qty_in := _wac_close_pool_qty_in(
        v_pool_acct, v_period_opens, v_period_closes
      );
      IF v_pool_qty_in IS NULL THEN
        RAISE EXCEPTION
          'wac_periodic_close: cannot resolve qty account for value pool %',
          v_pool_id USING ERRCODE = 'P0010';
      END IF;

      IF v_pool_qty_in = 0 THEN
        IF p_force_provisional THEN
          DELETE FROM _wac_pools WHERE pool_id = v_pool_id;
          v_processed := array_append(v_processed, v_pool_id);
          CONTINUE;
        END IF;
        RAISE EXCEPTION
          'wac_periodic_close_no_receipts: period % (id=%) has provisional '
          'depletions on pool kind=% sku=% loc=% op=% ccy=% but zero receipts in '
          'period; post receipts and retry the close, or close with '
          'p_force_provisional=TRUE.',
          v_period_code, p_period_id, v_pool_acct.kind, v_pool_acct.sku_id,
          v_pool_acct.location_id, v_pool_acct.routing_op, v_pool_acct.currency
          USING ERRCODE = 'P0020';
      END IF;

      v_final_avg := v_pool_value_in / v_pool_qty_in;

      -- Iterate provisionals from this pool. Lock for update; preserve
      -- transfer_id ordering for determinism.
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
        IF v_orig.credit_account_id <> v_pool_id THEN
          CONTINUE;
        END IF;
        v_orig_reason := v_orig.reason;

        v_provisional := v_orig.amount / v_row.qty;
        v_variance    := (v_final_avg - v_provisional) * v_row.qty;

        -- Internal-chain op_move_v: record variance, do NOT post a
        -- variance transfer. Cost shift propagates via the LEFT JOIN
        -- on variance_amount when computing downstream value_in.
        IF v_orig_reason = 'op_move_v' THEN
          UPDATE transfers_provisional
             SET finalized_at = clock_timestamp(),
                 variance_amount = v_variance,
                 variance_transfer_id = NULL
           WHERE transfer_id = v_row.transfer_id;
          v_count := v_count + 1;
          CONTINUE;
        END IF;

        -- Leaf depletion path (wo_complete_v / scrap_v / so_ship / etc.).
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

        -- Single-leg variance posting for inv_value_wip source pools
        -- (tier 1 pattern). The WIP pool is typically drained at WO close;
        -- 2-leg wash through it would violate debit-normal CHECK on
        -- multi-WO drift. variance routes between v_orig_debit and
        -- variance_wac_period; pool untouched. v_orig_debit for leaf
        -- depletions is FG / variance_scrap / cogs (debit-normal accounts
        -- that retain balance through period).
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
          -- Raw / fg source (non-WIP wac_periodic on canonical reasons).
          -- Keep the 2-leg wash from migrations 0029 / 0030 / 0064 since
          -- raw and fg pools retain balance.
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

      DELETE FROM _wac_pools WHERE pool_id = v_pool_id;
      v_processed := array_append(v_processed, v_pool_id);
    END LOOP;

    IF v_progress = 0 THEN
      -- No pool with zero unprocessed predecessors: cycle. Surface the
      -- remaining pools' identifiers to the caller.
      SELECT string_agg(pool_id::TEXT, ', ' ORDER BY pool_id)
        INTO v_cycle_pools
        FROM _wac_pools;
      RAISE EXCEPTION
        'wac_periodic_pool_cycle: period % (id=%) has rework cycles in '
        'wac_periodic op_move_v flow involving pools [%]; iterative-fixed-'
        'point handling deferred to acct-p7v-rework.',
        v_period_code, p_period_id, v_cycle_pools
        USING ERRCODE = 'P0036';
    END IF;
  END LOOP;

  RETURN v_count;
END;
$$;

COMMENT ON FUNCTION wac_periodic_close_hook(BIGINT, BOOLEAN) IS
  'wac_periodic period-close recompute. Tier 2 (acct-smn): topological '
  'pool walk handles multi-op WIP chains. For each pool in topological '
  'order, computes corrected_value_in via LEFT JOIN on '
  'transfers_provisional.variance_amount (upstream pools finalized first), '
  'final_avg, then iterates provisional depletions FROM that pool. '
  'op_move_v internal: records variance, does not post (cost shift '
  'flows downstream via cache). Leaf depletions: single-leg variance for '
  'inv_value_wip source, 2-leg wash for raw/fg. Rework cycles raise '
  'P0036; deferred to acct-p7v-rework.';
