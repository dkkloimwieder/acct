-- acct-du2.{4, 5, 13} — P3 consistency fixes.
--
-- acct-du2.4: _post_transfers_compute_amount switches debit-first
--   COALESCE to credit-first. Functionally equivalent today (every
--   cost-event reason has at most one SKU-bearing leg) but eliminates
--   drift risk against future reasons that put SKUs on both legs, and
--   aligns with the credit-first canonical pattern from acct-7py
--   (mig 0067 _post_transfers_apply_event).
--
-- acct-du2.5: post_op_move and post_scrap add the dual idempotency
--   replay check (acct-69p shape, used by post_wo_start /
--   post_wo_complete / post_wo_close_unproduced / post_osp_*). Pre-fix
--   relied on ON CONFLICT DO NOTHING at the wo_events INSERT. Safe but
--   wasteful under contention; this aligns the family.
--
-- acct-du2.13: _post_transfers_lookup_qty_account COMMENT updated to
--   warn callers about the per-class divisor hazard (returns
--   stock_available which is cross-class for raw + fg same-location
--   SKUs — callers must use per-class signed SUM on transfers.qty for
--   divisor work, not the returned account's balance).

-- ============================================================
-- acct-du2.4 — _post_transfers_compute_amount credit-first.
-- All branches identical to mig 0031 except line 85's COALESCE.
-- ============================================================

CREATE OR REPLACE FUNCTION _post_transfers_compute_amount(
  p_event        JSONB,
  p_d_acct       accounts,
  p_c_acct       accounts,
  p_cost_method  cost_method,
  p_idx          INT
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_qty            BIGINT;
  v_sku            UUID;
  v_unit           BIGINT;
  v_qty_balance    BIGINT;
  v_value_balance  BIGINT;
  v_business_date  DATE;
BEGIN
  v_qty := (p_event->>'qty')::BIGINT;
  IF v_qty IS NULL THEN
    RAISE EXCEPTION 'cost_method_not_implemented: cost-relevant value event missing qty at event index %',
                    p_idx
      USING ERRCODE = 'P0006';
  END IF;

  -- acct-du2.4 fix: credit-first SKU resolution. The credit account is
  -- the depletion source whose cost_method drives pricing across all
  -- canonical cost-event reasons. Debit-first is functionally
  -- equivalent today (cogs / variance debits have NULL sku_id) but
  -- drift-prone if a future reason puts SKUs on both legs. Aligns with
  -- _post_transfers_apply_event's credit-first switch in mig 0067.
  v_sku := COALESCE(p_c_acct.sku_id, p_d_acct.sku_id);
  IF v_sku IS NULL THEN
    RAISE EXCEPTION 'cost_method_not_implemented: sku not resolvable in compute_amount at event index %',
                    p_idx
      USING ERRCODE = 'P0006';
  END IF;

  CASE p_cost_method
    WHEN 'standard' THEN
      v_business_date := (p_event->>'business_date')::DATE;
      v_unit := resolve_standard_cost_at(v_sku, v_business_date);
      RETURN v_qty * v_unit;

    WHEN 'wac_perpetual' THEN
      IF p_c_acct.ledger_kind <> 'value' THEN
        RAISE EXCEPTION 'wac_perpetual requires credit-side value account, got % at event index %',
                        p_c_acct.kind, p_idx
          USING ERRCODE = 'P0006';
      END IF;

      SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = p_c_acct.id THEN  t.qty
                               WHEN t.credit_account_id = p_c_acct.id THEN -t.qty END), 0)
        INTO v_qty_balance
        FROM transfers t
       WHERE p_c_acct.id IN (t.debit_account_id, t.credit_account_id)
         AND t.qty IS NOT NULL;

      IF v_qty_balance <= 0 THEN
        RAISE EXCEPTION 'wac_perpetual qty balance is %, cannot divide for unit cost at event index %',
                        v_qty_balance, p_idx
          USING ERRCODE = 'P0006';
      END IF;

      v_value_balance := p_c_acct.debits_total - p_c_acct.credits_total;
      IF v_value_balance < 0 THEN v_value_balance := 0; END IF;

      v_unit := v_value_balance / v_qty_balance;
      RETURN v_qty * v_unit;

    WHEN 'wac_periodic' THEN
      IF p_c_acct.kind = 'inv_value_wip' THEN
        RAISE EXCEPTION
          'wac_periodic depletions from inv_value_wip not supported in Phase 1 '
          '(see acct-p7v Phase 2 Epic J); event index %',
          p_idx USING ERRCODE = 'P0006';
      END IF;
      IF p_c_acct.ledger_kind <> 'value' THEN
        RAISE EXCEPTION 'wac_periodic requires credit-side value account, got % at event index %',
                        p_c_acct.kind, p_idx
          USING ERRCODE = 'P0006';
      END IF;

      SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = p_c_acct.id THEN  t.qty
                               WHEN t.credit_account_id = p_c_acct.id THEN -t.qty END), 0)
        INTO v_qty_balance
        FROM transfers t
       WHERE p_c_acct.id IN (t.debit_account_id, t.credit_account_id)
         AND t.qty IS NOT NULL;

      IF v_qty_balance <= 0 THEN
        RAISE EXCEPTION 'wac_periodic qty balance is %, cannot divide for unit cost at event index %',
                        v_qty_balance, p_idx
          USING ERRCODE = 'P0006';
      END IF;

      v_value_balance := p_c_acct.debits_total - p_c_acct.credits_total;
      IF v_value_balance < 0 THEN v_value_balance := 0; END IF;

      v_unit := v_value_balance / v_qty_balance;
      RETURN v_qty * v_unit;

    WHEN 'wac_retroactive' THEN
      IF p_c_acct.kind = 'inv_value_wip' THEN
        RAISE EXCEPTION
          'wac_retroactive depletions from inv_value_wip not supported in Phase 1 '
          '(see acct-p7v Phase 2 Epic J); event index %',
          p_idx USING ERRCODE = 'P0006';
      END IF;
      IF p_c_acct.ledger_kind <> 'value' THEN
        RAISE EXCEPTION 'wac_retroactive requires credit-side value account, got % at event index %',
                        p_c_acct.kind, p_idx
          USING ERRCODE = 'P0006';
      END IF;

      SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = p_c_acct.id THEN  t.qty
                               WHEN t.credit_account_id = p_c_acct.id THEN -t.qty END), 0)
        INTO v_qty_balance
        FROM transfers t
       WHERE p_c_acct.id IN (t.debit_account_id, t.credit_account_id)
         AND t.qty IS NOT NULL;

      IF v_qty_balance <= 0 THEN
        RAISE EXCEPTION 'wac_retroactive qty balance is %, cannot divide for unit cost at event index %',
                        v_qty_balance, p_idx
          USING ERRCODE = 'P0006';
      END IF;

      v_value_balance := p_c_acct.debits_total - p_c_acct.credits_total;
      IF v_value_balance < 0 THEN v_value_balance := 0; END IF;

      v_unit := v_value_balance / v_qty_balance;
      RETURN v_qty * v_unit;

    WHEN 'lot' THEN
      RAISE EXCEPTION 'cost_method_not_implemented: lot (tracked as acct-8gg + lot infrastructure) at event index %',
                      p_idx
        USING ERRCODE = 'P0006';

    WHEN 'fifo' THEN
      RAISE EXCEPTION 'cost_method_not_implemented: fifo (tracked as acct-8gg + lot infrastructure) at event index %',
                      p_idx
        USING ERRCODE = 'P0006';
  END CASE;

  RAISE EXCEPTION 'cost_method_not_implemented: unhandled cost_method % at event index %',
                  p_cost_method, p_idx
    USING ERRCODE = 'P0006';
END;
$$;

-- ============================================================
-- acct-du2.5 — post_op_move + post_scrap dual replay-check.
-- Both functions: insert a second SELECT id FROM wo_events WHERE
-- idempotency_key = ... after the FOR UPDATE on work_orders.
-- ============================================================

-- post_op_move: same body as mig 0073, with the post-FOR-UPDATE check.

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
  v_lock_first       BIGINT;
  v_lock_second      BIGINT;
BEGIN
  -- Fast-path replay check.
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

  -- acct-du2.5 fix: race-safe replay re-check after the FOR UPDATE on
  -- work_orders. Aligns with the acct-69p dual-check pattern used by
  -- post_wo_start / post_wo_complete / post_wo_close_unproduced.
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

-- post_scrap: same body as mig 0038, with the post-FOR-UPDATE check.

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

  -- acct-du2.5 fix: race-safe replay re-check after FOR UPDATE.
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

-- ============================================================
-- acct-du2.13 — _post_transfers_lookup_qty_account doc warning.
-- ============================================================

COMMENT ON FUNCTION _post_transfers_lookup_qty_account(accounts) IS
  'Maps a value-side account (inv_value_raw / inv_value_fg / inv_value_wip) '
  'to its matching qty-side account (stock_available or stock_wip). Returns '
  'NULL if no clean match. CALLER WARNING (acct-du2.13): the returned '
  'stock_available pool is SHARED between inv_value_raw and inv_value_fg '
  'at the same (sku, location). Callers MUST use per-class signed SUM on '
  'transfers.qty filtered to the value account when computing per-class '
  'divisors — NOT the returned qty account''s debits_total - credits_total '
  'balance, which is cross-class. See acct-fii (mig 0069), acct-du2.11 '
  '(mig 0071) for the bug pattern this comment exists to prevent.';
