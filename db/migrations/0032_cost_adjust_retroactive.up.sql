-- acct-8qi / acct-og1.1 — Phase 1 Epic E: cost_adjustment retroactive.
--
-- Replaces the s6n stub cost_adjust_retroactive_hook with a real body and
-- adds the queue table + post_cost_adjustment_retroactive entry function.
-- Also threads p_force_provisional through close_period to the hook
-- (signature change: 1-arg → 2-arg).
--
-- Workflow:
--
--   1. Operator calls post_cost_adjustment_retroactive(target_period, sku,
--      location, currency, class, target_avg, business_date, ...) on the
--      CURRENTLY-OPEN target period. The function INSERTs a row into
--      inventory_cost_adjustments_retroactive (the queue table) and
--      returns the queue id.
--
--   2. close_period(target_period) eventually runs. Inside, the hook
--      walks un-finalized queue rows for that period; for each, walks
--      every credit-side depletion on the (sku, location, currency,
--      class) pool whose business_date falls in the period; for each
--      depletion computes provisional_unit_cost = amount / qty and
--      variance = qty × (target_avg − provisional_unit_cost); if
--      non-zero, posts a 2-transfer batch routing through
--      variance_cost_adjust_retro and accumulates per-row totals.
--
-- Design decisions (locked 2026-05-01):
--
--   D1 — Target period must currently be OPEN. Closed periods raise
--        P0021 referencing acct-7h4 (Phase 2 Epic K, period reopen).
--   D2 — Method-agnostic. Works for any cost_method (standard,
--        wac_perpetual, wac_periodic, wac_retroactive). Operator override
--        of whatever cost was originally applied. wac_periodic /
--        wac_retroactive double-correction is acceptable: their close
--        hooks ran first and posted their own variances; this layers on
--        top.
--   D3 — p_business_date must fall within target_period's
--        opens_at..closes_at bounds. No closed-period override needed
--        since target is open by D1.
--   D4 — Workflow: queue table → close-time hook (no synchronous
--        variance posting at queue time).
--   D5 — Variance routes through variance_cost_adjust_retro (already
--        seeded in s6n in USD + EUR; same 2-transfer pattern as
--        wac_periodic / wac_retroactive).
--   D6 — WIP class deferred. inventory_class='wip' raises P0006
--        referencing acct-p7v.
--   D7 — Idempotency at queue table via UNIQUE(idempotency_key).
--
-- Variance transfers carry qty=NULL (value-only correction), so the
-- in-period depletion walk's `qty IS NOT NULL` filter naturally excludes
-- variance rows from prior hook runs.
--
-- New error code:
--   P0021 — target_period_closed (queue insert refuses if target is
--           already closed; ref acct-7h4).

-- ============================================================
-- Queue table.
-- ============================================================

CREATE TABLE inventory_cost_adjustments_retroactive (
  id                UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  sku_id            UUID NOT NULL REFERENCES skus(id),
  location_id       UUID NOT NULL REFERENCES locations(id),
  currency          TEXT NOT NULL,
  inventory_class   TEXT NOT NULL CHECK (inventory_class IN ('raw','fg')),
  target_period_id  BIGINT NOT NULL REFERENCES periods(id),
  target_avg        BIGINT NOT NULL CHECK (target_avg >= 0),
  business_date     DATE NOT NULL,
  posted_by         UUID NOT NULL,
  posted_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  finalized_at      TIMESTAMPTZ,
  finalized_count   BIGINT,
  total_variance    BIGINT,
  idempotency_key   UUID NOT NULL UNIQUE,
  notes             TEXT
);

CREATE INDEX inv_cost_adj_retro_target_period_unfinalized
  ON inventory_cost_adjustments_retroactive (target_period_id)
  WHERE finalized_at IS NULL;

CREATE INDEX inv_cost_adj_retro_sku_period
  ON inventory_cost_adjustments_retroactive (sku_id, target_period_id);

CREATE INDEX inv_cost_adj_retro_posted_at
  ON inventory_cost_adjustments_retroactive (posted_at);

-- ============================================================
-- post_cost_adjustment_retroactive: queue entry function.
-- ============================================================

CREATE OR REPLACE FUNCTION post_cost_adjustment_retroactive(
  p_target_period_id BIGINT,
  p_sku_id           UUID,
  p_location_id      UUID,
  p_currency         TEXT,
  p_inventory_class  TEXT,
  p_target_avg       BIGINT,
  p_business_date    DATE,
  p_posted_by        UUID,
  p_idempotency_key  UUID,
  p_notes            TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id    UUID;
  v_period_opens   DATE;
  v_period_closes  DATE;
  v_period_closed  TIMESTAMPTZ;
  v_period_code    TEXT;
  v_value_kind     TEXT;
  v_val_acct       BIGINT;
  v_doc_id         UUID;
BEGIN
  -- Replay check.
  SELECT id INTO v_existing_id
    FROM inventory_cost_adjustments_retroactive
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN
    RETURN v_existing_id;
  END IF;

  -- D6: WIP deferred.
  IF p_inventory_class = 'wip' THEN
    RAISE EXCEPTION
      'cost_method_not_implemented: cost_adjustment_retroactive on '
      'inv_value_wip class not supported in Phase 1 (file future epic '
      'for WIP across all cost-adjustment workflows; see acct-p7v); sku=%',
      p_sku_id USING ERRCODE = 'P0006';
  END IF;

  -- target_avg sanity (CHECK on the table catches it; explicit error first
  -- so message is descriptive).
  IF p_target_avg < 0 THEN
    RAISE EXCEPTION 'p_target_avg must be >= 0 (got %)', p_target_avg
      USING ERRCODE = '23514';
  END IF;

  -- D1: target period must exist and be OPEN.
  SELECT opens_at, closes_at, closed_at, code
    INTO v_period_opens, v_period_closes, v_period_closed, v_period_code
    FROM periods
   WHERE id = p_target_period_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'target period id=% not found', p_target_period_id
      USING ERRCODE = 'P0014';
  END IF;
  IF v_period_closed IS NOT NULL THEN
    RAISE EXCEPTION
      'target_period_closed: period % (id=%) closed at %; '
      'reopen the period first (workflow tracked as acct-7h4 Phase 2 Epic K)',
      v_period_code, p_target_period_id, v_period_closed
      USING ERRCODE = 'P0021';
  END IF;

  -- D3: business_date in period bounds.
  IF p_business_date < v_period_opens OR p_business_date > v_period_closes THEN
    RAISE EXCEPTION
      'business_date % out of target period % bounds (%..%)',
      p_business_date, v_period_code, v_period_opens, v_period_closes
      USING ERRCODE = 'P0004';
  END IF;

  -- SKU exists.
  PERFORM 1 FROM skus WHERE id = p_sku_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'sku % not found', p_sku_id USING ERRCODE = 'P0010';
  END IF;

  -- Pool exists.
  v_value_kind := 'inv_value_' || p_inventory_class;
  EXECUTE format(
    'SELECT id FROM accounts WHERE kind = %L AND sku_id = $1 AND '
    'location_id = $2 AND currency = $3 AND NOT is_closed',
    v_value_kind
  ) INTO v_val_acct USING p_sku_id, p_location_id, p_currency;
  IF v_val_acct IS NULL THEN
    RAISE EXCEPTION
      'no open % account for sku=% loc=% ccy=%',
      v_value_kind, p_sku_id, p_location_id, p_currency
      USING ERRCODE = 'P0010';
  END IF;

  -- Queue the row.
  INSERT INTO inventory_cost_adjustments_retroactive (
    sku_id, location_id, currency, inventory_class, target_period_id,
    target_avg, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_sku_id, p_location_id, p_currency, p_inventory_class, p_target_period_id,
    p_target_avg, p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id
      FROM inventory_cost_adjustments_retroactive
     WHERE idempotency_key = p_idempotency_key;
  END IF;

  RETURN v_doc_id;
END;
$$;

-- ============================================================
-- cost_adjust_retroactive_hook: real body.
-- Drop the s6n 1-arg stub first; PG signature change requires DROP.
-- ============================================================

DROP FUNCTION IF EXISTS cost_adjust_retroactive_hook(BIGINT);

CREATE OR REPLACE FUNCTION cost_adjust_retroactive_hook(
  p_period_id         BIGINT,
  p_force_provisional BOOLEAN DEFAULT FALSE
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_period_opens   DATE;
  v_period_closes  DATE;
  v_period_code    TEXT;
  v_count          BIGINT := 0;
  v_queue          RECORD;
  v_pool_id        BIGINT;
  v_value_kind     TEXT;
  v_var_acct       BIGINT;
  v_event          RECORD;
  v_prov_unit      BIGINT;
  v_variance       BIGINT;
  v_total_variance BIGINT;
  v_dep_count      BIGINT;
  v_event_a        JSONB;
  v_event_b        JSONB;
  v_batch          JSONB;
BEGIN
  SELECT opens_at, closes_at, code
    INTO v_period_opens, v_period_closes, v_period_code
    FROM periods WHERE id = p_period_id;
  IF NOT FOUND THEN RETURN 0; END IF;

  FOR v_queue IN
    SELECT id, sku_id, location_id, currency, inventory_class, target_avg
      FROM inventory_cost_adjustments_retroactive
     WHERE target_period_id = p_period_id
       AND finalized_at IS NULL
     ORDER BY posted_at, id
     FOR UPDATE
  LOOP
    -- Resolve pool.
    v_value_kind := 'inv_value_' || v_queue.inventory_class;
    EXECUTE format(
      'SELECT id FROM accounts WHERE kind = %L AND sku_id = $1 AND '
      'location_id = $2 AND currency = $3 AND NOT is_closed',
      v_value_kind
    ) INTO v_pool_id USING v_queue.sku_id, v_queue.location_id, v_queue.currency;
    IF v_pool_id IS NULL THEN
      RAISE EXCEPTION
        'cost_adjust_retroactive: no open % for sku=% loc=% ccy=% (queue id=%)',
        v_value_kind, v_queue.sku_id, v_queue.location_id, v_queue.currency, v_queue.id
        USING ERRCODE = 'P0010';
    END IF;

    -- Resolve variance_cost_adjust_retro(currency).
    SELECT id INTO v_var_acct
      FROM accounts
     WHERE kind = 'variance_cost_adjust_retro'
       AND ledger_kind = 'value'
       AND currency = v_queue.currency
       AND NOT is_closed;
    IF v_var_acct IS NULL THEN
      RAISE EXCEPTION
        'cost_adjust_retroactive: no variance_cost_adjust_retro(value, ccy=%) '
        'account configured', v_queue.currency
        USING ERRCODE = 'P0010';
    END IF;

    v_total_variance := 0;
    v_dep_count      := 0;

    -- Walk every in-period credit-side qty-bearing transfer on the pool.
    -- Variance transfers from prior hook runs (wac_periodic /
    -- wac_retroactive close hooks) carry qty=NULL and are filtered out.
    FOR v_event IN
      SELECT t.id, t.amount, t.qty, t.debit_account_id
        FROM transfers t
       WHERE t.credit_account_id = v_pool_id
         AND t.business_date BETWEEN v_period_opens AND v_period_closes
         AND t.qty IS NOT NULL
         AND t.qty > 0
       ORDER BY t.business_date, t.posted_at, t.id
    LOOP
      v_prov_unit := v_event.amount / v_event.qty;
      v_variance  := v_event.qty * (v_queue.target_avg - v_prov_unit);

      IF v_variance = 0 THEN
        v_dep_count := v_dep_count + 1;
        CONTINUE;
      END IF;

      -- D5: 2-transfer routing through variance_cost_adjust_retro.
      -- Net effect:
      --   write-up   (variance > 0): original_debit += |v|, pool -= |v|
      --   write-down (variance < 0): original_debit -= |v|, pool += |v|
      IF v_variance > 0 THEN
        v_event_a := jsonb_build_object(
          'reason','cost_restate','document_kind','cost_adjust_retroactive_close',
          'document_id', v_queue.id,
          'debit_account_id', v_event.debit_account_id,
          'credit_account_id', v_var_acct,
          'amount', v_variance,
          'business_date', v_period_closes,
          'idempotency_key', gen_random_uuid(),
          'posted_by','00000000-0000-0000-0000-000000000000');
        v_event_b := jsonb_build_object(
          'reason','cost_restate','document_kind','cost_adjust_retroactive_close',
          'document_id', v_queue.id,
          'debit_account_id', v_var_acct,
          'credit_account_id', v_pool_id,
          'amount', v_variance,
          'business_date', v_period_closes,
          'idempotency_key', gen_random_uuid(),
          'posted_by','00000000-0000-0000-0000-000000000000');
      ELSE
        v_event_a := jsonb_build_object(
          'reason','cost_restate','document_kind','cost_adjust_retroactive_close',
          'document_id', v_queue.id,
          'debit_account_id', v_var_acct,
          'credit_account_id', v_event.debit_account_id,
          'amount', -v_variance,
          'business_date', v_period_closes,
          'idempotency_key', gen_random_uuid(),
          'posted_by','00000000-0000-0000-0000-000000000000');
        v_event_b := jsonb_build_object(
          'reason','cost_restate','document_kind','cost_adjust_retroactive_close',
          'document_id', v_queue.id,
          'debit_account_id', v_pool_id,
          'credit_account_id', v_var_acct,
          'amount', -v_variance,
          'business_date', v_period_closes,
          'idempotency_key', gen_random_uuid(),
          'posted_by','00000000-0000-0000-0000-000000000000');
      END IF;

      v_batch := jsonb_build_array(v_event_a, v_event_b);
      PERFORM post_transfers(v_batch, FALSE);

      v_total_variance := v_total_variance + v_variance;
      v_dep_count      := v_dep_count + 1;
    END LOOP;

    UPDATE inventory_cost_adjustments_retroactive
       SET finalized_at    = clock_timestamp(),
           finalized_count = v_dep_count,
           total_variance  = v_total_variance
     WHERE id = v_queue.id;

    v_count := v_count + 1;
  END LOOP;

  RETURN v_count;
END;
$$;

-- ============================================================
-- close_period: thread p_force_provisional to cost_adjust_retroactive_hook.
-- All three hooks now take (period_id, force_provisional).
-- ============================================================

CREATE OR REPLACE FUNCTION close_period(
  p_period_id         BIGINT,
  p_actor             UUID,
  p_force_provisional BOOLEAN DEFAULT FALSE,
  p_force_recon       BOOLEAN DEFAULT FALSE
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
  v_period_code            TEXT;
  v_already_closed         TIMESTAMPTZ;
  v_wac_period_count       BIGINT;
  v_wac_retro_count        BIGINT;
  v_cost_adj_retro_count   BIGINT;
  v_finalized_count        BIGINT;
  v_unfinalized_remaining  BIGINT;
  v_alerts                 INT;
  v_now                    TIMESTAMPTZ;
BEGIN
  SELECT code, closed_at INTO v_period_code, v_already_closed
    FROM periods WHERE id = p_period_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'period_close_invalid: period id=% not found', p_period_id USING ERRCODE = 'P0014';
  END IF;
  IF v_already_closed IS NOT NULL THEN
    RAISE EXCEPTION 'period_close_invalid: period % (id=%) already closed at %',
      v_period_code, p_period_id, v_already_closed USING ERRCODE = 'P0014';
  END IF;

  v_wac_period_count     := wac_periodic_close_hook(p_period_id, p_force_provisional);
  v_wac_retro_count      := wac_retroactive_close_hook(p_period_id, p_force_provisional);
  v_cost_adj_retro_count := cost_adjust_retroactive_hook(p_period_id, p_force_provisional);
  v_finalized_count      := v_wac_period_count + v_wac_retro_count + v_cost_adj_retro_count;

  SELECT COUNT(*) INTO v_unfinalized_remaining
    FROM transfers_provisional
   WHERE period_id = p_period_id AND finalized_at IS NULL;

  IF v_unfinalized_remaining > 0 AND NOT p_force_provisional THEN
    RAISE EXCEPTION
      'period_close_provisional: % un-finalized provisional rows '
      'remain in period % (id=%); pass p_force_provisional=TRUE to override',
      v_unfinalized_remaining, v_period_code, p_period_id USING ERRCODE = 'P0015';
  END IF;

  v_alerts := run_daily_reconciliation();
  IF v_alerts > 0 AND NOT p_force_recon THEN
    RAISE EXCEPTION
      'period_close_reconciliation: % new reconciliation alert(s) raised '
      'while closing period % (id=%); pass p_force_recon=TRUE to override',
      v_alerts, v_period_code, p_period_id USING ERRCODE = 'P0016';
  END IF;

  v_now := clock_timestamp();
  UPDATE periods SET closed_at = v_now, closed_by = p_actor WHERE id = p_period_id;

  RETURN jsonb_build_object(
    'period_id', p_period_id, 'period_code', v_period_code,
    'closed_at', v_now, 'closed_by', p_actor,
    'finalized_count', v_finalized_count,
    'hook_results', jsonb_build_object(
      'wac_periodic', v_wac_period_count,
      'wac_retroactive', v_wac_retro_count,
      'cost_adjust_retroactive', v_cost_adj_retro_count
    ),
    'unfinalized_remaining', v_unfinalized_remaining,
    'alerts', v_alerts,
    'forced', jsonb_build_object('provisional', p_force_provisional, 'recon', p_force_recon)
  );
END;
$$;
