-- acct-8rv / acct-hlr.2 — Phase 1 Epic F: post_standard_cost_roll()
-- function. Companion to migration 0027's schema refactor.
--
-- Establishes the canonical workflow for changing a standard SKU's
-- cost basis: INSERT a new row into standard_costs (no UPDATE), revalue
-- existing on-hand inventory at the new standard, post variance to a
-- dedicated P&L account.
--
-- DESIGN DECISIONS LOCKED IN (acct-hlr design memo, 2026-05-01):
--
--   Q1=(a) Retroactive rolls BLOCKED. p_effective_at must be strictly
--   greater than every existing standard_costs.effective_at for the
--   SKU. Otherwise raises P0019 (new code, retroactive_std_cost_roll
--   _blocked). Retroactive corrections are out-of-scope; if needed,
--   they'd be a separate workflow.
--
--   Q2=(a) WIP-present blocks the roll. If any open inv_value_wip
--   pool exists for the SKU at roll time, raises P0006 with reference
--   to acct-bru (Epic G — WIP material revaluation companion). Real
--   ERPs (Oracle, SAP) require WIP closeout or a parallel WIP-revaluation
--   workflow before standard cost rolls; we ship the companion workflow
--   in Epic G when high-volume use cases need it.
--
--   Q3 (NULLABLE prior_standard_cost). First roll has no prior, so
--   inventory_standard_cost_rolls.prior_standard_cost is NULLABLE.
--
--   Q4 (effective vs business date semantics). Caller passes both:
--     - p_effective_at: when the new standard takes effect
--     - p_business_date: when the variance transfers post (period assignment)
--   If p_effective_at > p_business_date: future-dated roll. INSERT only,
--   no revaluation. Audit row records the future-dated case (prior is
--   resolved at p_business_date but no transfers post). When inventory
--   is later transacted at a business_date >= p_effective_at, it
--   automatically picks up the new standard via resolve_standard_cost_at.
--   Phase 1 has no scheduled-revaluation mechanism for the future-
--   dated case — the WIP/raw/fg pools at the moment effective_at
--   passes will simply carry the prior standard until they flow out
--   or until a follow-up roll re-revalues. This is acknowledged
--   imprecision; out of scope for Phase 1 to schedule.
--
--   Q5 Optimistic concurrency. Optional p_expected_old_cost guard.
--   Mismatch (either direction) raises P0017.
--
--   Q6 Cost-method dispatch:
--     standard       → proceed
--     wac_perpetual  → P0011 (use post_cost_adjustment instead)
--     wac_periodic   → P0011 (same)
--     wac_retroactive→ P0011 (same)
--     fifo / lot     → P0006 (acct-8gg)
--
-- LOCK ORDER. skus row FOR UPDATE first (serializes concurrent rolls
-- on the same SKU even though we don't UPDATE the row directly), then
-- inv_value_raw + inv_value_fg accounts in ascending id order. WIP
-- accounts not locked because the WIP-present guard already ensures
-- no open WIP pools exist when we reach the revaluation pass.
--
-- NEW ERROR CODES (documented in db/README by acct-hre):
--   P0017 — optimistic_concurrency_violation
--   P0019 — retroactive_std_cost_roll_blocked

-- ============================================================
-- New transfer_reason and account_kind enum values
-- ============================================================

ALTER TYPE transfer_reason ADD VALUE IF NOT EXISTS 'standard_cost_roll';
ALTER TYPE account_kind   ADD VALUE IF NOT EXISTS 'variance_std_cost_roll';

-- ============================================================
-- inventory_standard_cost_rolls audit table.
-- prior_standard_cost is NULLABLE — first roll has no prior.
-- ============================================================

CREATE TABLE inventory_standard_cost_rolls (
  id                    UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  sku_id                UUID NOT NULL REFERENCES skus(id),
  prior_standard_cost   BIGINT,                                -- NULL on first roll
  target_standard_cost  BIGINT NOT NULL CHECK (target_standard_cost >= 0),
  effective_at          DATE NOT NULL,
  total_delta_value     BIGINT NOT NULL,                       -- signed; sum across variance transfers
  pool_qty              BIGINT NOT NULL CHECK (pool_qty >= 0), -- total raw+fg units revalued
  business_date         DATE NOT NULL,
  posted_by             UUID NOT NULL,
  posted_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key       UUID NOT NULL UNIQUE,
  notes                 TEXT
);

CREATE INDEX inv_std_roll_sku        ON inventory_standard_cost_rolls (sku_id);
CREATE INDEX inv_std_roll_posted_at  ON inventory_standard_cost_rolls (posted_at);

-- ============================================================
-- post_standard_cost_roll
-- ============================================================

CREATE OR REPLACE FUNCTION post_standard_cost_roll(
  p_sku_id            UUID,
  p_new_cost          BIGINT,
  p_effective_at      DATE,
  p_business_date     DATE,
  p_posted_by         UUID,
  p_idempotency_key   UUID,
  p_notes             TEXT   DEFAULT NULL,
  p_expected_old_cost BIGINT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id      UUID;
  v_doc_id           UUID;
  v_cost_method      cost_method;
  v_max_effective    DATE;
  v_prior            BIGINT;
  v_wip_count        BIGINT;
  v_var_acct         BIGINT;
  v_pool_record      RECORD;
  v_pool_qty         BIGINT;
  v_total_qty        BIGINT := 0;
  v_total_delta      BIGINT := 0;
  v_delta            BIGINT;
  v_amount           BIGINT;
  v_debit            BIGINT;
  v_credit           BIGINT;
  v_lock_ids         BIGINT[];
  v_lock_id          BIGINT;
  v_batch            JSONB := '[]'::JSONB;
  v_future_dated     BOOLEAN;
BEGIN
  -- Replay check.
  SELECT id INTO v_existing_id
    FROM inventory_standard_cost_rolls
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN
    RETURN v_existing_id;
  END IF;

  IF p_new_cost < 0 THEN
    RAISE EXCEPTION 'p_new_cost must be >= 0 (got %)', p_new_cost
      USING ERRCODE = '23514';
  END IF;

  -- Lock the SKU row; this serializes concurrent rolls on the same SKU.
  SELECT cost_method INTO v_cost_method
    FROM skus WHERE id = p_sku_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'sku % not found', p_sku_id USING ERRCODE = 'P0010';
  END IF;

  -- Cost-method dispatch.
  CASE v_cost_method
  WHEN 'standard' THEN
    NULL;  -- proceed
  WHEN 'wac_perpetual' THEN
    RAISE EXCEPTION
      'standard_cost_roll not applicable to wac_perpetual SKU % — use '
      'post_cost_adjustment for WAC pools (Epic D / acct-14m)',
      p_sku_id USING ERRCODE = 'P0011';
  WHEN 'wac_periodic' THEN
    RAISE EXCEPTION
      'standard_cost_roll not applicable to wac_periodic SKU % — use '
      'post_cost_adjustment for WAC pools (Epic D / acct-14m)',
      p_sku_id USING ERRCODE = 'P0011';
  WHEN 'wac_retroactive' THEN
    RAISE EXCEPTION
      'standard_cost_roll not applicable to wac_retroactive SKU % — use '
      'post_cost_adjustment for WAC pools (Epic D / acct-14m)',
      p_sku_id USING ERRCODE = 'P0011';
  WHEN 'fifo', 'lot' THEN
    RAISE EXCEPTION
      'cost_method_not_implemented: standard_cost_roll on % SKU %; see acct-8gg',
      v_cost_method, p_sku_id USING ERRCODE = 'P0006';
  ELSE
    RAISE EXCEPTION 'unknown cost_method % for sku=%', v_cost_method, p_sku_id
      USING ERRCODE = 'P0011';
  END CASE;

  -- Q1 retroactive guard. Reject rolls earlier than (or equal to) the
  -- latest existing effective_at for this SKU.
  SELECT MAX(effective_at) INTO v_max_effective
    FROM standard_costs WHERE sku_id = p_sku_id;

  IF v_max_effective IS NOT NULL AND p_effective_at <= v_max_effective THEN
    RAISE EXCEPTION
      'retroactive_std_cost_roll_blocked: sku=% has standard_costs row at '
      'effective_at=%; p_effective_at=% must be strictly greater. '
      'Retroactive standard cost corrections are not supported in Phase 1.',
      p_sku_id, v_max_effective, p_effective_at
      USING ERRCODE = 'P0019';
  END IF;

  -- Resolve the prior active standard at p_business_date for the
  -- optimistic-concurrency check and the revaluation delta. NULL means
  -- no standard exists at business_date (first roll, or business_date
  -- predates earliest effective).
  BEGIN
    v_prior := resolve_standard_cost_at(p_sku_id, p_business_date);
  EXCEPTION WHEN SQLSTATE 'P0018' THEN
    v_prior := NULL;
  END;

  -- Q5 optimistic concurrency. Mismatch in either direction → P0017.
  IF p_expected_old_cost IS DISTINCT FROM v_prior THEN
    RAISE EXCEPTION
      'optimistic_concurrency_violation: caller expected prior=%, actual prior=%',
      p_expected_old_cost, v_prior
      USING ERRCODE = 'P0017';
  END IF;

  -- Q2 WIP guard. Open inv_value_wip pools for this SKU block the roll.
  SELECT COUNT(*) INTO v_wip_count
    FROM accounts
   WHERE kind = 'inv_value_wip'
     AND sku_id = p_sku_id
     AND NOT is_closed
     AND (debits_total - credits_total) > 0;

  IF v_wip_count > 0 THEN
    RAISE EXCEPTION
      'wip_present_blocks_std_cost_roll: sku=% has % open inv_value_wip pool(s) '
      'with non-zero balance. Phase 1 blocks rolls when WIP exists; the '
      'WIP material revaluation companion workflow is tracked as Epic G '
      '(acct-bru). Close out WIP via wo_complete + scrap or wait for '
      'production to drain before rolling.',
      p_sku_id, v_wip_count
      USING ERRCODE = 'P0006';
  END IF;

  -- Future-dated detection: insert the row but skip revaluation.
  v_future_dated := (p_effective_at > p_business_date);

  -- INSERT new standard_costs row (canonical record of the cost change).
  INSERT INTO standard_costs (
    sku_id, cost, effective_at, posted_by, idempotency_key, notes
  ) VALUES (
    p_sku_id, p_new_cost, p_effective_at, p_posted_by,
    -- standard_costs.idempotency_key has its own UNIQUE; use a fresh
    -- key derived from the audit row's so replay (which falls through
    -- to the audit-table fast path above) doesn't double-INSERT here.
    gen_random_uuid(), p_notes
  );

  -- Resolve the variance account once (currency derived from the
  -- pools we revalue; for simplicity Phase 1 walks USD pools first
  -- and resolves variance per currency at lock time).
  -- Actual per-currency lookup happens inside the pool loop.

  -- Revaluation pass: walk inv_value_raw + inv_value_fg pools for this
  -- SKU. Skip if future-dated (revaluation timing unclear) or if no
  -- prior standard existed (nothing to revalue against).
  IF NOT v_future_dated AND v_prior IS NOT NULL AND v_prior <> p_new_cost THEN
    -- First, gather all account ids we'll touch and lock them in
    -- ascending order to match post_transfers' lock-order invariant.
    SELECT array_agg(id ORDER BY id) INTO v_lock_ids
      FROM accounts
     WHERE kind IN ('inv_value_raw', 'inv_value_fg')
       AND sku_id = p_sku_id
       AND NOT is_closed;

    IF v_lock_ids IS NOT NULL THEN
      FOREACH v_lock_id IN ARRAY v_lock_ids LOOP
        PERFORM 1 FROM accounts WHERE id = v_lock_id FOR UPDATE;
      END LOOP;
    END IF;

    -- Walk each pool, compute delta, build a transfer event if non-zero.
    FOR v_pool_record IN
      SELECT v.id          AS val_acct,
             v.currency    AS currency,
             v.location_id AS location_id,
             q.id          AS qty_acct
        FROM accounts v
        JOIN accounts q
          ON q.kind = 'stock_available'
         AND q.sku_id = v.sku_id
         AND q.location_id = v.location_id
         AND NOT q.is_closed
       WHERE v.kind IN ('inv_value_raw', 'inv_value_fg')
         AND v.sku_id = p_sku_id
         AND NOT v.is_closed
       ORDER BY v.id
    LOOP
      SELECT debits_total - credits_total INTO v_pool_qty
        FROM accounts WHERE id = v_pool_record.qty_acct;

      IF v_pool_qty IS NULL OR v_pool_qty = 0 THEN
        CONTINUE;  -- empty pool, nothing to revalue
      END IF;

      v_delta := v_pool_qty * (p_new_cost - v_prior);
      IF v_delta = 0 THEN
        CONTINUE;
      END IF;

      v_total_qty   := v_total_qty + v_pool_qty;
      v_total_delta := v_total_delta + v_delta;

      -- Resolve the variance counterpart for this currency.
      SELECT id INTO v_var_acct
        FROM accounts
       WHERE kind = 'variance_std_cost_roll'
         AND ledger_kind = 'value'
         AND currency = v_pool_record.currency
         AND NOT is_closed;
      IF v_var_acct IS NULL THEN
        RAISE EXCEPTION
          'no variance_std_cost_roll(value, ccy=%) account configured',
          v_pool_record.currency
          USING ERRCODE = 'P0010';
      END IF;

      -- Sign determines direction. Write-up: debit inventory, credit
      -- variance (revaluation gain). Write-down: reverse.
      IF v_delta > 0 THEN
        v_debit  := v_pool_record.val_acct;
        v_credit := v_var_acct;
        v_amount := v_delta;
      ELSE
        v_debit  := v_var_acct;
        v_credit := v_pool_record.val_acct;
        v_amount := -v_delta;
      END IF;

      v_batch := v_batch || jsonb_build_array(
        jsonb_build_object(
          'reason',            'standard_cost_roll',
          'document_kind',     'inventory_standard_cost_roll',
          -- document_id is filled in after the audit-row INSERT below
          'document_id',       NULL,
          'debit_account_id',  v_debit,
          'credit_account_id', v_credit,
          'amount',            v_amount,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        )
      );
    END LOOP;
  END IF;

  -- INSERT audit row.
  INSERT INTO inventory_standard_cost_rolls (
    sku_id, prior_standard_cost, target_standard_cost, effective_at,
    total_delta_value, pool_qty, business_date, posted_by,
    idempotency_key, notes
  ) VALUES (
    p_sku_id, v_prior, p_new_cost, p_effective_at,
    v_total_delta, v_total_qty, p_business_date, p_posted_by,
    p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    -- Concurrent insert won the race; replay-fetch.
    SELECT id INTO v_doc_id
      FROM inventory_standard_cost_rolls
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  -- Backfill document_id on each event in v_batch and post.
  IF jsonb_array_length(v_batch) > 0 THEN
    SELECT jsonb_agg(jsonb_set(ev, '{document_id}', to_jsonb(v_doc_id::TEXT)))
      INTO v_batch
      FROM jsonb_array_elements(v_batch) ev;
    PERFORM post_transfers(v_batch, FALSE);
  END IF;

  RETURN v_doc_id;
END;
$$;
