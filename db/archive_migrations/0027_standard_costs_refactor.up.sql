-- acct-x4t / acct-hlr.1 — Phase 1 Epic F: standard cost as a separate
-- transactional entity. The schema refactor that drops skus.standard_cost
-- and replaces it with the standard_costs table + resolve_standard_cost_at()
-- helper + P0018 gate.
--
-- WHY THIS REFACTOR. skus.standard_cost was set at SKU INSERT and never
-- updated. That conflated 'this item exists' with 'this item has a
-- standard cost effective on/after some date' — two distinct ERP
-- concepts. The column-as-attribute model:
--   - had no audit trail of who set the initial cost
--   - had no effective-date semantics
--   - couldn't represent 'item exists, cost not yet decided'
--   - made cost rolls an awkward UPDATE-and-revalue instead of an
--     append-only entry in a cost-history stream
--
-- TARGET MODEL.
--   * standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key, ...)
--     — append-only, time-ordered. Each row is a 'cost estimate' that
--     becomes active on its effective_at date.
--   * skus.standard_cost column DROPPED.
--   * resolve_standard_cost_at(p_sku_id, p_business_date) is the single
--     canonical lookup. Returns the active cost (latest effective_at <=
--     business_date) or raises P0018 (standard_cost_not_established) if
--     none exists.
--   * P0018 GATE: cost-relevant operations on standard SKUs that go
--     through resolve_standard_cost_at() inherit the gate automatically.
--     Currently caught by:
--       - _post_transfers_compute_amount (post_transfers' value-side
--         cost dispatcher; touches op_move, scrap, wo_complete, so_ship)
--       - post_inventory_adjustment (standard branch, NULL p_unit_cost)
--     Future Phase 1 workflows (PO receipt, SO ship, etc.) inherit the
--     gate by going through these helpers.
--   * Narrow gate (per user spec): operations that don't read the cost
--     are unaffected — qty-only events, account creation, metadata
--     queries. The gate fires only when cost is actually needed.
--
-- DOWN MIGRATION is full reversal — re-adds skus.standard_cost,
-- backfills from standard_costs (latest effective_at per SKU),
-- restores the prior bodies of _post_transfers_compute_amount and
-- post_inventory_adjustment from migration 0023, drops the new
-- objects. ci-check.sh round-trips cleanly.
--
-- DATA MIGRATION. Existing 'standard'-method SKUs in the dev DB get
-- one standard_costs row at effective_at='1970-01-01' so all
-- post-migration business_dates resolve. acct_test and acct_ci start
-- empty (skus is empty until seed.sql runs after migrations); the
-- INSERT below is a no-op there. seed.sql is updated in the same
-- change to use the new shape going forward.
--
-- BOOTSTRAP ACTOR. Migrated rows use posted_by =
-- '00000000-0000-0000-0000-000000000000' as the system-bootstrap UUID.
-- This is consistent with how data-migration sentinels are recorded
-- elsewhere in the project; not validated against any RBAC since
-- Q6 is still open.

-- ============================================================
-- standard_costs table
-- ============================================================

CREATE TABLE standard_costs (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  sku_id          UUID NOT NULL REFERENCES skus(id),
  cost            BIGINT NOT NULL CHECK (cost >= 0),
  effective_at    DATE NOT NULL,
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID NOT NULL UNIQUE,
  notes           TEXT
);

-- Hot path for resolve_standard_cost_at: latest effective_at <=
-- business_date for a given SKU. DESC index lets the LIMIT-1 walk
-- backwards from the requested date.
CREATE INDEX std_cost_sku_eff
  ON standard_costs (sku_id, effective_at DESC);

-- ============================================================
-- resolve_standard_cost_at: canonical lookup + P0018 gate.
-- ============================================================

CREATE OR REPLACE FUNCTION resolve_standard_cost_at(
  p_sku_id        UUID,
  p_business_date DATE
) RETURNS BIGINT
LANGUAGE plpgsql STABLE
AS $$
DECLARE
  v_cost BIGINT;
BEGIN
  SELECT cost INTO v_cost
    FROM standard_costs
   WHERE sku_id = p_sku_id
     AND effective_at <= p_business_date
   ORDER BY effective_at DESC
   LIMIT 1;
  IF NOT FOUND THEN
    RAISE EXCEPTION
      'standard_cost_not_established: sku=% has no standard cost in effect as of %',
      p_sku_id, p_business_date
      USING ERRCODE = 'P0018';
  END IF;
  RETURN v_cost;
END;
$$;

-- ============================================================
-- Data migration: copy existing skus.standard_cost into standard_costs.
-- No-op on freshly-created databases (test/CI). Captures dev-DB data
-- before the column drop on the next step.
-- ============================================================

INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
SELECT id, standard_cost, '1970-01-01'::DATE,
       '00000000-0000-0000-0000-000000000000'::UUID, gen_random_uuid()
  FROM skus
 WHERE cost_method = 'standard' AND standard_cost IS NOT NULL;

-- ============================================================
-- Drop skus.standard_cost.
-- ============================================================

ALTER TABLE skus DROP COLUMN standard_cost;

-- ============================================================
-- Refactor _post_transfers_compute_amount: standard branch reads via
-- resolve_standard_cost_at instead of skus.standard_cost. All other
-- branches identical to migration 0023.
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
  v_qty_id         BIGINT;
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

  v_sku := COALESCE(p_d_acct.sku_id, p_c_acct.sku_id);
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

      v_qty_id := _post_transfers_lookup_qty_account(p_c_acct);
      IF v_qty_id IS NULL THEN
        RAISE EXCEPTION 'wac_perpetual cannot resolve matching qty account for credit-side % at event index %',
                        p_c_acct.kind, p_idx
          USING ERRCODE = 'P0006';
      END IF;

      SELECT (debits_total - credits_total) INTO v_qty_balance
        FROM accounts WHERE id = v_qty_id;

      IF v_qty_balance IS NULL OR v_qty_balance <= 0 THEN
        RAISE EXCEPTION 'wac_perpetual qty balance is %, cannot divide for unit cost at event index %',
                        v_qty_balance, p_idx
          USING ERRCODE = 'P0006';
      END IF;

      v_value_balance := p_c_acct.debits_total - p_c_acct.credits_total;
      IF v_value_balance < 0 THEN
        v_value_balance := 0;
      END IF;

      v_unit := v_value_balance / v_qty_balance;
      RETURN v_qty * v_unit;

    WHEN 'wac_periodic' THEN
      RAISE EXCEPTION 'cost_method_not_implemented: wac_periodic (tracked as acct-qfj; depends on period-close machinery) at event index %',
                      p_idx
        USING ERRCODE = 'P0006';

    WHEN 'wac_retroactive' THEN
      RAISE EXCEPTION 'cost_method_not_implemented: wac_retroactive (tracked as acct-9tw; depends on period-close machinery) at event index %',
                      p_idx
        USING ERRCODE = 'P0006';

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
-- Refactor post_inventory_adjustment: drops v_std_cost local; the
-- standard branch reads via resolve_standard_cost_at. All other
-- behavior identical to migration 0023.
-- ============================================================

CREATE OR REPLACE FUNCTION post_inventory_adjustment(
  p_sku_id          UUID,
  p_location_id     UUID,
  p_qty_delta       BIGINT,
  p_unit_cost       BIGINT,
  p_currency        TEXT,
  p_inventory_class TEXT,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id   UUID;
  v_doc_id        UUID;
  v_cost_method   cost_method;
  v_qty_acct      BIGINT;
  v_val_acct      BIGINT;
  v_void_qty      BIGINT;
  v_void_val      BIGINT;
  v_value_kind    TEXT;
  v_lock_first    BIGINT;
  v_lock_second   BIGINT;
  v_qty_balance   BIGINT;
  v_val_balance   BIGINT;
  v_effective_uc  BIGINT;
  v_qty_amount    BIGINT;
  v_val_amount    BIGINT;
  v_qty_debit     BIGINT;
  v_qty_credit    BIGINT;
  v_val_debit     BIGINT;
  v_val_credit    BIGINT;
  v_batch         JSONB;
BEGIN
  SELECT id INTO v_existing_id
    FROM inventory_adjustments
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN
    RETURN v_existing_id;
  END IF;

  SELECT cost_method INTO v_cost_method
    FROM skus WHERE id = p_sku_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'sku % not found', p_sku_id USING ERRCODE = 'P0010';
  END IF;

  SELECT id INTO v_qty_acct
    FROM accounts
   WHERE kind        = 'stock_available'
     AND sku_id      = p_sku_id
     AND location_id = p_location_id
     AND NOT is_closed;
  IF v_qty_acct IS NULL THEN
    RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                    p_sku_id, p_location_id
      USING ERRCODE = 'P0010';
  END IF;

  v_value_kind := 'inv_value_' || p_inventory_class;
  EXECUTE format(
    'SELECT id FROM accounts
      WHERE kind = %L AND sku_id = $1 AND location_id = $2
        AND currency = $3 AND NOT is_closed',
    v_value_kind
  )
  INTO v_val_acct
  USING p_sku_id, p_location_id, p_currency;
  IF v_val_acct IS NULL THEN
    RAISE EXCEPTION 'no open % account for sku=% loc=% ccy=%',
                    v_value_kind, p_sku_id, p_location_id, p_currency
      USING ERRCODE = 'P0010';
  END IF;

  SELECT id INTO v_void_qty
    FROM accounts
   WHERE kind = 'creation_void' AND ledger_kind = 'qty' AND NOT is_closed;
  IF v_void_qty IS NULL THEN
    RAISE EXCEPTION 'no creation_void(qty) account configured'
      USING ERRCODE = 'P0010';
  END IF;

  SELECT id INTO v_void_val
    FROM accounts
   WHERE kind = 'inv_adj_expense' AND ledger_kind = 'value'
     AND currency = p_currency AND NOT is_closed;
  IF v_void_val IS NULL THEN
    RAISE EXCEPTION 'no inv_adj_expense(value, ccy=%) account configured', p_currency
      USING ERRCODE = 'P0010';
  END IF;

  CASE v_cost_method
  WHEN 'standard' THEN
    IF p_unit_cost IS NOT NULL THEN
      RAISE EXCEPTION
        'standard SKU % has a fixed standard cost; do not pass p_unit_cost (got %)',
        p_sku_id, p_unit_cost
        USING ERRCODE = 'P0011';
    END IF;
    -- resolve_standard_cost_at raises P0018 if no standard exists at
    -- p_business_date for this SKU.
    v_effective_uc := resolve_standard_cost_at(p_sku_id, p_business_date);

  WHEN 'wac_perpetual' THEN
    -- Lock qty + value accounts in ascending id order (matches
    -- post_transfers' lock-order invariant).
    v_lock_first  := LEAST(v_qty_acct, v_val_acct);
    v_lock_second := GREATEST(v_qty_acct, v_val_acct);
    PERFORM 1 FROM accounts WHERE id = v_lock_first  FOR UPDATE;
    PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;

    SELECT debits_total - credits_total INTO v_qty_balance
      FROM accounts WHERE id = v_qty_acct;
    SELECT debits_total - credits_total INTO v_val_balance
      FROM accounts WHERE id = v_val_acct;

    IF p_qty_delta > 0 THEN
      -- IN: NULL → pool avg (or P0011 if empty); explicit → re-averages.
      IF p_unit_cost IS NULL THEN
        IF v_qty_balance <= 0 THEN
          RAISE EXCEPTION
            'wac_perpetual SKU % at sku=% loc=% has empty pool (qty_balance=%); '
            'caller must pass p_unit_cost on first adjustment-in to seed',
            p_sku_id, p_sku_id, p_location_id, v_qty_balance
            USING ERRCODE = 'P0011';
        END IF;
        v_effective_uc := v_val_balance / v_qty_balance;
      ELSE
        v_effective_uc := p_unit_cost;
      END IF;
    ELSE
      -- OUT: NULL → pool avg only. Explicit → P0011 (asserted-cost-on-
      -- depletion belongs in 'lot' cost_method, acct-8gg). Pool empty → P0010.
      IF p_unit_cost IS NOT NULL THEN
        RAISE EXCEPTION
          'wac_perpetual depletion does not accept asserted unit_cost '
          '(got % on sku=% loc=%); use lot cost_method (acct-8gg) for '
          'asserted-cost-per-transaction needs',
          p_unit_cost, p_sku_id, p_location_id
          USING ERRCODE = 'P0011';
      END IF;
      IF v_qty_balance <= 0 THEN
        RAISE EXCEPTION
          'wac_perpetual SKU % at sku=% loc=% has empty pool; cannot deplete',
          p_sku_id, p_sku_id, p_location_id
          USING ERRCODE = 'P0010';
      END IF;
      v_effective_uc := v_val_balance / v_qty_balance;
    END IF;

  WHEN 'wac_periodic' THEN
    RAISE EXCEPTION
      'cost_method_not_implemented: wac_periodic (acct-qfj; depends on period-close machinery) for sku=%',
      p_sku_id USING ERRCODE = 'P0006';

  WHEN 'wac_retroactive' THEN
    RAISE EXCEPTION
      'cost_method_not_implemented: wac_retroactive (acct-9tw; depends on period-close machinery) for sku=%',
      p_sku_id USING ERRCODE = 'P0006';

  WHEN 'fifo', 'lot' THEN
    RAISE EXCEPTION
      'cost_method_not_implemented: % (sku=%); see acct-8gg',
      v_cost_method, p_sku_id
      USING ERRCODE = 'P0006';

  ELSE
    RAISE EXCEPTION
      'unknown cost_method % for sku=%', v_cost_method, p_sku_id
      USING ERRCODE = 'P0011';
  END CASE;

  v_qty_amount := abs(p_qty_delta);
  v_val_amount := v_qty_amount * v_effective_uc;

  IF p_qty_delta > 0 THEN
    v_qty_debit  := v_qty_acct;  v_qty_credit := v_void_qty;
    v_val_debit  := v_val_acct;  v_val_credit := v_void_val;
  ELSE
    v_qty_debit  := v_void_qty;  v_qty_credit := v_qty_acct;
    v_val_debit  := v_void_val;  v_val_credit := v_val_acct;
  END IF;

  INSERT INTO inventory_adjustments (
    sku_id, location_id, qty_delta, unit_cost, currency,
    inventory_class, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_sku_id, p_location_id, p_qty_delta, v_effective_uc, p_currency,
    p_inventory_class, p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id
      FROM inventory_adjustments
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  IF v_val_amount > 0 THEN
    v_batch := jsonb_build_array(
      jsonb_build_object(
        'reason',            'inventory_adjustment',
        'document_kind',     'inventory_adjustment',
        'document_id',       v_doc_id,
        'debit_account_id',  v_qty_debit,
        'credit_account_id', v_qty_credit,
        'amount',            v_qty_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      ),
      jsonb_build_object(
        'reason',            'inventory_adjustment',
        'document_kind',     'inventory_adjustment',
        'document_id',       v_doc_id,
        'debit_account_id',  v_val_debit,
        'credit_account_id', v_val_credit,
        'amount',            v_val_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      )
    );
  ELSE
    v_batch := jsonb_build_array(
      jsonb_build_object(
        'reason',            'inventory_adjustment',
        'document_kind',     'inventory_adjustment',
        'document_id',       v_doc_id,
        'debit_account_id',  v_qty_debit,
        'credit_account_id', v_qty_credit,
        'amount',            v_qty_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      )
    );
  END IF;

  PERFORM post_transfers(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;
