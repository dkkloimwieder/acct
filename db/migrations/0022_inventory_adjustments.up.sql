-- acct-dj8 / acct-sb6 — first Phase 1 feature: pure inventory adjustment.
--
-- A thin document-layer wrapper that lets callers adjust inventory
-- in/out at a given (qty, unit_cost) without constructing the JSONB
-- event batch by hand. Distinct from cycle_count_adj — that reason
-- stays for cycle-count-specific document workflows; this one is the
-- generic primitive.
--
-- Design notes:
--
--   * New transfer_reason enum value 'inventory_adjustment'. Adding an
--     enum value is one-way: Postgres can't cleanly drop a value once
--     added, so the down migration drops the table and function but
--     leaves the enum value in place. This matches the project's
--     convention (see migration 0020's down-comment) of "Phase 0/1 has
--     no production data; down is best-effort."
--
--   * Qty-side counterpart is creation_void (qty has no P&L concept).
--     Value-side counterpart is inv_adj_expense — a bidirectional P&L
--     account so adjustment-in posts as adjustment income (counterpart
--     credited) and adjustment-out posts as adjustment expense
--     (counterpart debited). Accumulated balance at period close is
--     the net adjustment gain/loss on the income statement.
--
--   * inventory_class ∈ ('raw','fg') for MVP. WIP adjustments with
--     routing_op are deferred.
--
--   * Idempotency is at the document-table level: a replay with the
--     same idempotency_key short-circuits before post_transfers is
--     called, so there's no risk of partial state.
--
--   * Cost-method-aware dispatch (per acct-sb6 design clarification):
--
--       Caller passes p_unit_cost NULL or an explicit value. Behavior
--       depends on the SKU's cost_method:
--
--         standard:  NULL  -> use skus.standard_cost
--                    value -> P0011 (standard SKUs have a fixed cost;
--                                    do not pass one)
--
--         wac IN:    NULL  -> use pool average; P0011 if pool empty
--                             (must seed at known cost)
--                    value -> use it; pool re-averages
--
--         wac OUT:   NULL  -> use pool average (classic WAC)
--                    value -> use it (caller asserts lot knowledge);
--                             pool average drifts to reflect the true
--                             cost of remaining material
--
--         fifo/lot:  always P0006 (not implemented)
--
--     Pool reads happen under FOR UPDATE on the qty + value accounts,
--     locked in ascending id order to match post_transfers' lock-order
--     invariant. The same accounts are re-locked by post_transfers
--     downstream — same-row same-tx locking is a no-op in PG.

ALTER TYPE transfer_reason ADD VALUE IF NOT EXISTS 'inventory_adjustment';

CREATE TABLE inventory_adjustments (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  sku_id          UUID NOT NULL REFERENCES skus(id),
  location_id     UUID NOT NULL REFERENCES locations(id),
  qty_delta       BIGINT NOT NULL CHECK (qty_delta <> 0),
  unit_cost       BIGINT NOT NULL CHECK (unit_cost >= 0),
  currency        TEXT   NOT NULL,
  inventory_class TEXT   NOT NULL CHECK (inventory_class IN ('raw','fg')),
  business_date   DATE   NOT NULL,
  posted_by       UUID   NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID   NOT NULL UNIQUE,
  notes           TEXT
);

CREATE INDEX inv_adj_sku_loc ON inventory_adjustments (sku_id, location_id);
CREATE INDEX inv_adj_posted_at ON inventory_adjustments (posted_at);

CREATE OR REPLACE FUNCTION post_inventory_adjustment(
  p_sku_id          UUID,
  p_location_id     UUID,
  p_qty_delta       BIGINT,
  p_unit_cost       BIGINT,            -- NULL = use system cost
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
  v_std_cost      BIGINT;
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
  -- Fast-path replay check before doing any work.
  SELECT id INTO v_existing_id
    FROM inventory_adjustments
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN
    RETURN v_existing_id;
  END IF;

  -- Look up the SKU's cost_method (also validates the sku exists).
  SELECT cost_method, standard_cost INTO v_cost_method, v_std_cost
    FROM skus WHERE id = p_sku_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'sku % not found', p_sku_id USING ERRCODE = 'P0010';
  END IF;

  -- Resolve qty side: stock_available(sku, location).
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

  -- Resolve value side: inv_value_{class}(sku, location, currency).
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

  -- Resolve counterparts.
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

  -- ===== Cost-method dispatch =====
  CASE v_cost_method
  WHEN 'standard' THEN
    IF p_unit_cost IS NOT NULL THEN
      RAISE EXCEPTION
        'standard SKU % has a fixed standard_cost; do not pass p_unit_cost (got %)',
        p_sku_id, p_unit_cost
        USING ERRCODE = 'P0011';
    END IF;
    v_effective_uc := v_std_cost;

  WHEN 'wac' THEN
    -- Lock qty + value accounts in ascending id order to match
    -- post_transfers' lock-order invariant. Locks are released at tx
    -- commit; post_transfers re-locking the same rows in this tx is
    -- a no-op.
    v_lock_first  := LEAST(v_qty_acct, v_val_acct);
    v_lock_second := GREATEST(v_qty_acct, v_val_acct);
    PERFORM 1 FROM accounts WHERE id = v_lock_first  FOR UPDATE;
    PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;

    SELECT debits_total - credits_total INTO v_qty_balance
      FROM accounts WHERE id = v_qty_acct;
    SELECT debits_total - credits_total INTO v_val_balance
      FROM accounts WHERE id = v_val_acct;

    IF p_unit_cost IS NULL THEN
      IF v_qty_balance <= 0 THEN
        RAISE EXCEPTION
          'wac SKU % at sku=% loc=% has empty pool (qty_balance=%); '
          'caller must pass p_unit_cost on first adjustment-in to seed',
          p_sku_id, p_sku_id, p_location_id, v_qty_balance
          USING ERRCODE = 'P0011';
      END IF;
      v_effective_uc := v_val_balance / v_qty_balance;  -- pool avg
    ELSE
      v_effective_uc := p_unit_cost;
    END IF;

  WHEN 'fifo', 'lot' THEN
    RAISE EXCEPTION
      'cost_method % not implemented for inventory_adjustment (sku=%); see acct-8gg',
      v_cost_method, p_sku_id
      USING ERRCODE = 'P0006';

  ELSE
    RAISE EXCEPTION
      'unknown cost_method % for sku=%', v_cost_method, p_sku_id
      USING ERRCODE = 'P0011';
  END CASE;
  -- ===== End cost-method dispatch =====

  v_qty_amount := abs(p_qty_delta);
  v_val_amount := v_qty_amount * v_effective_uc;

  -- Sign-flip on qty_delta < 0: an "in" adjustment debits stock_available
  -- (more inventory) and credits the counterpart; an "out" is the reverse.
  IF p_qty_delta > 0 THEN
    v_qty_debit  := v_qty_acct;  v_qty_credit := v_void_qty;
    v_val_debit  := v_val_acct;  v_val_credit := v_void_val;
  ELSE
    v_qty_debit  := v_void_qty;  v_qty_credit := v_qty_acct;
    v_val_debit  := v_void_val;  v_val_credit := v_val_acct;
  END IF;

  -- Insert the audit row with the EFFECTIVE unit_cost (what was actually
  -- applied), not the caller's input. This makes the audit trail
  -- self-explanatory. ON CONFLICT handles the race where two callers
  -- with the same idempotency_key arrive concurrently.
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
    -- Race: another tx beat us to the insert. Return its id; do not post.
    SELECT id INTO v_doc_id
      FROM inventory_adjustments
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  -- Build the 2-event batch. Skip the value leg if effective unit cost
  -- is 0 (only possible when standard_cost is 0 — uncommon but legal).
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
