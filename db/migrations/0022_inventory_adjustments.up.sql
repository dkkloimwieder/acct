-- acct-dj8 / acct-sb6 — first Phase 1 feature: pure inventory adjustment.
--
-- A thin document-layer wrapper around the existing cycle_count_adj
-- ledger primitive. Lets callers adjust inventory in/out at a given
-- (qty, unit_cost) without constructing the JSONB event batch by hand.
--
-- Design notes:
--
--   * Counterpart accounts are creation_void on both qty and value
--     sides — matches the existing conformance pattern. P&L counterpart
--     (inv_adj_expense) is a future revisit when accountant-facing
--     income statements are in scope.
--
--   * inventory_class ∈ ('raw','fg') for MVP. WIP adjustments with
--     routing_op are deferred (the value-side account would need to
--     resolve by (sku, routing_op, currency) instead of (sku, location,
--     currency); not on the critical path).
--
--   * Idempotency is at the document-table level: a replay with the
--     same idempotency_key short-circuits before post_transfers is
--     called, so there's no risk of partial state. The two
--     post_transfers events generate fresh idempotency_keys internally
--     (gen_random_uuid()) since their idempotency surface is a level
--     deeper than the document's.
--
--   * The reason on the underlying transfers is cycle_count_adj. The
--     reason name is historical; the primitive itself is general
--     "qty/value adjustment with creation_void counterpart." We don't
--     add a new transfer_reason enum value because Postgres can't
--     drop one cleanly on the down-migration.

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
  v_qty_acct      BIGINT;
  v_val_acct      BIGINT;
  v_void_qty      BIGINT;
  v_void_val      BIGINT;
  v_value_kind    TEXT;
  v_qty_amount    BIGINT;
  v_val_amount    BIGINT;
  v_qty_debit     BIGINT;
  v_qty_credit    BIGINT;
  v_val_debit     BIGINT;
  v_val_credit    BIGINT;
  v_batch         JSONB;
BEGIN
  -- Idempotent replay: if this idempotency_key already has a document
  -- row, return it without re-posting. The unique constraint on
  -- idempotency_key + the ON CONFLICT below give us atomicity.
  INSERT INTO inventory_adjustments (
    sku_id, location_id, qty_delta, unit_cost, currency,
    inventory_class, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_sku_id, p_location_id, p_qty_delta, p_unit_cost, p_currency,
    p_inventory_class, p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    -- Replay — return the existing document id; do NOT call post_transfers.
    SELECT id INTO v_existing_id
      FROM inventory_adjustments
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_existing_id;
  END IF;

  -- Resolve the qty side: stock_available account for (sku, location).
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

  -- Resolve the value side: inv_value_{class} for (sku, location, currency).
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

  -- Resolve creation_void counterparts.
  SELECT id INTO v_void_qty
    FROM accounts
   WHERE kind = 'creation_void' AND ledger_kind = 'qty' AND NOT is_closed;
  IF v_void_qty IS NULL THEN
    RAISE EXCEPTION 'no creation_void(qty) account configured'
      USING ERRCODE = 'P0010';
  END IF;

  SELECT id INTO v_void_val
    FROM accounts
   WHERE kind = 'creation_void' AND ledger_kind = 'value'
     AND currency = p_currency AND NOT is_closed;
  IF v_void_val IS NULL THEN
    RAISE EXCEPTION 'no creation_void(value, ccy=%) account configured', p_currency
      USING ERRCODE = 'P0010';
  END IF;

  -- Sign-flip on qty_delta < 0: an "in" adjustment debits stock_available
  -- (more inventory) and credits creation_void; an "out" adjustment is
  -- the reverse.
  v_qty_amount := abs(p_qty_delta);
  v_val_amount := v_qty_amount * p_unit_cost;

  IF p_qty_delta > 0 THEN
    v_qty_debit  := v_qty_acct;  v_qty_credit := v_void_qty;
    v_val_debit  := v_val_acct;  v_val_credit := v_void_val;
  ELSE
    v_qty_debit  := v_void_qty;  v_qty_credit := v_qty_acct;
    v_val_debit  := v_void_val;  v_val_credit := v_val_acct;
  END IF;

  -- Build the 2-event batch. Skip the value leg if amount is zero (a
  -- pure qty-only adjustment with unit_cost=0 — uncommon but legal).
  IF v_val_amount > 0 THEN
    v_batch := jsonb_build_array(
      jsonb_build_object(
        'reason',            'cycle_count_adj',
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
        'reason',            'cycle_count_adj',
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
        'reason',            'cycle_count_adj',
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
