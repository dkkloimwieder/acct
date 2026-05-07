-- acct-mv8 — post_so_allocate pre-ship workflow.
--
-- Adds the allocation step between reservation and shipment. Slice C
-- MVP went 'active' → 'shipped' directly; this migration introduces
-- 'active' → 'allocated' → 'shipped' so warehouse pickers can stage
-- goods (allocate) before truck loads them (ship). The 'allocated'
-- enum value already exists in reservation_status from mig 0002.
--
-- Allocation is a pure state transition. No ledger events post — qty
-- accounting was already handled at reserve time (qty deducted from
-- stock_available via inventory_reservations.qty). Allocation just
-- changes "this is reserved" to "this is reserved AND physically
-- picked." Shipment then drains the qty and posts cogs / revenue.
--
-- Cancellation from allocated state is a direct UPDATE to status =
-- 'cancelled' for now; an explicit post_so_cancel_reservation
-- workflow is left as a future follow-up (it'd cover cancellation
-- from any state, not just allocated).
--
-- post_so_ship's reservation predicate is widened to accept both
-- 'active' and 'allocated' so existing callers keep working and the
-- new allocate-then-ship path also works.
--
-- New error code: P0043 (so_allocate_invalid).

-- ============================================================
-- so_allocations
-- ============================================================
--
-- Header-only document — one row per post_so_allocate call. Carries
-- the idempotency_key UNIQUE for replay safety and gives operations
-- an audit trail (when was this SO allocated, by whom).

CREATE TABLE so_allocations (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  so_id           UUID NOT NULL REFERENCES sales_orders(id),
  business_date   DATE NOT NULL,
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID NOT NULL UNIQUE,
  notes           TEXT
);

CREATE INDEX so_allocations_so        ON so_allocations (so_id);
CREATE INDEX so_allocations_posted_at ON so_allocations (posted_at);

COMMENT ON TABLE so_allocations IS
  'Pre-ship allocation event. One row per post_so_allocate call. '
  'Flips matching active reservations to allocated state. No ledger '
  'events — pure state transition.';

-- ============================================================
-- post_so_allocate
-- ============================================================

CREATE OR REPLACE FUNCTION post_so_allocate(
  p_so_id           UUID,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id UUID;
  v_doc_id      UUID;
  v_so_check    UUID;
BEGIN
  -- Fast-path replay.
  SELECT id INTO v_existing_id FROM so_allocations
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  -- SO existence.
  SELECT id INTO v_so_check FROM sales_orders WHERE id = p_so_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'so_allocate_invalid: SO % not found', p_so_id
      USING ERRCODE = 'P0043';
  END IF;

  -- Insert allocation row (race-safe via UNIQUE on idempotency_key).
  INSERT INTO so_allocations (so_id, business_date, posted_by, idempotency_key, notes)
  VALUES (p_so_id, p_business_date, p_posted_by, p_idempotency_key, p_notes)
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM so_allocations
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  -- Flip active reservations to allocated. Idempotent — re-running
  -- after they're already allocated finds 0 rows to update, which is
  -- fine. Cancelled / shipped reservations are NOT touched.
  UPDATE inventory_reservations
     SET status = 'allocated'
   WHERE so_id = p_so_id
     AND status = 'active';

  RETURN v_doc_id;
END;
$$;

COMMENT ON FUNCTION post_so_allocate(UUID, DATE, UUID, UUID, TEXT) IS
  'Allocate reservations against an SO. Flips active reservations to '
  'allocated state. No ledger events — qty accounting was already '
  'handled at reserve time. Caller invokes post_so_ship next, which '
  'transitions allocated → shipped (or active → shipped on the MVP '
  'direct path).';

-- ============================================================
-- post_so_ship — widen reservation predicate
-- ============================================================
--
-- Re-create with the only change being the WHERE clause on the
-- reservation update (status IN ('active','allocated') instead of
-- = 'active'). DROP+CREATE is not needed since no parameter names
-- change; CREATE OR REPLACE preserves the existing signature.

CREATE OR REPLACE FUNCTION post_so_ship(
  p_so_id           UUID,
  p_lines           JSONB,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id    UUID;
  v_doc_id         UUID;
  v_customer_id    UUID;
  v_n              INT;
  v_idx            INT;
  v_line           JSONB;
  v_so_line_id     UUID;
  v_qty_shipped    BIGINT;
  v_unit_price     BIGINT;
  v_tax_amount     BIGINT;
  v_sl             RECORD;
  v_already_ship   BIGINT;
  v_cost_method    cost_method;
  v_unit_cost      BIGINT;
  v_qty_acct       BIGINT;
  v_val_acct       BIGINT;
  v_cust_qty       BIGINT;
  v_cust_unsettled BIGINT;
  v_revenue_acct   BIGINT;
  v_cogs_acct      BIGINT;
  v_tax_acct       BIGINT;
  v_qty_balance    BIGINT;
  v_value_balance  BIGINT;
  v_ship_line_id   UUID;
  v_batch          JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM so_shipments
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT customer_id INTO v_customer_id FROM sales_orders WHERE id = p_so_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'so_ship_invalid: SO % not found', p_so_id
      USING ERRCODE = 'P0037';
  END IF;
  IF v_customer_id IS NULL THEN
    RAISE EXCEPTION 'so_ship_invalid: SO % has no customer_id', p_so_id
      USING ERRCODE = 'P0037';
  END IF;

  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'so_ship_invalid: empty lines for SO %', p_so_id
      USING ERRCODE = 'P0037';
  END IF;

  INSERT INTO so_shipments (so_id, business_date, posted_by, idempotency_key, notes)
  VALUES (p_so_id, p_business_date, p_posted_by, p_idempotency_key, p_notes)
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM so_shipments WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  FOR v_idx IN 1..v_n LOOP
    v_line        := p_lines -> (v_idx - 1);
    v_so_line_id  := (v_line->>'so_line_id')::UUID;
    v_qty_shipped := (v_line->>'qty_shipped')::BIGINT;

    IF v_qty_shipped IS NULL OR v_qty_shipped <= 0 THEN
      RAISE EXCEPTION 'so_ship_invalid: line % qty_shipped must be > 0',
                      v_idx USING ERRCODE = 'P0037';
    END IF;

    SELECT sl.*
      INTO v_sl
      FROM sales_order_lines sl
     WHERE sl.id = v_so_line_id AND sl.so_id = p_so_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'so_ship_invalid: so_line % not found on SO %',
                      v_so_line_id, p_so_id USING ERRCODE = 'P0037';
    END IF;

    SELECT COALESCE(SUM(ssl.qty_shipped), 0)
      INTO v_already_ship
      FROM so_shipment_lines ssl
      JOIN so_shipments ss ON ss.id = ssl.shipment_id
     WHERE ssl.so_line_id = v_so_line_id AND ss.id <> v_doc_id;

    IF v_already_ship + v_qty_shipped > v_sl.qty_ordered THEN
      RAISE EXCEPTION
        'so_line_overshipped: so_line % qty_ordered=% already=% requested=%',
        v_so_line_id, v_sl.qty_ordered, v_already_ship, v_qty_shipped
        USING ERRCODE = 'P0038';
    END IF;

    v_unit_price := COALESCE((v_line->>'unit_price')::BIGINT, v_sl.unit_price);
    v_tax_amount := COALESCE((v_line->>'tax_amount')::BIGINT, v_sl.tax_amount);

    SELECT id INTO v_qty_acct FROM accounts
     WHERE kind='stock_available' AND sku_id=v_sl.sku_id
       AND location_id=v_sl.ship_location_id AND NOT is_closed;
    IF v_qty_acct IS NULL THEN
      RAISE EXCEPTION 'no open stock_available for sku=% loc=%',
                      v_sl.sku_id, v_sl.ship_location_id USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_val_acct FROM accounts
     WHERE kind='inv_value_fg' AND sku_id=v_sl.sku_id
       AND location_id=v_sl.ship_location_id AND currency=v_sl.currency
       AND NOT is_closed;
    IF v_val_acct IS NULL THEN
      RAISE EXCEPTION 'no open inv_value_fg for sku=% loc=% ccy=%',
                      v_sl.sku_id, v_sl.ship_location_id, v_sl.currency
        USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_cust_qty FROM accounts
     WHERE kind='customer_pool' AND counterparty_id=v_customer_id
       AND NOT is_closed;
    IF v_cust_qty IS NULL THEN
      SELECT id INTO v_cust_qty FROM accounts
       WHERE kind='customer_pool' AND counterparty_id IS NULL
         AND NOT is_closed;
    END IF;
    IF v_cust_qty IS NULL THEN
      RAISE EXCEPTION 'no open customer_pool for customer=%',
                      v_customer_id USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_cust_unsettled FROM accounts
     WHERE kind='ar_unsettled' AND counterparty_id=v_customer_id
       AND currency=v_sl.currency AND NOT is_closed;
    IF v_cust_unsettled IS NULL THEN
      SELECT id INTO v_cust_unsettled FROM accounts
       WHERE kind='ar_unsettled' AND counterparty_id IS NULL
         AND currency=v_sl.currency AND NOT is_closed;
    END IF;
    IF v_cust_unsettled IS NULL THEN
      RAISE EXCEPTION 'no open ar_unsettled for customer=% ccy=%',
                      v_customer_id, v_sl.currency USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_revenue_acct FROM accounts
     WHERE kind='revenue' AND currency=v_sl.currency AND NOT is_closed;
    IF v_revenue_acct IS NULL THEN
      RAISE EXCEPTION 'no open revenue account for ccy=%',
                      v_sl.currency USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_cogs_acct FROM accounts
     WHERE kind='cogs' AND currency=v_sl.currency AND NOT is_closed;
    IF v_cogs_acct IS NULL THEN
      RAISE EXCEPTION 'no open cogs account for ccy=%',
                      v_sl.currency USING ERRCODE = 'P0010';
    END IF;

    SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_sl.sku_id;
    IF v_cost_method = 'standard' THEN
      v_unit_cost := resolve_standard_cost_at(v_sl.sku_id, p_business_date);
    ELSIF v_cost_method IN ('wac_perpetual', 'wac_periodic', 'wac_retroactive') THEN
      SELECT debits_total - credits_total INTO v_value_balance
        FROM accounts WHERE id = v_val_acct FOR UPDATE;
      SELECT COALESCE(SUM(
               CASE WHEN t.debit_account_id = v_val_acct THEN t.qty
                    WHEN t.credit_account_id = v_val_acct THEN -t.qty
                    ELSE 0 END), 0)
        INTO v_qty_balance
        FROM transfers t
       WHERE (t.debit_account_id = v_val_acct OR t.credit_account_id = v_val_acct)
         AND t.qty IS NOT NULL;
      IF v_qty_balance <= 0 THEN
        RAISE EXCEPTION 'so_ship_invalid: empty inv_value_fg pool for sku=% loc=%',
                        v_sl.sku_id, v_sl.ship_location_id
          USING ERRCODE = 'P0006';
      END IF;
      v_unit_cost := v_value_balance / v_qty_balance;
    ELSE
      RAISE EXCEPTION 'cost_method_not_implemented: % for so_ship',
                      v_cost_method USING ERRCODE = 'P0006';
    END IF;

    INSERT INTO so_shipment_lines (
      shipment_id, so_line_id, qty_shipped, unit_cost, unit_price, tax_amount
    ) VALUES (
      v_doc_id, v_so_line_id, v_qty_shipped, v_unit_cost, v_unit_price, v_tax_amount
    ) RETURNING id INTO v_ship_line_id;

    v_batch := v_batch || jsonb_build_array(
      jsonb_build_object(
        'reason',            'so_ship',
        'document_kind',     'so_shipment',
        'document_id',       v_doc_id,
        'document_line_id',  v_ship_line_id,
        'debit_account_id',  v_cust_qty,
        'credit_account_id', v_qty_acct,
        'amount',            v_qty_shipped,
        'qty',               v_qty_shipped,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   v_customer_id,
        'posted_by',         p_posted_by
      ),
      jsonb_build_object(
        'reason',            'so_ship',
        'document_kind',     'so_shipment',
        'document_id',       v_doc_id,
        'document_line_id',  v_ship_line_id,
        'debit_account_id',  v_cogs_acct,
        'credit_account_id', v_val_acct,
        'amount',            v_qty_shipped * v_unit_cost,
        'qty',               v_qty_shipped,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   v_customer_id,
        'posted_by',         p_posted_by
      ),
      jsonb_build_object(
        'reason',            'so_ship',
        'document_kind',     'so_shipment',
        'document_id',       v_doc_id,
        'document_line_id',  v_ship_line_id,
        'debit_account_id',  v_cust_unsettled,
        'credit_account_id', v_revenue_acct,
        'amount',            v_qty_shipped * v_unit_price,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   v_customer_id,
        'posted_by',         p_posted_by
      )
    );

    IF v_tax_amount > 0 THEN
      SELECT id INTO v_tax_acct FROM accounts
       WHERE kind='sales_tax_payable' AND currency=v_sl.currency
         AND NOT is_closed;
      IF v_tax_acct IS NULL THEN
        RAISE EXCEPTION 'no open sales_tax_payable for ccy=%',
                        v_sl.currency USING ERRCODE = 'P0010';
      END IF;
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'so_ship',
        'document_kind',     'so_shipment',
        'document_id',       v_doc_id,
        'document_line_id',  v_ship_line_id,
        'debit_account_id',  v_cust_unsettled,
        'credit_account_id', v_tax_acct,
        'amount',            v_tax_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   v_customer_id,
        'posted_by',         p_posted_by
      ));
    END IF;
  END LOOP;

  -- Flip active OR allocated reservations on this SO to 'shipped'.
  -- Atomic with the shipment INSERT above. Widened from mig 0081
  -- (which only accepted 'active') to support post_so_allocate's
  -- pre-ship allocation step.
  UPDATE inventory_reservations
     SET status = 'shipped',
         resolved_at = clock_timestamp()
   WHERE so_id = p_so_id
     AND status IN ('active', 'allocated');

  PERFORM post_transfers(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;

COMMENT ON FUNCTION post_so_ship(UUID, JSONB, DATE, UUID, UUID, TEXT) IS
  'Ship goods against an SO. Per line: posts qty (customer_pool DR / '
  'stock_available CR), COGS (cogs DR / inv_value_fg CR — dispatcher-'
  'priced via cost_method), revenue (ar_unsettled DR / revenue CR), '
  'and tax (ar_unsettled DR / sales_tax_payable CR) when tax > 0. '
  'Strict over-ship rejection (P0038). GRNI-mirror: ar_unsettled is '
  'debited, not ar (cleared by post_customer_invoice). Reservations '
  'in active OR allocated state flip to shipped atomically; allocate '
  'is the pre-ship pick step (post_so_allocate).';
