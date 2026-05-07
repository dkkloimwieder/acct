-- 0018_outflow_documents — Slice C (sales / customer / AR cycle).
--
-- Body sources (in archive_migrations/):
--   * 0079 (acct-th7) — ar_unsettled / 'shipped' enum extensions
--                       (already applied in 0002_types_and_enums)
--   * 0080 (acct-th7) — sales_order_lines / so_shipments / so_shipment_lines
--                        / customer_invoices / customer_invoice_lines / ar_payments
--   * 0081 (acct-th7) — initial post_so_ship / post_customer_invoice /
--                        post_ar_payment (superseded by 0091/0092)
--   * 0083 (acct-mv8) — so_allocations + post_so_allocate;
--                        post_so_ship reservation predicate widened
--   * 0084 (acct-ari) — customer_returns / customer_return_lines + initial
--                        post_customer_return (superseded by 0086)
--   * 0086 (acct-tk7 + acct-dso) — state-aware return routing +
--                        p_override_closed_period; qty_to_ar_unsettled / qty_to_ar
--                        split columns on customer_return_lines
--   * 0087 (acct-6d8) — cost_method_at_ship snapshot on so_shipment_lines;
--                        post_so_ship captures it
--   * 0088 (acct-tae + acct-b6e) — customer_credit_memos / vendor_debit_memos
--                        + post_customer_credit_memo / post_vendor_debit_memo
--   * 0090 (acct-7mc) — three-way match tolerance on post_customer_invoice
--   * 0091 (acct-5prc + acct-nuw7) — post_so_ship lock v_val_acct (R4 + R7);
--                        post_customer_invoice zero-baseline arm
--   * 0092 (acct-3xcg) — post_ar_payment cross-currency settlement with
--                        realized FX gain / loss
--
-- Naming unifications baked in: transfers->posting_lines,
-- post_transfers->post_posting_lines, resolve_standard_cost_at->
-- _resolve_standard_cost_at, transfer_reason->posting_line_reason,
-- document_kind 'so_shipment'->'so_ship' (per-event).

-- ============================================================
-- 1. sales_order_lines
-- ============================================================

CREATE TABLE sales_order_lines (
  id               UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  so_id            UUID NOT NULL REFERENCES sales_orders(id),
  line_no          INT  NOT NULL,
  sku_id           UUID NOT NULL REFERENCES skus(id),
  ship_location_id UUID NOT NULL REFERENCES locations(id),
  qty_ordered      BIGINT NOT NULL CHECK (qty_ordered > 0),
  unit_price       BIGINT NOT NULL CHECK (unit_price >= 0),
  currency         CHAR(3) NOT NULL,
  tax_amount       BIGINT NOT NULL DEFAULT 0 CHECK (tax_amount >= 0),
  UNIQUE (so_id, line_no)
);

CREATE INDEX so_lines_so      ON sales_order_lines (so_id);
CREATE INDEX so_lines_sku_loc ON sales_order_lines (sku_id, ship_location_id);

-- ============================================================
-- 2. so_allocations (header-only; pre-ship reservation flip)
-- ============================================================

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

-- ============================================================
-- 3. so_shipments + so_shipment_lines (with cost_method_at_ship)
-- ============================================================

CREATE TABLE so_shipments (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  so_id           UUID NOT NULL REFERENCES sales_orders(id),
  business_date   DATE NOT NULL,
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID NOT NULL UNIQUE,
  notes           TEXT
);

CREATE INDEX so_shipments_so        ON so_shipments (so_id);
CREATE INDEX so_shipments_posted_at ON so_shipments (posted_at);

CREATE TABLE so_shipment_lines (
  id                  UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  shipment_id         UUID NOT NULL REFERENCES so_shipments(id),
  so_line_id          UUID NOT NULL REFERENCES sales_order_lines(id),
  qty_shipped         BIGINT NOT NULL CHECK (qty_shipped > 0),
  unit_cost           BIGINT NOT NULL CHECK (unit_cost >= 0),
  unit_price          BIGINT NOT NULL CHECK (unit_price >= 0),
  tax_amount          BIGINT NOT NULL DEFAULT 0 CHECK (tax_amount >= 0),
  cost_method_at_ship cost_method NOT NULL DEFAULT 'standard',
  UNIQUE (shipment_id, so_line_id)
);

CREATE INDEX so_shipment_lines_so_line ON so_shipment_lines (so_line_id);

-- ============================================================
-- 4. customer_invoices + customer_invoice_lines
-- ============================================================

CREATE TABLE customer_invoices (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  customer_id     UUID NOT NULL REFERENCES customers(id),
  currency        CHAR(3) NOT NULL,
  business_date   DATE NOT NULL,
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID NOT NULL UNIQUE,
  notes           TEXT
);

CREATE INDEX customer_invoices_customer  ON customer_invoices (customer_id);
CREATE INDEX customer_invoices_posted_at ON customer_invoices (posted_at);

CREATE TABLE customer_invoice_lines (
  id                 UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  invoice_id         UUID NOT NULL REFERENCES customer_invoices(id),
  line_no            INT  NOT NULL,
  kind               TEXT NOT NULL CHECK (kind IN ('so_match', 'service')),
  so_line_id         UUID REFERENCES sales_order_lines(id),
  revenue_account_id BIGINT REFERENCES accounts(id),
  qty                BIGINT,
  unit_price         BIGINT,
  amount             BIGINT NOT NULL CHECK (amount > 0),
  tax_amount         BIGINT NOT NULL DEFAULT 0 CHECK (tax_amount >= 0),
  UNIQUE (invoice_id, line_no),
  CHECK (
    (kind = 'so_match'
     AND so_line_id IS NOT NULL
     AND revenue_account_id IS NULL
     AND qty IS NOT NULL AND qty > 0
     AND unit_price IS NOT NULL AND unit_price >= 0)
    OR
    (kind = 'service'
     AND so_line_id IS NULL
     AND revenue_account_id IS NOT NULL
     AND qty IS NULL
     AND unit_price IS NULL)
  )
);

CREATE INDEX customer_invoice_lines_invoice ON customer_invoice_lines (invoice_id);
CREATE INDEX customer_invoice_lines_so_line ON customer_invoice_lines (so_line_id)
  WHERE so_line_id IS NOT NULL;

-- ============================================================
-- 5. ar_payments (header-only single-event document)
-- ============================================================

CREATE TABLE ar_payments (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  customer_id     UUID NOT NULL REFERENCES customers(id),
  currency        CHAR(3) NOT NULL,
  amount          BIGINT NOT NULL CHECK (amount > 0),
  business_date   DATE NOT NULL,
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID NOT NULL UNIQUE,
  notes           TEXT
);

CREATE INDEX ar_payments_customer  ON ar_payments (customer_id);
CREATE INDEX ar_payments_posted_at ON ar_payments (posted_at);

-- ============================================================
-- 6. customer_returns + customer_return_lines (state-aware split columns)
-- ============================================================

CREATE TABLE customer_returns (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  customer_id     UUID NOT NULL REFERENCES customers(id),
  business_date   DATE NOT NULL,
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID NOT NULL UNIQUE,
  notes           TEXT
);

CREATE INDEX customer_returns_customer  ON customer_returns (customer_id);
CREATE INDEX customer_returns_posted_at ON customer_returns (posted_at);

CREATE TABLE customer_return_lines (
  id                  UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  return_id           UUID NOT NULL REFERENCES customer_returns(id),
  line_no             INT  NOT NULL,
  ship_line_id        UUID NOT NULL REFERENCES so_shipment_lines(id),
  qty_returned        BIGINT NOT NULL CHECK (qty_returned > 0),
  disposition         return_disposition NOT NULL,
  unit_cost           BIGINT NOT NULL CHECK (unit_cost  >= 0),
  unit_price          BIGINT NOT NULL CHECK (unit_price >= 0),
  tax_amount          BIGINT NOT NULL DEFAULT 0 CHECK (tax_amount >= 0),
  qty_to_ar_unsettled BIGINT NOT NULL DEFAULT 0,
  qty_to_ar           BIGINT NOT NULL DEFAULT 0,
  UNIQUE (return_id, line_no),
  CONSTRAINT customer_return_lines_split_check
    CHECK (qty_to_ar_unsettled >= 0
           AND qty_to_ar >= 0
           AND qty_to_ar_unsettled + qty_to_ar = qty_returned)
);

CREATE INDEX customer_return_lines_ship_line ON customer_return_lines (ship_line_id);

-- ============================================================
-- 7. customer_credit_memos + customer_credit_memo_lines
-- ============================================================

CREATE TABLE customer_credit_memos (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  customer_id     UUID NOT NULL REFERENCES customers(id),
  currency        CHAR(3) NOT NULL,
  business_date   DATE NOT NULL,
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID NOT NULL UNIQUE,
  notes           TEXT
);

CREATE INDEX customer_credit_memos_customer  ON customer_credit_memos (customer_id);
CREATE INDEX customer_credit_memos_posted_at ON customer_credit_memos (posted_at);

CREATE TABLE customer_credit_memo_lines (
  id                 UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  memo_id            UUID NOT NULL REFERENCES customer_credit_memos(id),
  line_no            INT NOT NULL,
  kind               TEXT NOT NULL CHECK (kind IN ('financial', 'goods_return')),
  revenue_account_id BIGINT REFERENCES accounts(id),
  sku_id             UUID REFERENCES skus(id),
  location_id        UUID REFERENCES locations(id),
  qty                BIGINT,
  unit_cost          BIGINT,
  unit_price         BIGINT,
  disposition        return_disposition,
  amount             BIGINT NOT NULL,
  tax_amount         BIGINT NOT NULL DEFAULT 0,
  UNIQUE (memo_id, line_no),
  CHECK (
    (kind = 'financial'
       AND revenue_account_id IS NOT NULL
       AND sku_id IS NULL AND location_id IS NULL
       AND qty IS NULL AND unit_cost IS NULL AND disposition IS NULL)
    OR
    (kind = 'goods_return'
       AND revenue_account_id IS NULL
       AND sku_id IS NOT NULL AND location_id IS NOT NULL
       AND qty IS NOT NULL AND qty > 0
       AND unit_cost IS NOT NULL AND unit_cost >= 0
       AND unit_price IS NOT NULL AND unit_price >= 0
       AND disposition IS NOT NULL)
  ),
  CHECK (amount >= 0 AND tax_amount >= 0)
);

CREATE INDEX customer_credit_memo_lines_sku ON customer_credit_memo_lines (sku_id);

-- ============================================================
-- 8. vendor_debit_memos + vendor_debit_memo_lines
-- ============================================================

CREATE TABLE vendor_debit_memos (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  vendor_id       UUID NOT NULL REFERENCES vendors(id),
  currency        CHAR(3) NOT NULL,
  business_date   DATE NOT NULL,
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID NOT NULL UNIQUE,
  notes           TEXT
);

CREATE INDEX vendor_debit_memos_vendor    ON vendor_debit_memos (vendor_id);
CREATE INDEX vendor_debit_memos_posted_at ON vendor_debit_memos (posted_at);

CREATE TABLE vendor_debit_memo_lines (
  id                 UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  memo_id            UUID NOT NULL REFERENCES vendor_debit_memos(id),
  line_no            INT NOT NULL,
  kind               TEXT NOT NULL CHECK (kind IN ('financial', 'goods_return')),
  expense_account_id BIGINT REFERENCES accounts(id),
  sku_id             UUID REFERENCES skus(id),
  location_id        UUID REFERENCES locations(id),
  qty                BIGINT,
  unit_cost          BIGINT,
  amount             BIGINT NOT NULL,
  UNIQUE (memo_id, line_no),
  CHECK (
    (kind = 'financial'
       AND expense_account_id IS NOT NULL
       AND sku_id IS NULL AND location_id IS NULL
       AND qty IS NULL AND unit_cost IS NULL)
    OR
    (kind = 'goods_return'
       AND expense_account_id IS NULL
       AND sku_id IS NOT NULL AND location_id IS NOT NULL
       AND qty IS NOT NULL AND qty > 0
       AND unit_cost IS NOT NULL AND unit_cost >= 0)
  ),
  CHECK (amount >= 0)
);

CREATE INDEX vendor_debit_memo_lines_sku ON vendor_debit_memo_lines (sku_id);

-- ============================================================
-- 9. post_so_allocate (acct-mv8) — pre-ship state transition
-- ============================================================
--
-- No ledger events; flips active reservations to allocated.

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
  SELECT id INTO v_existing_id FROM so_allocations
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT id INTO v_so_check FROM sales_orders WHERE id = p_so_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'so_allocate_invalid: SO % not found', p_so_id
      USING ERRCODE = 'P0043';
  END IF;

  INSERT INTO so_allocations (so_id, business_date, posted_by, idempotency_key, notes)
  VALUES (p_so_id, p_business_date, p_posted_by, p_idempotency_key, p_notes)
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM so_allocations
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  UPDATE inventory_reservations
     SET status = 'allocated'
   WHERE so_id = p_so_id
     AND status = 'active';

  RETURN v_doc_id;
END;
$$;

-- ============================================================
-- 10. post_so_ship (latest acct-5prc / R4 + R7 + cost_method_at_ship snapshot)
-- ============================================================
--
-- Per line: posts qty (customer_pool DR / stock_available CR), COGS
-- (cogs DR / inv_value_fg CR — dispatcher-priced), revenue (ar_unsettled
-- DR / revenue CR), and tax (ar_unsettled DR / sales_tax_payable CR)
-- when tax > 0. Strict over-ship rejection (P0038).
--
-- WAC dispatch: locks v_val_acct BEFORE per-class qty SUM (R4 + R7) so
-- the unit_cost snapshotted to so_shipment_lines matches the post-lock
-- ledger amount.
--
-- Reservations in {active, allocated} flip to 'shipped' atomically.

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

    SELECT so_id, sku_id, ship_location_id, qty_ordered, unit_price,
           currency, tax_amount
      INTO v_sl
      FROM sales_order_lines WHERE id = v_so_line_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'so_ship_invalid: so_line % not found', v_so_line_id
        USING ERRCODE = 'P0037';
    END IF;
    IF v_sl.so_id <> p_so_id THEN
      RAISE EXCEPTION 'so_ship_invalid: so_line % belongs to SO % not %',
                      v_so_line_id, v_sl.so_id, p_so_id
        USING ERRCODE = 'P0037';
    END IF;

    SELECT COALESCE(SUM(qty_shipped), 0) INTO v_already_ship
      FROM so_shipment_lines WHERE so_line_id = v_so_line_id;
    IF v_already_ship + v_qty_shipped > v_sl.qty_ordered THEN
      RAISE EXCEPTION
        'so_line_overshipped: so_line %: ordered=%, already shipped=%, '
        'this shipment=%; cumulative would exceed qty_ordered',
        v_so_line_id, v_sl.qty_ordered, v_already_ship, v_qty_shipped
        USING ERRCODE = 'P0038';
    END IF;

    v_unit_price := COALESCE((v_line->>'unit_price')::BIGINT, v_sl.unit_price);
    v_tax_amount := COALESCE((v_line->>'tax_amount')::BIGINT, v_sl.tax_amount);

    SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_sl.sku_id;

    IF v_cost_method IN ('fifo', 'lot') THEN
      RAISE EXCEPTION
        'cost_method_not_implemented: % for so_ship (sku=%); see acct-8gg',
        v_cost_method, v_sl.sku_id USING ERRCODE = 'P0006';
    END IF;

    SELECT id INTO v_qty_acct FROM accounts
     WHERE kind='stock_available' AND sku_id=v_sl.sku_id
       AND location_id=v_sl.ship_location_id AND NOT is_closed;
    IF v_qty_acct IS NULL THEN
      RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                      v_sl.sku_id, v_sl.ship_location_id USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_val_acct FROM accounts
     WHERE kind='inv_value_fg' AND sku_id=v_sl.sku_id
       AND location_id=v_sl.ship_location_id
       AND currency=v_sl.currency AND NOT is_closed;
    IF v_val_acct IS NULL THEN
      RAISE EXCEPTION 'no open inv_value_fg account for sku=% loc=% ccy=%',
                      v_sl.sku_id, v_sl.ship_location_id, v_sl.currency
        USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_cust_qty FROM accounts
     WHERE kind='customer_pool' AND counterparty_id=v_customer_id
       AND ledger_kind='qty' AND NOT is_closed;
    IF v_cust_qty IS NULL THEN
      RAISE EXCEPTION 'no open customer_pool(qty) account for customer=%',
                      v_customer_id USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_cust_unsettled FROM accounts
     WHERE kind='ar_unsettled' AND counterparty_id=v_customer_id
       AND currency=v_sl.currency AND NOT is_closed;
    IF v_cust_unsettled IS NULL THEN
      RAISE EXCEPTION 'no open ar_unsettled account for customer=% ccy=%',
                      v_customer_id, v_sl.currency USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_revenue_acct FROM accounts
     WHERE kind='revenue' AND ledger_kind='value'
       AND currency=v_sl.currency AND NOT is_closed;
    IF v_revenue_acct IS NULL THEN
      RAISE EXCEPTION 'no open revenue account for ccy=%',
                      v_sl.currency USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_cogs_acct FROM accounts
     WHERE kind='cogs' AND ledger_kind='value'
       AND currency=v_sl.currency AND NOT is_closed;
    IF v_cogs_acct IS NULL THEN
      RAISE EXCEPTION 'no open cogs account for ccy=%',
                      v_sl.currency USING ERRCODE = 'P0010';
    END IF;

    IF v_cost_method = 'standard' THEN
      v_unit_cost := _resolve_standard_cost_at(v_sl.sku_id, p_business_date);
    ELSE
      -- acct-5prc / R4 + R7. Lock the value pool BEFORE reading
      -- per-class qty divisor + value balance so the unit_cost we
      -- snapshot into so_shipment_lines.unit_cost matches the
      -- post-lock dispatched amount that lands on posting_line.amount.
      PERFORM 1 FROM accounts WHERE id = v_val_acct FOR UPDATE;

      SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = v_val_acct THEN  t.qty
                               WHEN t.credit_account_id = v_val_acct THEN -t.qty END), 0)
        INTO v_qty_balance
        FROM posting_lines t
       WHERE v_val_acct IN (t.debit_account_id, t.credit_account_id)
         AND t.qty IS NOT NULL;

      IF v_qty_balance <= 0 THEN
        RAISE EXCEPTION
          'wac so_ship qty balance is %, cannot price (sku=%, loc=%, ccy=%)',
          v_qty_balance, v_sl.sku_id, v_sl.ship_location_id, v_sl.currency
          USING ERRCODE = 'P0006';
      END IF;
      SELECT debits_total - credits_total INTO v_value_balance
        FROM accounts WHERE id = v_val_acct;
      IF v_value_balance < 0 THEN v_value_balance := 0; END IF;
      v_unit_cost := v_value_balance / v_qty_balance;
    END IF;

    IF v_tax_amount > 0 THEN
      SELECT id INTO v_tax_acct FROM accounts
       WHERE kind='sales_tax_payable' AND ledger_kind='value'
         AND currency=v_sl.currency AND NOT is_closed;
      IF v_tax_acct IS NULL THEN
        RAISE EXCEPTION 'no open sales_tax_payable account for ccy=%',
                        v_sl.currency USING ERRCODE = 'P0010';
      END IF;
    END IF;

    INSERT INTO so_shipment_lines (
      shipment_id, so_line_id, qty_shipped, unit_cost, unit_price, tax_amount,
      cost_method_at_ship
    ) VALUES (
      v_doc_id, v_so_line_id, v_qty_shipped, v_unit_cost, v_unit_price, v_tax_amount,
      v_cost_method
    ) RETURNING id INTO v_ship_line_id;

    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'so_ship',
      'document_kind',     'so_ship',
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
    ));

    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'so_ship',
      'document_kind',     'so_ship',
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
    ));

    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'so_ship',
      'document_kind',     'so_ship',
      'document_id',       v_doc_id,
      'document_line_id',  v_ship_line_id,
      'debit_account_id',  v_cust_unsettled,
      'credit_account_id', v_revenue_acct,
      'amount',            v_qty_shipped * v_unit_price,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   v_customer_id,
      'posted_by',         p_posted_by
    ));

    IF v_tax_amount > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'so_ship',
        'document_kind',     'so_ship',
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

  UPDATE inventory_reservations
     SET status = 'shipped',
         resolved_at = clock_timestamp()
   WHERE so_id = p_so_id
     AND status IN ('active', 'allocated');

  PERFORM post_posting_lines(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;

-- ============================================================
-- 11. post_customer_invoice (latest acct-7mc + acct-nuw7)
-- ============================================================
--
-- Three-way match strict on (so_line, qty, unit_price) within
-- customer.unit_price_tolerance_pct; absorbs within-tolerance deltas
-- to variance_match_tolerance. Subtracts customer_return_lines.
-- qty_to_ar_unsettled from v_avail (round-trip correctness).
-- service line: caller-supplied revenue_account; tax leg fires here
-- (services do not park tax in ar_unsettled like ship-side does).

CREATE OR REPLACE FUNCTION post_customer_invoice(
  p_customer_id     UUID,
  p_currency        CHAR(3),
  p_lines           JSONB,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id     UUID;
  v_doc_id          UUID;
  v_customer_check  UUID;
  v_tolerance_pct   NUMERIC(5,2);
  v_n               INT;
  v_idx             INT;
  v_line            JSONB;
  v_kind            TEXT;
  v_so_line_id      UUID;
  v_qty             BIGINT;
  v_unit_price      BIGINT;
  v_amount          BIGINT;
  v_tax_amount      BIGINT;
  v_revenue_acct_id BIGINT;
  v_sl              RECORD;
  v_total_shipped   BIGINT;
  v_total_invoiced  BIGINT;
  v_returns_to_us   BIGINT;
  v_avail           BIGINT;
  v_cust_unsettled  BIGINT;
  v_cust_ar         BIGINT;
  v_cust_tax        BIGINT;
  v_match_tol_acct  BIGINT;
  v_rev_acct        accounts%ROWTYPE;
  v_inv_line_id     UUID;
  v_diff_total      BIGINT;
  v_diff_pct        NUMERIC(10,4);
  v_amount_at_so    BIGINT;
  v_batch           JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM customer_invoices
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT id, unit_price_tolerance_pct INTO v_customer_check, v_tolerance_pct
    FROM customers WHERE id = p_customer_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'customer_invoice_invalid_line: customer % not found',
                    p_customer_id USING ERRCODE = 'P0041';
  END IF;

  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'customer_invoice_invalid_line: empty invoice for customer %',
                    p_customer_id USING ERRCODE = 'P0041';
  END IF;

  SELECT id INTO v_cust_ar FROM accounts
   WHERE kind='ar' AND counterparty_id=p_customer_id
     AND currency=p_currency AND NOT is_closed;
  IF v_cust_ar IS NULL THEN
    RAISE EXCEPTION 'no open ar account for customer=% ccy=%',
                    p_customer_id, p_currency USING ERRCODE = 'P0010';
  END IF;

  INSERT INTO customer_invoices (
    customer_id, currency, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_customer_id, p_currency, p_business_date, p_posted_by,
    p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM customer_invoices
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  FOR v_idx IN 1..v_n LOOP
    v_line       := p_lines -> (v_idx - 1);
    v_kind       := v_line->>'kind';
    v_amount     := (v_line->>'amount')::BIGINT;
    v_tax_amount := COALESCE((v_line->>'tax_amount')::BIGINT, 0);

    IF v_kind = 'so_match' THEN
      v_so_line_id := (v_line->>'so_line_id')::UUID;
      v_qty        := (v_line->>'qty')::BIGINT;
      v_unit_price := (v_line->>'unit_price')::BIGINT;

      SELECT sl.so_id, sl.unit_price, sl.currency, so.customer_id
        INTO v_sl
        FROM sales_order_lines sl
        JOIN sales_orders so ON so.id = sl.so_id
       WHERE sl.id = v_so_line_id
         FOR UPDATE OF sl;
      IF NOT FOUND THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % so_line % not found',
          v_idx, v_so_line_id USING ERRCODE = 'P0041';
      END IF;
      IF v_sl.customer_id IS DISTINCT FROM p_customer_id THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % so_line % belongs to '
          'customer % but invoice is for customer %',
          v_idx, v_so_line_id, v_sl.customer_id, p_customer_id
          USING ERRCODE = 'P0041';
      END IF;
      IF v_sl.currency <> p_currency THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % so_line currency=% but invoice currency=%',
          v_idx, v_sl.currency, p_currency USING ERRCODE = 'P0041';
      END IF;

      IF v_unit_price <> v_sl.unit_price THEN
        IF v_tolerance_pct = 0 THEN
          RAISE EXCEPTION
            'customer_invoice_three_way_mismatch: line % unit_price % does '
            'not match so_line.unit_price %',
            v_idx, v_unit_price, v_sl.unit_price USING ERRCODE = 'P0040';
        END IF;
        IF v_sl.unit_price = 0 THEN
          RAISE EXCEPTION
            'customer_invoice_three_way_mismatch: line % so_line.unit_price '
            'is 0 but invoice unit_price is % (zero-baseline; out of '
            'tolerance, customer tolerance %%%)',
            v_idx, v_unit_price, v_tolerance_pct
            USING ERRCODE = 'P0040';
        END IF;
        v_diff_pct := ABS(v_unit_price - v_sl.unit_price) * 100.0 / v_sl.unit_price;
        IF v_diff_pct > v_tolerance_pct THEN
          RAISE EXCEPTION
            'customer_invoice_three_way_mismatch: line % unit_price % differs from '
            'so_line.unit_price % by %%% (customer tolerance %%%)',
            v_idx, v_unit_price, v_sl.unit_price, v_diff_pct, v_tolerance_pct
            USING ERRCODE = 'P0040';
        END IF;
      END IF;
      IF v_amount <> v_qty * v_unit_price THEN
        RAISE EXCEPTION
          'customer_invoice_three_way_mismatch: line % amount % <> qty % × unit_price %',
          v_idx, v_amount, v_qty, v_unit_price USING ERRCODE = 'P0040';
      END IF;

      SELECT COALESCE(SUM(qty_shipped), 0) INTO v_total_shipped
        FROM so_shipment_lines WHERE so_line_id = v_so_line_id;
      SELECT COALESCE(SUM(qty), 0) INTO v_total_invoiced
        FROM customer_invoice_lines
       WHERE so_line_id = v_so_line_id AND kind = 'so_match';
      SELECT COALESCE(SUM(crl.qty_to_ar_unsettled), 0) INTO v_returns_to_us
        FROM customer_return_lines crl
        JOIN so_shipment_lines     ssl ON ssl.id = crl.ship_line_id
       WHERE ssl.so_line_id = v_so_line_id;
      v_avail := v_total_shipped - v_total_invoiced - v_returns_to_us;
      IF v_qty > v_avail THEN
        RAISE EXCEPTION
          'customer_invoice_three_way_mismatch: line % qty % exceeds '
          'shipped-not-invoiced-not-returned remainder % for so_line % '
          '(shipped=%, already invoiced=%, prior returns to ar_unsettled=%)',
          v_idx, v_qty, v_avail, v_so_line_id, v_total_shipped,
          v_total_invoiced, v_returns_to_us
          USING ERRCODE = 'P0040';
      END IF;

      SELECT id INTO v_cust_unsettled FROM accounts
       WHERE kind='ar_unsettled' AND counterparty_id=p_customer_id
         AND currency=p_currency AND NOT is_closed;
      IF v_cust_unsettled IS NULL THEN
        RAISE EXCEPTION 'no open ar_unsettled account for customer=% ccy=%',
                        p_customer_id, p_currency USING ERRCODE = 'P0010';
      END IF;

      INSERT INTO customer_invoice_lines (
        invoice_id, line_no, kind, so_line_id, qty, unit_price, amount, tax_amount
      ) VALUES (
        v_doc_id, v_idx, 'so_match', v_so_line_id, v_qty, v_unit_price,
        v_amount, v_tax_amount
      ) RETURNING id INTO v_inv_line_id;

      v_amount_at_so := v_qty * v_sl.unit_price;
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ar_invoice',
        'document_kind',     'customer_invoice',
        'document_id',       v_doc_id,
        'document_line_id',  v_inv_line_id,
        'debit_account_id',  v_cust_ar,
        'credit_account_id', v_cust_unsettled,
        'amount',            v_amount_at_so,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_customer_id,
        'posted_by',         p_posted_by
      ));

      v_diff_total := v_amount - v_amount_at_so;
      IF v_diff_total <> 0 THEN
        SELECT id INTO v_match_tol_acct FROM accounts
         WHERE kind='variance_match_tolerance' AND ledger_kind='value'
           AND currency=p_currency AND NOT is_closed;
        IF v_match_tol_acct IS NULL THEN
          RAISE EXCEPTION 'no open variance_match_tolerance account for ccy=%',
                          p_currency USING ERRCODE = 'P0010';
        END IF;

        IF v_diff_total > 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'ar_invoice',
            'document_kind',     'customer_invoice',
            'document_id',       v_doc_id,
            'document_line_id',  v_inv_line_id,
            'debit_account_id',  v_cust_ar,
            'credit_account_id', v_match_tol_acct,
            'amount',            v_diff_total,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_customer_id,
            'posted_by',         p_posted_by
          ));
        ELSE
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'ar_invoice',
            'document_kind',     'customer_invoice',
            'document_id',       v_doc_id,
            'document_line_id',  v_inv_line_id,
            'debit_account_id',  v_match_tol_acct,
            'credit_account_id', v_cust_ar,
            'amount',            -v_diff_total,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_customer_id,
            'posted_by',         p_posted_by
          ));
        END IF;
      END IF;

    ELSIF v_kind = 'service' THEN
      v_revenue_acct_id := (v_line->>'revenue_account_id')::BIGINT;

      SELECT * INTO v_rev_acct FROM accounts WHERE id = v_revenue_acct_id;
      IF NOT FOUND THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % revenue_account_id % not found',
          v_idx, v_revenue_acct_id USING ERRCODE = 'P0041';
      END IF;
      IF v_rev_acct.is_closed THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % revenue account % is closed',
          v_idx, v_revenue_acct_id USING ERRCODE = 'P0041';
      END IF;
      IF v_rev_acct.ledger_kind <> 'value' THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % revenue account % is %, expected value',
          v_idx, v_revenue_acct_id, v_rev_acct.ledger_kind
          USING ERRCODE = 'P0041';
      END IF;
      IF v_rev_acct.currency <> p_currency THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % revenue account ccy=% but invoice ccy=%',
          v_idx, v_rev_acct.currency, p_currency USING ERRCODE = 'P0041';
      END IF;

      INSERT INTO customer_invoice_lines (
        invoice_id, line_no, kind, revenue_account_id, amount, tax_amount
      ) VALUES (
        v_doc_id, v_idx, 'service', v_revenue_acct_id, v_amount, v_tax_amount
      ) RETURNING id INTO v_inv_line_id;

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ar_invoice',
        'document_kind',     'customer_invoice',
        'document_id',       v_doc_id,
        'document_line_id',  v_inv_line_id,
        'debit_account_id',  v_cust_ar,
        'credit_account_id', v_revenue_acct_id,
        'amount',            v_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_customer_id,
        'posted_by',         p_posted_by
      ));

      IF v_tax_amount > 0 THEN
        SELECT id INTO v_cust_tax FROM accounts
         WHERE kind='sales_tax_payable' AND ledger_kind='value'
           AND currency=p_currency AND NOT is_closed;
        IF v_cust_tax IS NULL THEN
          RAISE EXCEPTION 'no open sales_tax_payable account for ccy=%',
                          p_currency USING ERRCODE = 'P0010';
        END IF;
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'ar_invoice',
          'document_kind',     'customer_invoice',
          'document_id',       v_doc_id,
          'document_line_id',  v_inv_line_id,
          'debit_account_id',  v_cust_ar,
          'credit_account_id', v_cust_tax,
          'amount',            v_tax_amount,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'counterparty_id',   p_customer_id,
          'posted_by',         p_posted_by
        ));
      END IF;

    ELSE
      RAISE EXCEPTION 'customer_invoice_invalid_line: line % unknown kind %',
                      v_idx, v_kind USING ERRCODE = 'P0041';
    END IF;
  END LOOP;

  PERFORM post_posting_lines(v_batch, FALSE);
  RETURN v_doc_id;
END;
$$;

-- ============================================================
-- 12. post_ar_payment (acct-3xcg cross-currency settlement)
-- ============================================================

CREATE OR REPLACE FUNCTION post_ar_payment(
  p_customer_id     UUID,
  p_currency        CHAR(3),
  p_amount          BIGINT,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT     DEFAULT NULL,
  p_cash_currency   CHAR(3)  DEFAULT NULL,
  p_cash_amount     BIGINT   DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id    UUID;
  v_doc_id         UUID;
  v_customer_check UUID;
  v_cash_acct      BIGINT;
  v_cust_ar        BIGINT;
  v_fx_clr_ccy     BIGINT;
  v_fx_clr_cash    BIGINT;
  v_fx_gain_acct   BIGINT;
  v_fx_loss_acct   BIGINT;
  v_rate           NUMERIC(20, 10);
  v_expected       BIGINT;
  v_delta          BIGINT;
  v_cross_ccy      BOOLEAN;
  v_batch          JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM ar_payments
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  IF p_amount IS NULL OR p_amount <= 0 THEN
    RAISE EXCEPTION 'ar_payment_invalid: amount must be > 0 (got %)', p_amount
      USING ERRCODE = 'P0039';
  END IF;

  SELECT id INTO v_customer_check FROM customers WHERE id = p_customer_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'ar_payment_invalid: customer % not found', p_customer_id
      USING ERRCODE = 'P0039';
  END IF;

  v_cross_ccy := p_cash_currency IS NOT NULL
             AND p_cash_currency <> p_currency;

  IF v_cross_ccy THEN
    IF p_cash_amount IS NULL OR p_cash_amount <= 0 THEN
      RAISE EXCEPTION
        'ar_payment_invalid: p_cash_amount required and > 0 when '
        'p_cash_currency (%) differs from p_currency (%)',
        p_cash_currency, p_currency
        USING ERRCODE = 'P0039';
    END IF;
  ELSE
    IF p_cash_amount IS NOT NULL AND p_cash_amount <> p_amount THEN
      RAISE EXCEPTION
        'ar_payment_invalid: same-currency settlement requires '
        'p_cash_amount (%) = p_amount (%) (or NULL)',
        p_cash_amount, p_amount
        USING ERRCODE = 'P0039';
    END IF;
  END IF;

  SELECT id INTO v_cust_ar FROM accounts
   WHERE kind='ar' AND counterparty_id=p_customer_id
     AND currency=p_currency AND NOT is_closed;
  IF v_cust_ar IS NULL THEN
    RAISE EXCEPTION 'no open ar account for customer=% ccy=%',
                    p_customer_id, p_currency USING ERRCODE = 'P0010';
  END IF;

  SELECT id INTO v_cash_acct FROM accounts
   WHERE kind='cash' AND ledger_kind='value'
     AND currency=COALESCE(p_cash_currency, p_currency) AND NOT is_closed;
  IF v_cash_acct IS NULL THEN
    RAISE EXCEPTION 'no open cash account for ccy=%',
                    COALESCE(p_cash_currency, p_currency)
      USING ERRCODE = 'P0010';
  END IF;

  IF v_cross_ccy THEN
    SELECT id INTO v_fx_clr_ccy FROM accounts
     WHERE kind='fx_clearing' AND ledger_kind='value'
       AND currency=p_currency AND NOT is_closed;
    IF v_fx_clr_ccy IS NULL THEN
      RAISE EXCEPTION 'no open fx_clearing account for ccy=%',
                      p_currency USING ERRCODE = 'P0010';
    END IF;
    SELECT id INTO v_fx_clr_cash FROM accounts
     WHERE kind='fx_clearing' AND ledger_kind='value'
       AND currency=p_cash_currency AND NOT is_closed;
    IF v_fx_clr_cash IS NULL THEN
      RAISE EXCEPTION 'no open fx_clearing account for ccy=%',
                      p_cash_currency USING ERRCODE = 'P0010';
    END IF;

    SELECT rate INTO v_rate
      FROM fx_rates
     WHERE from_currency = p_currency
       AND to_currency   = p_cash_currency
       AND effective_at::DATE <= p_business_date
     ORDER BY effective_at DESC
     LIMIT 1;
    IF v_rate IS NULL THEN
      RAISE EXCEPTION
        'missing_fx_rate: no fx_rates row found for % → % effective_at <= %',
        p_currency, p_cash_currency, p_business_date
        USING ERRCODE = 'P0050';
    END IF;

    v_expected := (p_amount::NUMERIC * v_rate)::BIGINT;
    v_delta    := p_cash_amount - v_expected;

    IF v_delta > 0 THEN
      SELECT id INTO v_fx_gain_acct FROM accounts
       WHERE kind='realized_fx_gain' AND ledger_kind='value'
         AND currency=p_cash_currency AND NOT is_closed;
      IF v_fx_gain_acct IS NULL THEN
        RAISE EXCEPTION 'no open realized_fx_gain account for ccy=%',
                        p_cash_currency USING ERRCODE = 'P0010';
      END IF;
    ELSIF v_delta < 0 THEN
      SELECT id INTO v_fx_loss_acct FROM accounts
       WHERE kind='realized_fx_loss' AND ledger_kind='value'
         AND currency=p_cash_currency AND NOT is_closed;
      IF v_fx_loss_acct IS NULL THEN
        RAISE EXCEPTION 'no open realized_fx_loss account for ccy=%',
                        p_cash_currency USING ERRCODE = 'P0010';
      END IF;
    END IF;
  END IF;

  INSERT INTO ar_payments (
    customer_id, currency, amount, business_date, posted_by,
    idempotency_key, notes
  ) VALUES (
    p_customer_id, p_currency, p_amount, p_business_date, p_posted_by,
    p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM ar_payments
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  IF v_cross_ccy THEN
    -- Event A (counterparty currency): fx_clearing DR / ar CR.
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'ar_payment',
      'document_kind',     'ar_payment',
      'document_id',       v_doc_id,
      'debit_account_id',  v_fx_clr_ccy,
      'credit_account_id', v_cust_ar,
      'amount',            p_amount,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   p_customer_id,
      'posted_by',         p_posted_by
    ));

    -- Event B Part 1 (cash currency): cash DR / fx_clearing CR.
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'ar_payment',
      'document_kind',     'ar_payment',
      'document_id',       v_doc_id,
      'debit_account_id',  v_cash_acct,
      'credit_account_id', v_fx_clr_cash,
      'amount',            p_cash_amount,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   p_customer_id,
      'posted_by',         p_posted_by
    ));

    -- Event B Part 2: realized FX gain/loss.
    IF v_delta > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ar_payment',
        'document_kind',     'ar_payment',
        'document_id',       v_doc_id,
        'debit_account_id',  v_fx_clr_cash,
        'credit_account_id', v_fx_gain_acct,
        'amount',            v_delta,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_customer_id,
        'posted_by',         p_posted_by
      ));
    ELSIF v_delta < 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ar_payment',
        'document_kind',     'ar_payment',
        'document_id',       v_doc_id,
        'debit_account_id',  v_fx_loss_acct,
        'credit_account_id', v_fx_clr_cash,
        'amount',            -v_delta,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_customer_id,
        'posted_by',         p_posted_by
      ));
    END IF;
  ELSE
    -- Same-currency: single 2-leg event.
    v_batch := jsonb_build_array(jsonb_build_object(
      'reason',            'ar_payment',
      'document_kind',     'ar_payment',
      'document_id',       v_doc_id,
      'debit_account_id',  v_cash_acct,
      'credit_account_id', v_cust_ar,
      'amount',            p_amount,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   p_customer_id,
      'posted_by',         p_posted_by
    ));
  END IF;

  PERFORM post_posting_lines(v_batch, FALSE);
  RETURN v_doc_id;
END;
$$;

-- ============================================================
-- 13. post_customer_return (acct-tk7 + acct-dso state-aware + override)
-- ============================================================
--
-- Per so_line: splits revenue between ar_unsettled (un-invoiced
-- portion) and ar (invoiced portion). qty_to_ar_unsettled / qty_to_ar
-- recorded on customer_return_lines. Tax always reverses against
-- ar_unsettled (post_customer_invoice does not migrate tax for so_match
-- — ship-side tax stays parked).

CREATE OR REPLACE FUNCTION post_customer_return(
  p_customer_id            UUID,
  p_lines                  JSONB,
  p_business_date          DATE,
  p_posted_by              UUID,
  p_idempotency_key        UUID,
  p_notes                  TEXT DEFAULT NULL,
  p_override_closed_period BOOLEAN DEFAULT FALSE
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id        UUID;
  v_doc_id             UUID;
  v_customer_check     UUID;
  v_n                  INT;
  v_idx                INT;
  v_line               JSONB;
  v_ship_line_id       UUID;
  v_qty_returned       BIGINT;
  v_disposition        return_disposition;
  v_sl                 RECORD;
  v_so_line_id         UUID;
  v_total_shipped      BIGINT;
  v_total_invoiced     BIGINT;
  v_prior_to_unsettled BIGINT;
  v_prior_to_ar        BIGINT;
  v_unsettled_rem      BIGINT;
  v_ar_rem             BIGINT;
  v_qty_to_unsettled   BIGINT;
  v_qty_to_ar          BIGINT;
  v_tax_pro            BIGINT;
  v_qty_dr_acct        BIGINT;
  v_val_dr_acct        BIGINT;
  v_cust_qty           BIGINT;
  v_cust_unsettled     BIGINT;
  v_cust_ar            BIGINT;
  v_cogs_acct          BIGINT;
  v_revenue_acct       BIGINT;
  v_tax_acct           BIGINT;
  v_var_scrap          BIGINT;
  v_return_line_id     UUID;
  v_batch              JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM customer_returns
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT id INTO v_customer_check FROM customers WHERE id = p_customer_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'customer_return_invalid: customer % not found', p_customer_id
      USING ERRCODE = 'P0044';
  END IF;

  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'customer_return_invalid: empty lines for customer %',
                    p_customer_id USING ERRCODE = 'P0044';
  END IF;

  INSERT INTO customer_returns (customer_id, business_date, posted_by, idempotency_key, notes)
  VALUES (p_customer_id, p_business_date, p_posted_by, p_idempotency_key, p_notes)
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM customer_returns WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  FOR v_idx IN 1..v_n LOOP
    v_line         := p_lines -> (v_idx - 1);
    v_ship_line_id := (v_line->>'ship_line_id')::UUID;
    v_qty_returned := (v_line->>'qty_returned')::BIGINT;
    v_disposition  := (v_line->>'disposition')::return_disposition;

    IF v_qty_returned IS NULL OR v_qty_returned <= 0 THEN
      RAISE EXCEPTION 'customer_return_invalid: line % qty_returned must be > 0',
                      v_idx USING ERRCODE = 'P0044';
    END IF;

    SELECT
      ssl.id              AS ship_line_id,
      ssl.so_line_id      AS so_line_id,
      ssl.qty_shipped     AS qty_shipped,
      ssl.unit_cost       AS unit_cost,
      ssl.unit_price      AS unit_price,
      ssl.tax_amount      AS tax_amount,
      sl.sku_id           AS sku_id,
      sl.ship_location_id AS ship_location_id,
      sl.currency         AS currency,
      so.customer_id      AS customer_id
    INTO v_sl
    FROM so_shipment_lines ssl
    JOIN sales_order_lines sl ON sl.id = ssl.so_line_id
    JOIN so_shipments      ss ON ss.id = ssl.shipment_id
    JOIN sales_orders      so ON so.id = ss.so_id
    WHERE ssl.id = v_ship_line_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'customer_return_invalid: ship_line % not found',
                      v_ship_line_id USING ERRCODE = 'P0044';
    END IF;
    IF v_sl.customer_id <> p_customer_id THEN
      RAISE EXCEPTION
        'customer_return_invalid: ship_line % belongs to customer %, not %',
        v_ship_line_id, v_sl.customer_id, p_customer_id
        USING ERRCODE = 'P0044';
    END IF;

    v_so_line_id := v_sl.so_line_id;

    PERFORM 1 FROM sales_order_lines WHERE id = v_so_line_id FOR UPDATE;

    SELECT COALESCE(SUM(qty_shipped), 0)
      INTO v_total_shipped
      FROM so_shipment_lines
     WHERE so_line_id = v_so_line_id;

    SELECT COALESCE(SUM(qty), 0)
      INTO v_total_invoiced
      FROM customer_invoice_lines
     WHERE so_line_id = v_so_line_id AND kind = 'so_match';

    SELECT
      COALESCE(SUM(crl.qty_to_ar_unsettled), 0),
      COALESCE(SUM(crl.qty_to_ar), 0)
      INTO v_prior_to_unsettled, v_prior_to_ar
      FROM customer_return_lines crl
      JOIN so_shipment_lines     ssl ON ssl.id = crl.ship_line_id
      JOIN customer_returns      cr  ON cr.id  = crl.return_id
     WHERE ssl.so_line_id = v_so_line_id
       AND cr.id <> v_doc_id;

    v_unsettled_rem := v_total_shipped - v_total_invoiced - v_prior_to_unsettled;
    v_ar_rem        := v_total_invoiced - v_prior_to_ar;

    v_qty_to_unsettled := LEAST(v_qty_returned, GREATEST(v_unsettled_rem, 0));
    v_qty_to_ar        := v_qty_returned - v_qty_to_unsettled;

    IF v_qty_to_ar > v_ar_rem THEN
      RAISE EXCEPTION
        'customer_return_overreturned: so_line % cumulative would exceed invoiced-not-returned + uninvoiced-not-returned (shipped=%, invoiced=%, prior_to_unsettled=%, prior_to_ar=%, requested=%)',
        v_so_line_id, v_total_shipped, v_total_invoiced,
        v_prior_to_unsettled, v_prior_to_ar, v_qty_returned
        USING ERRCODE = 'P0045';
    END IF;

    IF v_sl.tax_amount > 0 AND v_sl.qty_shipped > 0 THEN
      v_tax_pro := (v_sl.tax_amount * v_qty_returned) / v_sl.qty_shipped;
    ELSE
      v_tax_pro := 0;
    END IF;

    IF v_disposition = 'restock' THEN
      SELECT id INTO v_qty_dr_acct FROM accounts
       WHERE kind='stock_available' AND sku_id=v_sl.sku_id
         AND location_id=v_sl.ship_location_id AND NOT is_closed;
      IF v_qty_dr_acct IS NULL THEN
        RAISE EXCEPTION 'no open stock_available for sku=% loc=%',
                        v_sl.sku_id, v_sl.ship_location_id USING ERRCODE = 'P0010';
      END IF;
    ELSIF v_disposition = 'scrap' THEN
      SELECT id INTO v_qty_dr_acct FROM accounts
       WHERE kind='stock_scrap' AND sku_id=v_sl.sku_id AND NOT is_closed;
      IF v_qty_dr_acct IS NULL THEN
        RAISE EXCEPTION 'no open stock_scrap for sku=%',
                        v_sl.sku_id USING ERRCODE = 'P0010';
      END IF;
    ELSIF v_disposition = 'repair' THEN
      SELECT id INTO v_qty_dr_acct FROM accounts
       WHERE kind='stock_quarantine' AND sku_id=v_sl.sku_id
         AND location_id=v_sl.ship_location_id AND NOT is_closed;
      IF v_qty_dr_acct IS NULL THEN
        RAISE EXCEPTION 'no open stock_quarantine for sku=% loc=%',
                        v_sl.sku_id, v_sl.ship_location_id USING ERRCODE = 'P0010';
      END IF;
    END IF;

    IF v_disposition IN ('restock', 'repair') THEN
      SELECT id INTO v_val_dr_acct FROM accounts
       WHERE kind='inv_value_fg' AND sku_id=v_sl.sku_id
         AND location_id=v_sl.ship_location_id AND currency=v_sl.currency
         AND NOT is_closed;
      IF v_val_dr_acct IS NULL THEN
        RAISE EXCEPTION 'no open inv_value_fg for sku=% loc=% ccy=%',
                        v_sl.sku_id, v_sl.ship_location_id, v_sl.currency
          USING ERRCODE = 'P0010';
      END IF;
    ELSE
      SELECT id INTO v_var_scrap FROM accounts
       WHERE kind='variance_scrap' AND currency=v_sl.currency AND NOT is_closed;
      IF v_var_scrap IS NULL THEN
        RAISE EXCEPTION 'no open variance_scrap for ccy=%',
                        v_sl.currency USING ERRCODE = 'P0010';
      END IF;
      v_val_dr_acct := v_var_scrap;
    END IF;

    SELECT id INTO v_cust_qty FROM accounts
     WHERE kind='customer_pool' AND counterparty_id=p_customer_id
       AND NOT is_closed;
    IF v_cust_qty IS NULL THEN
      SELECT id INTO v_cust_qty FROM accounts
       WHERE kind='customer_pool' AND counterparty_id IS NULL
         AND NOT is_closed;
    END IF;
    IF v_cust_qty IS NULL THEN
      RAISE EXCEPTION 'no open customer_pool for customer=%',
                      p_customer_id USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_cogs_acct FROM accounts
     WHERE kind='cogs' AND currency=v_sl.currency AND NOT is_closed;
    IF v_cogs_acct IS NULL THEN
      RAISE EXCEPTION 'no open cogs account for ccy=%',
                      v_sl.currency USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_revenue_acct FROM accounts
     WHERE kind='revenue' AND currency=v_sl.currency AND NOT is_closed;
    IF v_revenue_acct IS NULL THEN
      RAISE EXCEPTION 'no open revenue account for ccy=%',
                      v_sl.currency USING ERRCODE = 'P0010';
    END IF;

    IF v_qty_to_unsettled > 0 OR v_tax_pro > 0 THEN
      SELECT id INTO v_cust_unsettled FROM accounts
       WHERE kind='ar_unsettled' AND counterparty_id=p_customer_id
         AND currency=v_sl.currency AND NOT is_closed;
      IF v_cust_unsettled IS NULL THEN
        SELECT id INTO v_cust_unsettled FROM accounts
         WHERE kind='ar_unsettled' AND counterparty_id IS NULL
           AND currency=v_sl.currency AND NOT is_closed;
      END IF;
      IF v_cust_unsettled IS NULL THEN
        RAISE EXCEPTION 'no open ar_unsettled for customer=% ccy=%',
                        p_customer_id, v_sl.currency USING ERRCODE = 'P0010';
      END IF;
    END IF;

    IF v_qty_to_ar > 0 THEN
      SELECT id INTO v_cust_ar FROM accounts
       WHERE kind='ar' AND counterparty_id=p_customer_id
         AND currency=v_sl.currency AND NOT is_closed;
      IF v_cust_ar IS NULL THEN
        RAISE EXCEPTION 'no open ar for customer=% ccy=%',
                        p_customer_id, v_sl.currency USING ERRCODE = 'P0010';
      END IF;
    END IF;

    INSERT INTO customer_return_lines (
      return_id, line_no, ship_line_id, qty_returned, disposition,
      unit_cost, unit_price, tax_amount,
      qty_to_ar_unsettled, qty_to_ar
    ) VALUES (
      v_doc_id, v_idx, v_ship_line_id, v_qty_returned, v_disposition,
      v_sl.unit_cost, v_sl.unit_price, v_tax_pro,
      v_qty_to_unsettled, v_qty_to_ar
    )
    RETURNING id INTO v_return_line_id;

    -- Event 1: qty leg.
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'customer_return',
      'document_kind',     'customer_return',
      'document_id',       v_doc_id,
      'document_line_id',  v_return_line_id,
      'debit_account_id',  v_qty_dr_acct,
      'credit_account_id', v_cust_qty,
      'amount',            v_qty_returned,
      'qty',               v_qty_returned,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   p_customer_id,
      'posted_by',         p_posted_by
    ));

    -- Event 2: cogs reversal.
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'customer_return',
      'document_kind',     'customer_return',
      'document_id',       v_doc_id,
      'document_line_id',  v_return_line_id,
      'debit_account_id',  v_val_dr_acct,
      'credit_account_id', v_cogs_acct,
      'amount',            v_qty_returned * v_sl.unit_cost,
      'qty',               v_qty_returned,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   p_customer_id,
      'posted_by',         p_posted_by
    ));

    -- Event 3a: revenue reversal — un-invoiced portion to ar_unsettled.
    IF v_qty_to_unsettled > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'customer_return',
        'document_kind',     'customer_return',
        'document_id',       v_doc_id,
        'document_line_id',  v_return_line_id,
        'debit_account_id',  v_revenue_acct,
        'credit_account_id', v_cust_unsettled,
        'amount',            v_qty_to_unsettled * v_sl.unit_price,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_customer_id,
        'posted_by',         p_posted_by
      ));
    END IF;

    -- Event 3b: revenue reversal — invoiced portion to ar.
    IF v_qty_to_ar > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'customer_return',
        'document_kind',     'customer_return',
        'document_id',       v_doc_id,
        'document_line_id',  v_return_line_id,
        'debit_account_id',  v_revenue_acct,
        'credit_account_id', v_cust_ar,
        'amount',            v_qty_to_ar * v_sl.unit_price,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_customer_id,
        'posted_by',         p_posted_by
      ));
    END IF;

    -- Event 4: tax reversal — always to ar_unsettled (ship-side parking).
    IF v_tax_pro > 0 THEN
      SELECT id INTO v_tax_acct FROM accounts
       WHERE kind='sales_tax_payable' AND currency=v_sl.currency
         AND NOT is_closed;
      IF v_tax_acct IS NULL THEN
        RAISE EXCEPTION 'no open sales_tax_payable for ccy=%',
                        v_sl.currency USING ERRCODE = 'P0010';
      END IF;
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'customer_return',
        'document_kind',     'customer_return',
        'document_id',       v_doc_id,
        'document_line_id',  v_return_line_id,
        'debit_account_id',  v_tax_acct,
        'credit_account_id', v_cust_unsettled,
        'amount',            v_tax_pro,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_customer_id,
        'posted_by',         p_posted_by
      ));
    END IF;
  END LOOP;

  PERFORM post_posting_lines(v_batch, p_override_closed_period);
  RETURN v_doc_id;
END;
$$;

-- ============================================================
-- 14. post_customer_credit_memo (acct-tae standalone)
-- ============================================================
--
-- 'financial' lines: caller-supplied account DR / ar CR.
-- 'goods_return' lines: qty + cogs reversal + revenue + tax against ar.
-- Always routes to ar (cleared); pre-invoice routing requires
-- post_customer_return.

CREATE OR REPLACE FUNCTION post_customer_credit_memo(
  p_customer_id            UUID,
  p_currency               CHAR(3),
  p_lines                  JSONB,
  p_business_date          DATE,
  p_posted_by              UUID,
  p_idempotency_key        UUID,
  p_notes                  TEXT DEFAULT NULL,
  p_override_closed_period BOOLEAN DEFAULT FALSE
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id      UUID;
  v_doc_id           UUID;
  v_customer_check   UUID;
  v_n                INT;
  v_idx              INT;
  v_line             JSONB;
  v_kind             TEXT;
  v_amount           BIGINT;
  v_tax_amount       BIGINT;
  v_revenue_acct_id  BIGINT;
  v_rev_acct         accounts%ROWTYPE;
  v_sku_id           UUID;
  v_location_id      UUID;
  v_qty              BIGINT;
  v_unit_cost        BIGINT;
  v_unit_price       BIGINT;
  v_disposition      return_disposition;
  v_qty_dr_acct      BIGINT;
  v_val_dr_acct      BIGINT;
  v_var_scrap        BIGINT;
  v_cust_qty         BIGINT;
  v_cust_ar          BIGINT;
  v_cogs_acct        BIGINT;
  v_tax_acct         BIGINT;
  v_memo_line_id     UUID;
  v_batch            JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM customer_credit_memos
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT id INTO v_customer_check FROM customers WHERE id = p_customer_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'customer_credit_memo_invalid: customer % not found',
                    p_customer_id USING ERRCODE = 'P0048';
  END IF;

  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'customer_credit_memo_invalid: empty lines for customer %',
                    p_customer_id USING ERRCODE = 'P0048';
  END IF;

  SELECT id INTO v_cust_ar FROM accounts
   WHERE kind='ar' AND counterparty_id=p_customer_id
     AND currency=p_currency AND NOT is_closed;
  IF v_cust_ar IS NULL THEN
    RAISE EXCEPTION 'no open ar account for customer=% ccy=%',
                    p_customer_id, p_currency USING ERRCODE = 'P0010';
  END IF;

  INSERT INTO customer_credit_memos (
    customer_id, currency, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_customer_id, p_currency, p_business_date, p_posted_by,
    p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM customer_credit_memos
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  FOR v_idx IN 1..v_n LOOP
    v_line       := p_lines -> (v_idx - 1);
    v_kind       := v_line->>'kind';
    v_amount     := (v_line->>'amount')::BIGINT;
    v_tax_amount := COALESCE((v_line->>'tax_amount')::BIGINT, 0);

    IF v_amount IS NULL OR v_amount < 0 THEN
      RAISE EXCEPTION 'customer_credit_memo_invalid: line % amount must be >= 0',
                      v_idx USING ERRCODE = 'P0048';
    END IF;

    IF v_kind = 'financial' THEN
      v_revenue_acct_id := (v_line->>'revenue_account_id')::BIGINT;

      SELECT * INTO v_rev_acct FROM accounts WHERE id = v_revenue_acct_id;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'customer_credit_memo_invalid: line % revenue_account_id % not found',
                        v_idx, v_revenue_acct_id USING ERRCODE = 'P0048';
      END IF;
      IF v_rev_acct.is_closed THEN
        RAISE EXCEPTION 'customer_credit_memo_invalid: line % revenue_account_id % is closed',
                        v_idx, v_revenue_acct_id USING ERRCODE = 'P0048';
      END IF;
      IF v_rev_acct.ledger_kind <> 'value' THEN
        RAISE EXCEPTION 'customer_credit_memo_invalid: line % revenue_account_id % is %, expected value',
                        v_idx, v_revenue_acct_id, v_rev_acct.ledger_kind
          USING ERRCODE = 'P0048';
      END IF;
      IF v_rev_acct.currency <> p_currency THEN
        RAISE EXCEPTION 'customer_credit_memo_invalid: line % revenue ccy=% but memo ccy=%',
                        v_idx, v_rev_acct.currency, p_currency USING ERRCODE = 'P0048';
      END IF;

      INSERT INTO customer_credit_memo_lines (
        memo_id, line_no, kind, revenue_account_id, amount, tax_amount
      ) VALUES (
        v_doc_id, v_idx, 'financial', v_revenue_acct_id, v_amount, v_tax_amount
      ) RETURNING id INTO v_memo_line_id;

      IF v_amount > 0 THEN
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'customer_return',
          'document_kind',     'customer_credit_memo',
          'document_id',       v_doc_id,
          'document_line_id',  v_memo_line_id,
          'debit_account_id',  v_revenue_acct_id,
          'credit_account_id', v_cust_ar,
          'amount',            v_amount,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'counterparty_id',   p_customer_id,
          'posted_by',         p_posted_by
        ));
      END IF;

      IF v_tax_amount > 0 THEN
        SELECT id INTO v_tax_acct FROM accounts
         WHERE kind='sales_tax_payable' AND ledger_kind='value'
           AND currency=p_currency AND NOT is_closed;
        IF v_tax_acct IS NULL THEN
          RAISE EXCEPTION 'no open sales_tax_payable for ccy=%',
                          p_currency USING ERRCODE = 'P0010';
        END IF;
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'customer_return',
          'document_kind',     'customer_credit_memo',
          'document_id',       v_doc_id,
          'document_line_id',  v_memo_line_id,
          'debit_account_id',  v_tax_acct,
          'credit_account_id', v_cust_ar,
          'amount',            v_tax_amount,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'counterparty_id',   p_customer_id,
          'posted_by',         p_posted_by
        ));
      END IF;

    ELSIF v_kind = 'goods_return' THEN
      v_sku_id      := (v_line->>'sku_id')::UUID;
      v_location_id := (v_line->>'location_id')::UUID;
      v_qty         := (v_line->>'qty')::BIGINT;
      v_unit_cost   := (v_line->>'unit_cost')::BIGINT;
      v_unit_price  := (v_line->>'unit_price')::BIGINT;
      v_disposition := (v_line->>'disposition')::return_disposition;

      IF v_qty IS NULL OR v_qty <= 0 THEN
        RAISE EXCEPTION 'customer_credit_memo_invalid: line % qty must be > 0',
                        v_idx USING ERRCODE = 'P0048';
      END IF;
      IF v_amount <> v_qty * v_unit_price THEN
        RAISE EXCEPTION 'customer_credit_memo_invalid: line % amount=% but qty=% × unit_price=%',
                        v_idx, v_amount, v_qty, v_unit_price USING ERRCODE = 'P0048';
      END IF;

      IF v_disposition = 'restock' THEN
        SELECT id INTO v_qty_dr_acct FROM accounts
         WHERE kind='stock_available' AND sku_id=v_sku_id
           AND location_id=v_location_id AND NOT is_closed;
        IF v_qty_dr_acct IS NULL THEN
          RAISE EXCEPTION 'no open stock_available for sku=% loc=%',
                          v_sku_id, v_location_id USING ERRCODE = 'P0010';
        END IF;
      ELSIF v_disposition = 'scrap' THEN
        SELECT id INTO v_qty_dr_acct FROM accounts
         WHERE kind='stock_scrap' AND sku_id=v_sku_id AND NOT is_closed;
        IF v_qty_dr_acct IS NULL THEN
          RAISE EXCEPTION 'no open stock_scrap for sku=%',
                          v_sku_id USING ERRCODE = 'P0010';
        END IF;
      ELSIF v_disposition = 'repair' THEN
        SELECT id INTO v_qty_dr_acct FROM accounts
         WHERE kind='stock_quarantine' AND sku_id=v_sku_id
           AND location_id=v_location_id AND NOT is_closed;
        IF v_qty_dr_acct IS NULL THEN
          RAISE EXCEPTION 'no open stock_quarantine for sku=% loc=%',
                          v_sku_id, v_location_id USING ERRCODE = 'P0010';
        END IF;
      END IF;

      IF v_disposition IN ('restock', 'repair') THEN
        SELECT id INTO v_val_dr_acct FROM accounts
         WHERE kind='inv_value_fg' AND sku_id=v_sku_id
           AND location_id=v_location_id AND currency=p_currency
           AND NOT is_closed;
        IF v_val_dr_acct IS NULL THEN
          RAISE EXCEPTION 'no open inv_value_fg for sku=% loc=% ccy=%',
                          v_sku_id, v_location_id, p_currency USING ERRCODE = 'P0010';
        END IF;
      ELSE
        SELECT id INTO v_var_scrap FROM accounts
         WHERE kind='variance_scrap' AND currency=p_currency AND NOT is_closed;
        IF v_var_scrap IS NULL THEN
          RAISE EXCEPTION 'no open variance_scrap for ccy=%',
                          p_currency USING ERRCODE = 'P0010';
        END IF;
        v_val_dr_acct := v_var_scrap;
      END IF;

      SELECT id INTO v_cust_qty FROM accounts
       WHERE kind='customer_pool' AND counterparty_id=p_customer_id
         AND NOT is_closed;
      IF v_cust_qty IS NULL THEN
        SELECT id INTO v_cust_qty FROM accounts
         WHERE kind='customer_pool' AND counterparty_id IS NULL
           AND NOT is_closed;
      END IF;
      IF v_cust_qty IS NULL THEN
        RAISE EXCEPTION 'no open customer_pool for customer=%',
                        p_customer_id USING ERRCODE = 'P0010';
      END IF;

      SELECT id INTO v_cogs_acct FROM accounts
       WHERE kind='cogs' AND currency=p_currency AND NOT is_closed;
      IF v_cogs_acct IS NULL THEN
        RAISE EXCEPTION 'no open cogs for ccy=%',
                        p_currency USING ERRCODE = 'P0010';
      END IF;

      INSERT INTO customer_credit_memo_lines (
        memo_id, line_no, kind, sku_id, location_id, qty,
        unit_cost, unit_price, disposition, amount, tax_amount
      ) VALUES (
        v_doc_id, v_idx, 'goods_return', v_sku_id, v_location_id, v_qty,
        v_unit_cost, v_unit_price, v_disposition, v_amount, v_tax_amount
      ) RETURNING id INTO v_memo_line_id;

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'customer_return',
        'document_kind',     'customer_credit_memo',
        'document_id',       v_doc_id,
        'document_line_id',  v_memo_line_id,
        'debit_account_id',  v_qty_dr_acct,
        'credit_account_id', v_cust_qty,
        'amount',            v_qty,
        'qty',               v_qty,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_customer_id,
        'posted_by',         p_posted_by
      ));

      IF v_qty * v_unit_cost > 0 THEN
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'customer_return',
          'document_kind',     'customer_credit_memo',
          'document_id',       v_doc_id,
          'document_line_id',  v_memo_line_id,
          'debit_account_id',  v_val_dr_acct,
          'credit_account_id', v_cogs_acct,
          'amount',            v_qty * v_unit_cost,
          'qty',               v_qty,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'counterparty_id',   p_customer_id,
          'posted_by',         p_posted_by
        ));
      END IF;

      IF v_amount > 0 THEN
        SELECT id INTO v_revenue_acct_id FROM accounts
         WHERE kind='revenue' AND currency=p_currency AND NOT is_closed;
        IF v_revenue_acct_id IS NULL THEN
          RAISE EXCEPTION 'no open revenue for ccy=%',
                          p_currency USING ERRCODE = 'P0010';
        END IF;
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'customer_return',
          'document_kind',     'customer_credit_memo',
          'document_id',       v_doc_id,
          'document_line_id',  v_memo_line_id,
          'debit_account_id',  v_revenue_acct_id,
          'credit_account_id', v_cust_ar,
          'amount',            v_amount,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'counterparty_id',   p_customer_id,
          'posted_by',         p_posted_by
        ));
      END IF;

      IF v_tax_amount > 0 THEN
        SELECT id INTO v_tax_acct FROM accounts
         WHERE kind='sales_tax_payable' AND currency=p_currency
           AND NOT is_closed;
        IF v_tax_acct IS NULL THEN
          RAISE EXCEPTION 'no open sales_tax_payable for ccy=%',
                          p_currency USING ERRCODE = 'P0010';
        END IF;
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'customer_return',
          'document_kind',     'customer_credit_memo',
          'document_id',       v_doc_id,
          'document_line_id',  v_memo_line_id,
          'debit_account_id',  v_tax_acct,
          'credit_account_id', v_cust_ar,
          'amount',            v_tax_amount,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'counterparty_id',   p_customer_id,
          'posted_by',         p_posted_by
        ));
      END IF;

    ELSE
      RAISE EXCEPTION 'customer_credit_memo_invalid: line % unknown kind %',
                      v_idx, v_kind USING ERRCODE = 'P0048';
    END IF;
  END LOOP;

  PERFORM post_posting_lines(v_batch, p_override_closed_period);
  RETURN v_doc_id;
END;
$$;

-- ============================================================
-- 15. post_vendor_debit_memo (acct-b6e standalone)
-- ============================================================
--
-- 'financial' lines: ap DR / caller-supplied account CR.
-- 'goods_return' lines: qty out (vendor_pool / stock_available); value
-- out (ap / inv_value_raw at caller-supplied unit_cost; no PPV).
-- Always routes to ap (cleared); pre-bill routing requires post_po_return.

CREATE OR REPLACE FUNCTION post_vendor_debit_memo(
  p_vendor_id              UUID,
  p_currency               CHAR(3),
  p_lines                  JSONB,
  p_business_date          DATE,
  p_posted_by              UUID,
  p_idempotency_key        UUID,
  p_notes                  TEXT DEFAULT NULL,
  p_override_closed_period BOOLEAN DEFAULT FALSE
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id      UUID;
  v_doc_id           UUID;
  v_vendor_check     UUID;
  v_n                INT;
  v_idx              INT;
  v_line             JSONB;
  v_kind             TEXT;
  v_amount           BIGINT;
  v_expense_acct_id  BIGINT;
  v_exp_acct         accounts%ROWTYPE;
  v_sku_id           UUID;
  v_location_id      UUID;
  v_qty              BIGINT;
  v_unit_cost        BIGINT;
  v_qty_acct         BIGINT;
  v_val_acct         BIGINT;
  v_ven_qty          BIGINT;
  v_ven_ap           BIGINT;
  v_memo_line_id     UUID;
  v_batch            JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM vendor_debit_memos
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT id INTO v_vendor_check FROM vendors WHERE id = p_vendor_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'vendor_debit_memo_invalid: vendor % not found',
                    p_vendor_id USING ERRCODE = 'P0049';
  END IF;

  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'vendor_debit_memo_invalid: empty lines for vendor %',
                    p_vendor_id USING ERRCODE = 'P0049';
  END IF;

  SELECT id INTO v_ven_ap FROM accounts
   WHERE kind='ap' AND counterparty_id=p_vendor_id
     AND currency=p_currency AND NOT is_closed;
  IF v_ven_ap IS NULL THEN
    RAISE EXCEPTION 'no open ap account for vendor=% ccy=%',
                    p_vendor_id, p_currency USING ERRCODE = 'P0010';
  END IF;

  INSERT INTO vendor_debit_memos (
    vendor_id, currency, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_vendor_id, p_currency, p_business_date, p_posted_by,
    p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM vendor_debit_memos
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  FOR v_idx IN 1..v_n LOOP
    v_line   := p_lines -> (v_idx - 1);
    v_kind   := v_line->>'kind';
    v_amount := (v_line->>'amount')::BIGINT;

    IF v_amount IS NULL OR v_amount < 0 THEN
      RAISE EXCEPTION 'vendor_debit_memo_invalid: line % amount must be >= 0',
                      v_idx USING ERRCODE = 'P0049';
    END IF;

    IF v_kind = 'financial' THEN
      v_expense_acct_id := (v_line->>'expense_account_id')::BIGINT;

      SELECT * INTO v_exp_acct FROM accounts WHERE id = v_expense_acct_id;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'vendor_debit_memo_invalid: line % expense_account_id % not found',
                        v_idx, v_expense_acct_id USING ERRCODE = 'P0049';
      END IF;
      IF v_exp_acct.is_closed THEN
        RAISE EXCEPTION 'vendor_debit_memo_invalid: line % expense_account_id % is closed',
                        v_idx, v_expense_acct_id USING ERRCODE = 'P0049';
      END IF;
      IF v_exp_acct.ledger_kind <> 'value' THEN
        RAISE EXCEPTION 'vendor_debit_memo_invalid: line % expense account % is %, expected value',
                        v_idx, v_expense_acct_id, v_exp_acct.ledger_kind
          USING ERRCODE = 'P0049';
      END IF;
      IF v_exp_acct.currency <> p_currency THEN
        RAISE EXCEPTION 'vendor_debit_memo_invalid: line % expense ccy=% but memo ccy=%',
                        v_idx, v_exp_acct.currency, p_currency USING ERRCODE = 'P0049';
      END IF;

      INSERT INTO vendor_debit_memo_lines (
        memo_id, line_no, kind, expense_account_id, amount
      ) VALUES (
        v_doc_id, v_idx, 'financial', v_expense_acct_id, v_amount
      ) RETURNING id INTO v_memo_line_id;

      IF v_amount > 0 THEN
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'po_return_to_vendor',
          'document_kind',     'vendor_debit_memo',
          'document_id',       v_doc_id,
          'document_line_id',  v_memo_line_id,
          'debit_account_id',  v_ven_ap,
          'credit_account_id', v_expense_acct_id,
          'amount',            v_amount,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'counterparty_id',   p_vendor_id,
          'posted_by',         p_posted_by
        ));
      END IF;

    ELSIF v_kind = 'goods_return' THEN
      v_sku_id      := (v_line->>'sku_id')::UUID;
      v_location_id := (v_line->>'location_id')::UUID;
      v_qty         := (v_line->>'qty')::BIGINT;
      v_unit_cost   := (v_line->>'unit_cost')::BIGINT;

      IF v_qty IS NULL OR v_qty <= 0 THEN
        RAISE EXCEPTION 'vendor_debit_memo_invalid: line % qty must be > 0',
                        v_idx USING ERRCODE = 'P0049';
      END IF;
      IF v_amount <> v_qty * v_unit_cost THEN
        RAISE EXCEPTION 'vendor_debit_memo_invalid: line % amount=% but qty=% × unit_cost=%',
                        v_idx, v_amount, v_qty, v_unit_cost USING ERRCODE = 'P0049';
      END IF;

      SELECT id INTO v_qty_acct FROM accounts
       WHERE kind='stock_available' AND sku_id=v_sku_id
         AND location_id=v_location_id AND NOT is_closed;
      IF v_qty_acct IS NULL THEN
        RAISE EXCEPTION 'no open stock_available for sku=% loc=%',
                        v_sku_id, v_location_id USING ERRCODE = 'P0010';
      END IF;

      SELECT id INTO v_val_acct FROM accounts
       WHERE kind='inv_value_raw' AND sku_id=v_sku_id
         AND location_id=v_location_id AND currency=p_currency
         AND NOT is_closed;
      IF v_val_acct IS NULL THEN
        RAISE EXCEPTION 'no open inv_value_raw for sku=% loc=% ccy=%',
                        v_sku_id, v_location_id, p_currency USING ERRCODE = 'P0010';
      END IF;

      SELECT id INTO v_ven_qty FROM accounts
       WHERE kind='vendor_pool' AND counterparty_id=p_vendor_id
         AND NOT is_closed;
      IF v_ven_qty IS NULL THEN
        SELECT id INTO v_ven_qty FROM accounts
         WHERE kind='vendor_pool' AND counterparty_id IS NULL
           AND NOT is_closed;
      END IF;
      IF v_ven_qty IS NULL THEN
        RAISE EXCEPTION 'no open vendor_pool for vendor=%',
                        p_vendor_id USING ERRCODE = 'P0010';
      END IF;

      INSERT INTO vendor_debit_memo_lines (
        memo_id, line_no, kind, sku_id, location_id, qty, unit_cost, amount
      ) VALUES (
        v_doc_id, v_idx, 'goods_return', v_sku_id, v_location_id, v_qty,
        v_unit_cost, v_amount
      ) RETURNING id INTO v_memo_line_id;

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'po_return_to_vendor',
        'document_kind',     'vendor_debit_memo',
        'document_id',       v_doc_id,
        'document_line_id',  v_memo_line_id,
        'debit_account_id',  v_ven_qty,
        'credit_account_id', v_qty_acct,
        'amount',            v_qty,
        'qty',               v_qty,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_vendor_id,
        'posted_by',         p_posted_by
      ));

      IF v_amount > 0 THEN
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'po_return_to_vendor',
          'document_kind',     'vendor_debit_memo',
          'document_id',       v_doc_id,
          'document_line_id',  v_memo_line_id,
          'debit_account_id',  v_ven_ap,
          'credit_account_id', v_val_acct,
          'amount',            v_amount,
          'qty',               v_qty,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'counterparty_id',   p_vendor_id,
          'posted_by',         p_posted_by
        ));
      END IF;

    ELSE
      RAISE EXCEPTION 'vendor_debit_memo_invalid: line % unknown kind %',
                      v_idx, v_kind USING ERRCODE = 'P0049';
    END IF;
  END LOOP;

  PERFORM post_posting_lines(v_batch, p_override_closed_period);
  RETURN v_doc_id;
END;
$$;
