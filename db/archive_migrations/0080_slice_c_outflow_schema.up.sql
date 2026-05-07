-- acct-th7 / Slice C Part B (schema) — outflow-cycle tables.
--
-- Adds the customer counterparty + SO line + shipment + customer-
-- invoice + AR-payment document model. Function bodies (post_so_ship,
-- post_customer_invoice, post_ar_payment) land in migration 0081.
--
-- Tables introduced:
--
--   customers                — customer master data. FK target for
--                              sales_orders.customer_id (the existing
--                              stub from mig 0005 keeps customer_id
--                              nullable for back-compat).
--   sales_order_lines        — line items on an SO (sku, ship_loc,
--                              qty_ordered, unit_price, currency,
--                              tax_amount).
--   so_shipments             — shipment-event header (one per
--                              post_so_ship call).
--   so_shipment_lines        — per-line shipped quantity. Drives the
--                              "shipped-not-invoiced" remainder used
--                              by three-way match in
--                              post_customer_invoice.
--   customer_invoices        — invoice header.
--   customer_invoice_lines   — per-line invoice (kind = 'so_match'
--                              clears ar_unsettled → ar with strict
--                              three-way match; kind = 'service'
--                              posts directly against ar with
--                              caller-supplied revenue / service-
--                              credit account).
--   ar_payments              — flat payment header (single-event:
--                              cash DR / ar CR). No detail table.
--
-- The existing stub `sales_orders` (mig 0005) is kept; we add the FK
-- from customer_id → customers but keep it nullable per the same
-- rationale Slice A used for purchase_orders.supplier_id.
--
-- Account partitioning:
--
--   ar, ar_unsettled — partitioned by (counterparty_id, currency).
--                      Mirror of ap / ap_unsettled. Un-partitioned
--                      forms (counterparty_id NULL, currency-only)
--                      remain valid for fixture tests.
--   customer_pool    — partitioned by counterparty_id (qty side, no
--                      currency). Mirror of supplier_pool.
--
-- Slice C locks in (per planning conversation 2026-05-04):
--   D1 — GRNI semantics on outflow: so_ship credits ar_unsettled (not
--        ar); customer_invoice clears ar_unsettled → ar. Revises
--        design doc §3.3.
--   D3 — 'shipped' added to reservation_status (mig 0079); post_so_
--        ship transitions reservations 'active' → 'shipped' atomically.
--   D4 — Three-way match strict on (sku, ship_location, qty,
--        unit_price). Mirror of Slice A D3.

-- ============================================================
-- customers
-- ============================================================

CREATE TABLE customers (
  id               UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  code             TEXT NOT NULL UNIQUE,
  name             TEXT NOT NULL,
  default_currency CHAR(3) NOT NULL,
  active_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  inactive_at      TIMESTAMPTZ,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON TABLE customers IS
  'Customer master data for the outflow cycle (Slice C). FK target '
  'for sales_orders.customer_id, customer_invoices.customer_id, and '
  'ar_payments.customer_id. default_currency is the customer''s '
  'default billing currency; individual SO lines and invoices carry '
  'their own currency.';

ALTER TABLE sales_orders
  ADD CONSTRAINT so_customer_fk
  FOREIGN KEY (customer_id) REFERENCES customers(id);

-- ============================================================
-- sales_order_lines
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

CREATE INDEX so_lines_so ON sales_order_lines (so_id);
CREATE INDEX so_lines_sku_loc ON sales_order_lines (sku_id, ship_location_id);

COMMENT ON TABLE sales_order_lines IS
  'Line items on a sales order. Each line carries its own currency '
  '(SO-level currency could be added later if multi-currency SOs '
  'become a thing). qty_ordered + unit_price drive ship validation '
  'and three-way match. tax_amount is caller-supplied (Phase 1 does '
  'not compute tax; jurisdictions / exemptions / nexus is a Phase 2 '
  'integration concern).';

-- ============================================================
-- so_shipments + so_shipment_lines
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

CREATE INDEX so_shipments_so ON so_shipments (so_id);
CREATE INDEX so_shipments_posted_at ON so_shipments (posted_at);

CREATE TABLE so_shipment_lines (
  id           UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  shipment_id  UUID NOT NULL REFERENCES so_shipments(id),
  so_line_id   UUID NOT NULL REFERENCES sales_order_lines(id),
  qty_shipped  BIGINT NOT NULL CHECK (qty_shipped > 0),
  unit_cost    BIGINT NOT NULL CHECK (unit_cost >= 0),
  unit_price   BIGINT NOT NULL CHECK (unit_price >= 0),
  tax_amount   BIGINT NOT NULL DEFAULT 0 CHECK (tax_amount >= 0),
  UNIQUE (shipment_id, so_line_id)
);

CREATE INDEX so_shipment_lines_so_line ON so_shipment_lines (so_line_id);

COMMENT ON TABLE so_shipments IS
  'Shipment event header. One row per post_so_ship call.';

COMMENT ON TABLE so_shipment_lines IS
  'Per-line shipped quantity. unit_cost is the dispatcher-resolved '
  'cost basis at ship time (standard or wac running avg, persisted '
  'for audit even though authoritative is the COGS transfer). '
  'unit_price is captured for invoice-time three-way match. '
  'SUM(qty_shipped) over so_line_id minus SUM(customer_invoice_'
  'lines.qty WHERE kind=''so_match'' AND so_line_id=this) is the '
  'shipped-not-invoiced remainder used by post_customer_invoice.';

-- ============================================================
-- customer_invoices + customer_invoice_lines
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

CREATE INDEX customer_invoices_customer ON customer_invoices (customer_id);
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

COMMENT ON TABLE customer_invoice_lines IS
  'Invoice line items. kind=''so_match'' clears a portion of '
  'shipped-not-invoiced remainder for the referenced so_line '
  '(ar_unsettled → ar clearance) with strict three-way match on '
  'unit_price. kind=''service'' posts directly to ar with caller-'
  'supplied revenue_account (covers consulting fees, subscription '
  'charges, etc.). CHECK enforces the right columns are set per mode.';

-- ============================================================
-- ar_payments
-- ============================================================
--
-- Payments are a flat single-event document: cash DR / ar CR. No
-- detail table. Mirrors §3.7 of the design doc verbatim.

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

CREATE INDEX ar_payments_customer ON ar_payments (customer_id);
CREATE INDEX ar_payments_posted_at ON ar_payments (posted_at);

COMMENT ON TABLE ar_payments IS
  'AR payment event. One row per post_ar_payment call. Single '
  'event: cash(currency) DR / ar(customer, currency) CR.';

-- ============================================================
-- Account partitioning indexes for ar / ar_unsettled / customer_pool
-- ============================================================

CREATE UNIQUE INDEX accounts_ar_partitioned_uk
  ON accounts (kind, counterparty_id, currency)
  WHERE kind IN ('ar', 'ar_unsettled')
    AND counterparty_id IS NOT NULL
    AND NOT is_closed;

CREATE UNIQUE INDEX accounts_customer_pool_uk
  ON accounts (counterparty_id)
  WHERE kind = 'customer_pool'
    AND counterparty_id IS NOT NULL
    AND NOT is_closed;

COMMENT ON INDEX accounts_ar_partitioned_uk IS
  'ar / ar_unsettled accounts partitioned by (counterparty_id, '
  'currency). Per-customer receivable tracking. Un-partitioned forms '
  '(counterparty_id NULL) remain valid for fixture tests that don''t '
  'need per-customer reporting.';

COMMENT ON INDEX accounts_customer_pool_uk IS
  'customer_pool qty-side accounts partitioned by counterparty_id. '
  'No currency on qty side. Mirrors supplier_pool partitioning.';
