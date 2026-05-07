-- acct-7mg / Slice A.1 Part B (schema) — inflow-cycle tables.
--
-- Adds the supplier counterparty + PO line + receipt + vendor-bill
-- document model. Function bodies (post_po_receipt, post_ap_bill)
-- land in migration 0035.
--
-- Tables introduced:
--
--   suppliers              — vendor master data. Replaces the
--                            unconstrained `purchase_orders.supplier_id
--                            UUID` reference with a real FK target.
--   purchase_order_lines   — line items on a PO (sku, location,
--                            qty_ordered, unit_cost, currency).
--   po_receipts            — receipt-event header (one per
--                            post_po_receipt call).
--   po_receipt_lines       — per-line received quantity. Drives the
--                            "received-not-billed" remainder used by
--                            three-way match.
--   vendor_bills           — vendor-bill header.
--   vendor_bill_lines      — per-line bill (kind = 'po_match' clears
--                            ap_unsettled → ap; kind = 'service'
--                            posts caller-supplied expense_account
--                            → ap).
--
-- The existing stub `purchase_orders` (migration 0005) is kept; we
-- add the FK from supplier_id → suppliers.id but keep it nullable
-- because the stub is also used by `commodity_receipts` (migration
-- 0011) which doesn't always carry a supplier.
--
-- Account partitioning:
--
--   ap, ap_unsettled — partitioned by (counterparty_id, currency).
--                      The existing un-partitioned forms (currency
--                      only, no counterparty_id) remain valid for
--                      tests that don't model per-supplier liability;
--                      conditional unique index only catches the
--                      partitioned form.
--   supplier_pool     — partitioned by counterparty_id (qty-side; no
--                       currency).
--
-- Slice A locks in (per planning conversation 2026-05-01):
--   D1 — GRNI semantics: po_receipt credits ap_unsettled (not ap);
--        ap_bill clears ap_unsettled → ap. Revises design doc §3.1.
--   D2 — Service bills: caller passes expense_account_id per line;
--        no fixed expense taxonomy at this slice (acct-063 follow-up).
--   D3 — Three-way match strict on (sku, location, qty, unit_cost).

-- ============================================================
-- suppliers
-- ============================================================

CREATE TABLE suppliers (
  id            UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  code          TEXT NOT NULL UNIQUE,
  name          TEXT NOT NULL,
  currency      CHAR(3) NOT NULL,
  payment_terms TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON TABLE suppliers IS
  'Vendor master data for the inflow cycle (Slice A). FK target for '
  'purchase_orders.supplier_id and vendor_bills.supplier_id. Currency '
  'is the supplier''s default billing currency; individual PO lines '
  'and bills carry their own currency.';

ALTER TABLE purchase_orders
  ADD CONSTRAINT po_supplier_fk
  FOREIGN KEY (supplier_id) REFERENCES suppliers(id);

-- ============================================================
-- purchase_order_lines
-- ============================================================

CREATE TABLE purchase_order_lines (
  id          UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  po_id       UUID NOT NULL REFERENCES purchase_orders(id),
  line_no     INT  NOT NULL,
  sku_id      UUID NOT NULL REFERENCES skus(id),
  location_id UUID NOT NULL REFERENCES locations(id),
  qty_ordered BIGINT NOT NULL CHECK (qty_ordered > 0),
  unit_cost   BIGINT NOT NULL CHECK (unit_cost >= 0),
  currency    CHAR(3) NOT NULL,
  UNIQUE (po_id, line_no)
);

CREATE INDEX po_lines_po ON purchase_order_lines (po_id);
CREATE INDEX po_lines_sku_loc ON purchase_order_lines (sku_id, location_id);

COMMENT ON TABLE purchase_order_lines IS
  'Line items on a purchase order. Each line carries its own '
  'currency (PO-level currency could be added later if multi-currency '
  'POs become a thing). qty_ordered and unit_cost drive PPV and '
  'three-way match.';

-- ============================================================
-- po_receipts + po_receipt_lines
-- ============================================================

CREATE TABLE po_receipts (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  po_id           UUID NOT NULL REFERENCES purchase_orders(id),
  business_date   DATE NOT NULL,
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID NOT NULL UNIQUE,
  notes           TEXT
);

CREATE INDEX po_receipts_po ON po_receipts (po_id);
CREATE INDEX po_receipts_posted_at ON po_receipts (posted_at);

CREATE TABLE po_receipt_lines (
  id           UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  receipt_id   UUID NOT NULL REFERENCES po_receipts(id),
  po_line_id   UUID NOT NULL REFERENCES purchase_order_lines(id),
  qty_received BIGINT NOT NULL CHECK (qty_received > 0),
  UNIQUE (receipt_id, po_line_id)
);

CREATE INDEX po_receipt_lines_po_line ON po_receipt_lines (po_line_id);

COMMENT ON TABLE po_receipts IS
  'Receipt event header. One row per post_po_receipt call.';

COMMENT ON TABLE po_receipt_lines IS
  'Per-line received quantity. SUM(qty_received) over po_line_id '
  'minus SUM(vendor_bill_lines.qty WHERE kind=''po_match'' AND '
  'po_line_id=this) is the received-not-billed remainder used by '
  'three-way match in post_ap_bill.';

-- ============================================================
-- vendor_bills + vendor_bill_lines
-- ============================================================

CREATE TABLE vendor_bills (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  supplier_id     UUID NOT NULL REFERENCES suppliers(id),
  currency        CHAR(3) NOT NULL,
  business_date   DATE NOT NULL,
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID NOT NULL UNIQUE,
  notes           TEXT
);

CREATE INDEX vendor_bills_supplier ON vendor_bills (supplier_id);
CREATE INDEX vendor_bills_posted_at ON vendor_bills (posted_at);

CREATE TABLE vendor_bill_lines (
  id                 UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  bill_id            UUID NOT NULL REFERENCES vendor_bills(id),
  line_no            INT  NOT NULL,
  kind               TEXT NOT NULL CHECK (kind IN ('po_match', 'service')),
  po_line_id         UUID REFERENCES purchase_order_lines(id),
  expense_account_id BIGINT REFERENCES accounts(id),
  qty                BIGINT,
  unit_cost          BIGINT,
  amount             BIGINT NOT NULL CHECK (amount > 0),
  UNIQUE (bill_id, line_no),
  CHECK (
    (kind = 'po_match'
     AND po_line_id IS NOT NULL
     AND expense_account_id IS NULL
     AND qty IS NOT NULL AND qty > 0
     AND unit_cost IS NOT NULL AND unit_cost >= 0)
    OR
    (kind = 'service'
     AND po_line_id IS NULL
     AND expense_account_id IS NOT NULL
     AND qty IS NULL
     AND unit_cost IS NULL)
  )
);

CREATE INDEX vendor_bill_lines_bill ON vendor_bill_lines (bill_id);
CREATE INDEX vendor_bill_lines_po_line ON vendor_bill_lines (po_line_id)
  WHERE po_line_id IS NOT NULL;

COMMENT ON TABLE vendor_bill_lines IS
  'Bill line items. kind=''po_match'' clears a portion of received-'
  'not-billed remainder for the referenced po_line (ap_unsettled → '
  'ap clearance). kind=''service'' posts the caller-supplied '
  'expense_account → ap directly with no PO reference. CHECK '
  'enforces that the right columns are set for each mode.';

-- ============================================================
-- Account partitioning indexes for ap / ap_unsettled / supplier_pool
-- ============================================================

CREATE UNIQUE INDEX accounts_ap_partitioned_uk
  ON accounts (kind, counterparty_id, currency)
  WHERE kind IN ('ap', 'ap_unsettled')
    AND counterparty_id IS NOT NULL
    AND NOT is_closed;

CREATE UNIQUE INDEX accounts_supplier_pool_uk
  ON accounts (counterparty_id)
  WHERE kind = 'supplier_pool'
    AND counterparty_id IS NOT NULL
    AND NOT is_closed;

COMMENT ON INDEX accounts_ap_partitioned_uk IS
  'ap / ap_unsettled accounts partitioned by (counterparty_id, '
  'currency). Per-supplier liability tracking. Un-partitioned forms '
  '(counterparty_id NULL) remain valid for tests that don''t need '
  'per-supplier reporting.';

COMMENT ON INDEX accounts_supplier_pool_uk IS
  'supplier_pool qty-side accounts partitioned by counterparty_id. '
  'No currency on qty side. Per Slice A inflow design.';
