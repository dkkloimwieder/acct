-- Reference master data: skus, locations, vendors, customers.
--
-- Consolidates archive migs 0005 + 0027 (drop standard_cost; standard_costs
-- is its own append-only table) + 0034 (vendors, originally 'suppliers' —
-- name baked in from start) + 0040 (BOM2 skus columns) + 0061 (yield_mode)
-- + 0080 (customers) + 0090 (tolerance windows).
--
-- All TEXT-with-CHECK columns now use proper ENUM types from 0002.

-- ============================================================
-- skus: SKU master with cost_method, BOM2 metadata
-- ============================================================

CREATE TABLE skus (
  id                  UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  code                TEXT NOT NULL UNIQUE,
  description         TEXT,
  uom                 TEXT NOT NULL,
  cost_method         cost_method NOT NULL DEFAULT 'standard',
  default_lot_size    BIGINT NOT NULL DEFAULT 1
                        CHECK (default_lot_size > 0),
  is_phantom          BOOLEAN NOT NULL DEFAULT FALSE,
  consumption_policy  consumption_policy NOT NULL DEFAULT 'forward',
  yield_mode          yield_mode NOT NULL DEFAULT 'plan_only',
  created_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON COLUMN skus.cost_method IS
  'Per-SKU costing strategy. Dispatched in _post_posting_lines_compute_amount '
  'via cost_method_strategies registry (acct-w0lo).';

COMMENT ON COLUMN skus.default_lot_size IS
  'Default WO lot size for amortizing per-lot BOM charges into per-unit '
  'standard cost (BOM2 acct-jg2). Lot-size variance surfaces in '
  'variance_wo_close when actual lot != default.';

COMMENT ON COLUMN skus.is_phantom IS
  'TRUE for cost-rollup-only SKUs that have no physical inventory; '
  '_wo_explode_bom flattens phantom child BOMs into the parent at the '
  'parent applies_at_op (BOM2 acct-jg2).';

COMMENT ON COLUMN skus.consumption_policy IS
  'WO material consumption timing. Only ''forward'' is dispatched today; '
  '''backflush_at_op'' / ''backflush_at_complete'' raise P0035 at WO start '
  '(acct-BACKFLUSH / acct-oi4 follow-up).';

COMMENT ON COLUMN skus.yield_mode IS
  'BOM rollup behavior. ''plan_only'' (default) ignores bom_lines.yield_pct '
  'in rollup; ''absorbed'' inflates parent_std by 1/(yield_pct/100) per item '
  'line. Affects only the rollup tool''s output and what variance shows up '
  'at wo_complete (BOM2 acct-6jq).';

-- ============================================================
-- locations: location master
-- ============================================================

CREATE TABLE locations (
  id         UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  code       TEXT NOT NULL UNIQUE,
  name       TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- ============================================================
-- vendors: vendor master (Slice A — was 'suppliers' historically)
-- ============================================================

CREATE TABLE vendors (
  id                       UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  code                     TEXT NOT NULL UNIQUE,
  name                     TEXT NOT NULL,
  currency                 CHAR(3) NOT NULL,
  payment_terms            TEXT,
  unit_cost_tolerance_pct  NUMERIC(5,2) NOT NULL DEFAULT 0
                             CHECK (unit_cost_tolerance_pct >= 0
                                    AND unit_cost_tolerance_pct < 100),
  created_at               TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON TABLE vendors IS
  'Vendor master for the inflow cycle (Slice A acct-7mg, originally '
  'named ''suppliers''; renamed to ''vendors'' in acct-397). FK target '
  'for purchase_orders.vendor_id, vendor_bills.vendor_id, '
  'po_returns.vendor_id, etc. unit_cost_tolerance_pct gates the '
  'three-way-match in post_ap_bill (acct-7mc).';

-- ============================================================
-- customers: customer master (Slice C)
-- ============================================================

CREATE TABLE customers (
  id                        UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  code                      TEXT NOT NULL UNIQUE,
  name                      TEXT NOT NULL,
  default_currency          CHAR(3) NOT NULL,
  unit_price_tolerance_pct  NUMERIC(5,2) NOT NULL DEFAULT 0
                              CHECK (unit_price_tolerance_pct >= 0
                                     AND unit_price_tolerance_pct < 100),
  active_at                 TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  inactive_at               TIMESTAMPTZ,
  created_at                TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON TABLE customers IS
  'Customer master for the outflow cycle (Slice C acct-th7). FK target '
  'for sales_orders.customer_id, customer_invoices.customer_id, '
  'ar_payments.customer_id, etc. unit_price_tolerance_pct gates the '
  'three-way-match in post_customer_invoice (acct-7mc).';

-- ============================================================
-- sales_orders + purchase_orders: header tables. Defined here so
-- inventory_reservations (0011) can FK them. Line tables (and any
-- additional columns) come in 0016 / 0018 with the document wrappers.
-- ============================================================

CREATE TABLE sales_orders (
  id          UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  customer_id UUID REFERENCES customers(id),
  status      TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE purchase_orders (
  id          UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  vendor_id   UUID REFERENCES vendors(id),
  status      TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
