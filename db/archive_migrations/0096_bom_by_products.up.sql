-- acct-ksnh / acct-7t4.1 — bom_by_products: BOM-side declaration of
-- by-products with three accounting treatments.
--
-- This migration is schema-only. The wo_by_products snapshot table,
-- post_wo_start auto-init, post_wo_complete pre-pass, and post_ap_bill
-- disposal_match handling all land in subsequent acct-7t4.* sub-issues
-- (v5r6 / u1n9 / 6g47 / 3yno / a41h).
--
-- Three treatments (per ASC 330 / IAS 2 NRV definition + mainstream-
-- ERP convention):
--
--   * nrv_credit    — by-product has positive market value; drains
--                     parent WIP at NRV at wo_complete (co-products
--                     absorb less). unit_value > 0.
--   * negligible    — by-product worth ~zero; qty leg only (no value).
--                     unit_value = 0. Main carries full cost.
--   * disposal_cost — by-product has NEGATIVE value (disposal must be
--                     paid for). unit_value < 0. Two bases:
--                       • inventoriable: disposal cost flows into
--                         co-product COGS basis (allocation_pct);
--                         disposal_expense_account_kind MUST be NULL
--                         (it routes to inv_value_fg, not an expense)
--                       • period: disposal posts directly to a P&L
--                         expense account; disposal_expense_account_kind
--                         is caller-supplied (NULL means default).
--                     disposal_vendor_id NOT NULL — bridge to AP.
--
-- The CHECK is a single OR-of-three to encode the per-treatment field
-- requirements as a relational invariant rather than scattered
-- function-side validation.
--
-- Lifecycle: bom_by_products inherits revision/ECO from bom_headers
-- via the bom_id FK. ON DELETE CASCADE matches the existing bom_lines
-- shape. Phantom expansion is NOT relevant — by-products attach to a
-- parent's primary BOM, not phantoms.

CREATE TABLE bom_by_products (
  bom_id              BIGINT NOT NULL REFERENCES bom_headers(id) ON DELETE CASCADE,
  by_product_no       INT  NOT NULL,
  output_sku_id       UUID NOT NULL REFERENCES skus(id),
  fg_location_id      UUID NOT NULL REFERENCES locations(id),
  qty_per_parent      NUMERIC(18,6) NOT NULL CHECK (qty_per_parent > 0),
  unit_value          BIGINT NOT NULL,
  treatment           TEXT NOT NULL CHECK (treatment IN
    ('nrv_credit', 'negligible', 'disposal_cost')),
  disposal_basis      TEXT CHECK (disposal_basis IN ('inventoriable', 'period')),
  disposal_vendor_id  UUID REFERENCES vendors(id),
  disposal_expense_account_kind account_kind,
  PRIMARY KEY (bom_id, by_product_no),
  CHECK (
    (treatment = 'nrv_credit'
       AND unit_value > 0
       AND disposal_basis IS NULL
       AND disposal_vendor_id IS NULL
       AND disposal_expense_account_kind IS NULL)
    OR
    (treatment = 'negligible'
       AND unit_value = 0
       AND disposal_basis IS NULL
       AND disposal_vendor_id IS NULL
       AND disposal_expense_account_kind IS NULL)
    OR
    (treatment = 'disposal_cost'
       AND unit_value < 0
       AND disposal_basis IS NOT NULL
       AND disposal_vendor_id IS NOT NULL
       AND (disposal_basis = 'period' OR disposal_expense_account_kind IS NULL))
  )
);

CREATE INDEX bom_by_products_lookup
  ON bom_by_products (bom_id, output_sku_id);

CREATE INDEX bom_by_products_vendor
  ON bom_by_products (disposal_vendor_id)
  WHERE disposal_vendor_id IS NOT NULL;

COMMENT ON TABLE bom_by_products IS
  'BOM-side declaration of by-products. Three treatments (nrv_credit / '
  'negligible / disposal_cost) with per-treatment field requirements '
  'enforced via CHECK. Snapshotted at wo_start into wo_by_products. '
  'Inherits revision/ECO lifecycle via bom_id FK. acct-7t4.1.';

COMMENT ON COLUMN bom_by_products.unit_value IS
  'Per-unit value (BIGINT minor units). Sign discriminates treatment: '
  '> 0 nrv_credit, = 0 negligible, < 0 disposal_cost. CHECK enforced.';

COMMENT ON COLUMN bom_by_products.disposal_basis IS
  'For disposal_cost only: inventoriable (flows to co-product fg cost '
  'via allocation_pct) or period (flows directly to disposal_expense). '
  'NULL for nrv_credit / negligible.';

COMMENT ON COLUMN bom_by_products.disposal_expense_account_kind IS
  'For disposal_cost period only: caller-supplied P&L expense kind. '
  'MUST be NULL for inventoriable (routes through co-product fg). '
  'May be NULL for period (caller defaults at post_wo_complete time).';
