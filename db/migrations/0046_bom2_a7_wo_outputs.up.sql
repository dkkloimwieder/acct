-- acct-e50 / BOM2 Phase A7 — wo_outputs table for co-products / by-products.
--
-- Multi-output WO support. Default behavior (when wo_outputs has zero
-- rows for a WO): single output to work_orders.parent_sku_id ×
-- qty_target at work_orders.fg_location_id, allocation 100%. Today's
-- Slice B behavior unchanged.
--
-- When wo_outputs has rows: post_wo_complete (B8) distributes total
-- WIP value across the output rows per allocation_method:
--   primary       — caller designates one output as primary
--                   (allocation_pct=100); others ride along at zero
--                   value (true by-products).
--   sales_value   — caller-supplied allocation_pct = relative
--                   selling-price share (must sum to 100).
--   fixed_ratio   — caller-declared split (must sum to 100).
--   market_price  — reference a market_price table (column reserved
--                   for future; B8 raises P0033 today on this method).
--
-- Validation: post_wo_complete (B8) raises P0033 if Σ allocation_pct
-- across the WO's wo_outputs rows ≠ 100 (when allocation_method is
-- sales_value or fixed_ratio).

CREATE TABLE wo_outputs (
  wo_id              UUID NOT NULL REFERENCES work_orders(id),
  output_no          INT  NOT NULL,
  output_sku_id      UUID NOT NULL REFERENCES skus(id),
  fg_location_id     UUID NOT NULL REFERENCES locations(id),
  qty                BIGINT NOT NULL CHECK (qty > 0),
  allocation_method  TEXT NOT NULL CHECK (
    allocation_method IN ('primary', 'sales_value', 'fixed_ratio', 'market_price')
  ),
  allocation_pct     NUMERIC(5,2)
                     CHECK (allocation_pct IS NULL OR (allocation_pct >= 0 AND allocation_pct <= 100)),
  PRIMARY KEY (wo_id, output_no)
);

CREATE INDEX wo_outputs_output_sku
  ON wo_outputs (output_sku_id);

CREATE INDEX wo_outputs_method
  ON wo_outputs (wo_id, allocation_method);

COMMENT ON TABLE wo_outputs IS
  'Per-WO output rows for multi-output (co-product / by-product) '
  'production. Empty = single-output WO using work_orders.parent_sku_id '
  '+ fg_location_id (Slice B default). Populated = post_wo_complete '
  '(B8) distributes WIP value across rows per allocation_method. '
  'acct-e50.';

COMMENT ON COLUMN wo_outputs.allocation_method IS
  'How total WIP value distributes across outputs: primary (one full, '
  'others zero — by-products), sales_value (relative selling price), '
  'fixed_ratio (caller declares split), market_price (reserved; '
  'P0033 today). Σ allocation_pct must = 100 for sales_value/fixed_ratio.';
