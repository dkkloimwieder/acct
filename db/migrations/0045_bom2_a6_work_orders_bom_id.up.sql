-- acct-zqy / BOM2 Phase A6 — ALTER work_orders ADD COLUMN bom_id.
--
-- Per-WO BOM selection. NULL means "use the primary active BOM for
-- parent_sku at business_date" — that's the default behavior the
-- _wo_resolve_bom_for resolver (B3) implements. Set means "this
-- specific BOM was chosen at WO creation" — useful for alternates
-- (substitute material), pinned future-effective drafts (testing),
-- or any caller-driven override.
--
-- This replaces the original plan's wo_recipe_overrides table idea
-- (per user feedback): a per-WO substitution is just *another BOM*
-- (with caller-meaningful code, e.g., 'WIDGET-EUR-VENDOR-2026-04')
-- rather than a one-off override row. The substitute persists, can
-- be reused, has its own effectivity window — far more durable.

ALTER TABLE work_orders ADD COLUMN bom_id BIGINT REFERENCES bom_headers(id);

CREATE INDEX work_orders_bom_id ON work_orders (bom_id) WHERE bom_id IS NOT NULL;

COMMENT ON COLUMN work_orders.bom_id IS
  'Per-WO BOM selection. NULL = use primary active BOM for parent_sku '
  'at business_date (default; resolved by _wo_resolve_bom_for in B3). '
  'Set = pinned BOM (alternate, draft pinned for test, etc.). acct-zqy.';
