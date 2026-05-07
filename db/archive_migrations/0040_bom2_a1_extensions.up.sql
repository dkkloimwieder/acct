-- acct-89i / BOM2 Phase A1 — enum + SKU master extensions.
--
-- Pure additive foundation for the unified BOM refactor (epic acct-jg2).
-- Zero behavior change at this commit boundary; existing 271 tests still
-- pass after this migration applies. New values become callable in
-- subsequent migrations (PG ALTER TYPE ... ADD VALUE commits the new
-- value into the type definition; references in later migrations work
-- once this migration has applied).
--
-- Adds to transfer_reason enum:
--   burden_apply         — generic absorption-class apply (replaces
--                          per-class reasons for new burden types added
--                          via absorption_classes runtime data).
--   lot_charge_apply     — per-lot fixed charge absorption event.
--   osp_ship             — units leave plant for vendor (OSP custody).
--   osp_receive          — units return from vendor (OSP custody).
--   phantom_explode      — informational marker on phantom-flattened
--                          rm_issue events (same accounting effect as
--                          rm_issue_to_wo; the marker lets transfer logs
--                          distinguish phantom-derived consumption).
--
-- Adds to account_kind enum:
--   absorption_pool             — generic value-side absorption account
--                                 for runtime-configurable burden types
--                                 (avoids ALTER TYPE per new burden).
--   stock_consigned_at_vendor   — qty-side pool for OSP units physically
--                                 at the vendor. Partitioned by
--                                 (parent_sku, vendor_id, routing_op)
--                                 in subsequent slice work.
--
-- (Note: labor_expense already exists in account_kind from migration
-- 0002_enum_types — no need to add again. The Phase 2
-- absorption-variance close hook uses the existing value.)
--
-- Adds skus columns:
--   default_lot_size  — default lot size for amortizing per-lot BOM
--                       charges (setup, freight) into per-unit
--                       parent_std_cost. SAP/Oracle convention.
--                       Lot-size variance lands in wo_close_v if actual
--                       run differs from this.
--   is_phantom        — true for cost-rollup-only pseudo-SKUs with no
--                       physical inventory. BOM lines referencing a
--                       phantom child trigger flat expansion at WO_start
--                       (the phantom's own BOM lines flatten into the
--                       parent at the parent's applies_at_op).
--                       Implementation in B4 (_wo_explode_bom).
--   consumption_policy — material consumption timing for this SKU as a
--                       parent: 'forward' (today's behavior, drain at
--                       fire_at); 'backflush_at_op' (drain at op-out);
--                       'backflush_at_complete' (drain at wo_complete).
--                       Only 'forward' is dispatched today; backflush
--                       variants are gated by acct-oi4 (BACKFLUSH).

ALTER TYPE transfer_reason ADD VALUE 'burden_apply';
ALTER TYPE transfer_reason ADD VALUE 'lot_charge_apply';
ALTER TYPE transfer_reason ADD VALUE 'osp_ship';
ALTER TYPE transfer_reason ADD VALUE 'osp_receive';
ALTER TYPE transfer_reason ADD VALUE 'phantom_explode';

ALTER TYPE account_kind ADD VALUE 'absorption_pool';
ALTER TYPE account_kind ADD VALUE 'stock_consigned_at_vendor';
-- labor_expense already exists from 0002_enum_types — skipped here.

ALTER TABLE skus ADD COLUMN default_lot_size BIGINT NOT NULL DEFAULT 1
  CHECK (default_lot_size > 0);

ALTER TABLE skus ADD COLUMN is_phantom BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE skus ADD COLUMN consumption_policy TEXT NOT NULL DEFAULT 'forward'
  CHECK (consumption_policy IN ('forward', 'backflush_at_op', 'backflush_at_complete'));

COMMENT ON COLUMN skus.default_lot_size IS
  'Default lot size for amortizing per-lot BOM charges into per-unit '
  'parent_std_cost. acct-89i.';

COMMENT ON COLUMN skus.is_phantom IS
  'true for cost-rollup-only pseudo-SKUs with no physical inventory. '
  'Phantom-child BOM lines flatten into parent at WO_start. acct-89i.';

COMMENT ON COLUMN skus.consumption_policy IS
  'Material consumption timing as parent: forward (today), '
  'backflush_at_op, backflush_at_complete. Only forward is dispatched '
  'today; backflush variants gated by acct-oi4 (BACKFLUSH). acct-89i.';
