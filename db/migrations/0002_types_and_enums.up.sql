-- Consolidated enum definitions. Replaces archive_migrations 0002 +
-- all subsequent ALTER TYPE statements. All naming unifications
-- baked in:
--   - account_kind: 'variance_wac_period' → 'variance_wac_periodic'
--                   'variance_cost_adjust_retro' → 'variance_cost_adjust_retroactive'
--                   'stock_consigned_at_vendor' → 'stock_consigned'
--                   'supplier_pool' → 'vendor_pool' (rename per acct-397 / mig 0036)
--   - cost_method: 'wac' → 'wac_perpetual' (rename per mig 0023)
--   - posting_line_reason: renamed from 'transfer_reason'
--   - ledger_kind: NEW enum (was TEXT on accounts)
--   - yield_mode: NEW enum (was TEXT with CHECK on skus)
--   - consumption_policy: NEW enum (was TEXT with CHECK on skus)

-- ============================================================
-- ledger_kind: qty vs value pools (NEW; was TEXT on accounts)
-- ============================================================

CREATE TYPE ledger_kind AS ENUM ('qty', 'value');

-- ============================================================
-- balance_direction: which side an account allows
-- ============================================================

CREATE TYPE balance_direction AS ENUM ('debit', 'credit', 'unrestricted');

-- ============================================================
-- account_kind: every account class in the system
-- ============================================================

CREATE TYPE account_kind AS ENUM (
  -- Inventory quantity (stock_*)
  'stock_available',
  'stock_reserved',
  'stock_quarantine',
  'stock_scrap',
  'stock_in_transit',
  'stock_consumed',
  'stock_wip',
  'stock_consigned',                       -- renamed from stock_consigned_at_vendor

  -- Counterparty pools (vendor_pool, customer_pool — qty side)
  'vendor_pool',                           -- renamed from supplier_pool
  'customer_pool',

  -- Inventory value
  'inv_value_raw',
  'inv_value_wip',
  'inv_value_fg',

  -- P&L / settlement
  'cogs',
  'ap',
  'ap_unsettled',
  'ar',
  'ar_unsettled',
  'cash',
  'revenue',
  'sales_tax_payable',

  -- Applied / expense
  'labor_applied',
  'oh_applied',
  'labor_expense',
  'inv_adj_expense',
  'disposal_expense',
  'absorption_pool',

  -- Variance accounts
  'variance_ppv',
  'variance_muv',
  'variance_lv',
  'variance_ohv',
  'variance_scrap',
  'variance_wo_close',
  'variance_price_settlement',
  'variance_cost_adjustment',
  'variance_wac_periodic',                 -- renamed from variance_wac_period
  'variance_wac_retroactive',
  'variance_cost_adjust_retroactive',      -- renamed from variance_cost_adjust_retro
  'variance_std_cost_roll',
  'variance_material_mixed',
  'variance_wip_revaluation',
  'variance_ppv_prior_period_adj',
  'variance_match_tolerance',
  'variance_yield_byproduct',

  -- FX
  'fx_revaluation',
  'fx_clearing',
  'realized_fx_gain',
  'realized_fx_loss',

  -- Disposal liability (vendor-partitioned, GRNI-style)
  'accrued_disposal_liability',

  -- System
  'creation_void'
);

-- ============================================================
-- posting_line_reason: every reason a posting line can carry
-- (renamed from transfer_reason)
-- ============================================================

CREATE TYPE posting_line_reason AS ENUM (
  -- Inbound
  'po_receipt',
  'po_receipt_provisional',
  'po_return_to_vendor',

  -- Outbound
  'so_ship',
  'customer_return',

  -- Internal moves
  'rm_issue_to_wo',
  'to_release',
  'to_receipt',
  'bin_move',

  -- Conversion (qty-leg + value-leg variants)
  'wo_start',
  'op_move',
  'op_move_v',
  'wo_complete',
  'wo_complete_v',
  'rework',
  'scrap',
  'scrap_v',
  'wo_close_v',

  -- Absorption / burdens
  'labor_apply',
  'oh_apply',
  'burden_apply',
  'lot_charge_apply',

  -- BOM2 / phantom / OSP
  'phantom_explode',
  'osp_ship',
  'osp_receive',

  -- Status moves
  'quarantine',
  'release_from_quarantine',
  'damage',

  -- AR / AP
  'ar_invoice',
  'ar_payment',
  'ap_bill',
  'ap_payment',

  -- Variance reasons (legacy abbreviations preserved)
  'ppv',
  'muv',
  'lv',
  'ohv',
  'price_settlement',

  -- Adjustments / restatements
  'cycle_count_adj',
  'inventory_adjustment',
  'cost_adjustment',
  'cost_restate',
  'standard_cost_roll',
  'reversal',

  -- FX
  'fx_leg',
  'fx_spread',

  -- Provisional commodity settlement
  'po_settlement',
  'price_trueup_inventory',
  'price_trueup_cogs',
  'price_trueup_wip'
);

-- ============================================================
-- cost_method: per-SKU costing strategy (renamed wac → wac_perpetual)
-- ============================================================

CREATE TYPE cost_method AS ENUM (
  'standard',
  'wac_perpetual',                         -- renamed from 'wac'
  'wac_periodic',
  'wac_retroactive',
  'fifo',
  'lot'
);

-- ============================================================
-- reservation_status: lifecycle states for inventory_reservations
-- ============================================================

CREATE TYPE reservation_status AS ENUM (
  'active',
  'allocated',
  'shipped',
  'cancelled',
  'expired'
);

-- ============================================================
-- return_disposition: routing for customer_returns lines
-- ============================================================

CREATE TYPE return_disposition AS ENUM (
  'restock',
  'scrap',
  'repair'
);

-- ============================================================
-- yield_mode: BOM rollup behavior (was TEXT with CHECK on skus)
-- ============================================================

CREATE TYPE yield_mode AS ENUM (
  'plan_only',
  'absorbed'
);

-- ============================================================
-- consumption_policy: WO material consumption timing (was TEXT with CHECK)
-- ============================================================

CREATE TYPE consumption_policy AS ENUM (
  'forward',
  'backflush_at_op',
  'backflush_at_complete'
);
