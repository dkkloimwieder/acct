CREATE TYPE account_kind AS ENUM (
  -- Inventory quantity
  'stock_available', 'stock_reserved', 'stock_quarantine', 'stock_scrap',
  'stock_in_transit', 'stock_consumed', 'stock_wip',
  -- Counterparty (qty side, optional)
  'supplier_pool', 'customer_pool',
  -- Value
  'inv_value_raw', 'inv_value_wip', 'inv_value_fg', 'cogs',
  'ap', 'ap_unsettled', 'ar', 'cash',
  'revenue', 'sales_tax_payable',
  'labor_applied', 'oh_applied', 'labor_expense',
  'variance_ppv', 'variance_muv', 'variance_lv', 'variance_ohv',
  'variance_scrap', 'variance_wo_close', 'variance_price_settlement',
  'fx_revaluation', 'inv_adj_expense',
  -- System
  'creation_void'
);

CREATE TYPE balance_direction AS ENUM ('debit', 'credit', 'unrestricted');

CREATE TYPE transfer_reason AS ENUM (
  'po_receipt', 'po_receipt_provisional', 'po_return_to_vendor', 'customer_return',
  'so_ship', 'rm_issue_to_wo',
  'to_release', 'to_receipt', 'bin_move',
  'wo_start', 'op_move', 'wo_complete', 'rework',
  'labor_apply', 'oh_apply',
  'quarantine', 'release_from_quarantine', 'scrap', 'damage',
  'ar_invoice', 'ar_payment', 'ap_bill', 'ap_payment',
  'ppv', 'muv', 'lv', 'ohv', 'scrap_v', 'wo_close_v', 'price_settlement',
  'cycle_count_adj', 'cost_restate', 'reversal',
  'fx_leg', 'fx_spread',
  'po_settlement',
  'price_trueup_inventory', 'price_trueup_cogs', 'price_trueup_wip'
);

CREATE TYPE reservation_status AS ENUM (
  'active', 'allocated', 'cancelled', 'expired'
);

CREATE TYPE cost_method AS ENUM (
  'standard', 'wac', 'fifo', 'lot'
);
