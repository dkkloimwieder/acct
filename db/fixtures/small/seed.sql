-- Phase 0 small fixture: minimal-but-realistic seed for cargo test.
-- Idempotent w.r.t. order; safe to load into a freshly-migrated DB.
-- Currency policy: Phase 0 is USD + EUR only.

-- ============================================================
-- SKUs (10): 8 standard, 1 wac, 1 fifo
-- ============================================================
INSERT INTO skus (code, uom, standard_cost, cost_method) VALUES
  ('SKU-A',  'EA',  100, 'standard'),
  ('SKU-B',  'EA',  200, 'standard'),
  ('SKU-C',  'EA',   50, 'standard'),
  ('SKU-D',  'EA',  150, 'standard'),
  ('SKU-E',  'EA',   75, 'standard'),
  ('SKU-F',  'EA',  300, 'standard'),
  ('SKU-G',  'EA',   25, 'standard'),
  ('SKU-H',  'EA',  500, 'standard'),
  ('SKU-WAC','EA',  100, 'wac_perpetual'),
  ('SKU-FIF','EA',  100, 'fifo');

-- ============================================================
-- Locations (3)
-- ============================================================
INSERT INTO locations (code, name) VALUES
  ('MAIN',  'Main warehouse'),
  ('ALT',   'Alternate warehouse'),
  ('OUT',   'Outbound staging');

-- ============================================================
-- Periods (4): 1 closed, 1 open, 2 future
-- ============================================================
INSERT INTO periods (code, opens_at, closes_at, closed_at, closed_by) VALUES
  ('2026-03', '2026-03-01', '2026-03-31', '2026-04-01 00:00:00+00', NULL);

INSERT INTO periods (code, opens_at, closes_at) VALUES
  ('2026-04', '2026-04-01', '2026-04-30'),
  ('2026-05', '2026-05-01', '2026-05-31'),
  ('2026-06', '2026-06-01', '2026-06-30');

-- ============================================================
-- FX rates (1): USD <-> EUR
-- ============================================================
INSERT INTO fx_rates (from_currency, to_currency, rate, effective_at, source) VALUES
  ('USD', 'EUR', 0.9200000000, '2026-04-01 00:00:00+00', 'fixture'),
  ('EUR', 'USD', 1.0869565000, '2026-04-01 00:00:00+00', 'fixture');

-- ============================================================
-- Accounts (~20): spanning the kinds T2/T3 invariant tests need
-- ============================================================

-- System
INSERT INTO accounts (kind, ledger_kind, currency, normal_side) VALUES
  ('creation_void', 'qty',   NULL,  'unrestricted'),
  ('creation_void', 'value', 'USD', 'unrestricted');

-- Cash, AR, AP, revenue (USD)
INSERT INTO accounts (kind, ledger_kind, currency, normal_side) VALUES
  ('cash',    'value', 'USD', 'debit'),
  ('ar',      'value', 'USD', 'debit'),
  ('ap',      'value', 'USD', 'credit'),
  ('revenue', 'value', 'USD', 'credit');

-- Cash + revenue (EUR) — for currency-mismatch tests
INSERT INTO accounts (kind, ledger_kind, currency, normal_side) VALUES
  ('cash',    'value', 'EUR', 'debit'),
  ('revenue', 'value', 'EUR', 'credit');

-- COGS + WO close variance (USD)
INSERT INTO accounts (kind, ledger_kind, currency, normal_side) VALUES
  ('cogs',                 'value', 'USD', 'debit'),
  ('variance_wo_close',    'value', 'USD', 'unrestricted');

-- Inventory adjustment account (USD + EUR). Bidirectional P&L line:
-- credit balance = net adjustment income (we found inventory),
-- debit  balance = net adjustment expense (we lost inventory).
-- normal_side='unrestricted' so the CHECK constraint allows either
-- direction; period-close reporting splits gain vs loss by walking
-- the underlying transfers.
INSERT INTO accounts (kind, ledger_kind, currency, normal_side) VALUES
  ('inv_adj_expense', 'value', 'USD', 'unrestricted'),
  ('inv_adj_expense', 'value', 'EUR', 'unrestricted');

-- Cost adjustment variance (USD + EUR). Bidirectional P&L line for
-- explicit per-unit cost revaluations of WAC pools (acct-14m). Distinct
-- from inv_adj_expense — qty-driven adjustments and value-only
-- revaluations land on different income-statement lines.
INSERT INTO accounts (kind, ledger_kind, currency, normal_side) VALUES
  ('variance_cost_adjustment', 'value', 'USD', 'unrestricted'),
  ('variance_cost_adjustment', 'value', 'EUR', 'unrestricted');

-- SKU-A value accounts (USD).
-- After acct-nfr (migration 0020), value accounts partition by the
-- same dimensions as their qty-side counterparts:
--   inv_value_raw / inv_value_fg -> (sku, location, currency)
--   inv_value_wip                -> (sku, routing_op, currency)
INSERT INTO accounts (kind, ledger_kind, currency, normal_side, sku_id, location_id)
  SELECT 'inv_value_raw', 'value', 'USD', 'debit', s.id, l.id
    FROM skus s, locations l WHERE s.code='SKU-A' AND l.code='MAIN';
INSERT INTO accounts (kind, ledger_kind, currency, normal_side, sku_id, routing_op)
  SELECT 'inv_value_wip', 'value', 'USD', 'debit', s.id, 10 FROM skus s WHERE s.code='SKU-A';
INSERT INTO accounts (kind, ledger_kind, currency, normal_side, sku_id, routing_op)
  SELECT 'inv_value_wip', 'value', 'USD', 'debit', s.id, 20 FROM skus s WHERE s.code='SKU-A';
INSERT INTO accounts (kind, ledger_kind, currency, normal_side, sku_id, location_id)
  SELECT 'inv_value_fg',  'value', 'USD', 'debit', s.id, l.id
    FROM skus s, locations l WHERE s.code='SKU-A' AND l.code='MAIN';

-- SKU-A qty accounts: stock_available x 2 locations, stock_wip x 2 routing_ops, stock_consumed
INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side)
  SELECT 'stock_available', 'qty', s.id, l.id, 'debit'
    FROM skus s, locations l WHERE s.code='SKU-A' AND l.code='MAIN';
INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side)
  SELECT 'stock_available', 'qty', s.id, l.id, 'debit'
    FROM skus s, locations l WHERE s.code='SKU-A' AND l.code='OUT';
INSERT INTO accounts (kind, ledger_kind, sku_id, routing_op, normal_side)
  SELECT 'stock_wip', 'qty', s.id, 10, 'debit' FROM skus s WHERE s.code='SKU-A';
INSERT INTO accounts (kind, ledger_kind, sku_id, routing_op, normal_side)
  SELECT 'stock_wip', 'qty', s.id, 20, 'debit' FROM skus s WHERE s.code='SKU-A';
INSERT INTO accounts (kind, ledger_kind, sku_id, normal_side)
  SELECT 'stock_consumed', 'qty', s.id, 'debit' FROM skus s WHERE s.code='SKU-A';

-- SKU-WAC accounts (used by acct-uxu WAC tests + acct-93b.15 P0006 dispatch).
-- Qty side: stock_available (MAIN), stock_wip (op10, op20).
INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side)
  SELECT 'stock_available', 'qty', s.id, l.id, 'debit'
    FROM skus s, locations l WHERE s.code='SKU-WAC' AND l.code='MAIN';
INSERT INTO accounts (kind, ledger_kind, sku_id, routing_op, normal_side)
  SELECT 'stock_wip', 'qty', s.id, 10, 'debit' FROM skus s WHERE s.code='SKU-WAC';
INSERT INTO accounts (kind, ledger_kind, sku_id, routing_op, normal_side)
  SELECT 'stock_wip', 'qty', s.id, 20, 'debit' FROM skus s WHERE s.code='SKU-WAC';
-- Value side: inv_value_raw (MAIN), inv_value_wip (op10, op20), inv_value_fg (MAIN).
-- Same partition shape as SKU-A so WAC tests can use either SKU.
INSERT INTO accounts (kind, ledger_kind, currency, normal_side, sku_id, location_id)
  SELECT 'inv_value_raw', 'value', 'USD', 'debit', s.id, l.id
    FROM skus s, locations l WHERE s.code='SKU-WAC' AND l.code='MAIN';
INSERT INTO accounts (kind, ledger_kind, currency, normal_side, sku_id, routing_op)
  SELECT 'inv_value_wip', 'value', 'USD', 'debit', s.id, 10 FROM skus s WHERE s.code='SKU-WAC';
INSERT INTO accounts (kind, ledger_kind, currency, normal_side, sku_id, routing_op)
  SELECT 'inv_value_wip', 'value', 'USD', 'debit', s.id, 20 FROM skus s WHERE s.code='SKU-WAC';
INSERT INTO accounts (kind, ledger_kind, currency, normal_side, sku_id, location_id)
  SELECT 'inv_value_fg', 'value', 'USD', 'debit', s.id, l.id
    FROM skus s, locations l WHERE s.code='SKU-WAC' AND l.code='MAIN';

-- SKU-FIF accounts (used to verify the qty-side gate still raises P0006
-- for non-implemented cost methods after acct-uxu relaxes WAC).
-- Minimal: just stock_wip op10/op20 + stock_available MAIN.
INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side)
  SELECT 'stock_available', 'qty', s.id, l.id, 'debit'
    FROM skus s, locations l WHERE s.code='SKU-FIF' AND l.code='MAIN';
INSERT INTO accounts (kind, ledger_kind, sku_id, routing_op, normal_side)
  SELECT 'stock_wip', 'qty', s.id, 10, 'debit' FROM skus s WHERE s.code='SKU-FIF';
INSERT INTO accounts (kind, ledger_kind, sku_id, routing_op, normal_side)
  SELECT 'stock_wip', 'qty', s.id, 20, 'debit' FROM skus s WHERE s.code='SKU-FIF';
