-- Phase 0 small fixture: minimal-but-realistic seed for cargo test.
-- Idempotent w.r.t. order; safe to load into a freshly-migrated DB.
-- Currency policy: Phase 0 is USD + EUR only.

-- ============================================================
-- SKUs (10): 8 standard, 1 wac, 1 fifo
-- After acct-x4t (migration 0027), standard cost is no longer a
-- column on skus. Standard SKUs need a row in standard_costs (below)
-- to be usable; non-standard SKUs (WAC, fifo) don't.
-- ============================================================
INSERT INTO skus (code, uom, cost_method) VALUES
  ('SKU-A',  'EA', 'standard'),
  ('SKU-B',  'EA', 'standard'),
  ('SKU-C',  'EA', 'standard'),
  ('SKU-D',  'EA', 'standard'),
  ('SKU-E',  'EA', 'standard'),
  ('SKU-F',  'EA', 'standard'),
  ('SKU-G',  'EA', 'standard'),
  ('SKU-H',  'EA', 'standard'),
  ('SKU-WAC','EA', 'wac_perpetual'),
  ('SKU-FIF','EA', 'fifo');

-- Standard costs for the 8 standard SKUs, effective from epoch so
-- every test's business_date resolves. Posted by the system-bootstrap
-- UUID — RBAC is Q6, still open.
INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
SELECT s.id, v.cost, '1970-01-01'::DATE,
       '00000000-0000-0000-0000-000000000000'::UUID, gen_random_uuid()
  FROM (VALUES
          ('SKU-A', 100), ('SKU-B', 200), ('SKU-C', 50),
          ('SKU-D', 150), ('SKU-E',  75), ('SKU-F', 300),
          ('SKU-G',  25), ('SKU-H', 500)
       ) AS v(code, cost)
  JOIN skus s ON s.code = v.code;

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

-- Slice A (acct-7mg) inflow accounts. Un-partitioned (no
-- counterparty_id) — shared by conformance harness cases that don't
-- model per-vendor liability. Per-vendor partitioned versions are
-- created inline by Slice A matrix tests via the vendor scaffold.
INSERT INTO accounts (kind, ledger_kind, currency, normal_side) VALUES
  ('ap_unsettled', 'value', 'USD', 'credit'),  -- GRNI accrual
  ('vendor_pool',  'qty',   NULL,  'credit'),  -- per-vendor qty pool
  ('variance_ppv', 'value', 'USD', 'unrestricted');

-- Cash + revenue (EUR) — for currency-mismatch tests
INSERT INTO accounts (kind, ledger_kind, currency, normal_side) VALUES
  ('cash',    'value', 'EUR', 'debit'),
  ('revenue', 'value', 'EUR', 'credit');

-- COGS + WO close variance (USD)
INSERT INTO accounts (kind, ledger_kind, currency, normal_side) VALUES
  ('cogs',                 'value', 'USD', 'debit'),
  ('variance_wo_close',    'value', 'USD', 'unrestricted');

-- Slice B (acct-wl1) absorption + scrap-variance accounts (USD).
-- Currency-only kinds; not partitioned by SKU/location. Per-SKU
-- stock_scrap is scaffolded per-test (sku-specific qty kind).
INSERT INTO accounts (kind, ledger_kind, currency, normal_side) VALUES
  ('labor_applied',  'value', 'USD', 'credit'),
  ('oh_applied',     'value', 'USD', 'credit'),
  ('variance_scrap', 'value', 'USD', 'unrestricted');

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

-- Standard cost roll variance (USD + EUR; acct-8rv / acct-hlr).
-- Bidirectional P&L for revaluation transfers when standard cost
-- changes via post_standard_cost_roll. Distinct from
-- variance_cost_adjustment (WAC pool revaluation) and inv_adj_expense
-- (qty-driven adjustments).
INSERT INTO accounts (kind, ledger_kind, currency, normal_side) VALUES
  ('variance_std_cost_roll', 'value', 'USD', 'unrestricted'),
  ('variance_std_cost_roll', 'value', 'EUR', 'unrestricted');

-- Period-close variance lines (USD + EUR; acct-4mt / acct-s6n).
-- All bidirectional — write-ups credit, write-downs debit. Three
-- separate kinds (not one shared 'variance_close') so the income
-- statement reports the three close-time correction stories
-- separately:
--   variance_wac_period          — wac_periodic close adjustment
--                                   (Epic B, acct-qfj)
--   variance_wac_retroactive     — wac_retroactive late-data
--                                   correction (Epic C, acct-9tw)
--   variance_cost_adjust_retro   — retroactive cost_adjustment
--                                   correction (Epic E, acct-og1)
INSERT INTO accounts (kind, ledger_kind, currency, normal_side) VALUES
  ('variance_wac_period',         'value', 'USD', 'unrestricted'),
  ('variance_wac_period',         'value', 'EUR', 'unrestricted'),
  ('variance_wac_retroactive',    'value', 'USD', 'unrestricted'),
  ('variance_wac_retroactive',    'value', 'EUR', 'unrestricted'),
  ('variance_cost_adjust_retro',  'value', 'USD', 'unrestricted'),
  ('variance_cost_adjust_retro',  'value', 'EUR', 'unrestricted');

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
INSERT INTO accounts (kind, ledger_kind, sku_id, normal_side)
  SELECT 'stock_scrap', 'qty', s.id, 'debit' FROM skus s WHERE s.code='SKU-A';

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
