-- ============================================================
-- Phase E2 E2.2 prelude — extend cost_method enum with 'lot_fifo'.
--
-- PG forbids using a newly-added enum value in the same transaction
-- as the ALTER TYPE that added it. The strategy registry seed
-- (INSERT INTO cost_method_strategies VALUES ('lot_fifo', ...))
-- in mig 0046 needs the value to exist; this migration is the
-- thin "enum lift" that runs in its own transaction so 0046 can
-- proceed.
--
-- 'lot_fifo' is the cost-method enum value for lot-tracked SKUs;
-- depletion walks named lots ORDER BY receipt_date ASC by default
-- (FEFO + customer-allocation strategies are E2.7 follow-up). The
-- accompanying lot subledger lives in inventory_lots /
-- inventory_lot_events from E2.1.
-- ============================================================

ALTER TYPE cost_method ADD VALUE IF NOT EXISTS 'lot_fifo';
