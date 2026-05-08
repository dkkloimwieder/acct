-- ============================================================
-- Phase D D1 — inventory_movements schema (acct-wb75.3.1)
--
-- KEYSTONE PHASE schema. inventory_movements is the foundational
-- subledger that all real-cost methods write to. Standard + WAC
-- + FIFO + lot + serial all converge here. After D6 ships, the
-- subledger is source-of-truth for cost-flow questions while
-- posting_lines carries GL aggregate.
--
-- This migration ships the SCHEMA only. Dispatcher writes land
-- in 0026 (standard cost) and 0027 (WAC family); backfill in
-- 0028; recon invariant in 0029; close-hook corrections in 0030.
--
-- Design choices (from plan-phase-d-inventory-movements memory
-- + research/posting-lines-convergence-plan.md §4.D):
--
--   - quantity NUMERIC(19,6) is SIGNED. Positive = receipt-like;
--     negative = issue-like. The sign convention is the subledger's
--     own; posting_lines.qty stays unsigned (sign lives in
--     debit/credit direction there).
--
--   - Two lookup tables ride here, not in 0002_types_and_enums:
--     event_types (16 rows) and cost_books (1 row, id=1 'primary').
--     Lookups are runtime-extensible (per the
--     not-a-PG-enum philosophy from acct-2thf and absorption_classes).
--
--   - PARTITION BY RANGE (movement_date) monthly. The partition key
--     must appear in the primary key per PG rules; PK is
--     (movement_id, movement_date). 24 monthly partitions baked
--     2026-01 through 2027-12 cover fixture dates + reasonable
--     dev/test horizon. Production rollover via pg_cron at month-25.
--
--   - Helper _create_inventory_movements_partition(p_month_start)
--     is idempotent (CREATE TABLE IF NOT EXISTS) so both bake-loop
--     and cron rollover can call it safely.
--
--   - posting_line_id is NOT NULL — every movement traces back to
--     a posting_line. INDEX (not UNIQUE) leaves the door open for
--     Phase E patterns (FIFO depletions point at the "first" layer
--     while detail lives in cost_layer_depletions; multiple movement
--     rows per posting_line may emerge there).
--
--   - cost_book_id SMALLINT NOT NULL DEFAULT 1 prelays multi-book.
--     Implementation deferred to acct-zf80; today every dispatcher
--     writes 1.
--
--   - ppv_amount NUMERIC(19,4) DEFAULT 0. Populated only at
--     standard-cost PO receipts (D2); zero everywhere else.
--
--   - actual_unit_cost NOT NULL. Standard-cost issues set it equal
--     to standard_unit_cost; WAC issues set it to running average.
-- ============================================================

CREATE TABLE inventory_movement_event_types (
  event_type SMALLINT PRIMARY KEY,
  name       VARCHAR(64) NOT NULL UNIQUE
);

INSERT INTO inventory_movement_event_types (event_type, name) VALUES
  ( 1, 'receipt'),
  ( 2, 'issue'),
  ( 3, 'transfer_out'),
  ( 4, 'transfer_in'),
  ( 5, 'adjustment_in'),
  ( 6, 'adjustment_out'),
  ( 7, 'scrap'),
  ( 8, 'wo_consume'),
  ( 9, 'wo_produce'),
  (10, 'op_move_out'),
  (11, 'op_move_in'),
  (12, 'return_in'),
  (13, 'return_out'),
  (14, 'standard_revaluation'),
  (15, 'periodic_revaluation'),
  (16, 'cost_adjustment');

CREATE TABLE cost_books (
  id   SMALLINT PRIMARY KEY,
  name VARCHAR(64) NOT NULL UNIQUE
);

INSERT INTO cost_books (id, name) VALUES
  (1, 'primary');

CREATE TABLE inventory_movements (
  movement_id        BIGSERIAL,
  product_id         UUID         NOT NULL REFERENCES skus(id),
  legal_entity_id    SMALLINT     NOT NULL REFERENCES legal_entities(id),
  cost_book_id       SMALLINT     NOT NULL DEFAULT 1 REFERENCES cost_books(id),
  location_id        UUID         NOT NULL REFERENCES locations(id),
  event_type         SMALLINT     NOT NULL REFERENCES inventory_movement_event_types(event_type),
  movement_date      DATE         NOT NULL,
  quantity           NUMERIC(19, 6) NOT NULL,
  standard_unit_cost NUMERIC(19, 4),
  actual_unit_cost   NUMERIC(19, 4) NOT NULL,
  cost_currency      CHAR(3)      NOT NULL,
  ppv_amount         NUMERIC(19, 4) NOT NULL DEFAULT 0,
  posting_line_id    BIGINT       NOT NULL REFERENCES posting_lines(id),
  source_doc_type    SMALLINT,
  source_doc_id      UUID,
  source_doc_line_id UUID,
  created_at         TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (movement_id, movement_date)
) PARTITION BY RANGE (movement_date);

CREATE INDEX inventory_movements_product_loc_date
  ON inventory_movements (product_id, location_id, movement_date);
CREATE INDEX inventory_movements_posting_line
  ON inventory_movements (posting_line_id);

-- ============================================================
-- Partition helper. Idempotent via CREATE TABLE IF NOT EXISTS,
-- so the bake-loop below and the pg_cron rollover job can both
-- call it without coordination.
-- ============================================================

CREATE OR REPLACE FUNCTION _create_inventory_movements_partition(p_month_start DATE)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
  v_partition_name TEXT;
  v_next_month     DATE;
BEGIN
  v_partition_name := 'inventory_movements_' || to_char(p_month_start, 'YYYY_MM');
  v_next_month     := (p_month_start + INTERVAL '1 month')::DATE;
  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS %I PARTITION OF inventory_movements
       FOR VALUES FROM (%L) TO (%L)',
    v_partition_name, p_month_start, v_next_month
  );
END;
$$;

-- Bake 24 monthly partitions: 2026-01 through 2027-12. Covers all
-- fixture periods (2026-03 through 2026-06) and gives a comfortable
-- horizon for dev/test work. Production rollover via pg_cron below.

DO $$
DECLARE
  v_d DATE := DATE '2026-01-01';
BEGIN
  WHILE v_d < DATE '2028-01-01' LOOP
    PERFORM _create_inventory_movements_partition(v_d);
    v_d := (v_d + INTERVAL '1 month')::DATE;
  END LOOP;
END $$;

-- ============================================================
-- pg_cron monthly rollover. Runs on the 25th at 00:00 UTC, creates
-- next-month + month-after partitions. Idempotent (CREATE IF NOT
-- EXISTS) so re-runs are safe; two-month horizon gives a buffer
-- against missed schedules. Tolerant install for non-cron databases
-- (test/CI ephemeral DBs; mirrors the 0011 reservation expiry job).
--
-- Operational concerns (long-horizon partition lifecycle, archival,
-- compression) tracked as acct-sbr2.
-- ============================================================

DO $$
BEGIN
  PERFORM cron.schedule(
    'inventory_movements_partition_rollover',
    '0 0 25 * *',
    $cron$
      SELECT _create_inventory_movements_partition(
        date_trunc('month', current_date + INTERVAL '1 month')::DATE
      );
      SELECT _create_inventory_movements_partition(
        date_trunc('month', current_date + INTERVAL '2 month')::DATE
      );
    $cron$
  );
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'pg_cron unavailable in % (%): %',
               current_database(), SQLSTATE, SQLERRM;
END $$;
