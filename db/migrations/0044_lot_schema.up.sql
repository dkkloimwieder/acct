-- ============================================================
-- Phase E2 E2.1 — inventory_lots + inventory_lot_events schema
-- (acct-ua3r, sub-issue of acct-uze).
--
-- Lot-based costing foundation. Two append-only tables ride on
-- top of the inventory_movements subledger from Phase D and the
-- cost_layers / cost_layer_depletions from Phase E1 — the lot
-- subledger is parallel to (not derived from) FIFO layers; lot
-- identity is meaningful and lot SKUs use cost_method='lot_fifo'
-- (registered in E2.2). The four cost_method values for inventory:
-- standard / wac_* / fifo (anonymous-batch) / lot_fifo (named-batch).
--
-- Two new tables, mirrors cost_layers / cost_layer_depletions
-- partition pattern from mig 0031:
--
--   inventory_lots
--     One row per receipt of a lot-tracked SKU. Immutable after
--     creation. Composite PK (lot_id, receipt_date) for partition
--     compatibility. Receipt event is NOT separately recorded in
--     inventory_lot_events — original_quantity IS the receipt
--     (matches cost_layers convention).
--
--   inventory_lot_events
--     Tracks all post-receipt events: issues, transfers, holds,
--     releases, adjustments, expirations. quantity_change is
--     SIGNED (negative for issue/writeoff/adjust_out, positive
--     for adjust_in, zero for transfer/hold/release/status_change).
--     Append-only. Composite FK to inventory_lots on
--     (lot_id, receipt_date).
--
-- Schema extensions to existing tables:
--
--   skus.tracked_by inventory_tracking enum NOT NULL DEFAULT 'none'.
--   Controls which dimensions partition on the SKU. 'none' is the
--   default; lot SKUs require 'lot' or 'lot_and_serial'. The
--   'serial' / 'lot_and_serial' values are pre-allocated for E3
--   so adding serial doesn't need an ALTER TYPE.
--
--   accounts.lot_id BIGINT NULL. Lot-partitioning the value pool
--   keeps R1 (per-class qty divisor) class-isolated when lot SKUs
--   coexist with non-lot SKUs at the same (sku, location). The
--   partial UNIQUE indexes are extended to include
--   COALESCE(lot_id, 0) so existing lot=NULL rows continue to
--   collide with each other (existing behavior). Scope: only
--   stock_available + inv_value_raw + inv_value_fg get
--   lot-partitioned at MVP. stock_wip + inv_value_wip do NOT —
--   WIP-lot is out of scope (mirrors E1 W4's FG-FIFO MVP scope
--   from mig 0037).
--
--   inventory_reservations.lot_id BIGINT NULL + lot_specific
--   BOOLEAN NOT NULL DEFAULT FALSE. Allows reserving a specific
--   lot (lot_specific=TRUE pins; FALSE allows allocator to pick
--   any lot at allocate-time). reserve_inventory function
--   extension is E2.5; this just lays the schema.
--
-- NOT in this migration (deliberately scoped to E2.1):
--   - cost_method 'lot_fifo' enum value (E2.2)
--   - _compute_amount_lot_fifo_outbound (E2.2)
--   - _post_posting_lines_apply_event lot-event writeback (E2.2)
--   - inventory_movements.lot_id stamping column (E2.2)
--   - posting_line_inventory.lot_id stamping column (E2.2)
--   - reserve_inventory parameter extensions (E2.5)
--   - Wrapper functions accepting lot_id parameters (E2.3)
--   - Recon invariant (subledger lot residual ↔ accounts) (E2.4)
--   - Property test (E2.6)
--   - lot_genealogy table (filed as E2.7 follow-up)
-- ============================================================

-- ---------- 1. inventory_tracking enum + skus.tracked_by ---------

CREATE TYPE inventory_tracking AS ENUM (
  'none', 'lot', 'serial', 'lot_and_serial'
);

ALTER TABLE skus
  ADD COLUMN tracked_by inventory_tracking NOT NULL DEFAULT 'none';

COMMENT ON COLUMN skus.tracked_by IS
  'Tracking dimension toggle (D365 model). ''none'' = anonymous '
  '(WAC/FIFO/standard); ''lot'' requires lot_id at receipt/issue '
  '(cost_method=lot_fifo); ''serial'' requires unit_id (E3, '
  'cost_method=specific); ''lot_and_serial'' requires both.';

-- ---------- 2. inventory_lots ----------

CREATE TABLE inventory_lots (
  lot_id              BIGSERIAL,
  product_id          UUID         NOT NULL REFERENCES skus(id),
  legal_entity_id     SMALLINT     NOT NULL DEFAULT 1 REFERENCES legal_entities(id),
  cost_book_id        SMALLINT     NOT NULL DEFAULT 1 REFERENCES cost_books(id),
  location_id         UUID         NOT NULL REFERENCES locations(id),
  lot_code            VARCHAR(64)  NOT NULL,
  receipt_posting_line_id BIGINT   NOT NULL REFERENCES posting_lines(id),
  receipt_date        DATE         NOT NULL,
  original_quantity   NUMERIC(19, 6) NOT NULL CHECK (original_quantity > 0),
  unit_cost           NUMERIC(19, 4) NOT NULL CHECK (unit_cost >= 0),
  cost_currency       CHAR(3)      NOT NULL,
  manufacture_date    DATE,
  expiration_date     DATE,
  quality_status      VARCHAR(16)  NOT NULL DEFAULT 'approved'
                        CHECK (quality_status IN
                               ('pending','approved','rejected','on_hold','expired')),
  supplier_lot_number VARCHAR(64),
  attributes          JSONB,
  created_at          TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (lot_id, receipt_date)
) PARTITION BY RANGE (receipt_date);

-- Business-key uniqueness: same lot_code can't repeat for the same
-- (product, entity) on the same receipt_date. Partition-key inclusion
-- is required by PG; receipt_date in the index makes this a
-- partition-local UK rather than a global business-key UK. For MVP
-- a same-day collision is the realistic failure mode (vendor sends
-- duplicate ASN); cross-day collisions are pathological and would
-- show up as multiple lots with same code (recoverable application-side).
CREATE UNIQUE INDEX inventory_lots_business_key
  ON inventory_lots (product_id, legal_entity_id, lot_code, receipt_date);

CREATE INDEX inventory_lots_fifo_walk
  ON inventory_lots (product_id, location_id, cost_book_id, receipt_date, lot_id);

CREATE INDEX inventory_lots_fefo_walk
  ON inventory_lots (product_id, location_id, expiration_date, lot_id)
  WHERE expiration_date IS NOT NULL;

CREATE INDEX inventory_lots_posting_line
  ON inventory_lots (receipt_posting_line_id);

-- ---------- 3. inventory_lot_events ----------

-- event_type SMALLINT codes (matches inventory_movement_event_types
-- spirit but distinct numbering — lot events are a separate domain
-- from movement events):
--   1 = issue                  (qty_change negative)
--   2 = transfer               (qty_change = 0; location_id_from/to set)
--   3 = hold                   (qty_change = 0; quality_status -> 'on_hold')
--   4 = release                (qty_change = 0; quality_status -> 'approved' typically)
--   5 = expiration_writeoff    (qty_change negative)
--   6 = status_change          (qty_change = 0; new_quality_status set)
--   7 = adjust_in              (qty_change positive)
--   8 = adjust_out             (qty_change negative)
--
-- Receipt is NOT recorded as an event — inventory_lots row IS the
-- receipt (matches cost_layers convention; residual computation
-- uses original_quantity + SUM(quantity_change)).

CREATE TABLE inventory_lot_events (
  event_id            BIGSERIAL,
  lot_id              BIGINT       NOT NULL,
  lot_receipt_date    DATE         NOT NULL,
  event_date          DATE         NOT NULL,
  event_type          SMALLINT     NOT NULL CHECK (event_type BETWEEN 1 AND 8),
  quantity_change     NUMERIC(19, 6) NOT NULL,
  posting_line_id     BIGINT       REFERENCES posting_lines(id),
  movement_id         BIGINT,                    -- soft pointer to inventory_movements (composite PK there)
  movement_date       DATE,                      -- partner key for movement_id
  location_id_from    UUID         REFERENCES locations(id),
  location_id_to      UUID         REFERENCES locations(id),
  new_quality_status  VARCHAR(16)
                        CHECK (new_quality_status IS NULL OR
                               new_quality_status IN
                               ('pending','approved','rejected','on_hold','expired')),
  notes               TEXT,
  created_at          TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (event_id, event_date),
  FOREIGN KEY (lot_id, lot_receipt_date)
    REFERENCES inventory_lots (lot_id, receipt_date),
  -- qty-affecting event_types must have non-zero quantity_change.
  -- transfer/hold/release/status_change must have zero.
  CHECK (
    (event_type IN (1, 5, 8)        AND quantity_change <  0) OR
    (event_type =  7                AND quantity_change >  0) OR
    (event_type IN (2, 3, 4, 6)     AND quantity_change =  0)
  ),
  -- transfer requires both from/to locations.
  CHECK (
    event_type <> 2 OR
    (location_id_from IS NOT NULL AND location_id_to IS NOT NULL
     AND location_id_from <> location_id_to)
  ),
  -- status_change requires new_quality_status.
  CHECK (event_type <> 6 OR new_quality_status IS NOT NULL),
  -- movement_id and movement_date are paired.
  CHECK (
    (movement_id IS NULL AND movement_date IS NULL) OR
    (movement_id IS NOT NULL AND movement_date IS NOT NULL)
  )
) PARTITION BY RANGE (event_date);

CREATE INDEX inventory_lot_events_lot
  ON inventory_lot_events (lot_id, lot_receipt_date, event_date);
CREATE INDEX inventory_lot_events_posting_line
  ON inventory_lot_events (posting_line_id) WHERE posting_line_id IS NOT NULL;
CREATE INDEX inventory_lot_events_movement
  ON inventory_lot_events (movement_id, movement_date) WHERE movement_id IS NOT NULL;

-- ---------- 4. accounts.lot_id partition ----------

ALTER TABLE accounts ADD COLUMN lot_id BIGINT;

-- Lot can only attach to inventory account kinds that we lot-partition
-- at MVP: stock_available + inv_value_raw + inv_value_fg. WIP-side
-- (stock_wip / inv_value_wip) does not get lot-partitioned (out of
-- MVP scope; mirrors E1 W4's FG-FIFO scope rule).
ALTER TABLE accounts ADD CONSTRAINT accounts_lot_id_kind CHECK (
  lot_id IS NULL OR
  kind IN ('stock_available', 'inv_value_raw', 'inv_value_fg')
);

-- Extend per-class partition UKs to include COALESCE(lot_id, 0).
-- Existing lot_id=NULL rows continue to share the same UK slot
-- as each other — backward compatible.

DROP INDEX accounts_stock_avail_uk;
CREATE UNIQUE INDEX accounts_stock_avail_uk
  ON accounts (sku_id, location_id, COALESCE(lot_id, 0))
  WHERE kind = 'stock_available' AND NOT is_closed;

DROP INDEX accounts_value_loc_uk;
CREATE UNIQUE INDEX accounts_value_loc_uk
  ON accounts (kind, sku_id, location_id, currency, COALESCE(lot_id, 0))
  WHERE ledger_kind = 'value'
    AND kind IN ('inv_value_raw', 'inv_value_fg')
    AND sku_id IS NOT NULL
    AND NOT is_closed;

-- (accounts_wip_uk and accounts_value_wip_uk left untouched —
--  WIP is not lot-partitioned at MVP.)

CREATE INDEX accounts_lot_id
  ON accounts (lot_id) WHERE lot_id IS NOT NULL;

-- ---------- 5. inventory_reservations.lot_id + lot_specific ----------

ALTER TABLE inventory_reservations
  ADD COLUMN lot_id BIGINT,
  ADD COLUMN lot_specific BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE inventory_reservations
  ADD CONSTRAINT inventory_reservations_lot_specific_requires_lot
  CHECK (NOT lot_specific OR lot_id IS NOT NULL);

CREATE INDEX inventory_reservations_lot
  ON inventory_reservations (lot_id) WHERE lot_id IS NOT NULL;

-- ---------- 6. Append-only triggers ----------

CREATE OR REPLACE FUNCTION block_inventory_lot_modifications()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION '% are append-only; UPDATE/DELETE rejected', TG_TABLE_NAME
    USING ERRCODE = 'P9999';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_inventory_lots_append_only
  BEFORE UPDATE OR DELETE ON inventory_lots
  FOR EACH ROW EXECUTE FUNCTION block_inventory_lot_modifications();

CREATE TRIGGER trg_inventory_lot_events_append_only
  BEFORE UPDATE OR DELETE ON inventory_lot_events
  FOR EACH ROW EXECUTE FUNCTION block_inventory_lot_modifications();

-- ---------- 7. Residual helper ----------

CREATE OR REPLACE FUNCTION _inventory_lot_remaining_qty(
  p_lot_id BIGINT, p_receipt_date DATE
) RETURNS NUMERIC
LANGUAGE plpgsql STABLE AS $$
DECLARE
  v_orig    NUMERIC;
  v_change  NUMERIC;
BEGIN
  SELECT original_quantity INTO v_orig
    FROM inventory_lots
   WHERE lot_id = p_lot_id AND receipt_date = p_receipt_date;
  IF NOT FOUND THEN RETURN NULL; END IF;
  -- Sum all events; transfer/hold/release/status_change have qty_change=0
  -- so they contribute nothing. Issues/writeoffs are negative; adjust_in
  -- positive.
  SELECT COALESCE(SUM(quantity_change), 0) INTO v_change
    FROM inventory_lot_events
   WHERE lot_id = p_lot_id
     AND lot_receipt_date = p_receipt_date;
  RETURN v_orig + v_change;
END;
$$;

-- ---------- 8. Partition helpers + 24-month bake ----------

CREATE OR REPLACE FUNCTION _create_inventory_lots_partition(p_month_start DATE)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
  v_partition_name TEXT;
  v_next_month     DATE;
BEGIN
  v_partition_name := 'inventory_lots_' || to_char(p_month_start, 'YYYY_MM');
  v_next_month     := (p_month_start + INTERVAL '1 month')::DATE;
  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS %I PARTITION OF inventory_lots
       FOR VALUES FROM (%L) TO (%L)',
    v_partition_name, p_month_start, v_next_month
  );
END;
$$;

CREATE OR REPLACE FUNCTION _create_inventory_lot_events_partition(p_month_start DATE)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
  v_partition_name TEXT;
  v_next_month     DATE;
BEGIN
  v_partition_name := 'inventory_lot_events_' || to_char(p_month_start, 'YYYY_MM');
  v_next_month     := (p_month_start + INTERVAL '1 month')::DATE;
  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS %I PARTITION OF inventory_lot_events
       FOR VALUES FROM (%L) TO (%L)',
    v_partition_name, p_month_start, v_next_month
  );
END;
$$;

DO $$
DECLARE
  v_d DATE := DATE '2026-01-01';
BEGIN
  WHILE v_d < DATE '2028-01-01' LOOP
    PERFORM _create_inventory_lots_partition(v_d);
    PERFORM _create_inventory_lot_events_partition(v_d);
    v_d := (v_d + INTERVAL '1 month')::DATE;
  END LOOP;
END $$;

-- ---------- 9. pg_cron monthly rollover ----------

DO $$
BEGIN
  PERFORM cron.schedule(
    'inventory_lots_partition_rollover',
    '0 0 25 * *',
    $cron$
      SELECT _create_inventory_lots_partition(
        date_trunc('month', current_date + INTERVAL '1 month')::DATE
      );
      SELECT _create_inventory_lots_partition(
        date_trunc('month', current_date + INTERVAL '2 month')::DATE
      );
      SELECT _create_inventory_lot_events_partition(
        date_trunc('month', current_date + INTERVAL '1 month')::DATE
      );
      SELECT _create_inventory_lot_events_partition(
        date_trunc('month', current_date + INTERVAL '2 month')::DATE
      );
    $cron$
  );
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'pg_cron unavailable in % (%): %',
               current_database(), SQLSTATE, SQLERRM;
END $$;
