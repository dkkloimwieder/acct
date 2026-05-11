-- sxl2.1 (acct-sxl2.1): inventory_units + inventory_unit_events schema.
--
-- WHY: tracked_by='lot_and_serial' SKUs need per-unit traceability
-- (warranty, recall, FDA UDI) while costing stays at lot grain
-- (cost_method='lot_fifo' on the SKU). Industry-standard pattern: SAP
-- "Batch-with-Serial-Profile", Oracle "Lot and Serial Numbers",
-- D365 "Tracking dimension group" with cost-dim toggle off.
--
-- DESIGN CALLS (sxl2 Q1-Q8, confirmed 2026-05-10):
--
-- Q1: wrappers accept BOTH unit_serials TEXT[] AND unit_ids BIGINT[],
--     XOR'd; resolution happens at wrapper layer (sxl2.3+).
--
-- Q2: TWO serial columns:
--       serial_no VARCHAR(128) NOT NULL  — system-canonical, auto-
--         generated when caller doesn't supply. Partial UNIQUE
--         (product_id, serial_no) WHERE status IN (active states).
--       external_serial_no VARCHAR(128) NULL — supplier/manufacturer
--         label as printed on the unit. Partial UNIQUE
--         (product_id, external_serial_no) WHERE NOT NULL AND status
--         IN (active states). Protects against supplier-side dupes.
--
-- Q3: Caller-supplied unit_ids[] / unit_serials[] (the "actual
--     consumption" path) used directly; otherwise allocator picks via
--     skus.allocation_strategy (FIFO/FEFO). Same dispatcher as lot
--     allocator (sxl2.3+).
--
-- Q4: post_wo_complete auto-generates FG-lot serials if not supplied
--     (format: <lot_code>-U<padded-sequence>); caller override
--     accepted (sxl2.4).
--
-- Q5: Reservations gain unit_ids BIGINT[] NULL — NULL = any-units,
--     non-NULL = pinned to these specific unit_ids. Status flips
--     'available' → 'reserved' on reserve_inventory (sxl2.4+).
--
-- Q6: MUTABLE identity. Single unit_id across lifecycle; UPDATE on
--     transfer (current_location_id, lot_id, lot_receipt_date),
--     issue (status), hold/release (status). inventory_unit_events
--     is the append-only audit trail. inventory_lots itself is
--     append-only (mig 0044), but units differ because: (a) lot =
--     fungible quantity (split-on-move creates new lot at dest);
--     (b) unit = singular physical identity (1 physical item = 1
--     row forever); (c) external_serial_no is a stable physical
--     attribute that mutability preserves; (d) matches industry
--     unit patterns (SAP HU, D365 license plate, Oracle WMS serial).
--     Append-only audit lives in inventory_unit_events; recon
--     check #14 (sxl2.6) catches row-vs-events drift.
--
-- Q7: rm_issue tracking uses a SEPARATE wo_unit_consumption mirror
--     table (sxl2.5), not extension of wo_lot_consumption. One row
--     per consumed unit; multi-unit-from-same-lot avoids 1:1 forcing.
--
-- Q8: Recon check #14: per (lot_id, lot_receipt_date), COUNT(units
--     WHERE status='available') = lot_residual_qty from check #9.
--     Catches drift in both directions (sxl2.6).
--
-- SCOPE OF THIS MIG: schema only — tables, indexes, append-only
-- trigger on events, reservations.unit_ids column, partitions,
-- pg_cron rollover. NO dispatcher hooks, NO wrapper changes.

-- ---------- 1. inventory_unit_status enum ----------

CREATE TYPE inventory_unit_status AS ENUM (
  'available',         -- created at receipt; ready for any consumption
  'reserved',          -- listed in inventory_reservations.unit_ids
  'allocated',         -- so_allocation pre-ship pinning
  'shipped',           -- post_so_ship completed
  'consumed',          -- rm_issue_to_wo or wo scrap consumed
  'transferred_out',   -- lot_transfer moved unit to another loc/lot
  'scrapped',          -- post_scrap consumed
  'lost',              -- cycle_count_adj wrote off
  'on_hold',           -- quality hold
  'returned'           -- customer_return back into stock
);

COMMENT ON TYPE inventory_unit_status IS
  'Lifecycle status for inventory_units. Active statuses '
  '(available/reserved/allocated/on_hold/returned) hold the serial '
  'slot in the partial UNIQUE indexes; terminal/transitional '
  'statuses release the slot for serial reuse. Mainstream-ERP '
  'convention for tracked units.';

-- ---------- 2. inventory_units (mutable, NOT partitioned) ----------
--
-- NOT partitioned: PG requires the partition key in any UNIQUE
-- constraint on a partitioned table. Serial uniqueness must be
-- GLOBAL across the system (not partition-local — a serial reused
-- across months would be a real data error), so partitioning by
-- received_at would force either partition-local uniqueness (wrong)
-- or no uniqueness enforcement (worse). inventory_units is identity-
-- table grain (one row per physical unit, mutated across lifecycle)
-- — same shape as skus / customers / vendors which aren't partitioned.
-- inventory_unit_events IS partitioned (append-only event-grain).

CREATE TABLE inventory_units (
  unit_id              BIGSERIAL    PRIMARY KEY,
  product_id           UUID         NOT NULL REFERENCES skus(id),
  legal_entity_id     SMALLINT      NOT NULL DEFAULT 1 REFERENCES legal_entities(id),
  cost_book_id        SMALLINT      NOT NULL DEFAULT 1 REFERENCES cost_books(id),
  lot_id               BIGINT       NOT NULL,
  lot_receipt_date     DATE         NOT NULL,
  serial_no            VARCHAR(128) NOT NULL,
  external_serial_no   VARCHAR(128),
  status               inventory_unit_status NOT NULL DEFAULT 'available',
  current_location_id  UUID         NOT NULL REFERENCES locations(id),
  work_order_id        UUID,                                    -- for in-WIP / consumed-by-WO units
  customer_id          UUID         REFERENCES customers(id),   -- for shipped / returned units
  mfg_date             DATE,
  expiry_date          DATE,
  attributes           JSONB,
  receipt_posting_line_id BIGINT    NOT NULL REFERENCES posting_lines(id),
  received_at          TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
  updated_at           TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
  FOREIGN KEY (lot_id, lot_receipt_date)
    REFERENCES inventory_lots (lot_id, receipt_date)
);

COMMENT ON TABLE inventory_units IS
  'Per-unit traceability rows for tracked_by=''lot_and_serial'' SKUs. '
  'MUTABLE identity (Q6): single unit_id across lifecycle; UPDATE on '
  'transfer/issue/hold/release. NOT partitioned (identity-table grain '
  'with global UNIQUE on serial_no). Append-only audit lives in '
  'inventory_unit_events.';

COMMENT ON COLUMN inventory_units.serial_no IS
  'System-canonical serial. Auto-generated at receipt when caller does '
  'not supply (<lot_code>-U<padded-seq> format from wrapper). UNIQUE '
  'within (product_id, serial_no) for active statuses.';

COMMENT ON COLUMN inventory_units.external_serial_no IS
  'Supplier/manufacturer serial as printed on the physical unit. '
  'NULL when only system-generated. UNIQUE within (product_id, '
  'external_serial_no) for active statuses when present — protects '
  'against supplier-side duplicates.';

-- Partial UNIQUE: system serial held only while status is active.
-- Transferred / consumed / scrapped / lost / shipped units release
-- the slot for serial reuse (mainstream-ERP convention).
CREATE UNIQUE INDEX inventory_units_serial_unique
  ON inventory_units (product_id, serial_no)
  WHERE status IN ('available', 'reserved', 'allocated',
                   'on_hold', 'returned');

-- Partial UNIQUE: external serial held only when present AND active.
CREATE UNIQUE INDEX inventory_units_external_serial_unique
  ON inventory_units (product_id, external_serial_no)
  WHERE external_serial_no IS NOT NULL
    AND status IN ('available', 'reserved', 'allocated',
                   'on_hold', 'returned');

CREATE INDEX inventory_units_lot
  ON inventory_units (lot_id, lot_receipt_date);

CREATE INDEX inventory_units_location_available
  ON inventory_units (product_id, current_location_id)
  WHERE status = 'available';

CREATE INDEX inventory_units_wo
  ON inventory_units (work_order_id)
  WHERE work_order_id IS NOT NULL;

CREATE INDEX inventory_units_customer
  ON inventory_units (customer_id)
  WHERE customer_id IS NOT NULL;

CREATE INDEX inventory_units_receipt_posting_line
  ON inventory_units (receipt_posting_line_id);

-- ---------- 3. inventory_unit_events (append-only, partitioned by event_date) ----------

-- event_type SMALLINT codes (unit lifecycle taxonomy):
--   1  = receipt        (status -> available; created at receipt)
--   2  = issue          (status -> consumed / shipped / scrapped per
--                        caller context — value-leg posting drives
--                        terminal status; unit row gets UPDATEd)
--   3  = transfer       (status stays active; location_id_from/to set;
--                        lot_id may change)
--   4  = hold           (status -> on_hold)
--   5  = release        (status -> available from hold/reserved)
--   6  = warranty_register   (notes carry warranty terms)
--   7  = warranty_expire     (status unchanged)
--   8  = scrap          (status -> scrapped)
--   9  = lost           (status -> lost)
--   10 = return         (status -> returned)
--
-- Receipt is recorded explicitly as event_type=1 (unlike lots,
-- which encode receipt in the inventory_lots row itself). Reason:
-- units are mutable, so the events table is the canonical audit
-- trail and recording every state change there — including the
-- initial 'available' — keeps the trail uniform.

CREATE TABLE inventory_unit_events (
  event_id             BIGSERIAL,
  unit_id              BIGINT       NOT NULL REFERENCES inventory_units(unit_id),
  event_date           DATE         NOT NULL,
  event_type           SMALLINT     NOT NULL CHECK (event_type BETWEEN 1 AND 10),
  posting_line_id      BIGINT       REFERENCES posting_lines(id),
  movement_id          BIGINT,
  movement_date        DATE,
  location_id_from     UUID         REFERENCES locations(id),
  location_id_to       UUID         REFERENCES locations(id),
  new_status           inventory_unit_status,
  new_lot_id           BIGINT,
  new_lot_receipt_date DATE,
  customer_id          UUID         REFERENCES customers(id),
  work_order_id        UUID,
  notes                TEXT,
  created_at           TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (event_id, event_date),
  -- movement_id and movement_date are paired (composite FK target).
  CHECK (
    (movement_id IS NULL AND movement_date IS NULL) OR
    (movement_id IS NOT NULL AND movement_date IS NOT NULL)
  ),
  -- transfer requires both from/to locations and they must differ.
  CHECK (
    event_type <> 3 OR
    (location_id_from IS NOT NULL AND location_id_to IS NOT NULL
     AND location_id_from <> location_id_to)
  ),
  -- new_lot_id and new_lot_receipt_date are paired (composite FK target
  -- to inventory_lots; not declared as a real FK because partition
  -- routing on event_date makes cross-partition FKs expensive — same
  -- soft-pointer pattern as inventory_lot_events.movement_id).
  CHECK (
    (new_lot_id IS NULL AND new_lot_receipt_date IS NULL) OR
    (new_lot_id IS NOT NULL AND new_lot_receipt_date IS NOT NULL)
  )
) PARTITION BY RANGE (event_date);

CREATE INDEX inventory_unit_events_unit
  ON inventory_unit_events (unit_id, event_date);

CREATE INDEX inventory_unit_events_posting_line
  ON inventory_unit_events (posting_line_id)
  WHERE posting_line_id IS NOT NULL;

CREATE INDEX inventory_unit_events_movement
  ON inventory_unit_events (movement_id, movement_date)
  WHERE movement_id IS NOT NULL;

CREATE INDEX inventory_unit_events_wo
  ON inventory_unit_events (work_order_id)
  WHERE work_order_id IS NOT NULL;

-- Append-only trigger reuses the shared fn from mig 0044.
-- (inventory_units itself is MUTABLE per Q6, so NO trigger on it.)
CREATE TRIGGER trg_inventory_unit_events_append_only
  BEFORE UPDATE OR DELETE ON inventory_unit_events
  FOR EACH ROW EXECUTE FUNCTION block_inventory_lot_modifications();

-- ---------- 4. inventory_reservations.unit_ids ----------

ALTER TABLE inventory_reservations
  ADD COLUMN unit_ids BIGINT[];

COMMENT ON COLUMN inventory_reservations.unit_ids IS
  'NULL = any-units reservation (allocator picks at allocate/ship). '
  'Non-NULL = pinned to these specific unit_ids; allocator must '
  'respect the pinning. For tracked_by=''lot_and_serial'' SKUs the '
  'caller may pin units up-front (e.g., customer requested specific '
  'manufacture date or serial range).';

CREATE INDEX inventory_reservations_unit_ids
  ON inventory_reservations USING GIN (unit_ids)
  WHERE unit_ids IS NOT NULL;

-- ---------- 5. Partition helper + 24-month bake (events only) ----------

CREATE OR REPLACE FUNCTION _create_inventory_unit_events_partition(p_month_start DATE)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
  v_partition_name TEXT;
  v_next_month     DATE;
BEGIN
  v_partition_name := 'inventory_unit_events_' || to_char(p_month_start, 'YYYY_MM');
  v_next_month     := (p_month_start + INTERVAL '1 month')::DATE;
  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS %I PARTITION OF inventory_unit_events
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
    PERFORM _create_inventory_unit_events_partition(v_d);
    v_d := (v_d + INTERVAL '1 month')::DATE;
  END LOOP;
END $$;

-- ---------- 6. pg_cron monthly rollover ----------

DO $$
BEGIN
  PERFORM cron.schedule(
    'inventory_unit_events_partition_rollover',
    '0 0 25 * *',
    $cron$
      SELECT _create_inventory_unit_events_partition(
        date_trunc('month', current_date + INTERVAL '1 month')::DATE
      );
      SELECT _create_inventory_unit_events_partition(
        date_trunc('month', current_date + INTERVAL '2 month')::DATE
      );
    $cron$
  );
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'pg_cron unavailable in % (%): %',
               current_database(), SQLSTATE, SQLERRM;
END $$;
