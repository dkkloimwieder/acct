-- ============================================================
-- Phase E1 E1.1 — cost_layers + cost_layer_depletions schema
-- (acct-3b3a, sub-issue of acct-cbss).
--
-- FIFO foundation. Two append-only tables ride on top of the
-- inventory_movements subledger from Phase D:
--
--   cost_layers
--     One row per receipt of a FIFO SKU. Immutable after creation.
--     No current_quantity column — residual is computed via
--     SUM(cost_layer_depletions.depleted_quantity) per layer.
--
--   cost_layer_depletions
--     One or more rows per issue of a FIFO SKU. Each row records
--     consumption from a single layer. Append-only.
--
-- Key design choices (cost-methods-subledger-design.md §7.2-§7.3):
--
--   - quantities NUMERIC(19,6); cost_amount BIGINT cents (matches
--     posting_lines.amount integer convention from CLAUDE.md).
--
--   - Partitioned by RANGE(receipt_date) and RANGE(issue_date)
--     respectively, monthly windows. PK includes the partition
--     key per PG rules. Composite FK from depletions to layers
--     on (layer_id, receipt_date).
--
--   - 24 monthly partitions baked 2026-01 through 2027-12 (covers
--     fixture range + reasonable horizon). Production rollover
--     via pg_cron at month-25.
--
--   - Append-only enforced by row-level trigger (mirrors mig 0010
--     pattern, P9999 SQLSTATE).
--
--   - Helper _cost_layer_remaining_qty(layer_id, receipt_date)
--     returns the live residual. STABLE function so PG can plan
--     it; not VOLATILE because it reads-only of subledger state.
--
--   - posting_line_id is canonical link (not receipt_movement_id)
--     for FK simplicity. Both inventory_movements and cost_layers
--     ride on the same posting_line; cross-ref is via posting_line_id.
--
--   - cost_layer_id BIGINT on posting_line_inventory (already
--     declared in mig 0024) stays a soft pointer — composite PK
--     on cost_layers means a single-column FK won't work cleanly,
--     and the recon check in E1.3 catches drift.
-- ============================================================

CREATE TABLE cost_layers (
  layer_id            BIGSERIAL,
  product_id          UUID         NOT NULL REFERENCES skus(id),
  legal_entity_id     SMALLINT     NOT NULL DEFAULT 1 REFERENCES legal_entities(id),
  cost_book_id        SMALLINT     NOT NULL DEFAULT 1 REFERENCES cost_books(id),
  location_id         UUID         NOT NULL REFERENCES locations(id),
  receipt_posting_line_id BIGINT   NOT NULL REFERENCES posting_lines(id),
  receipt_date        DATE         NOT NULL,
  original_quantity   NUMERIC(19, 6) NOT NULL CHECK (original_quantity > 0),
  unit_cost           NUMERIC(19, 4) NOT NULL CHECK (unit_cost >= 0),
  cost_currency       CHAR(3)      NOT NULL,
  created_at          TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (layer_id, receipt_date)
) PARTITION BY RANGE (receipt_date);

CREATE INDEX cost_layers_fifo_walk
  ON cost_layers (product_id, location_id, cost_book_id, receipt_date, layer_id);
CREATE INDEX cost_layers_posting_line
  ON cost_layers (receipt_posting_line_id);

CREATE TABLE cost_layer_depletions (
  depletion_id        BIGSERIAL,
  layer_id            BIGINT       NOT NULL,
  layer_receipt_date  DATE         NOT NULL,
  issue_date          DATE         NOT NULL,
  depleted_quantity   NUMERIC(19, 6) NOT NULL CHECK (depleted_quantity > 0),
  unit_cost           NUMERIC(19, 4) NOT NULL CHECK (unit_cost >= 0),
  cost_amount         BIGINT       NOT NULL,
  posting_line_id     BIGINT       NOT NULL REFERENCES posting_lines(id),
  created_at          TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (depletion_id, issue_date),
  FOREIGN KEY (layer_id, layer_receipt_date)
    REFERENCES cost_layers (layer_id, receipt_date)
) PARTITION BY RANGE (issue_date);

CREATE INDEX cost_layer_depletions_layer
  ON cost_layer_depletions (layer_id, layer_receipt_date);
CREATE INDEX cost_layer_depletions_posting_line
  ON cost_layer_depletions (posting_line_id);

-- ============================================================
-- Append-only enforcement. Mirrors mig 0010 pattern.
-- ============================================================

CREATE OR REPLACE FUNCTION block_cost_layer_modifications()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION '% are append-only; UPDATE/DELETE rejected', TG_TABLE_NAME
    USING ERRCODE = 'P9999';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_cost_layers_append_only
  BEFORE UPDATE OR DELETE ON cost_layers
  FOR EACH ROW EXECUTE FUNCTION block_cost_layer_modifications();

CREATE TRIGGER trg_cost_layer_depletions_append_only
  BEFORE UPDATE OR DELETE ON cost_layer_depletions
  FOR EACH ROW EXECUTE FUNCTION block_cost_layer_modifications();

-- ============================================================
-- Residual helper. Returns NULL when the layer doesn't exist.
-- STABLE because it reads-only without side effects (PG can
-- cache results within a single statement).
-- ============================================================

CREATE OR REPLACE FUNCTION _cost_layer_remaining_qty(
  p_layer_id BIGINT, p_receipt_date DATE
) RETURNS NUMERIC
LANGUAGE plpgsql STABLE AS $$
DECLARE
  v_orig     NUMERIC;
  v_depleted NUMERIC;
BEGIN
  SELECT original_quantity INTO v_orig
    FROM cost_layers
   WHERE layer_id = p_layer_id AND receipt_date = p_receipt_date;
  IF NOT FOUND THEN RETURN NULL; END IF;
  SELECT COALESCE(SUM(depleted_quantity), 0) INTO v_depleted
    FROM cost_layer_depletions
   WHERE layer_id = p_layer_id AND layer_receipt_date = p_receipt_date;
  RETURN v_orig - v_depleted;
END;
$$;

-- ============================================================
-- Partition helpers. Idempotent (CREATE IF NOT EXISTS) so the
-- bake-loop and the cron rollover both call them safely.
-- ============================================================

CREATE OR REPLACE FUNCTION _create_cost_layers_partition(p_month_start DATE)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
  v_partition_name TEXT;
  v_next_month     DATE;
BEGIN
  v_partition_name := 'cost_layers_' || to_char(p_month_start, 'YYYY_MM');
  v_next_month     := (p_month_start + INTERVAL '1 month')::DATE;
  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS %I PARTITION OF cost_layers
       FOR VALUES FROM (%L) TO (%L)',
    v_partition_name, p_month_start, v_next_month
  );
END;
$$;

CREATE OR REPLACE FUNCTION _create_cost_layer_depletions_partition(p_month_start DATE)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
  v_partition_name TEXT;
  v_next_month     DATE;
BEGIN
  v_partition_name := 'cost_layer_depletions_' || to_char(p_month_start, 'YYYY_MM');
  v_next_month     := (p_month_start + INTERVAL '1 month')::DATE;
  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS %I PARTITION OF cost_layer_depletions
       FOR VALUES FROM (%L) TO (%L)',
    v_partition_name, p_month_start, v_next_month
  );
END;
$$;

-- Bake 24 monthly partitions for both tables: 2026-01 through 2027-12.

DO $$
DECLARE
  v_d DATE := DATE '2026-01-01';
BEGIN
  WHILE v_d < DATE '2028-01-01' LOOP
    PERFORM _create_cost_layers_partition(v_d);
    PERFORM _create_cost_layer_depletions_partition(v_d);
    v_d := (v_d + INTERVAL '1 month')::DATE;
  END LOOP;
END $$;

-- ============================================================
-- pg_cron monthly rollover. Runs on the 25th at 00:00 UTC,
-- creates next-month + month-after partitions for both tables.
-- Tolerant install for non-cron databases (CI / ephemeral DBs).
-- Operational lifecycle (archival, compression) tracked under
-- acct-sbr2 alongside inventory_movements.
-- ============================================================

DO $$
BEGIN
  PERFORM cron.schedule(
    'cost_layers_partition_rollover',
    '0 0 25 * *',
    $cron$
      SELECT _create_cost_layers_partition(
        date_trunc('month', current_date + INTERVAL '1 month')::DATE
      );
      SELECT _create_cost_layers_partition(
        date_trunc('month', current_date + INTERVAL '2 month')::DATE
      );
      SELECT _create_cost_layer_depletions_partition(
        date_trunc('month', current_date + INTERVAL '1 month')::DATE
      );
      SELECT _create_cost_layer_depletions_partition(
        date_trunc('month', current_date + INTERVAL '2 month')::DATE
      );
    $cron$
  );
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'pg_cron unavailable in % (%): %',
               current_database(), SQLSTATE, SQLERRM;
END $$;
