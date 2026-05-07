-- Inventory reservations: first-class state rows for pre-ship qty holds.
--
-- Consolidates archive migs 0009 (table) + 0014 (reserve_inventory fn)
-- + 0015 (pg_cron expiry job).
--
-- Reservations remain state-not-postings per CLAUDE.md load-bearing
-- decisions. Lifecycle: active → allocated (post_so_allocate) → shipped
-- (post_so_ship) | cancelled | expired (cron sweep).

CREATE TABLE inventory_reservations (
  id          UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  sku_id      UUID NOT NULL REFERENCES skus(id),
  location_id UUID NOT NULL REFERENCES locations(id),
  qty         BIGINT NOT NULL CHECK (qty > 0),
  so_id       UUID NOT NULL REFERENCES sales_orders(id),
  so_line_id  UUID NOT NULL,
  status      reservation_status NOT NULL DEFAULT 'active',
  expires_at  TIMESTAMPTZ NOT NULL,
  reserved_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  resolved_at TIMESTAMPTZ,
  unit_price  BIGINT,
  notes       TEXT
);

CREATE INDEX rsv_sku_loc_active
  ON inventory_reservations (sku_id, location_id)
  WHERE status = 'active';

CREATE INDEX rsv_so
  ON inventory_reservations (so_id);

CREATE INDEX rsv_expires
  ON inventory_reservations (expires_at)
  WHERE status = 'active';

-- reserve_inventory — atomic SO reservation against a (sku, location).
--
-- Returns the new reservation id on success, NULL when qty_promisable
-- is insufficient. Raises P0010 if there is no open stock_available
-- account for the (sku, location) pair (caller bug).
--
-- Why a function and not a CTE+INSERT: in plpgsql each SQL statement
-- takes its own snapshot in READ COMMITTED; the SELECT that computes
-- qty_promisable AFTER the FOR UPDATE wait sees the prior winner's
-- INSERT, so concurrent reservers serialize correctly.

CREATE OR REPLACE FUNCTION reserve_inventory(
  p_sku_id      UUID,
  p_location_id UUID,
  p_qty         BIGINT,
  p_so_id       UUID,
  p_so_line_id  UUID,
  p_expires_at  TIMESTAMPTZ,
  p_unit_price  BIGINT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_avail_id       BIGINT;
  v_qty_promisable BIGINT;
  v_new_id         UUID;
BEGIN
  SELECT id INTO v_avail_id
    FROM accounts
   WHERE kind        = 'stock_available'
     AND sku_id      = p_sku_id
     AND location_id = p_location_id
     AND NOT is_closed
   FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                    p_sku_id, p_location_id
      USING ERRCODE = 'P0010';
  END IF;

  SELECT (a.debits_total - a.credits_total)
       - COALESCE((
           SELECT SUM(qty) FROM inventory_reservations r
            WHERE r.sku_id      = p_sku_id
              AND r.location_id = p_location_id
              AND r.status      = 'active'
         ), 0)
    INTO v_qty_promisable
    FROM accounts a
   WHERE a.id = v_avail_id;

  IF v_qty_promisable < p_qty THEN
    RETURN NULL;
  END IF;

  INSERT INTO inventory_reservations
    (sku_id, location_id, qty, so_id, so_line_id, expires_at, unit_price)
  VALUES
    (p_sku_id, p_location_id, p_qty, p_so_id, p_so_line_id, p_expires_at, p_unit_price)
  RETURNING id INTO v_new_id;

  RETURN v_new_id;
END;
$$;

-- pg_cron expiry sweep: every 30 seconds. Runs only in the database
-- named by `cron.database_name` GUC (acct in docker-compose.yml).
-- Tolerant install for non-cron databases (test/CI ephemeral DBs).

DO $$
BEGIN
  PERFORM cron.schedule(
    'reservation_expiry',
    '30 seconds',
    $cron$
      UPDATE inventory_reservations
         SET status      = 'expired',
             resolved_at = clock_timestamp()
       WHERE status      = 'active'
         AND expires_at  < clock_timestamp();
    $cron$
  );
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'pg_cron unavailable in % (%): %',
               current_database(), SQLSTATE, SQLERRM;
END $$;
