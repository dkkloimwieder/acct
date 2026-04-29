-- reserve_inventory — atomic SO reservation against a (sku, location).
--
-- Returns the new reservation id on success, NULL when qty_promisable
-- is insufficient. Raises P0010 if there is no open stock_available
-- account for the (sku, location) pair (caller bug — accounts must be
-- pre-created).
--
-- Why a function and not the CTE+INSERT shown in doc §3.3:
--
--   The §3.3 single-statement CTE is unsafe under concurrent reservers
--   in READ COMMITTED. Postgres takes one snapshot per command; the
--   `FOR UPDATE` wait correctly serializes contenders on the
--   stock_available row, but the inner `SELECT SUM(qty) FROM
--   inventory_reservations …` subquery still uses the original
--   pre-wait snapshot, so each waiter computes the same
--   qty_promisable and over-promises. Demonstrated by the T3
--   reserve_concurrency test (acct-93b.18).
--
--   In PL/pgSQL, each SQL statement takes its own snapshot in
--   READ COMMITTED, so the SELECT that computes qty_promisable
--   AFTER the FOR UPDATE wait sees the prior winner's INSERT.
--   Doc-fix tracking issue separately filed.

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
