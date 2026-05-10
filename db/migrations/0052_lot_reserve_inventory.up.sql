-- ============================================================
-- Phase E2 E2.5 — reserve_inventory lot pin (acct-knuu,
-- sub-issue of acct-uze).
--
-- Extends reserve_inventory with two trailing optional parameters
-- (CREATE OR REPLACE; preserves existing call signature so
-- non-lot callers don't change):
--
--   p_lot_id       BIGINT  DEFAULT NULL
--   p_lot_specific BOOLEAN DEFAULT FALSE
--
-- Schema columns inventory_reservations.lot_id +
-- inventory_reservations.lot_specific were laid by E2.1 (mig
-- 0044), with the table CHECK enforcing
-- `NOT lot_specific OR lot_id IS NOT NULL`. This migration is
-- the function-side extension only.
--
-- DESIGN — promisable bounds:
--
--   lot_specific = FALSE  (existing semantics; default for
--                          non-lot SKUs)
--     Lock all stock_available rows for (sku, location) FOR UPDATE
--     — for non-lot SKUs there's exactly one row at lot_id=NULL;
--     for lot SKUs that adopt per-lot account partitioning there's
--     one per lot. Promisable qty = SUM(on_hand) across all rows
--     minus SUM(active reservations) for (sku, location).
--
--   lot_specific = TRUE
--     Pin the reservation to a specific lot. Promisable is bounded
--     by the lot's SUBLEDGER residual (from inventory_lots +
--     inventory_lot_events), not by an account-level partition,
--     since the L1-L4 wrapper integration uses SKU-level
--     stock_available (lot_id=NULL) rather than per-lot accounts.
--     The mig 0044 schema permits both modes; this implementation
--     is account-mode-agnostic.
--
--     Lock pattern: PERFORM 1 ... FOR UPDATE on stock_available
--     rows for (sku, location) — serializes reservers against
--     issue-time dispatchers (which lock the same rows).
--
--     Promisable computation:
--       lot_residual = original_quantity + SUM(events.qty_change)
--                       (via _inventory_lot_remaining_qty helper)
--       could_draw   = active reservations that could consume this
--                       lot — pinned-to-this-lot OR non-pinned for
--                       (sku, location). Conservative: non-pinned
--                       might allocate to any lot, so we assume
--                       they could pick this one. Acceptable
--                       under-promise at MVP; smarter
--                       allocator-aware promisable is a future
--                       follow-up (E2.7 FEFO).
--       promisable   = lot_residual - could_draw
--
--     Validation:
--       The lot must exist for (sku, location) — a stray lot_id
--       that points at a different SKU's lot raises P0010
--       (no_open_account semantics, reused for "lot mismatch").
--
-- VALIDATION:
--   p_lot_specific=TRUE with p_lot_id IS NULL → P0052
--     'reservation_invalid_lot' (raised before account lookup
--     for clear caller-side error; the table CHECK would also
--     reject the INSERT but later in the call path).
--
-- Errors:
--   P0010 — no open stock_available account for the requested
--           (sku, location); also raised for pinned reservations
--           when the lot_id doesn't belong to (sku, location).
--   P0052 — caller passed p_lot_specific=TRUE without p_lot_id.
--
-- Returns:
--   New reservation UUID on success, NULL when promisable < qty.
--
-- Out of scope (E2.5):
--   - FEFO allocator (E2.7).
--   - post_so_allocate handling pinned reservations: today's
--     allocate flow handles non-pinned only; pinned-aware
--     allocate is a separate follow-up (E2.5-followup) once a
--     real workflow demands it.
--   - Per-lot stock_available account auto-creation; the L1-L4
--     wrapper path uses SKU-level accounts and this function
--     reads the lot subledger directly for pinned promisable.
-- ============================================================

-- DROP the old 7-arg signature first; CREATE OR REPLACE on a
-- different parameter list creates a separate overload rather
-- than replacing, leaving callers ambiguous between the two.
DROP FUNCTION IF EXISTS reserve_inventory(
  UUID, UUID, BIGINT, UUID, UUID, TIMESTAMPTZ, BIGINT
);

CREATE OR REPLACE FUNCTION reserve_inventory(
  p_sku_id        UUID,
  p_location_id   UUID,
  p_qty           BIGINT,
  p_so_id         UUID,
  p_so_line_id    UUID,
  p_expires_at    TIMESTAMPTZ,
  p_unit_price    BIGINT  DEFAULT NULL,
  p_lot_id        BIGINT  DEFAULT NULL,
  p_lot_specific  BOOLEAN DEFAULT FALSE
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_avail_id       BIGINT;
  v_qty_promisable BIGINT;
  v_new_id         UUID;
  v_recv_date      DATE;
  v_lot_residual   NUMERIC;
BEGIN
  -- Validation: lot_specific requires lot_id (mirrors table CHECK
  -- but raised earlier for caller-clear semantics).
  IF p_lot_specific AND p_lot_id IS NULL THEN
    RAISE EXCEPTION 'reserve_inventory: p_lot_specific=TRUE requires p_lot_id'
      USING ERRCODE = 'P0052';
  END IF;

  IF p_lot_specific THEN
    -- Verify the lot belongs to (sku, location) and capture receipt_date
    -- for the residual lookup.
    SELECT receipt_date
      INTO v_recv_date
      FROM inventory_lots
     WHERE lot_id      = p_lot_id
       AND product_id  = p_sku_id
       AND location_id = p_location_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'lot=% does not belong to sku=% loc=%',
                      p_lot_id, p_sku_id, p_location_id
        USING ERRCODE = 'P0010';
    END IF;

    -- Lock the SKU-level stock_available rows FOR UPDATE — this
    -- serializes us against issue-time dispatchers, which also
    -- lock these rows. For SKUs whose stock_available is partitioned
    -- per-lot, multiple rows are locked; for SKU-level (lot_id=NULL)
    -- mode, exactly one row is locked.
    PERFORM 1
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

    v_lot_residual := COALESCE(
      _inventory_lot_remaining_qty(p_lot_id, v_recv_date), 0
    );

    v_qty_promisable := (
        v_lot_residual
      - COALESCE((
          SELECT SUM(qty) FROM inventory_reservations r
           WHERE r.sku_id      = p_sku_id
             AND r.location_id = p_location_id
             AND r.status      = 'active'
             AND (
               (r.lot_specific AND r.lot_id = p_lot_id)
               OR NOT r.lot_specific
             )
        ), 0)
    )::BIGINT;
  ELSE
    -- Non-pinned: lock all stock_available rows for (sku, location).
    -- For SKU-level (lot_id NULL) mode there's one row; for per-lot
    -- partitioned mode there's one per lot. FOR UPDATE on each
    -- serializes concurrent reservers.
    PERFORM 1
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

    SELECT COALESCE(SUM(a.debits_total - a.credits_total), 0)
         - COALESCE((
             SELECT SUM(qty) FROM inventory_reservations r
              WHERE r.sku_id      = p_sku_id
                AND r.location_id = p_location_id
                AND r.status      = 'active'
           ), 0)
      INTO v_qty_promisable
      FROM accounts a
     WHERE a.kind        = 'stock_available'
       AND a.sku_id      = p_sku_id
       AND a.location_id = p_location_id
       AND NOT a.is_closed;
  END IF;

  IF v_qty_promisable < p_qty THEN
    RETURN NULL;
  END IF;

  INSERT INTO inventory_reservations
    (sku_id, location_id, qty, so_id, so_line_id, expires_at, unit_price,
     lot_id, lot_specific)
  VALUES
    (p_sku_id, p_location_id, p_qty, p_so_id, p_so_line_id, p_expires_at,
     p_unit_price, p_lot_id, p_lot_specific)
  RETURNING id INTO v_new_id;

  RETURN v_new_id;
END;
$$;
