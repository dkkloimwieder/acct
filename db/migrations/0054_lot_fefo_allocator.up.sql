-- ============================================================
-- Phase E2.7 — FEFO allocator + skus.allocation_strategy enum
-- (acct-mxll).
--
-- Adds per-SKU lot allocation strategy ENUM('fifo','fefo'). When
-- a SKU's allocation_strategy is 'fefo' (First-Expiry-First-Out),
-- _lot_walk_layers walks lots ORDER BY expiration_date ASC NULLS
-- LAST, lot_id ASC instead of by receipt_date. NULL expiry sorts
-- to the END so unexpiring stock isn't picked first (mainstream
-- ERP convention: SAP MILL, Oracle WMS).
--
-- Default 'fifo' preserves all existing behavior — every existing
-- SKU continues to walk by receipt_date.
--
-- Specific lot pin (p_specific_lot_id NOT NULL) bypasses strategy
-- entirely. The 5 callers of _lot_walk_layers (mig 0046's
-- _compute_amount_lot_fifo_outbound + _lot_write_issues, mig 0048
-- post_inventory_adjustment, mig 0049 _wo_emit_bom_lines, mig
-- 0050+0053 post_so_ship) all pass through transparently — only
-- _lot_walk_layers itself changes.
--
-- Mid-life strategy change is just an UPDATE on
-- skus.allocation_strategy. No backfill / no reservation flush.
-- Pre-existing reservations keep their pin (which was set at
-- reserve time); future reservations + walks pick up the new
-- strategy.
--
-- Out of scope (separate follow-ups):
--   * Smart promisable in reserve_inventory (allocator-aware
--     bounds for FEFO).
--   * Per-line override of allocation strategy via wrapper JSONB.
--   * LIFO / LEFO variants.
--   * skus.allocation_strategy effect on serial-tracked SKUs (E3).
--
-- Infrastructure already in place from mig 0044:
--   * inventory_lots.expiration_date DATE NULL column.
--   * inventory_lots_fefo_walk index on (product_id, location_id,
--     expiration_date, lot_id) WHERE expiration_date IS NOT NULL.
-- ============================================================

-- ---------- 1. allocation_strategy ENUM ----------

CREATE TYPE allocation_strategy AS ENUM ('fifo', 'fefo');

-- ---------- 2. skus.allocation_strategy column ----------

ALTER TABLE skus
  ADD COLUMN allocation_strategy allocation_strategy
                                  NOT NULL DEFAULT 'fifo';

COMMENT ON COLUMN skus.allocation_strategy IS
  'Lot allocation walk order. ''fifo'' (default) walks by '
  'receipt_date ASC, lot_id ASC. ''fefo'' walks by expiration_date '
  'ASC NULLS LAST, lot_id ASC — production semantic for industries '
  'where expiry-first dispatch is required (pharma, food, dairy). '
  'NULL expiration_date sorts LAST so unexpiring stock isn''t picked '
  'first. Specific-lot pin via p_specific_lot_id bypasses the '
  'strategy. Inert for non-lot cost methods. (acct-mxll)';

-- ---------- 3. _lot_walk_layers — branched ORDER BY ----------

-- Replaces mig 0046's _lot_walk_layers verbatim except for the
-- strategy lookup at entry and the dual-branch ORDER BY. The
-- residual computation, FOR UPDATE locking, allocation walk loop,
-- and exhaustion error handling are all preserved unchanged.

CREATE OR REPLACE FUNCTION _lot_walk_layers(
  p_product_id      UUID,
  p_location_id     UUID,
  p_cost_book_id    SMALLINT,
  p_qty             NUMERIC,
  p_specific_lot_id BIGINT DEFAULT NULL
) RETURNS TABLE (
  lot_id       BIGINT,
  receipt_date DATE,
  allocation   NUMERIC,
  unit_cost    NUMERIC,
  cost_amount  BIGINT
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_remaining NUMERIC := p_qty;
  v_lot       RECORD;
  v_alloc     NUMERIC;
  v_strategy  allocation_strategy;
BEGIN
  IF p_qty IS NULL OR p_qty <= 0 THEN
    RAISE EXCEPTION 'lot_walk_invalid_qty: % must be positive', p_qty
      USING ERRCODE = 'P0006';
  END IF;

  -- Resolve strategy. Specific-lot pin bypasses (caller pinned a
  -- lot, walk order is irrelevant to a single-lot lookup).
  IF p_specific_lot_id IS NULL THEN
    SELECT allocation_strategy INTO v_strategy
      FROM skus WHERE id = p_product_id;
  END IF;

  IF v_strategy = 'fefo' THEN
    FOR v_lot IN
      SELECT
        il.lot_id        AS lid,
        il.receipt_date  AS rd,
        il.unit_cost     AS uc,
        il.original_quantity + COALESCE(
          (SELECT SUM(e.quantity_change)
             FROM inventory_lot_events e
            WHERE e.lot_id = il.lot_id
              AND e.lot_receipt_date = il.receipt_date),
          0
        )                AS residual
        FROM inventory_lots il
       WHERE il.product_id   = p_product_id
         AND il.location_id  = p_location_id
         AND il.cost_book_id = p_cost_book_id
       ORDER BY il.expiration_date ASC NULLS LAST, il.lot_id ASC
         FOR UPDATE
    LOOP
      IF v_remaining <= 0 THEN EXIT; END IF;
      IF v_lot.residual <= 0 THEN CONTINUE; END IF;

      v_alloc := LEAST(v_lot.residual, v_remaining);

      lot_id       := v_lot.lid;
      receipt_date := v_lot.rd;
      allocation   := v_alloc;
      unit_cost    := v_lot.uc;
      cost_amount  := ROUND(v_alloc * v_lot.uc)::BIGINT;
      RETURN NEXT;

      v_remaining := v_remaining - v_alloc;
    END LOOP;
  ELSE
    -- 'fifo' or specific-lot pin: walk by receipt_date.
    FOR v_lot IN
      SELECT
        il.lot_id        AS lid,
        il.receipt_date  AS rd,
        il.unit_cost     AS uc,
        il.original_quantity + COALESCE(
          (SELECT SUM(e.quantity_change)
             FROM inventory_lot_events e
            WHERE e.lot_id = il.lot_id
              AND e.lot_receipt_date = il.receipt_date),
          0
        )                AS residual
        FROM inventory_lots il
       WHERE il.product_id   = p_product_id
         AND il.location_id  = p_location_id
         AND il.cost_book_id = p_cost_book_id
         AND (p_specific_lot_id IS NULL OR il.lot_id = p_specific_lot_id)
       ORDER BY il.receipt_date ASC, il.lot_id ASC
         FOR UPDATE
    LOOP
      IF v_remaining <= 0 THEN EXIT; END IF;
      IF v_lot.residual <= 0 THEN CONTINUE; END IF;

      v_alloc := LEAST(v_lot.residual, v_remaining);

      lot_id       := v_lot.lid;
      receipt_date := v_lot.rd;
      allocation   := v_alloc;
      unit_cost    := v_lot.uc;
      cost_amount  := ROUND(v_alloc * v_lot.uc)::BIGINT;
      RETURN NEXT;

      v_remaining := v_remaining - v_alloc;
    END LOOP;
  END IF;

  IF v_remaining > 0 THEN
    IF p_specific_lot_id IS NOT NULL THEN
      RAISE EXCEPTION
        'lot_residual_short: lot=% requested=% short=%',
        p_specific_lot_id, p_qty, v_remaining
        USING ERRCODE = 'P0006';
    ELSE
      RAISE EXCEPTION
        'lot_layers_exhausted: product=% location=% requested=% short=%',
        p_product_id, p_location_id, p_qty, v_remaining
        USING ERRCODE = 'P0006';
    END IF;
  END IF;
END;
$$;
