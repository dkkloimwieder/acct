-- Reverts E2.7 — restores _lot_walk_layers to mig 0046's body
-- (single FIFO-by-receipt walk), drops skus.allocation_strategy
-- column, drops allocation_strategy enum.

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
BEGIN
  IF p_qty IS NULL OR p_qty <= 0 THEN
    RAISE EXCEPTION 'lot_walk_invalid_qty: % must be positive', p_qty
      USING ERRCODE = 'P0006';
  END IF;

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

ALTER TABLE skus DROP COLUMN allocation_strategy;

DROP TYPE allocation_strategy;
