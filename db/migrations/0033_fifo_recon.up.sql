-- ============================================================
-- Phase E1 E1.3 — FIFO layer residual recon (acct-swwp,
-- sub-issue of acct-cbss).
--
-- Adds check #8 'fifo_layer_residual_mismatch' to
-- run_daily_reconciliation. Per (product_id, location_id) for
-- FIFO SKUs:
--
--   layer_residual = SUM(cost_layers.original_quantity)
--                  - SUM(cost_layer_depletions.depleted_quantity)
--   on_hand        = stock_available.debits_total - .credits_total
--
-- They must match exactly. Drift signals a bug — value-leg without
-- corresponding qty-leg, layer creation without stock_available
-- debit, or layer/depletion direct INSERT bypassing apply_event.
--
-- Filter: only inspect (product_id, location_id) pairs for SKUs
-- whose cost_method = 'fifo'. Other cost methods don't produce
-- layers; the GL inventory check #7 already covers their integrity.
--
-- The residual is computed as a pair of SUMs over partitioned
-- tables — the planner picks up cost_layers_fifo_walk index for
-- the layers side and cost_layer_depletions_layer index for
-- depletions.
-- ============================================================

CREATE OR REPLACE FUNCTION run_daily_reconciliation() RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
  v_total INT := 0;
  v_step  INT;
BEGIN
  -- Check 1: per-ledger double-entry.
  WITH imbalances AS (
    SELECT ledger_kind, currency,
           SUM(debits_total)::BIGINT  AS dr,
           SUM(credits_total)::BIGINT AS cr
      FROM accounts
     GROUP BY ledger_kind, currency
    HAVING SUM(debits_total) <> SUM(credits_total)
  )
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'double_entry_imbalance',
         jsonb_build_object(
           'ledger_kind', ledger_kind,
           'currency',    currency,
           'debits',      dr,
           'credits',     cr,
           'imbalance',   dr - cr
         )
    FROM imbalances;
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 2: reservation over-promise.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'reservation_over_promise',
         jsonb_build_object(
           'sku_id',      a.sku_id,
           'location_id', a.location_id,
           'on_hand',     (a.debits_total - a.credits_total),
           'reserved',    COALESCE(r.total, 0),
           'deficit',     (a.debits_total - a.credits_total) - COALESCE(r.total, 0)
         )
    FROM accounts a
    LEFT JOIN (
      SELECT sku_id, location_id, SUM(qty)::BIGINT AS total
        FROM inventory_reservations
       WHERE status = 'active'
       GROUP BY sku_id, location_id
    ) r ON r.sku_id = a.sku_id AND r.location_id = a.location_id
   WHERE a.kind = 'stock_available'
     AND NOT a.is_closed
     AND (a.debits_total - a.credits_total) < COALESCE(r.total, 0);
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 3: B2 currency extension amount consistency.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'currency_extension_amount_mismatch',
         jsonb_build_object(
           'posting_line_id',      plc.posting_line_id,
           'amount_transaction',   plc.amount_transaction,
           'amount',               pl.amount,
           'currency_transaction', plc.currency_transaction,
           'fx_rate_to_functional',plc.fx_rate_to_functional::TEXT
         )
    FROM posting_line_currencies plc
    JOIN posting_lines pl ON pl.id = plc.posting_line_id
   WHERE plc.amount_transaction <> pl.amount;
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 4: B3 dimensions — product coverage.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'dimension_product_missing',
         jsonb_build_object(
           'posting_line_id',  pl.id,
           'expected_sku_id',  COALESCE(c.sku_id, d.sku_id)
         )
    FROM posting_lines pl
    JOIN accounts c ON c.id = pl.credit_account_id
    JOIN accounts d ON d.id = pl.debit_account_id
    LEFT JOIN posting_line_dimensions pld
      ON pld.posting_line_id = pl.id AND pld.dimension_type = 3
   WHERE COALESCE(c.sku_id, d.sku_id) IS NOT NULL
     AND pld.posting_line_id IS NULL;
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 5: B3 dimensions — counterparty coverage.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'dimension_counterparty_missing',
         jsonb_build_object(
           'posting_line_id', pl.id,
           'expected_kind',
              CASE
                WHEN c.kind IN ('ar','ar_unsettled','customer_pool')
                  OR d.kind IN ('ar','ar_unsettled','customer_pool')
                  THEN 'customer'
                ELSE 'vendor'
              END
         )
    FROM posting_lines pl
    JOIN accounts c ON c.id = pl.credit_account_id
    JOIN accounts d ON d.id = pl.debit_account_id
    LEFT JOIN posting_line_dimensions pld
      ON pld.posting_line_id = pl.id AND pld.dimension_type IN (1, 2)
   WHERE COALESCE(pl.counterparty_id, c.counterparty_id, d.counterparty_id) IS NOT NULL
     AND (c.kind IN ('ar','ar_unsettled','customer_pool',
                     'ap','ap_unsettled','vendor_pool','accrued_disposal_liability')
       OR d.kind IN ('ar','ar_unsettled','customer_pool',
                     'ap','ap_unsettled','vendor_pool','accrued_disposal_liability'))
     AND pld.posting_line_id IS NULL;
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 6: C inventory extension count match.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'inventory_extension_missing',
         jsonb_build_object(
           'posting_line_id', pl.id,
           'reason',          pl.reason::TEXT,
           'qty',             pl.qty
         )
    FROM posting_lines pl
    JOIN accounts c ON c.id = pl.credit_account_id
    JOIN accounts d ON d.id = pl.debit_account_id
    LEFT JOIN posting_line_inventory pli ON pli.posting_line_id = pl.id
   WHERE pl.qty IS NOT NULL
     AND COALESCE(c.sku_id, d.sku_id) IS NOT NULL
     AND pli.posting_line_id IS NULL;
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 7: D subledger ↔ GL divergence per (product, location, period).
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  WITH gl_inv AS (
    SELECT
      d.sku_id        AS product_id,
      d.location_id   AS location_id,
      pl.period_id    AS period_id,
      pl.amount::NUMERIC AS gl_net
      FROM posting_lines pl
      JOIN accounts d ON d.id = pl.debit_account_id
     WHERE d.kind::TEXT LIKE 'inv_value_%'
       AND d.sku_id IS NOT NULL
       AND d.location_id IS NOT NULL
       AND pl.qty IS NOT NULL
    UNION ALL
    SELECT
      c.sku_id,
      c.location_id,
      pl.period_id,
      -pl.amount::NUMERIC
      FROM posting_lines pl
      JOIN accounts c ON c.id = pl.credit_account_id
     WHERE c.kind::TEXT LIKE 'inv_value_%'
       AND c.sku_id IS NOT NULL
       AND c.location_id IS NOT NULL
       AND pl.qty IS NOT NULL
  ),
  gl AS (
    SELECT product_id, location_id, period_id,
           SUM(gl_net) AS gl_net
      FROM gl_inv
     GROUP BY 1, 2, 3
  ),
  sub AS (
    SELECT
      im.product_id,
      im.location_id,
      p.id AS period_id,
      SUM(im.quantity * im.actual_unit_cost)::NUMERIC AS sub_net
      FROM inventory_movements im
      JOIN periods p
        ON p.opens_at  <= im.movement_date
       AND p.closes_at >= im.movement_date
     GROUP BY 1, 2, 3
  )
  SELECT 'subledger_gl_divergence',
         jsonb_build_object(
           'product_id',  COALESCE(g.product_id, s.product_id),
           'location_id', COALESCE(g.location_id, s.location_id),
           'period_id',   COALESCE(g.period_id, s.period_id),
           'gl_net',      COALESCE(g.gl_net, 0)::TEXT,
           'sub_net',     COALESCE(s.sub_net, 0)::TEXT,
           'diff',        (COALESCE(g.gl_net, 0) - COALESCE(s.sub_net, 0))::TEXT
         )
    FROM gl g
    FULL OUTER JOIN sub s
      ON g.product_id  = s.product_id
     AND g.location_id = s.location_id
     AND g.period_id   = s.period_id
   WHERE ABS(COALESCE(g.gl_net, 0) - COALESCE(s.sub_net, 0)) > 1;
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 8: FIFO layer residual ↔ on-hand qty.
  --
  -- Per FIFO (product, location) the layer residual must equal the
  -- stock_available net qty. Compares NUMERIC(19,6) for layers vs
  -- BIGINT for stock_available — exact match required.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  WITH layers AS (
    SELECT cl.product_id,
           cl.location_id,
           SUM(cl.original_quantity) AS layer_total
      FROM cost_layers cl
      JOIN skus s ON s.id = cl.product_id
     WHERE s.cost_method = 'fifo'
     GROUP BY 1, 2
  ),
  depletions AS (
    SELECT cl.product_id,
           cl.location_id,
           COALESCE(SUM(d.depleted_quantity), 0) AS depleted_total
      FROM cost_layers cl
      JOIN skus s ON s.id = cl.product_id
      LEFT JOIN cost_layer_depletions d
        ON d.layer_id = cl.layer_id
       AND d.layer_receipt_date = cl.receipt_date
     WHERE s.cost_method = 'fifo'
     GROUP BY 1, 2
  ),
  layer_residual AS (
    SELECT l.product_id,
           l.location_id,
           l.layer_total - d.depleted_total AS residual
      FROM layers l
      JOIN depletions d
        ON d.product_id  = l.product_id
       AND d.location_id = l.location_id
  ),
  on_hand AS (
    SELECT a.sku_id      AS product_id,
           a.location_id AS location_id,
           (a.debits_total - a.credits_total)::NUMERIC AS qty
      FROM accounts a
      JOIN skus s ON s.id = a.sku_id
     WHERE a.kind = 'stock_available'
       AND s.cost_method = 'fifo'
       AND NOT a.is_closed
  )
  SELECT 'fifo_layer_residual_mismatch',
         jsonb_build_object(
           'product_id',     COALESCE(lr.product_id, oh.product_id),
           'location_id',    COALESCE(lr.location_id, oh.location_id),
           'layer_residual', COALESCE(lr.residual, 0)::TEXT,
           'on_hand',        COALESCE(oh.qty, 0)::TEXT,
           'diff',           (COALESCE(lr.residual, 0) - COALESCE(oh.qty, 0))::TEXT
         )
    FROM layer_residual lr
    FULL OUTER JOIN on_hand oh
      ON oh.product_id  = lr.product_id
     AND oh.location_id = lr.location_id
   WHERE COALESCE(lr.residual, 0) <> COALESCE(oh.qty, 0);
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  RETURN v_total;
END;
$$;
