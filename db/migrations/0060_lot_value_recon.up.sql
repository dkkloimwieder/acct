-- ============================================================
-- acct-20y0 — per-lot value-level subledger ↔ GL recon (check #13).
--
-- WHAT:
--   Extends run_daily_reconciliation with a deeper variant of
--   check #7. Check #7 reconciles GL inv_value_* postings against
--   inventory_movements at (product, location, period) grain.
--   Check #13 adds lot_id to the grouping for lot_fifo SKUs.
--
-- WHY:
--   Check #7 catches drift where the GL and the subledger disagree
--   on per-(product, location, period) net value. Check #13 catches
--   drift WITHIN that grain — when a FIFO/FEFO walk depletes lot A
--   but the apply_event D-block stamped lot B's id on
--   inventory_movements, or when posting_line_inventory.lot_id
--   wasn't stamped for a lot-touching leg. The two stamping points
--   (inventory_movements.lot_id, posting_line_inventory.lot_id)
--   must AGREE per-lot.
--
--   The keystone D5 invariant ("subledger ↔ GL at product/location/
--   period grain") guaranteed value integrity at the bucket level;
--   check #13 promotes that guarantee to the lot level for
--   lot-tracked SKUs.
--
-- HOW (Option A — subledger-based):
--   Mirrors check #7's structure with these extensions:
--     * GL CTE: filters posting_line_inventory.lot_id IS NOT NULL,
--       includes pli.lot_id in the GROUP BY.
--     * Subledger CTE: filters inventory_movements.lot_id IS NOT
--       NULL, includes im.lot_id in the GROUP BY.
--     * FULL OUTER JOIN on the 4-tuple (product, location, period,
--       lot_id).
--     * 1¢ tolerance matches check #7.
--
--   Option B (accounts.lot_id partition at inv_value_*) was
--   considered and DEFERRED: it would multiply account count by
--   active lot count and require dispatcher + wrapper + close-hook
--   reflows. Option A leverages the existing per-leg lot stamps
--   without schema changes; if Option B is later adopted, check
--   #13 still composes (the per-lot account balances become an
--   additional GL source that can be checked separately).
--
-- DESIGN CALLS (from saved plan-finish-lot-epic-2026-05-10 §20y0):
--   Q1 tolerance: 1¢ matches check #7. Confirmed.
--   Q2 period boundary: inventory_movements has movement_date
--     (joined to periods.opens_at/closes_at) while posting_lines
--     has period_id directly. apply_event sets pl.period_id from
--     pl.business_date and sets im.movement_date = pl.business_date,
--     so the join boundaries align by construction.
--
-- NOT IN SCOPE:
--   - Per-lot value-level negative residual sentinel. Check #10
--     already covers per-lot QTY negative residuals; value-side
--     would require Option B's per-lot accounts.
--   - Catching lot stamps that ARE consistent between subledger
--     and GL but POINT AT THE WRONG LOT (e.g., FIFO walk picked
--     lot B then both inventory_movements.lot_id and pli.lot_id
--     get stamped as B even though it should have been A). That
--     requires an external truth source (the FIFO order from
--     inventory_lots.receipt_date and the planned consumption
--     order); deferred as acct-20y0-followup-walk-correctness.
--   - inventory_movements has no append-only trigger today; the
--     phantom-row synthesis pattern in T1 R2 relies on direct
--     INSERTs. Append-only enforcement is a separate cross-cutting
--     concern (acct-du2 epic adjacency).
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

  -- Check 9: lot residual ↔ on-hand qty (lot_fifo SKUs).
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  WITH lot_aggr AS (
    SELECT il.product_id,
           il.location_id,
           SUM(il.original_quantity) AS lot_total
      FROM inventory_lots il
      JOIN skus s ON s.id = il.product_id
     WHERE s.cost_method = 'lot_fifo'
     GROUP BY 1, 2
  ),
  event_aggr AS (
    SELECT il.product_id,
           il.location_id,
           COALESCE(SUM(ev.quantity_change), 0) AS event_total
      FROM inventory_lots il
      JOIN skus s ON s.id = il.product_id
      LEFT JOIN inventory_lot_events ev
        ON ev.lot_id           = il.lot_id
       AND ev.lot_receipt_date = il.receipt_date
     WHERE s.cost_method = 'lot_fifo'
     GROUP BY 1, 2
  ),
  lot_residual AS (
    SELECT l.product_id,
           l.location_id,
           l.lot_total + e.event_total AS residual
      FROM lot_aggr l
      JOIN event_aggr e
        ON e.product_id  = l.product_id
       AND e.location_id = l.location_id
  ),
  on_hand AS (
    SELECT a.sku_id      AS product_id,
           a.location_id AS location_id,
           SUM(a.debits_total - a.credits_total)::NUMERIC AS qty
      FROM accounts a
      JOIN skus s ON s.id = a.sku_id
     WHERE a.kind = 'stock_available'
       AND s.cost_method = 'lot_fifo'
       AND NOT a.is_closed
     GROUP BY 1, 2
  )
  SELECT 'lot_residual_mismatch',
         jsonb_build_object(
           'product_id',   COALESCE(lr.product_id, oh.product_id),
           'location_id',  COALESCE(lr.location_id, oh.location_id),
           'lot_residual', COALESCE(lr.residual, 0)::TEXT,
           'on_hand',      COALESCE(oh.qty, 0)::TEXT,
           'diff',         (COALESCE(lr.residual, 0) - COALESCE(oh.qty, 0))::TEXT
         )
    FROM lot_residual lr
    FULL OUTER JOIN on_hand oh
      ON oh.product_id  = lr.product_id
     AND oh.location_id = lr.location_id
   WHERE COALESCE(lr.residual, 0) <> COALESCE(oh.qty, 0);
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 10: per-lot negative residual sentinel (lot_fifo SKUs).
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'lot_negative_residual',
         jsonb_build_object(
           'lot_id',            il.lot_id,
           'lot_receipt_date',  il.receipt_date::TEXT,
           'product_id',        il.product_id,
           'location_id',       il.location_id,
           'lot_code',          il.lot_code,
           'original_quantity', il.original_quantity::TEXT,
           'event_total',       COALESCE(ev_sum.total, 0)::TEXT,
           'residual',          (il.original_quantity + COALESCE(ev_sum.total, 0))::TEXT
         )
    FROM inventory_lots il
    JOIN skus s ON s.id = il.product_id
    LEFT JOIN (
      SELECT lot_id, lot_receipt_date,
             SUM(quantity_change) AS total
        FROM inventory_lot_events
       GROUP BY lot_id, lot_receipt_date
    ) ev_sum
      ON ev_sum.lot_id           = il.lot_id
     AND ev_sum.lot_receipt_date = il.receipt_date
   WHERE s.cost_method = 'lot_fifo'
     AND (il.original_quantity + COALESCE(ev_sum.total, 0)) < 0;
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 11a: forward — orphan rm_issue_to_wo on lot_fifo component
  -- lacking wo_lot_consumption coverage.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'wo_lot_consumption_orphan_forward',
         jsonb_build_object(
           'posting_line_id',  pl.id,
           'document_id',      pl.document_id,
           'component_sku_id', a_c.sku_id
         )
    FROM posting_lines pl
    JOIN accounts a_d ON a_d.id = pl.debit_account_id
    JOIN accounts a_c ON a_c.id = pl.credit_account_id
    JOIN skus       s ON s.id   = a_c.sku_id
   WHERE pl.reason = 'rm_issue_to_wo'
     AND a_d.kind  = 'inv_value_wip'
     AND a_c.kind  = 'inv_value_raw'
     AND s.cost_method = 'lot_fifo'
     AND NOT EXISTS (
       SELECT 1 FROM wo_lot_consumption wlc
        WHERE wlc.posting_line_id = pl.id
     );
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 11b: inverse — wo_lot_consumption row lacks matching
  -- inventory_lot_events type=1 issue with same posting_line_id +
  -- lot identity + qty.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'wo_lot_consumption_orphan_inverse',
         jsonb_build_object(
           'wo_lot_consumption_id', wlc.id,
           'posting_line_id',       wlc.posting_line_id,
           'lot_id',                wlc.lot_id,
           'lot_receipt_date',      wlc.lot_receipt_date::TEXT,
           'qty',                   wlc.qty::TEXT
         )
    FROM wo_lot_consumption wlc
   WHERE NOT EXISTS (
     SELECT 1 FROM inventory_lot_events ev
      WHERE ev.posting_line_id  = wlc.posting_line_id
        AND ev.lot_id           = wlc.lot_id
        AND ev.lot_receipt_date = wlc.lot_receipt_date
        AND ev.event_type       = 1
        AND ev.quantity_change  = -wlc.qty
   );
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 12: lot_genealogy qty_consumed overshoot per parent lot.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  WITH genealogy_per_lot AS (
    SELECT parent_lot_id, parent_receipt_date,
           SUM(qty_consumed) AS g_total
      FROM lot_genealogy
     GROUP BY 1, 2
  ),
  events_per_lot AS (
    SELECT lot_id AS parent_lot_id,
           lot_receipt_date AS parent_receipt_date,
           ABS(SUM(quantity_change)) AS e_total
      FROM inventory_lot_events
     WHERE event_type IN (1, 5, 8)
     GROUP BY 1, 2
  )
  SELECT 'lot_genealogy_qty_overshoot',
         jsonb_build_object(
           'parent_lot_id',       g.parent_lot_id,
           'parent_receipt_date', g.parent_receipt_date::TEXT,
           'genealogy_total',     g.g_total::TEXT,
           'events_total',        COALESCE(e.e_total, 0)::TEXT,
           'overshoot',           (g.g_total - COALESCE(e.e_total, 0))::TEXT
         )
    FROM genealogy_per_lot g
    LEFT JOIN events_per_lot e
      ON e.parent_lot_id       = g.parent_lot_id
     AND e.parent_receipt_date = g.parent_receipt_date
   WHERE g.g_total > COALESCE(e.e_total, 0);
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 13: per-lot value-level subledger ↔ GL divergence.
  --
  -- Mirrors check #7 with lot_id added to the grouping. Catches
  -- per-lot stamping disagreement between inventory_movements
  -- (subledger) and posting_line_inventory (GL). 1¢ tolerance
  -- consistent with check #7.
  --
  -- Filter both sides to lot_id IS NOT NULL so the check only
  -- fires for lot-stamped legs; non-lot postings are out of scope
  -- (covered by #7's coarser grain).
  --
  -- lot_transfer EXCLUDED. posting_line_inventory is per-posting-
  -- line not per-leg, so a single pli.lot_id can carry only ONE
  -- lot identity per posting_line. Per mig 0057, lot_transfer
  -- stamps pli.lot_id with the SOURCE lot (mirrors mig 0046's
  -- issue-side convention) while inventory_movements stamps per
  -- leg (source on CR/FROM, dest on DR/TO). The two stamps
  -- legitimately disagree at the destination side, which would
  -- false-fire check #13. inventory_movements' per-leg per-lot
  -- attribution is the canonical truth for lot_transfer — check
  -- #9 (qty residual) and the per-leg stamping in mig 0057's
  -- lot_transfer_lines audit fields cover the per-lot integrity
  -- guarantee on its own.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  WITH gl_inv_lot AS (
    SELECT
      d.sku_id        AS product_id,
      d.location_id   AS location_id,
      pl.period_id    AS period_id,
      pli.lot_id      AS lot_id,
      pl.amount::NUMERIC AS gl_net
      FROM posting_lines pl
      JOIN accounts d ON d.id = pl.debit_account_id
      JOIN posting_line_inventory pli ON pli.posting_line_id = pl.id
     WHERE d.kind::TEXT LIKE 'inv_value_%'
       AND d.sku_id IS NOT NULL
       AND d.location_id IS NOT NULL
       AND pl.qty IS NOT NULL
       AND pli.lot_id IS NOT NULL
       AND pl.reason <> 'lot_transfer'
    UNION ALL
    SELECT
      c.sku_id,
      c.location_id,
      pl.period_id,
      pli.lot_id,
      -pl.amount::NUMERIC
      FROM posting_lines pl
      JOIN accounts c ON c.id = pl.credit_account_id
      JOIN posting_line_inventory pli ON pli.posting_line_id = pl.id
     WHERE c.kind::TEXT LIKE 'inv_value_%'
       AND c.sku_id IS NOT NULL
       AND c.location_id IS NOT NULL
       AND pl.qty IS NOT NULL
       AND pli.lot_id IS NOT NULL
       AND pl.reason <> 'lot_transfer'
  ),
  gl_lot AS (
    SELECT product_id, location_id, period_id, lot_id,
           SUM(gl_net) AS gl_net
      FROM gl_inv_lot
     GROUP BY 1, 2, 3, 4
  ),
  sub_lot AS (
    SELECT
      im.product_id,
      im.location_id,
      p.id AS period_id,
      im.lot_id,
      SUM(im.quantity * im.actual_unit_cost)::NUMERIC AS sub_net
      FROM inventory_movements im
      JOIN periods p
        ON p.opens_at  <= im.movement_date
       AND p.closes_at >= im.movement_date
     WHERE im.lot_id IS NOT NULL
       AND NOT EXISTS (
         SELECT 1 FROM posting_lines pl
          WHERE pl.id = im.posting_line_id
            AND pl.reason = 'lot_transfer'
       )
     GROUP BY 1, 2, 3, 4
  )
  SELECT 'subledger_gl_lot_divergence',
         jsonb_build_object(
           'product_id',  COALESCE(g.product_id, s.product_id),
           'location_id', COALESCE(g.location_id, s.location_id),
           'period_id',   COALESCE(g.period_id, s.period_id),
           'lot_id',      COALESCE(g.lot_id, s.lot_id),
           'gl_net',      COALESCE(g.gl_net, 0)::TEXT,
           'sub_net',     COALESCE(s.sub_net, 0)::TEXT,
           'diff',        (COALESCE(g.gl_net, 0) - COALESCE(s.sub_net, 0))::TEXT
         )
    FROM gl_lot g
    FULL OUTER JOIN sub_lot s
      ON g.product_id  = s.product_id
     AND g.location_id = s.location_id
     AND g.period_id   = s.period_id
     AND g.lot_id      = s.lot_id
   WHERE ABS(COALESCE(g.gl_net, 0) - COALESCE(s.sub_net, 0)) > 1;
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  RETURN v_total;
END;
$$;
