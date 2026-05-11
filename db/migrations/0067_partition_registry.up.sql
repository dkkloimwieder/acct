-- ============================================================
-- acct-sbr2 — Partition rollover hardening: registry + recon + ops.
--
-- WHAT:
--   1. partitioned_tables_registry — central inventory of all
--      PARTITION BY RANGE parent tables in the schema, with their
--      bake window, partition column, cron job name, and a minimum
--      horizon below which run_daily_reconciliation should alert.
--   2. _partition_max_upper_bound(p_table) — walks pg_inherits +
--      pg_class.relpartbound to find the latest "TO ('YYYY-MM-DD')"
--      across a parent table's child partitions.
--   3. _extend_partition_horizon(p_table, p_months) — operator
--      escape hatch: calls the registered helper for the next N
--      months starting from the current max upper bound. Idempotent.
--   4. run_daily_reconciliation CREATE OR REPLACE — verbatim copy
--      from mig 0066 + new check #15 partition_horizon_low for any
--      registered table whose latest upper bound is less than
--      current_date + min_horizon_months.
--
-- WHY:
--   The schema now has SIX partitioned parent tables shipped across
--   four migrations (0025 inventory_movements / 0031 cost_layers +
--   cost_layer_depletions / 0044 inventory_lots + inventory_lot_events
--   / 0061 inventory_unit_events). Each ships its own monthly-bake
--   helper, a 24-month deploy-time bake loop (2026-01 through 2027-12),
--   and a pg_cron job at '0 0 25 * *' creating next-month + month-after
--   (tolerant of pg_cron unavailable). Items 1-2 of acct-sbr2's
--   acceptance criteria are therefore in place per-table; item 3
--   (centralized visibility for operators) was not. As more tables
--   get partitioned the audit cost of "does each rollover cron exist
--   + run + still have horizon" grows linearly without an index. The
--   registry collapses that to one lookup; check #15 turns horizon
--   drift into a recon alert; _extend_partition_horizon gives ops a
--   one-call ad-hoc bake-forward (e.g., before a long test cycle that
--   runs past the current horizon).
--
-- DESIGN CALLS:
--   - Composite of (bake_start, bake_end) per row keeps history of
--     the deploy-time window even after pg_cron extends past it.
--     bake_end is exclusive (matches the helper's WHILE v_d < ...).
--   - cron_job_name is the registered job in cron.job; the registry
--     does NOT enforce existence (some test/CI DBs lack pg_cron).
--     Operators verify via SELECT * FROM cron.job WHERE jobname = ...
--     manually if a horizon alert fires.
--   - Multiple tables can share a cron_job_name (mig 0031 cost_layers
--     + cost_layer_depletions share 'cost_layers_partition_rollover';
--     mig 0044 inventory_lots + inventory_lot_events share
--     'inventory_lots_partition_rollover'). The registry treats each
--     table independently — its own helper, its own horizon check.
--   - partition_helper_fn stored as plain text 'public._create_<x>'.
--     _extend_partition_horizon EXECUTE-formats SELECT %s($1).
--   - _partition_max_upper_bound parses pg_get_expr() text — there is
--     no exposed C-level partbound API. The expression shape is
--     'FOR VALUES FROM ('YYYY-MM-DD') TO ('YYYY-MM-DD')' for monthly
--     DATE partitions; the regex extracts the TO bound. Returns NULL
--     for an empty parent (no children) or a non-monthly partition.
--   - Check #15 fires when latest upper bound is NULL OR less than
--     current_date + min_horizon_months. Default min_horizon_months
--     is 3 — comfortable buffer above the cron's 2-month look-ahead
--     so a single missed cron run does not immediately alert; a
--     three-month gap means something is genuinely wrong.
--   - min_horizon_months is per-table — high-volume tables can be
--     tightened (e.g., 6 months); low-volume can stay at 3.
--   - The append-only trigger pattern used elsewhere is NOT applied
--     here. Registry rows are admin-managed config; UPDATE on
--     min_horizon_months / notes is intentional. INSERT/DELETE in
--     practice happens only at migration time when a new partitioned
--     table is added (or one is retired).
--
-- ACCEPTANCE NOTE (acct-sbr2 item 3):
--   CLAUDE.md "Partition lifecycle" subsection added in the same
--   change (sibling commit if it ends up split).
-- ============================================================

CREATE TABLE partitioned_tables_registry (
  table_name           TEXT PRIMARY KEY,
  partition_helper_fn  TEXT NOT NULL,
  partition_column     TEXT NOT NULL,
  cron_job_name        TEXT NOT NULL,
  bake_start           DATE NOT NULL,
  bake_end             DATE NOT NULL,
  min_horizon_months   INT  NOT NULL DEFAULT 3,
  notes                TEXT,
  registered_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CHECK (bake_start < bake_end),
  CHECK (min_horizon_months > 0)
);

CREATE INDEX partitioned_tables_registry_cron_job
  ON partitioned_tables_registry (cron_job_name);

INSERT INTO partitioned_tables_registry
  (table_name, partition_helper_fn, partition_column,
   cron_job_name, bake_start, bake_end, min_horizon_months, notes)
VALUES
  ('inventory_movements',
   'public._create_inventory_movements_partition',
   'movement_date',
   'inventory_movements_partition_rollover',
   DATE '2026-01-01', DATE '2028-01-01', 3,
   'Phase D KEYSTONE subledger (mig 0025). High write volume; ' ||
   'one row per inventory leg per posting_line.'),

  ('cost_layers',
   'public._create_cost_layers_partition',
   'receipt_date',
   'cost_layers_partition_rollover',
   DATE '2026-01-01', DATE '2028-01-01', 3,
   'Phase E1 FIFO inflow ledger (mig 0031). One row per receipt ' ||
   'on a fifo SKU; cron shared with cost_layer_depletions.'),

  ('cost_layer_depletions',
   'public._create_cost_layer_depletions_partition',
   'issue_date',
   'cost_layers_partition_rollover',
   DATE '2026-01-01', DATE '2028-01-01', 3,
   'Phase E1 FIFO outflow ledger (mig 0031). One row per layer ' ||
   'depletion; same cron as cost_layers.'),

  ('inventory_lots',
   'public._create_inventory_lots_partition',
   'receipt_date',
   'inventory_lots_partition_rollover',
   DATE '2026-01-01', DATE '2028-01-01', 3,
   'Phase E2 lot_fifo inflow ledger (mig 0044). One row per ' ||
   'received lot; cron shared with inventory_lot_events.'),

  ('inventory_lot_events',
   'public._create_inventory_lot_events_partition',
   'event_date',
   'inventory_lots_partition_rollover',
   DATE '2026-01-01', DATE '2028-01-01', 3,
   'Phase E2 lot lifecycle ledger (mig 0044). One row per lot ' ||
   'event (receipt/issue/transfer/hold/release/etc.).'),

  ('inventory_unit_events',
   'public._create_inventory_unit_events_partition',
   'event_date',
   'inventory_unit_events_partition_rollover',
   DATE '2026-01-01', DATE '2028-01-01', 3,
   'Phase E3 / sxl2 unit lifecycle ledger (mig 0061). One row per ' ||
   'unit event for lot_and_serial SKUs.');

-- ============================================================
-- _partition_max_upper_bound: walks the parent's child partitions
-- and returns the latest TO bound found, or NULL if the parent has
-- no children or a non-monthly partition shape.
--
-- pg_class.relpartbound is a partbound text representation reachable
-- via pg_get_expr. Shape for our monthly partitions:
--   FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')
-- Regex extracts the TO date.
-- ============================================================

CREATE OR REPLACE FUNCTION _partition_max_upper_bound(p_table TEXT)
RETURNS DATE
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_max  DATE := NULL;
  v_part RECORD;
  v_to   DATE;
  v_m    TEXT[];
BEGIN
  FOR v_part IN
    SELECT pg_get_expr(c.relpartbound, c.oid) AS bound_expr
      FROM pg_inherits i
      JOIN pg_class    c ON c.oid = i.inhrelid
     WHERE i.inhparent = p_table::regclass
  LOOP
    v_m := regexp_match(v_part.bound_expr, 'TO \(''([^'']+)''\)');
    IF v_m IS NULL THEN
      CONTINUE;
    END IF;
    BEGIN
      v_to := v_m[1]::DATE;
    EXCEPTION WHEN OTHERS THEN
      CONTINUE;
    END;
    IF v_max IS NULL OR v_to > v_max THEN
      v_max := v_to;
    END IF;
  END LOOP;
  RETURN v_max;
END;
$$;

-- ============================================================
-- _extend_partition_horizon: operator escape hatch. Calls the
-- registered helper for the next p_months starting from the
-- table's current max upper bound. Returns the count of partitions
-- the call attempted to create (helpers are CREATE TABLE IF NOT
-- EXISTS, so re-calls are idempotent). Returns -1 if the table is
-- not in the registry.
--
-- Example:
--   SELECT _extend_partition_horizon('inventory_movements', 6);
-- ============================================================

CREATE OR REPLACE FUNCTION _extend_partition_horizon(
  p_table  TEXT,
  p_months INT
) RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
  v_fn       TEXT;
  v_max      DATE;
  v_next     DATE;
  v_created  INT := 0;
  v_i        INT;
BEGIN
  IF p_months <= 0 THEN
    RAISE EXCEPTION 'p_months must be positive (got %)', p_months
      USING ERRCODE = 'P0006';
  END IF;

  SELECT partition_helper_fn
    INTO v_fn
    FROM partitioned_tables_registry
   WHERE table_name = p_table;

  IF v_fn IS NULL THEN
    RETURN -1;
  END IF;

  v_max := _partition_max_upper_bound(p_table);
  IF v_max IS NULL THEN
    v_max := date_trunc('month', current_date)::DATE;
  END IF;

  v_next := v_max;
  FOR v_i IN 1..p_months LOOP
    EXECUTE format('SELECT %s($1)', v_fn) USING v_next;
    v_created := v_created + 1;
    v_next := (v_next + INTERVAL '1 month')::DATE;
  END LOOP;

  RETURN v_created;
END;
$$;

-- ============================================================
-- run_daily_reconciliation CREATE OR REPLACE.
--
-- Body is byte-for-byte copy from mig 0066 (checks #1-#14) plus a
-- new check #15 partition_horizon_low inserted before RETURN.
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

  -- Check 14: lot_and_serial unit count ↔ lot residual.
  --
  -- For tracked_by='lot_and_serial' SKUs only. Per (lot_id,
  -- lot_receipt_date), COUNT(inventory_units WHERE status IN
  -- active) must equal the lot's residual qty (original_quantity
  -- + SUM(events.quantity_change)). Catches drift where:
  --   (a) phantom unit  — unit row at status=available but parent
  --       lot fully drained,
  --   (b) phantom residual — lot has residual but no active units.
  --
  -- Active statuses match the partial UNIQUE in mig 0061:
  -- available / reserved / allocated / on_hold / returned.
  -- Terminal statuses (shipped / consumed / transferred_out /
  -- scrapped / lost) correctly drop out — they should pair with
  -- the lot's matching outflow event.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  WITH lot_residual AS (
    SELECT il.lot_id,
           il.receipt_date,
           il.product_id,
           il.location_id,
           il.original_quantity + COALESCE(ev.total, 0) AS residual
      FROM inventory_lots il
      JOIN skus s ON s.id = il.product_id
      LEFT JOIN (
        SELECT lot_id, lot_receipt_date,
               SUM(quantity_change) AS total
          FROM inventory_lot_events
         GROUP BY lot_id, lot_receipt_date
      ) ev
        ON ev.lot_id           = il.lot_id
       AND ev.lot_receipt_date = il.receipt_date
     WHERE s.tracked_by = 'lot_and_serial'
  ),
  unit_count AS (
    SELECT iu.lot_id,
           iu.lot_receipt_date,
           COUNT(*) AS units
      FROM inventory_units iu
     WHERE iu.status IN ('available', 'reserved', 'allocated',
                         'on_hold', 'returned')
     GROUP BY iu.lot_id, iu.lot_receipt_date
  )
  SELECT 'lot_unit_count_mismatch',
         jsonb_build_object(
           'lot_id',           lr.lot_id,
           'lot_receipt_date', lr.receipt_date::TEXT,
           'product_id',       lr.product_id,
           'location_id',      lr.location_id,
           'residual',         lr.residual::TEXT,
           'unit_count',       COALESCE(uc.units, 0)::TEXT,
           'diff',             (lr.residual - COALESCE(uc.units, 0))::TEXT
         )
    FROM lot_residual lr
    LEFT JOIN unit_count uc
      ON uc.lot_id           = lr.lot_id
     AND uc.lot_receipt_date = lr.receipt_date
   WHERE lr.residual <> COALESCE(uc.units, 0);
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  -- Check 15: partition horizon low.
  --
  -- For each row in partitioned_tables_registry, fires when the
  -- latest upper bound across the parent's child partitions is
  -- NULL (no children at all) OR less than current_date +
  -- min_horizon_months. The cron rollover job at '0 0 25 * *'
  -- creates next-month + month-after partitions, so the default
  -- min_horizon_months of 3 gives a one-cron-miss buffer; a
  -- horizon of less than 3 months means something is genuinely
  -- wrong (cron disabled, helper raising, multiple missed runs).
  -- Operator escape hatch: SELECT _extend_partition_horizon(
  -- '<table>', <months>) creates partitions ad-hoc.
  INSERT INTO reconciliation_alerts (alert_kind, payload)
  SELECT 'partition_horizon_low',
         jsonb_build_object(
           'table_name',         r.table_name,
           'partition_column',   r.partition_column,
           'cron_job_name',      r.cron_job_name,
           'min_horizon_months', r.min_horizon_months,
           'latest_bound',
              COALESCE(_partition_max_upper_bound(r.table_name)::TEXT,
                       'none'),
           'threshold',
              (current_date + (r.min_horizon_months || ' months')
                 ::INTERVAL)::DATE::TEXT
         )
    FROM partitioned_tables_registry r
   WHERE _partition_max_upper_bound(r.table_name) IS NULL
      OR _partition_max_upper_bound(r.table_name)
           < (current_date + (r.min_horizon_months || ' months')
                ::INTERVAL)::DATE;
  GET DIAGNOSTICS v_step = ROW_COUNT;
  v_total := v_total + v_step;

  RETURN v_total;
END;
$$;
