-- ============================================================
-- Phase D D4 — backfill inventory_movements from existing
-- posting_lines (acct-wb75.3.4).
--
-- Pre-D2/D3 posting_lines have no inventory_movements row. This
-- migration ships a reusable helper `_backfill_inventory_movements()`
-- and invokes it once at migration apply time. The helper can be
-- re-invoked operationally if late posts arrive that bypass
-- apply_event (e.g., direct DB writes for incident recovery).
--
-- Gate cascade mirrors the apply_event D-block:
--
--   1. JOIN posting_line_inventory inherits C1's filter — only
--      posting_lines with a resolvable SKU. Non-inventory qty
--      postings (e.g., by-product disposal_cost period-basis with
--      planned_qty as audit metadata) are skipped automatically
--      because they have no posting_line_inventory row.
--   2. ledger_kind='value' (movement is the cost-flow subledger).
--   3. cost_method_at_event ∈ {standard, wac_*} — FIFO/lot still
--      block at dispatcher (P0006) so they have no
--      posting_line_inventory rows yet (Phase E ships those).
--   4. at least one of (DR, CR) is inv_value_*.
--   5. location_id resolves (skip op_move_v / wo_close_v on
--      inv_value_wip-only postings — per-routing-op flow stays at
--      posting_lines grain).
--   6. event_type maps to non-NULL via _inventory_movement_event_type.
--
-- Idempotent via LEFT JOIN inventory_movements ... WHERE
-- im.posting_line_id IS NULL — re-running writes only posting_lines
-- without a movement. After D2/D3 ship, newly-applied apply_event
-- writes go through the dispatcher so re-running this is mostly a
-- no-op except for any genuinely-orphaned rows.
--
-- Sign convention + event_type derivation mirror the apply_event
-- D-block. standard_unit_cost via the tolerant lookup (NULL if no
-- standard at business_date).
--
-- Ordering: posted_at ASC + id ASC. Phase E (FIFO/lot) downstream
-- consumers will walk the subledger in cost-flow chronology;
-- preserve it now to avoid re-sorting later.
--
-- Scale note: at MVP, dev/test data is well under 1M posting_lines.
-- A single INSERT...SELECT is fine. Production-scale chunked
-- backfill (`WHERE pl.id BETWEEN N AND M`) is tracked as an
-- operational follow-up under acct-sbr2.
-- ============================================================

CREATE OR REPLACE FUNCTION _backfill_inventory_movements()
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_count BIGINT;
BEGIN
  WITH ins AS (
    INSERT INTO inventory_movements (
      product_id, legal_entity_id, cost_book_id, location_id,
      event_type, movement_date, quantity,
      standard_unit_cost, actual_unit_cost,
      cost_currency, posting_line_id, created_at
    )
    SELECT
      pli.product_id,
      c.legal_entity_id,
      1,
      COALESCE(c.location_id, d.location_id),
      _inventory_movement_event_type(
        pl.reason,
        CASE
          WHEN c.kind::TEXT LIKE 'inv_value_%' OR c.kind::TEXT LIKE 'stock_%'
          THEN -ABS(pl.qty)::NUMERIC
          ELSE  ABS(pl.qty)::NUMERIC
        END
      ),
      pl.business_date,
      CASE
        WHEN c.kind::TEXT LIKE 'inv_value_%' OR c.kind::TEXT LIKE 'stock_%'
        THEN -ABS(pl.qty)::NUMERIC
        ELSE  ABS(pl.qty)::NUMERIC
      END,
      (
        SELECT cost::NUMERIC
          FROM standard_costs sc
         WHERE sc.sku_id = pli.product_id
           AND sc.effective_at <= pl.business_date
         ORDER BY sc.effective_at DESC
         LIMIT 1
      ),
      pli.unit_cost,
      c.currency,
      pl.id,
      pl.posted_at
      FROM posting_lines pl
      INNER JOIN posting_line_inventory pli ON pli.posting_line_id = pl.id
      INNER JOIN accounts d ON d.id = pl.debit_account_id
      INNER JOIN accounts c ON c.id = pl.credit_account_id
       LEFT JOIN inventory_movements im ON im.posting_line_id = pl.id
     WHERE pli.cost_method_at_event IN
             ('standard', 'wac_perpetual', 'wac_periodic', 'wac_retroactive')
       AND d.ledger_kind = 'value'
       AND pl.qty IS NOT NULL
       AND pl.qty <> 0
       AND (d.kind::TEXT LIKE 'inv_value_%' OR c.kind::TEXT LIKE 'inv_value_%')
       AND COALESCE(c.location_id, d.location_id) IS NOT NULL
       AND _inventory_movement_event_type(
             pl.reason,
             CASE
               WHEN c.kind::TEXT LIKE 'inv_value_%' OR c.kind::TEXT LIKE 'stock_%'
               THEN -ABS(pl.qty)::NUMERIC
               ELSE  ABS(pl.qty)::NUMERIC
             END
           ) IS NOT NULL
       AND im.posting_line_id IS NULL
     ORDER BY pl.posted_at, pl.id
    RETURNING 1
  )
  SELECT COUNT(*)::BIGINT INTO v_count FROM ins;
  RETURN v_count;
END;
$$;

-- Run once at migration apply time. After D2/D3 are wired, any
-- pre-existing posting_line that lacks a movement gets one here.
-- Result is captured in a NOTICE for visibility (returned BIGINT
-- from a top-level SELECT is silently dropped by sqlx-migrate).

DO $$
DECLARE
  v_n BIGINT;
BEGIN
  v_n := _backfill_inventory_movements();
  RAISE NOTICE 'D4 backfill: inserted % inventory_movements row(s)', v_n;
END $$;
