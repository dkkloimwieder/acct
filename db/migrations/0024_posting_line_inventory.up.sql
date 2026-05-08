-- ============================================================
-- Phase C — posting_line_inventory extension (acct-wb75.2.2)
--
-- Two-step shape mirroring 0022 (B2) and 0023 (B3):
--   1. CREATE TABLE posting_line_inventory.
--   2. CREATE OR REPLACE _post_posting_lines_apply_event with the
--      C-block appended after B3, before RETURN.
--   3. Backfill from existing posting_lines where qty IS NOT NULL.
--   4. CREATE OR REPLACE run_daily_reconciliation with check #6.
--
-- Design (research/posting-lines-convergence-plan.md §4.C):
--   - product_id = COALESCE(credit.sku_id, debit.sku_id) (R2 credit-first).
--   - quantity   = ABS(posting_lines.qty), NUMERIC(19,6).
--   - qty_uom    = 'EA' default (no UOM column on accounts/skus today).
--   - unit_cost  = NULLIF(amount,0)/NULLIF(quantity,0) for value-leg;
--                  NULL for qty-leg. Carries 4 decimals for partial-cents.
--   - cost_method_at_event = sku.cost_method (snapshot like
--                  cost_method_at_receipt — acct-6d8 pattern).
--   - cost_layer_id / lot_id / unit_id NULL until Phase E ships.
--
-- Pre-condition: Phase C C0 preflight audit (acct-wb75.2.1) confirmed
-- 0 exceptions — every posting_line with qty IS NOT NULL has resolvable
-- COALESCE(credit.sku_id, debit.sku_id). Backfill is safe to run
-- without per-row exception handling.
-- ============================================================

CREATE TABLE posting_line_inventory (
  posting_line_id      BIGINT PRIMARY KEY REFERENCES posting_lines(id),
  product_id           UUID NOT NULL REFERENCES skus(id),
  quantity             NUMERIC(19, 6) NOT NULL,
  qty_uom              VARCHAR(10) NOT NULL DEFAULT 'EA',
  unit_cost            NUMERIC(19, 4),
  cost_layer_id        BIGINT,                          -- FK target Phase E1
  lot_id               BIGINT,                          -- FK target Phase E2
  unit_id              BIGINT,                          -- FK target Phase E3
  cost_method_at_event cost_method NOT NULL,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CHECK (quantity > 0),
  CHECK (unit_cost IS NULL OR unit_cost >= 0)
);

CREATE INDEX posting_line_inventory_product
  ON posting_line_inventory (product_id);
CREATE INDEX posting_line_inventory_cost_layer
  ON posting_line_inventory (cost_layer_id) WHERE cost_layer_id IS NOT NULL;
CREATE INDEX posting_line_inventory_lot
  ON posting_line_inventory (lot_id) WHERE lot_id IS NOT NULL;
CREATE INDEX posting_line_inventory_unit
  ON posting_line_inventory (unit_id) WHERE unit_id IS NOT NULL;

-- ============================================================
-- _post_posting_lines_apply_event — extended with C inventory write.
--
-- Body identical to 0023's version through the B3 extension block,
-- with a new C block appended before RETURN.
-- ============================================================

CREATE OR REPLACE FUNCTION _post_posting_lines_apply_event(
  p_event           JSONB,
  p_idx             INT,
  p_amount          BIGINT,
  p_d_acct          accounts,
  p_c_acct          accounts,
  p_cost_method     cost_method,
  p_override_closed BOOLEAN
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_period_id        BIGINT;
  v_period_closed    TIMESTAMPTZ;
  v_business_date    DATE;
  v_qty_for_row      BIGINT;
  v_reason           posting_line_reason;
  v_idem_key         UUID;
  v_new_id           BIGINT;
  v_event_qty        BIGINT;
  v_resolved_cm      cost_method;
  v_cost_sku         UUID;
  v_reverses_id      BIGINT;
  v_parent_doc       UUID;
  v_ic_pair          UUID;
  v_proc             VARCHAR;
  v_functional_ccy   CHAR(3);
  v_fx_rate          NUMERIC(20, 10);
  v_dim_sku          UUID;
  v_dim_loc          UUID;
  v_dim_routing_op   INT;
  v_event_cp         UUID;
  v_dim_cp           UUID;
  v_dim_cp_type      SMALLINT;
  v_inv_unit_cost    NUMERIC(19, 4);
  v_inv_cost_method  cost_method;
BEGIN
  v_business_date := (p_event->>'business_date')::DATE;
  v_reason        := (p_event->>'reason')::posting_line_reason;
  v_idem_key      := (p_event->>'idempotency_key')::UUID;

  IF p_d_acct.is_closed OR p_c_acct.is_closed THEN
    RAISE EXCEPTION 'account_closed: event index %', p_idx
      USING ERRCODE = 'P0001';
  END IF;
  IF p_d_acct.ledger_kind <> p_c_acct.ledger_kind THEN
    RAISE EXCEPTION 'ledger_mismatch: event index % (% vs %)',
                    p_idx, p_d_acct.ledger_kind, p_c_acct.ledger_kind
      USING ERRCODE = 'P0002';
  END IF;
  IF p_d_acct.ledger_kind = 'value'
     AND p_d_acct.currency <> p_c_acct.currency THEN
    RAISE EXCEPTION 'currency_mismatch: event index % (% vs %)',
                    p_idx, p_d_acct.currency, p_c_acct.currency
      USING ERRCODE = 'P0003';
  END IF;

  SELECT id, closed_at INTO v_period_id, v_period_closed
    FROM periods
   WHERE opens_at <= v_business_date AND closes_at >= v_business_date;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'period_missing: event index % business_date %',
                    p_idx, v_business_date USING ERRCODE = 'P0004';
  END IF;
  IF v_period_closed IS NOT NULL AND NOT p_override_closed THEN
    RAISE EXCEPTION 'period_closed: event index % business_date %',
                    p_idx, v_business_date USING ERRCODE = 'P0005';
  END IF;

  v_qty_for_row := (p_event->>'qty')::BIGINT;
  IF v_qty_for_row IS NULL
     AND p_d_acct.ledger_kind = 'qty'
     AND p_c_acct.ledger_kind = 'qty' THEN
    v_qty_for_row := p_amount;
  END IF;

  UPDATE accounts SET debits_total  = debits_total  + p_amount
    WHERE id = p_d_acct.id;
  UPDATE accounts SET credits_total = credits_total + p_amount
    WHERE id = p_c_acct.id;
  INSERT INTO posting_lines (
    reason, document_kind, document_id, document_line_id,
    debit_account_id, credit_account_id, amount, qty,
    routing_op, counterparty_id, period_id, business_date,
    idempotency_key, posted_by
  ) VALUES (
    v_reason, p_event->>'document_kind', (p_event->>'document_id')::UUID,
    (p_event->>'document_line_id')::UUID, p_d_acct.id, p_c_acct.id,
    p_amount, v_qty_for_row,
    (p_event->>'routing_op')::INT, (p_event->>'counterparty_id')::UUID,
    v_period_id, v_business_date, v_idem_key,
    (p_event->>'posted_by')::UUID
  ) RETURNING id INTO v_new_id;

  -- Provisional flag for wac_periodic / wac_retroactive depletions.
  IF v_reason IN ('op_move','scrap','wo_complete','so_ship',
                  'op_move_v','scrap_v','wo_complete_v',
                  'rm_issue_to_wo')
     AND p_d_acct.ledger_kind = 'value' THEN
    v_resolved_cm := p_cost_method;
    IF v_resolved_cm IS NULL THEN
      v_cost_sku := COALESCE(p_c_acct.sku_id, p_d_acct.sku_id);
      IF v_cost_sku IS NOT NULL THEN
        SELECT cost_method INTO v_resolved_cm FROM skus WHERE id = v_cost_sku;
      END IF;
    END IF;
    IF v_resolved_cm IN ('wac_periodic', 'wac_retroactive') THEN
      v_event_qty := (p_event->>'qty')::BIGINT;
      INSERT INTO posting_lines_provisional (posting_line_id, period_id, cost_method, qty)
      VALUES (v_new_id, v_period_id, v_resolved_cm, v_event_qty);
    END IF;
  END IF;

  -- B1 extension write.
  v_reverses_id := (p_event->>'reverses_posting_line_id')::BIGINT;
  v_parent_doc  := (p_event->>'parent_document_id')::UUID;
  v_ic_pair     := (p_event->>'intercompany_pair_id')::UUID;
  v_proc        := p_event->>'created_by_process';
  IF v_reverses_id IS NOT NULL
     OR v_parent_doc  IS NOT NULL
     OR v_ic_pair     IS NOT NULL
     OR v_proc        IS NOT NULL THEN
    INSERT INTO posting_line_sources (
      posting_line_id, reverses_posting_line_id, parent_document_id,
      intercompany_pair_id, created_by_process
    ) VALUES (
      v_new_id, v_reverses_id, v_parent_doc, v_ic_pair, v_proc
    );
  END IF;

  -- B2 extension write.
  IF p_c_acct.ledger_kind = 'value' THEN
    SELECT functional_currency INTO v_functional_ccy
      FROM legal_entities WHERE id = p_c_acct.legal_entity_id;

    IF v_functional_ccy IS NOT NULL
       AND p_c_acct.currency <> v_functional_ccy THEN
      SELECT rate INTO v_fx_rate
        FROM fx_rates
       WHERE from_currency = p_c_acct.currency
         AND to_currency   = v_functional_ccy
         AND effective_at::DATE <= v_business_date
       ORDER BY effective_at DESC LIMIT 1;
      IF v_fx_rate IS NULL THEN
        RAISE EXCEPTION
          'missing_fx_rate: no fx_rates row found for % → % effective_at <= %',
          p_c_acct.currency, v_functional_ccy, v_business_date
          USING ERRCODE = 'P0050';
      END IF;

      INSERT INTO posting_line_currencies (
        posting_line_id, amount_transaction, currency_transaction,
        fx_rate_to_functional
      ) VALUES (
        v_new_id, p_amount, p_c_acct.currency, v_fx_rate
      );
    END IF;
  END IF;

  -- B3 extension writes. Credit-first composition resolution per R2.
  v_dim_sku := COALESCE(p_c_acct.sku_id, p_d_acct.sku_id);
  IF v_dim_sku IS NOT NULL THEN
    INSERT INTO posting_line_dimensions
      (posting_line_id, dimension_type, dimension_value_uuid)
      VALUES (v_new_id, 3, v_dim_sku);
  END IF;

  v_dim_loc := COALESCE(p_c_acct.location_id, p_d_acct.location_id);
  IF v_dim_loc IS NOT NULL THEN
    INSERT INTO posting_line_dimensions
      (posting_line_id, dimension_type, dimension_value_uuid)
      VALUES (v_new_id, 4, v_dim_loc);
  END IF;

  v_dim_routing_op := COALESCE(
    (p_event->>'routing_op')::INT,
    p_c_acct.routing_op,
    p_d_acct.routing_op
  );
  IF v_dim_routing_op IS NOT NULL THEN
    INSERT INTO posting_line_dimensions
      (posting_line_id, dimension_type, dimension_value)
      VALUES (v_new_id, 5, v_dim_routing_op::BIGINT);
  END IF;

  v_event_cp := (p_event->>'counterparty_id')::UUID;
  v_dim_cp := COALESCE(v_event_cp, p_c_acct.counterparty_id, p_d_acct.counterparty_id);
  IF v_dim_cp IS NOT NULL THEN
    IF p_c_acct.kind IN ('ar','ar_unsettled','customer_pool')
       OR p_d_acct.kind IN ('ar','ar_unsettled','customer_pool') THEN
      v_dim_cp_type := 1;
    ELSIF p_c_acct.kind IN ('ap','ap_unsettled','vendor_pool','accrued_disposal_liability')
       OR p_d_acct.kind IN ('ap','ap_unsettled','vendor_pool','accrued_disposal_liability') THEN
      v_dim_cp_type := 2;
    ELSE
      v_dim_cp_type := NULL;
    END IF;
    IF v_dim_cp_type IS NOT NULL THEN
      INSERT INTO posting_line_dimensions
        (posting_line_id, dimension_type, dimension_value_uuid)
        VALUES (v_new_id, v_dim_cp_type, v_dim_cp);
    END IF;
  END IF;

  -- C extension write. One row per qty-bearing inventory posting_line.
  -- Schema requires product_id NOT NULL; emit only when a SKU resolves.
  -- Postings that carry qty as audit metadata but have no inventory
  -- semantics on either side (e.g., by-product disposal_cost period-
  -- basis: disposal_expense ↔ accrued_disposal_liability with
  -- planned_qty for traceability) are deliberately skipped — the qty
  -- is reporting metadata, not an inventory movement. Recon check #6
  -- mirrors this filter so non-inventory qty postings don't false-
  -- positive.
  IF v_qty_for_row IS NOT NULL
     AND COALESCE(p_c_acct.sku_id, p_d_acct.sku_id) IS NOT NULL THEN
    -- unit_cost only meaningful on value-leg postings (ledger_kind='value');
    -- on qty-leg postings amount IS the qty so amount/qty == 1, not a cost.
    IF p_d_acct.ledger_kind = 'value' AND v_qty_for_row <> 0 THEN
      v_inv_unit_cost := p_amount::NUMERIC / ABS(v_qty_for_row)::NUMERIC;
    ELSE
      v_inv_unit_cost := NULL;
    END IF;

    SELECT cost_method INTO v_inv_cost_method
      FROM skus
     WHERE id = COALESCE(p_c_acct.sku_id, p_d_acct.sku_id);

    INSERT INTO posting_line_inventory (
      posting_line_id, product_id, quantity, qty_uom,
      unit_cost, cost_method_at_event
    ) VALUES (
      v_new_id,
      COALESCE(p_c_acct.sku_id, p_d_acct.sku_id),
      ABS(v_qty_for_row)::NUMERIC,
      'EA',
      v_inv_unit_cost,
      v_inv_cost_method
    );
  END IF;

  RETURN v_new_id;
END;
$$;

-- ============================================================
-- Backfill: one row per existing posting_line with qty IS NOT NULL.
-- Credit-first per R2; ABS(qty) for unsigned quantity (sign lives in
-- debit/credit direction, not the extension column). unit_cost only
-- on value-leg postings; NULLIF guards against div-by-zero (the C0
-- audit confirmed no qty=0 rows but the guard is defensive).
-- ON CONFLICT DO NOTHING for replay idempotency.
-- ============================================================

INSERT INTO posting_line_inventory (
  posting_line_id, product_id, quantity, qty_uom,
  unit_cost, cost_method_at_event, created_at
)
SELECT
  pl.id,
  COALESCE(c.sku_id, d.sku_id) AS product_id,
  ABS(pl.qty)::NUMERIC,
  'EA',
  CASE
    WHEN d.ledger_kind = 'value' AND pl.qty <> 0
    THEN pl.amount::NUMERIC / NULLIF(ABS(pl.qty), 0)::NUMERIC
    ELSE NULL
  END,
  s.cost_method,
  pl.posted_at
  FROM posting_lines pl
  JOIN accounts d ON d.id = pl.debit_account_id
  JOIN accounts c ON c.id = pl.credit_account_id
  JOIN skus     s ON s.id = COALESCE(c.sku_id, d.sku_id)
 WHERE pl.qty IS NOT NULL
ON CONFLICT (posting_line_id) DO NOTHING;

-- ============================================================
-- Extend run_daily_reconciliation with C check #6.
--
-- Check #6: count(posting_lines WHERE qty IS NOT NULL) ==
--           count(posting_line_inventory). Surfaces any drift
--           between the source-of-truth posting_lines.qty column
--           and the extension table — would catch a dispatcher
--           regression where a qty-bearing posting_line was
--           inserted without a paired extension row.
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

  -- Check 6: C inventory extension count match. Every qty-bearing
  -- INVENTORY posting_line (one with a resolvable SKU on either side)
  -- must have a paired posting_line_inventory row. Postings carrying
  -- qty as audit metadata with no SKU on either side (by-product
  -- disposal_cost period-basis) are deliberately excluded — they
  -- aren't inventory movements.
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

  RETURN v_total;
END;
$$;
