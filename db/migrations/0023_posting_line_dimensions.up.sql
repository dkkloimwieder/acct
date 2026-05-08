-- Phase B3 of the posting-lines convergence plan
-- (research/posting-lines-convergence-plan.md §4.B.B3; acct-wb75.1.3).
--
-- Dimensional analytics extension. The accounts table inlines four
-- composition columns (sku_id, location_id, routing_op,
-- counterparty_id), and posting_lines carries its own routing_op +
-- counterparty_id per-event overrides. Querying "all 2026-Q2 postings
-- for product X across both warehouses" today requires joining
-- accounts + posting_lines and walking inline columns; B3 hoists
-- those into a normalized extension so analytics queries become
-- direct EAV-style filters.
--
-- Scope: 5 active dimension types (customer, vendor, product,
-- location, routing_op) sourced from credit-first composition
-- columns. 4 forward-looking dimension types (cost_center,
-- profit_center, project, department) are seeded for callers to opt
-- into via event JSONB once their source tables exist (out of scope
-- here).
--
-- Counterparty disambiguation. accounts.counterparty_id is a generic
-- UUID; whether it's a customer or a vendor is implied by the kind
-- of the account it sits on. Dispatcher walks credit-first; the
-- account kind list is hardcoded:
--   customer kinds: ar, ar_unsettled, customer_pool
--   vendor   kinds: ap, ap_unsettled, vendor_pool,
--                   accrued_disposal_liability
-- Other kinds with counterparty_id (none today, but possible) get
-- skipped — the dimension would be ambiguous without explicit
-- caller intent.
--
-- Conflict resolution. PK = (posting_line_id, dimension_type), so
-- only one row per (posting, dim_type). When debit and credit
-- accounts disagree on a composition column (e.g., op_move with
-- raw_sku on debit, fg_sku on credit), credit-side governs (R2:
-- depletion source / per-event resolution rule used elsewhere in
-- the dispatcher).
--
-- Recon. New check #4 verifies every posting whose credit/debit
-- has sku_id surfaces as a product dimension row, and every posting
-- with a customer/vendor counterparty kind surfaces under either
-- customer or vendor.

CREATE TABLE dimension_types (
  dimension_type   SMALLINT     PRIMARY KEY,
  name             VARCHAR(64)  NOT NULL UNIQUE,
  reference_table  VARCHAR(64)  NOT NULL,
  description      TEXT
);

INSERT INTO dimension_types (dimension_type, name, reference_table, description) VALUES
  (1, 'customer',      'customers',
     'AR-side counterparty resolved from accounts.counterparty_id when kind in ar/ar_unsettled/customer_pool.'),
  (2, 'vendor',        'vendors',
     'AP-side counterparty resolved from accounts.counterparty_id when kind in ap/ap_unsettled/vendor_pool/accrued_disposal_liability.'),
  (3, 'product',       'skus',
     'SKU resolved from credit-first accounts.sku_id (same as cost-method dispatch R2).'),
  (4, 'location',      'locations',
     'Location resolved from credit-first accounts.location_id.'),
  (5, 'routing_op',    'wo_routings',
     'Routing op resolved from posting_lines.routing_op override, else credit/debit accounts.routing_op.'),
  (6, 'cost_center',   'cost_centers',
     'Forward-looking; source table not yet defined. Callers may opt in via event JSONB once available.'),
  (7, 'profit_center', 'profit_centers',
     'Forward-looking; source table not yet defined.'),
  (8, 'project',       'projects',
     'Forward-looking; source table not yet defined.'),
  (9, 'department',    'departments',
     'Forward-looking; source table not yet defined.');

CREATE TABLE posting_line_dimensions (
  posting_line_id      BIGINT   NOT NULL REFERENCES posting_lines(id),
  dimension_type       SMALLINT NOT NULL REFERENCES dimension_types(dimension_type),
  dimension_value      BIGINT,
  dimension_value_uuid UUID,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (posting_line_id, dimension_type),

  CONSTRAINT posting_line_dimensions_value_present CHECK (
    dimension_value IS NOT NULL OR dimension_value_uuid IS NOT NULL
  ),
  CONSTRAINT posting_line_dimensions_value_exclusive CHECK (
    NOT (dimension_value IS NOT NULL AND dimension_value_uuid IS NOT NULL)
  )
);

CREATE INDEX posting_line_dimensions_int
  ON posting_line_dimensions (dimension_type, dimension_value)
  WHERE dimension_value IS NOT NULL;

CREATE INDEX posting_line_dimensions_uuid
  ON posting_line_dimensions (dimension_type, dimension_value_uuid)
  WHERE dimension_value_uuid IS NOT NULL;

COMMENT ON TABLE posting_line_dimensions IS
  'Phase B3 dimensional analytics extension. EAV-typed; one row per '
  '(posting_line, dimension_type). dimension_value (BIGINT) for '
  'integer-keyed dims (routing_op); dimension_value_uuid for '
  'UUID-keyed dims (customer/vendor/product/location). Exactly one '
  'value column populated per row.';

-- ============================================================
-- _post_posting_lines_apply_event: extended with B3 dimension write.
--
-- Body identical to 0022's version through the B2 extension block,
-- with a new B3 block appended before RETURN.
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
  -- One conditional INSERT per dimension type. Counterparty kind
  -- dispatch hardcoded; "ambiguous kind" (counterparty_id present
  -- on a non-AR/AP kind) skipped to keep the dimension assignment
  -- unambiguous.

  -- Product (UUID).
  v_dim_sku := COALESCE(p_c_acct.sku_id, p_d_acct.sku_id);
  IF v_dim_sku IS NOT NULL THEN
    INSERT INTO posting_line_dimensions
      (posting_line_id, dimension_type, dimension_value_uuid)
      VALUES (v_new_id, 3, v_dim_sku);
  END IF;

  -- Location (UUID).
  v_dim_loc := COALESCE(p_c_acct.location_id, p_d_acct.location_id);
  IF v_dim_loc IS NOT NULL THEN
    INSERT INTO posting_line_dimensions
      (posting_line_id, dimension_type, dimension_value_uuid)
      VALUES (v_new_id, 4, v_dim_loc);
  END IF;

  -- Routing op (BIGINT). Per-event override > credit > debit.
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

  -- Counterparty (customer or vendor; UUID). Per-event override >
  -- credit > debit.
  v_event_cp := (p_event->>'counterparty_id')::UUID;
  v_dim_cp := COALESCE(v_event_cp, p_c_acct.counterparty_id, p_d_acct.counterparty_id);
  IF v_dim_cp IS NOT NULL THEN
    IF p_c_acct.kind IN ('ar','ar_unsettled','customer_pool')
       OR p_d_acct.kind IN ('ar','ar_unsettled','customer_pool') THEN
      v_dim_cp_type := 1; -- customer
    ELSIF p_c_acct.kind IN ('ap','ap_unsettled','vendor_pool','accrued_disposal_liability')
       OR p_d_acct.kind IN ('ap','ap_unsettled','vendor_pool','accrued_disposal_liability') THEN
      v_dim_cp_type := 2; -- vendor
    ELSE
      v_dim_cp_type := NULL; -- ambiguous; skip
    END IF;
    IF v_dim_cp_type IS NOT NULL THEN
      INSERT INTO posting_line_dimensions
        (posting_line_id, dimension_type, dimension_value_uuid)
        VALUES (v_new_id, v_dim_cp_type, v_dim_cp);
    END IF;
  END IF;

  RETURN v_new_id;
END;
$$;

-- ============================================================
-- Backfill: existing posting_lines get one row per non-null
-- composition column. Credit-first per R2 to avoid PK collisions
-- on multi-SKU postings (e.g., op_move with raw on debit, fg on
-- credit). ON CONFLICT DO NOTHING for replay idempotency.
-- ============================================================

-- Product
INSERT INTO posting_line_dimensions
  (posting_line_id, dimension_type, dimension_value_uuid)
SELECT pl.id, 3, COALESCE(c.sku_id, d.sku_id)
  FROM posting_lines pl
  JOIN accounts c ON c.id = pl.credit_account_id
  JOIN accounts d ON d.id = pl.debit_account_id
 WHERE COALESCE(c.sku_id, d.sku_id) IS NOT NULL
ON CONFLICT (posting_line_id, dimension_type) DO NOTHING;

-- Location
INSERT INTO posting_line_dimensions
  (posting_line_id, dimension_type, dimension_value_uuid)
SELECT pl.id, 4, COALESCE(c.location_id, d.location_id)
  FROM posting_lines pl
  JOIN accounts c ON c.id = pl.credit_account_id
  JOIN accounts d ON d.id = pl.debit_account_id
 WHERE COALESCE(c.location_id, d.location_id) IS NOT NULL
ON CONFLICT (posting_line_id, dimension_type) DO NOTHING;

-- Routing op
INSERT INTO posting_line_dimensions
  (posting_line_id, dimension_type, dimension_value)
SELECT pl.id, 5, COALESCE(pl.routing_op, c.routing_op, d.routing_op)::BIGINT
  FROM posting_lines pl
  JOIN accounts c ON c.id = pl.credit_account_id
  JOIN accounts d ON d.id = pl.debit_account_id
 WHERE COALESCE(pl.routing_op, c.routing_op, d.routing_op) IS NOT NULL
ON CONFLICT (posting_line_id, dimension_type) DO NOTHING;

-- Customer
INSERT INTO posting_line_dimensions
  (posting_line_id, dimension_type, dimension_value_uuid)
SELECT pl.id, 1, COALESCE(pl.counterparty_id, c.counterparty_id, d.counterparty_id)
  FROM posting_lines pl
  JOIN accounts c ON c.id = pl.credit_account_id
  JOIN accounts d ON d.id = pl.debit_account_id
 WHERE COALESCE(pl.counterparty_id, c.counterparty_id, d.counterparty_id) IS NOT NULL
   AND (c.kind IN ('ar','ar_unsettled','customer_pool')
     OR d.kind IN ('ar','ar_unsettled','customer_pool'))
ON CONFLICT (posting_line_id, dimension_type) DO NOTHING;

-- Vendor
INSERT INTO posting_line_dimensions
  (posting_line_id, dimension_type, dimension_value_uuid)
SELECT pl.id, 2, COALESCE(pl.counterparty_id, c.counterparty_id, d.counterparty_id)
  FROM posting_lines pl
  JOIN accounts c ON c.id = pl.credit_account_id
  JOIN accounts d ON d.id = pl.debit_account_id
 WHERE COALESCE(pl.counterparty_id, c.counterparty_id, d.counterparty_id) IS NOT NULL
   AND (c.kind IN ('ap','ap_unsettled','vendor_pool','accrued_disposal_liability')
     OR d.kind IN ('ap','ap_unsettled','vendor_pool','accrued_disposal_liability'))
ON CONFLICT (posting_line_id, dimension_type) DO NOTHING;

-- ============================================================
-- Extend run_daily_reconciliation with B3 checks.
--
-- Check #4: every posting_line whose credit/debit account has
-- sku_id should have a 'product' dimension row.
--
-- Check #5: every posting_line whose credit/debit kind is a known
-- customer or vendor kind should have a corresponding dimension
-- row (customer or vendor, matching).
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

  -- Check 4: B3 dimensions — product coverage. Every posting whose
  -- credit/debit has sku_id must have a 'product' dimension row.
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

  -- Check 5: B3 dimensions — counterparty coverage. Every posting
  -- whose credit/debit is a known customer/vendor kind with non-null
  -- counterparty must have a customer or vendor dimension row.
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

  RETURN v_total;
END;
$$;
