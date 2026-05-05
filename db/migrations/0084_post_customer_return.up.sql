-- acct-ari — post_customer_return / credit memo workflow.
--
-- Inverse of post_so_ship + post_customer_invoice. Customer returns
-- goods; we issue a credit memo against ar (mainstream-ERP convention:
-- SAP / Oracle / NetSuite / D365 all post credit memos to ar
-- directly, bypassing the ar_unsettled staging used for the forward
-- ship-then-invoice path).
--
-- Why ar (not ar_unsettled): ar_unsettled is debit-normal and was
-- already cleared to ar by post_customer_invoice. Crediting it for
-- the return would push it negative (accounts_check trip). The
-- realistic flow is invoice-then-return, so the return reverses
-- against ar. Pre-invoice returns are out of scope (they'd be a
-- "void shipment" workflow — different shape).
--
-- Per line, post_customer_return posts (per disposition):
--
--   restock:  stock_available(sku, location)   DR / customer_pool   CR
--             inv_value_fg(sku, location, ccy) DR / cogs(ccy)       CR
--   scrap:    stock_scrap(sku)                 DR / customer_pool   CR
--             variance_scrap(ccy)              DR / cogs(ccy)       CR
--   repair:   stock_quarantine(sku, location)  DR / customer_pool   CR
--             inv_value_fg(sku, location, ccy) DR / cogs(ccy)       CR
--
-- Plus revenue + tax reversal (always):
--   revenue(ccy)                       DR / ar(customer, ccy)       CR
--   sales_tax_payable(ccy)             DR / ar_unsettled(customer)  CR
--                                                  (only if tax > 0)
--
-- Revenue reverses against ar because post_customer_invoice cleared
-- it from ar_unsettled to ar at invoice time. Tax reverses against
-- ar_unsettled because post_customer_invoice does NOT touch tax for
-- so_match lines (mig 0081 §3 design note: "ship-side tax is the
-- source of truth"; tax stays parked in ar_unsettled). Routing tax
-- to ar would push ar negative when only revenue had cleared.
--
-- Unit cost / price / tax are SNAPSHOTTED from so_shipment_lines —
-- returns reverse exactly what shipped (audit-trail integrity over
-- current-WAC accuracy). Partial returns pro-rate tax with integer
-- truncation; residual goes to the last partial.
--
-- Reservations are NOT touched (they were already 'shipped' or
-- 'cancelled' by the time of return).
--
-- New error codes:
--   P0044 customer_return_invalid
--   P0045 customer_return_overreturned

-- ============================================================
-- return_disposition enum
-- ============================================================

CREATE TYPE return_disposition AS ENUM ('restock', 'scrap', 'repair');

-- ============================================================
-- customer_returns + customer_return_lines
-- ============================================================

CREATE TABLE customer_returns (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  customer_id     UUID NOT NULL REFERENCES customers(id),
  business_date   DATE NOT NULL,
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID NOT NULL UNIQUE,
  notes           TEXT
);

CREATE INDEX customer_returns_customer  ON customer_returns (customer_id);
CREATE INDEX customer_returns_posted_at ON customer_returns (posted_at);

COMMENT ON TABLE customer_returns IS
  'Credit memo / return event header. One row per post_customer_return '
  'call. Reverses prior so_shipment lines (snapshotted unit_cost / '
  'unit_price / tax_amount) into stock_available / stock_scrap / '
  'stock_quarantine per disposition.';

CREATE TABLE customer_return_lines (
  id           UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  return_id    UUID NOT NULL REFERENCES customer_returns(id),
  line_no      INT  NOT NULL,
  ship_line_id UUID NOT NULL REFERENCES so_shipment_lines(id),
  qty_returned BIGINT NOT NULL CHECK (qty_returned > 0),
  disposition  return_disposition NOT NULL,
  unit_cost    BIGINT NOT NULL CHECK (unit_cost  >= 0),
  unit_price   BIGINT NOT NULL CHECK (unit_price >= 0),
  tax_amount   BIGINT NOT NULL DEFAULT 0 CHECK (tax_amount >= 0),
  UNIQUE (return_id, line_no)
);

CREATE INDEX customer_return_lines_ship_line ON customer_return_lines (ship_line_id);

COMMENT ON TABLE customer_return_lines IS
  'Per-line return detail. ship_line_id pins the originating shipment '
  'line; qty_returned must respect cumulative not-yet-returned '
  'remainder. unit_cost / unit_price / tax_amount snapshotted from '
  'the ship line at return-post time.';

-- ============================================================
-- post_customer_return
-- ============================================================

CREATE OR REPLACE FUNCTION post_customer_return(
  p_customer_id     UUID,
  p_lines           JSONB,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id      UUID;
  v_doc_id           UUID;
  v_customer_check   UUID;
  v_n                INT;
  v_idx              INT;
  v_line             JSONB;
  v_ship_line_id     UUID;
  v_qty_returned     BIGINT;
  v_disposition      return_disposition;
  v_sl               RECORD;          -- so_shipment_lines + so_line cols
  v_already_returned BIGINT;
  v_remaining        BIGINT;
  v_tax_pro          BIGINT;
  v_qty_dr_acct      BIGINT;
  v_val_dr_acct      BIGINT;
  v_cust_qty         BIGINT;
  v_cust_unsettled   BIGINT;
  v_cogs_acct        BIGINT;
  v_revenue_acct     BIGINT;
  v_tax_acct         BIGINT;
  v_cust_ar          BIGINT;
  v_var_scrap        BIGINT;
  v_return_line_id   UUID;
  v_batch            JSONB := '[]'::JSONB;
BEGIN
  -- Fast-path replay.
  SELECT id INTO v_existing_id FROM customer_returns
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  -- Customer existence.
  SELECT id INTO v_customer_check FROM customers WHERE id = p_customer_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'customer_return_invalid: customer % not found', p_customer_id
      USING ERRCODE = 'P0044';
  END IF;

  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'customer_return_invalid: empty lines for customer %',
                    p_customer_id USING ERRCODE = 'P0044';
  END IF;

  -- Reserve doc id (race-safe via UNIQUE on idempotency_key).
  INSERT INTO customer_returns (customer_id, business_date, posted_by, idempotency_key, notes)
  VALUES (p_customer_id, p_business_date, p_posted_by, p_idempotency_key, p_notes)
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM customer_returns WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  FOR v_idx IN 1..v_n LOOP
    v_line         := p_lines -> (v_idx - 1);
    v_ship_line_id := (v_line->>'ship_line_id')::UUID;
    v_qty_returned := (v_line->>'qty_returned')::BIGINT;
    v_disposition  := (v_line->>'disposition')::return_disposition;

    IF v_qty_returned IS NULL OR v_qty_returned <= 0 THEN
      RAISE EXCEPTION 'customer_return_invalid: line % qty_returned must be > 0',
                      v_idx USING ERRCODE = 'P0044';
    END IF;

    -- so_shipment_line + so_line + ship's customer ownership check.
    SELECT
      ssl.id            AS ship_line_id,
      ssl.qty_shipped   AS qty_shipped,
      ssl.unit_cost     AS unit_cost,
      ssl.unit_price    AS unit_price,
      ssl.tax_amount    AS tax_amount,
      sl.sku_id         AS sku_id,
      sl.ship_location_id AS ship_location_id,
      sl.currency       AS currency,
      so.customer_id    AS customer_id
    INTO v_sl
    FROM so_shipment_lines ssl
    JOIN sales_order_lines sl ON sl.id = ssl.so_line_id
    JOIN so_shipments      ss ON ss.id = ssl.shipment_id
    JOIN sales_orders      so ON so.id = ss.so_id
    WHERE ssl.id = v_ship_line_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'customer_return_invalid: ship_line % not found',
                      v_ship_line_id USING ERRCODE = 'P0044';
    END IF;
    IF v_sl.customer_id <> p_customer_id THEN
      RAISE EXCEPTION
        'customer_return_invalid: ship_line % belongs to customer %, not %',
        v_ship_line_id, v_sl.customer_id, p_customer_id
        USING ERRCODE = 'P0044';
    END IF;

    -- Cumulative-not-yet-returned check.
    SELECT COALESCE(SUM(crl.qty_returned), 0)
      INTO v_already_returned
      FROM customer_return_lines crl
      JOIN customer_returns cr ON cr.id = crl.return_id
     WHERE crl.ship_line_id = v_ship_line_id
       AND cr.id <> v_doc_id;

    v_remaining := v_sl.qty_shipped - v_already_returned;
    IF v_qty_returned > v_remaining THEN
      RAISE EXCEPTION
        'customer_return_overreturned: ship_line % shipped=% already_returned=% requested=% remaining=%',
        v_ship_line_id, v_sl.qty_shipped, v_already_returned, v_qty_returned, v_remaining
        USING ERRCODE = 'P0045';
    END IF;

    -- Pro-rate tax (integer truncation; residue accumulates on last partial).
    IF v_sl.tax_amount > 0 AND v_sl.qty_shipped > 0 THEN
      v_tax_pro := (v_sl.tax_amount * v_qty_returned) / v_sl.qty_shipped;
    ELSE
      v_tax_pro := 0;
    END IF;

    -- Resolve qty-side debit account by disposition.
    IF v_disposition = 'restock' THEN
      SELECT id INTO v_qty_dr_acct FROM accounts
       WHERE kind='stock_available' AND sku_id=v_sl.sku_id
         AND location_id=v_sl.ship_location_id AND NOT is_closed;
      IF v_qty_dr_acct IS NULL THEN
        RAISE EXCEPTION 'no open stock_available for sku=% loc=%',
                        v_sl.sku_id, v_sl.ship_location_id USING ERRCODE = 'P0010';
      END IF;
    ELSIF v_disposition = 'scrap' THEN
      SELECT id INTO v_qty_dr_acct FROM accounts
       WHERE kind='stock_scrap' AND sku_id=v_sl.sku_id AND NOT is_closed;
      IF v_qty_dr_acct IS NULL THEN
        RAISE EXCEPTION 'no open stock_scrap for sku=%',
                        v_sl.sku_id USING ERRCODE = 'P0010';
      END IF;
    ELSIF v_disposition = 'repair' THEN
      SELECT id INTO v_qty_dr_acct FROM accounts
       WHERE kind='stock_quarantine' AND sku_id=v_sl.sku_id
         AND location_id=v_sl.ship_location_id AND NOT is_closed;
      IF v_qty_dr_acct IS NULL THEN
        RAISE EXCEPTION 'no open stock_quarantine for sku=% loc=%',
                        v_sl.sku_id, v_sl.ship_location_id USING ERRCODE = 'P0010';
      END IF;
    END IF;

    -- Resolve value-side debit account by disposition.
    IF v_disposition IN ('restock', 'repair') THEN
      SELECT id INTO v_val_dr_acct FROM accounts
       WHERE kind='inv_value_fg' AND sku_id=v_sl.sku_id
         AND location_id=v_sl.ship_location_id AND currency=v_sl.currency
         AND NOT is_closed;
      IF v_val_dr_acct IS NULL THEN
        RAISE EXCEPTION 'no open inv_value_fg for sku=% loc=% ccy=%',
                        v_sl.sku_id, v_sl.ship_location_id, v_sl.currency
          USING ERRCODE = 'P0010';
      END IF;
    ELSE -- scrap
      SELECT id INTO v_var_scrap FROM accounts
       WHERE kind='variance_scrap' AND currency=v_sl.currency AND NOT is_closed;
      IF v_var_scrap IS NULL THEN
        RAISE EXCEPTION 'no open variance_scrap for ccy=%',
                        v_sl.currency USING ERRCODE = 'P0010';
      END IF;
      v_val_dr_acct := v_var_scrap;
    END IF;

    -- Customer pool (qty credit).
    SELECT id INTO v_cust_qty FROM accounts
     WHERE kind='customer_pool' AND counterparty_id=p_customer_id
       AND NOT is_closed;
    IF v_cust_qty IS NULL THEN
      SELECT id INTO v_cust_qty FROM accounts
       WHERE kind='customer_pool' AND counterparty_id IS NULL
         AND NOT is_closed;
    END IF;
    IF v_cust_qty IS NULL THEN
      RAISE EXCEPTION 'no open customer_pool for customer=%',
                      p_customer_id USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_cogs_acct FROM accounts
     WHERE kind='cogs' AND currency=v_sl.currency AND NOT is_closed;
    IF v_cogs_acct IS NULL THEN
      RAISE EXCEPTION 'no open cogs account for ccy=%',
                      v_sl.currency USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_revenue_acct FROM accounts
     WHERE kind='revenue' AND currency=v_sl.currency AND NOT is_closed;
    IF v_revenue_acct IS NULL THEN
      RAISE EXCEPTION 'no open revenue account for ccy=%',
                      v_sl.currency USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_cust_ar FROM accounts
     WHERE kind='ar' AND counterparty_id=p_customer_id
       AND currency=v_sl.currency AND NOT is_closed;
    IF v_cust_ar IS NULL THEN
      RAISE EXCEPTION 'no open ar for customer=% ccy=%',
                      p_customer_id, v_sl.currency USING ERRCODE = 'P0010';
    END IF;

    INSERT INTO customer_return_lines (
      return_id, line_no, ship_line_id, qty_returned, disposition,
      unit_cost, unit_price, tax_amount
    ) VALUES (
      v_doc_id, v_idx, v_ship_line_id, v_qty_returned, v_disposition,
      v_sl.unit_cost, v_sl.unit_price, v_tax_pro
    )
    RETURNING id INTO v_return_line_id;

    -- Event 1: qty leg.
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'customer_return',
      'document_kind',     'customer_return',
      'document_id',       v_doc_id,
      'document_line_id',  v_return_line_id,
      'debit_account_id',  v_qty_dr_acct,
      'credit_account_id', v_cust_qty,
      'amount',            v_qty_returned,
      'qty',               v_qty_returned,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   p_customer_id,
      'posted_by',         p_posted_by
    ));

    -- Event 2: value leg (cogs reversal — to fg / quarantine for
    -- restock+repair, to variance_scrap for scrap).
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'customer_return',
      'document_kind',     'customer_return',
      'document_id',       v_doc_id,
      'document_line_id',  v_return_line_id,
      'debit_account_id',  v_val_dr_acct,
      'credit_account_id', v_cogs_acct,
      'amount',            v_qty_returned * v_sl.unit_cost,
      'qty',               v_qty_returned,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   p_customer_id,
      'posted_by',         p_posted_by
    ));

    -- Event 3: revenue reversal (always — credit memo against ar).
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'customer_return',
      'document_kind',     'customer_return',
      'document_id',       v_doc_id,
      'document_line_id',  v_return_line_id,
      'debit_account_id',  v_revenue_acct,
      'credit_account_id', v_cust_ar,
      'amount',            v_qty_returned * v_sl.unit_price,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   p_customer_id,
      'posted_by',         p_posted_by
    ));

    -- Event 4: tax reversal — credits ar_unsettled, not ar (see header
    -- note: post_customer_invoice does not touch tax for so_match
    -- lines, so the ship-side ar_unsettled DR for tax is still
    -- standing).
    IF v_tax_pro > 0 THEN
      SELECT id INTO v_tax_acct FROM accounts
       WHERE kind='sales_tax_payable' AND currency=v_sl.currency
         AND NOT is_closed;
      IF v_tax_acct IS NULL THEN
        RAISE EXCEPTION 'no open sales_tax_payable for ccy=%',
                        v_sl.currency USING ERRCODE = 'P0010';
      END IF;

      SELECT id INTO v_cust_unsettled FROM accounts
       WHERE kind='ar_unsettled' AND counterparty_id=p_customer_id
         AND currency=v_sl.currency AND NOT is_closed;
      IF v_cust_unsettled IS NULL THEN
        SELECT id INTO v_cust_unsettled FROM accounts
         WHERE kind='ar_unsettled' AND counterparty_id IS NULL
           AND currency=v_sl.currency AND NOT is_closed;
      END IF;
      IF v_cust_unsettled IS NULL THEN
        RAISE EXCEPTION 'no open ar_unsettled for customer=% ccy=%',
                        p_customer_id, v_sl.currency USING ERRCODE = 'P0010';
      END IF;

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'customer_return',
        'document_kind',     'customer_return',
        'document_id',       v_doc_id,
        'document_line_id',  v_return_line_id,
        'debit_account_id',  v_tax_acct,
        'credit_account_id', v_cust_unsettled,
        'amount',            v_tax_pro,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_customer_id,
        'posted_by',         p_posted_by
      ));
    END IF;
  END LOOP;

  PERFORM post_transfers(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;

COMMENT ON FUNCTION post_customer_return(UUID, JSONB, DATE, UUID, UUID, TEXT) IS
  'Customer return / credit memo. Reverses prior so_shipment lines: '
  'qty back per disposition (restock→stock_available, scrap→'
  'stock_scrap, repair→stock_quarantine); value back per disposition '
  '(restock+repair→inv_value_fg, scrap→variance_scrap); revenue '
  'reversal against ar (credit memo, assumes prior invoice cleared '
  'revenue from ar_unsettled to ar); tax reversal against '
  'ar_unsettled (ship-side tax parking, untouched by invoice on '
  'so_match lines). Strict over-return rejection (P0045). '
  'unit_cost / unit_price / tax_amount snapshotted from '
  'so_shipment_lines for audit-trail integrity.';
