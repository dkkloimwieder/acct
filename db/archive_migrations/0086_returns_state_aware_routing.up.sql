-- acct-tk7 + acct-dso — state-aware routing for returns + period override.
--
-- Two issues bundled into one migration because both DROP+CREATE the
-- same two functions (post_po_return, post_customer_return).
--
-- ============================================================
-- acct-tk7: state-aware routing
-- ============================================================
--
-- Mig 0084 (post_customer_return) and 0085 (post_po_return) hard-code
-- the post-clearance route: AR returns always credit ar; AP returns
-- always debit ap. That is correct only when the invoice / bill has
-- fully cleared the staging account. For pre-invoice / pre-bill
-- returns the staging side (ar_unsettled / ap_unsettled) holds the
-- balance, and posting the return against the cleared account trips
-- accounts_check on a debit-/credit-normal account at zero balance.
--
-- The fix: per return line, dispatch the routing on cumulative
-- billed/invoiced state per po_line / so_line. Drain the un-billed /
-- un-invoiced portion FIRST (against the staging account), then the
-- billed / invoiced portion (against the cleared account).
--
--   unsettled_remaining = total_recv  - total_billed - prior_returns_to_unsettled
--   ap_remaining        = total_billed                - prior_returns_to_ap
--   qty_to_unsettled    = LEAST(qty_returned, MAX(unsettled_remaining, 0))
--   qty_to_ap           = qty_returned - qty_to_unsettled
--
-- The split is recorded on po_return_lines (qty_to_ap_unsettled,
-- qty_to_ap) and customer_return_lines (qty_to_ar_unsettled,
-- qty_to_ar) so subsequent returns can compute their state without
-- replaying transfer rows.
--
-- PPV reversal on the AP side splits the same way (variance against
-- ap_unsettled if pre-bill, against ap if post-bill). The acct-quk
-- event-ordering insight (PPV BEFORE value leg for credit-normal
-- accounts_check safety) carries forward per route.
--
-- Bill / invoice availability checks (post_ap_bill, post_customer_invoice)
-- subtract returns-to-staging from v_avail so post-return billing
-- reflects the un-returned, un-billed remainder.
--
-- ============================================================
-- acct-dso: p_override_closed_period exposure
-- ============================================================
--
-- Both return functions gain an optional p_override_closed_period
-- BOOLEAN DEFAULT FALSE pass-through to post_transfers. Required when
-- a closed period has been formally reopened (acct-7h4 future
-- workflow) and a return must back-post to it.

-- ============================================================
-- 1. po_return_lines: add split tracking columns
-- ============================================================

ALTER TABLE po_return_lines
  ADD COLUMN qty_to_ap_unsettled BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN qty_to_ap            BIGINT NOT NULL DEFAULT 0;

UPDATE po_return_lines SET qty_to_ap = qty_returned;

ALTER TABLE po_return_lines
  ADD CONSTRAINT po_return_lines_split_check
    CHECK (qty_to_ap_unsettled >= 0
           AND qty_to_ap >= 0
           AND qty_to_ap_unsettled + qty_to_ap = qty_returned);

COMMENT ON COLUMN po_return_lines.qty_to_ap_unsettled IS
  'Portion of qty_returned routed against ap_unsettled (pre-bill / un-billed).';
COMMENT ON COLUMN po_return_lines.qty_to_ap IS
  'Portion of qty_returned routed against ap (billed / cleared).';

-- ============================================================
-- 2. customer_return_lines: add split tracking columns
-- ============================================================

ALTER TABLE customer_return_lines
  ADD COLUMN qty_to_ar_unsettled BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN qty_to_ar            BIGINT NOT NULL DEFAULT 0;

UPDATE customer_return_lines SET qty_to_ar = qty_returned;

ALTER TABLE customer_return_lines
  ADD CONSTRAINT customer_return_lines_split_check
    CHECK (qty_to_ar_unsettled >= 0
           AND qty_to_ar >= 0
           AND qty_to_ar_unsettled + qty_to_ar = qty_returned);

COMMENT ON COLUMN customer_return_lines.qty_to_ar_unsettled IS
  'Portion of qty_returned routed against ar_unsettled (pre-invoice / un-invoiced).';
COMMENT ON COLUMN customer_return_lines.qty_to_ar IS
  'Portion of qty_returned routed against ar (invoiced / cleared).';

-- ============================================================
-- 3. post_po_return — state-aware + p_override_closed_period
-- ============================================================

DROP FUNCTION IF EXISTS post_po_return(UUID, JSONB, DATE, UUID, UUID, TEXT);

CREATE FUNCTION post_po_return(
  p_vendor_id              UUID,
  p_lines                  JSONB,
  p_business_date          DATE,
  p_posted_by              UUID,
  p_idempotency_key        UUID,
  p_notes                  TEXT DEFAULT NULL,
  p_override_closed_period BOOLEAN DEFAULT FALSE
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id        UUID;
  v_doc_id             UUID;
  v_vendor_check       UUID;
  v_n                  INT;
  v_idx                INT;
  v_line               JSONB;
  v_recv_line_id       UUID;
  v_qty_returned       BIGINT;
  v_pl                 RECORD;
  v_po_line_id         UUID;
  v_total_recv         BIGINT;
  v_total_billed       BIGINT;
  v_prior_to_unsettled BIGINT;
  v_prior_to_ap        BIGINT;
  v_unsettled_rem      BIGINT;
  v_ap_rem             BIGINT;
  v_qty_to_unsettled   BIGINT;
  v_qty_to_ap          BIGINT;
  v_cost_method        cost_method;
  v_inv_unit           BIGINT;
  v_qty_acct           BIGINT;
  v_val_acct           BIGINT;
  v_ven_qty            BIGINT;
  v_ven_unsettled      BIGINT;
  v_ven_ap             BIGINT;
  v_var_acct           BIGINT;
  v_return_line_id     UUID;
  v_batch              JSONB := '[]'::JSONB;
BEGIN
  -- Fast-path replay.
  SELECT id INTO v_existing_id FROM po_returns
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT id INTO v_vendor_check FROM vendors WHERE id = p_vendor_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'po_return_invalid: vendor % not found', p_vendor_id
      USING ERRCODE = 'P0046';
  END IF;

  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'po_return_invalid: empty lines for vendor %',
                    p_vendor_id USING ERRCODE = 'P0046';
  END IF;

  INSERT INTO po_returns (vendor_id, business_date, posted_by, idempotency_key, notes)
  VALUES (p_vendor_id, p_business_date, p_posted_by, p_idempotency_key, p_notes)
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM po_returns WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  FOR v_idx IN 1..v_n LOOP
    v_line         := p_lines -> (v_idx - 1);
    v_recv_line_id := (v_line->>'recv_line_id')::UUID;
    v_qty_returned := (v_line->>'qty_returned')::BIGINT;

    IF v_qty_returned IS NULL OR v_qty_returned <= 0 THEN
      RAISE EXCEPTION 'po_return_invalid: line % qty_returned must be > 0',
                      v_idx USING ERRCODE = 'P0046';
    END IF;

    -- Lookup recv_line + po_line + vendor metadata.
    SELECT
      prl.id           AS recv_line_id,
      prl.po_line_id   AS po_line_id,
      pol.sku_id       AS sku_id,
      pol.location_id  AS location_id,
      pol.unit_cost    AS unit_cost,
      pol.currency     AS currency,
      po.vendor_id     AS vendor_id
    INTO v_pl
    FROM po_receipt_lines prl
    JOIN purchase_order_lines pol ON pol.id = prl.po_line_id
    JOIN po_receipts          pr  ON pr.id  = prl.receipt_id
    JOIN purchase_orders      po  ON po.id  = pr.po_id
    WHERE prl.id = v_recv_line_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'po_return_invalid: recv_line % not found',
                      v_recv_line_id USING ERRCODE = 'P0046';
    END IF;
    IF v_pl.vendor_id <> p_vendor_id THEN
      RAISE EXCEPTION
        'po_return_invalid: recv_line % belongs to vendor %, not %',
        v_recv_line_id, v_pl.vendor_id, p_vendor_id
        USING ERRCODE = 'P0046';
    END IF;

    v_po_line_id := v_pl.po_line_id;

    -- Lock the po_line row (acct-du2.9 pattern) so cumulative reads
    -- below serialize against concurrent receipts / bills / returns
    -- on the same po_line.
    PERFORM 1 FROM purchase_order_lines WHERE id = v_po_line_id FOR UPDATE;

    -- Cumulative state per po_line.
    SELECT COALESCE(SUM(qty_received), 0)
      INTO v_total_recv
      FROM po_receipt_lines
     WHERE po_line_id = v_po_line_id;

    SELECT COALESCE(SUM(qty), 0)
      INTO v_total_billed
      FROM vendor_bill_lines
     WHERE po_line_id = v_po_line_id AND kind = 'po_match';

    SELECT
      COALESCE(SUM(prl.qty_to_ap_unsettled), 0),
      COALESCE(SUM(prl.qty_to_ap), 0)
      INTO v_prior_to_unsettled, v_prior_to_ap
      FROM po_return_lines prl
      JOIN po_receipt_lines rcl ON rcl.id = prl.recv_line_id
      JOIN po_returns       pr  ON pr.id  = prl.return_id
     WHERE rcl.po_line_id = v_po_line_id
       AND pr.id <> v_doc_id;

    v_unsettled_rem := v_total_recv - v_total_billed - v_prior_to_unsettled;
    v_ap_rem        := v_total_billed - v_prior_to_ap;

    -- Drain ap_unsettled first (un-billed portion), then ap (billed
    -- portion). Negative unsettled_rem is impossible under the
    -- ap_unsettled-credit invariant but clamp for defensive safety.
    v_qty_to_unsettled := LEAST(v_qty_returned, GREATEST(v_unsettled_rem, 0));
    v_qty_to_ap        := v_qty_returned - v_qty_to_unsettled;

    IF v_qty_to_ap > v_ap_rem THEN
      RAISE EXCEPTION
        'po_return_overreturned: po_line % cumulative would exceed billed-not-returned + unsettled-not-returned (recv=%, billed=%, prior_to_unsettled=%, prior_to_ap=%, requested=%)',
        v_po_line_id, v_total_recv, v_total_billed,
        v_prior_to_unsettled, v_prior_to_ap, v_qty_returned
        USING ERRCODE = 'P0047';
    END IF;

    -- Cost dispatch.
    SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_pl.sku_id;
    IF v_cost_method = 'standard' THEN
      v_inv_unit := resolve_standard_cost_at(v_pl.sku_id, p_business_date);
    ELSIF v_cost_method IN ('wac_perpetual', 'wac_periodic', 'wac_retroactive') THEN
      v_inv_unit := v_pl.unit_cost;
    ELSE
      RAISE EXCEPTION 'cost_method_not_implemented: % for po_return',
                      v_cost_method USING ERRCODE = 'P0006';
    END IF;

    -- Resolve accounts.
    SELECT id INTO v_qty_acct FROM accounts
     WHERE kind='stock_available' AND sku_id=v_pl.sku_id
       AND location_id=v_pl.location_id AND NOT is_closed;
    IF v_qty_acct IS NULL THEN
      RAISE EXCEPTION 'no open stock_available for sku=% loc=%',
                      v_pl.sku_id, v_pl.location_id USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_val_acct FROM accounts
     WHERE kind='inv_value_raw' AND sku_id=v_pl.sku_id
       AND location_id=v_pl.location_id AND currency=v_pl.currency
       AND NOT is_closed;
    IF v_val_acct IS NULL THEN
      RAISE EXCEPTION 'no open inv_value_raw for sku=% loc=% ccy=%',
                      v_pl.sku_id, v_pl.location_id, v_pl.currency
        USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_ven_qty FROM accounts
     WHERE kind='vendor_pool' AND counterparty_id=p_vendor_id
       AND NOT is_closed;
    IF v_ven_qty IS NULL THEN
      SELECT id INTO v_ven_qty FROM accounts
       WHERE kind='vendor_pool' AND counterparty_id IS NULL
         AND NOT is_closed;
    END IF;
    IF v_ven_qty IS NULL THEN
      RAISE EXCEPTION 'no open vendor_pool for vendor=%',
                      p_vendor_id USING ERRCODE = 'P0010';
    END IF;

    IF v_qty_to_unsettled > 0 THEN
      SELECT id INTO v_ven_unsettled FROM accounts
       WHERE kind='ap_unsettled' AND counterparty_id=p_vendor_id
         AND currency=v_pl.currency AND NOT is_closed;
      IF v_ven_unsettled IS NULL THEN
        RAISE EXCEPTION 'no open ap_unsettled for vendor=% ccy=%',
                        p_vendor_id, v_pl.currency USING ERRCODE = 'P0010';
      END IF;
    END IF;

    IF v_qty_to_ap > 0 THEN
      SELECT id INTO v_ven_ap FROM accounts
       WHERE kind='ap' AND counterparty_id=p_vendor_id
         AND currency=v_pl.currency AND NOT is_closed;
      IF v_ven_ap IS NULL THEN
        RAISE EXCEPTION 'no open ap for vendor=% ccy=%',
                        p_vendor_id, v_pl.currency USING ERRCODE = 'P0010';
      END IF;
    END IF;

    IF v_cost_method = 'standard' AND v_pl.unit_cost <> v_inv_unit THEN
      SELECT id INTO v_var_acct FROM accounts
       WHERE kind='variance_ppv' AND currency=v_pl.currency AND NOT is_closed;
      IF v_var_acct IS NULL THEN
        RAISE EXCEPTION 'no open variance_ppv for ccy=%',
                        v_pl.currency USING ERRCODE = 'P0010';
      END IF;
    END IF;

    INSERT INTO po_return_lines (
      return_id, line_no, recv_line_id, qty_returned, unit_cost,
      qty_to_ap_unsettled, qty_to_ap
    ) VALUES (
      v_doc_id, v_idx, v_recv_line_id, v_qty_returned, v_pl.unit_cost,
      v_qty_to_unsettled, v_qty_to_ap
    )
    RETURNING id INTO v_return_line_id;

    -- Event 1: qty leg (single, full qty_returned).
    -- vendor_pool DR / stock_available CR.
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'po_return_to_vendor',
      'document_kind',     'po_return',
      'document_id',       v_doc_id,
      'document_line_id',  v_return_line_id,
      'debit_account_id',  v_ven_qty,
      'credit_account_id', v_qty_acct,
      'amount',            v_qty_returned,
      'qty',               v_qty_returned,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   p_vendor_id,
      'posted_by',         p_posted_by
    ));

    -- ap_unsettled route legs (PPV BEFORE value, per acct-quk).
    IF v_qty_to_unsettled > 0 THEN
      DECLARE
        v_inv_amt_us BIGINT := v_qty_to_unsettled * v_inv_unit;
        v_ap_amt_us  BIGINT := v_qty_to_unsettled * v_pl.unit_cost;
        v_ppv_amt_us BIGINT := v_qty_to_unsettled * v_pl.unit_cost
                              - v_qty_to_unsettled * v_inv_unit;
      BEGIN
        IF v_ppv_amt_us > 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'ppv',
            'document_kind',     'po_return',
            'document_id',       v_doc_id,
            'document_line_id',  v_return_line_id,
            'debit_account_id',  v_ven_unsettled,
            'credit_account_id', v_var_acct,
            'amount',            v_ppv_amt_us,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_vendor_id,
            'posted_by',         p_posted_by
          ));
        ELSIF v_ppv_amt_us < 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'ppv',
            'document_kind',     'po_return',
            'document_id',       v_doc_id,
            'document_line_id',  v_return_line_id,
            'debit_account_id',  v_var_acct,
            'credit_account_id', v_ven_unsettled,
            'amount',            -v_ppv_amt_us,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_vendor_id,
            'posted_by',         p_posted_by
          ));
        END IF;

        IF v_inv_amt_us > 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'po_return_to_vendor',
            'document_kind',     'po_return',
            'document_id',       v_doc_id,
            'document_line_id',  v_return_line_id,
            'debit_account_id',  v_ven_unsettled,
            'credit_account_id', v_val_acct,
            'amount',            v_inv_amt_us,
            'qty',               v_qty_to_unsettled,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_vendor_id,
            'posted_by',         p_posted_by
          ));
        END IF;
      END;
    END IF;

    -- ap route legs (PPV BEFORE value, per acct-quk).
    IF v_qty_to_ap > 0 THEN
      DECLARE
        v_inv_amt_ap BIGINT := v_qty_to_ap * v_inv_unit;
        v_ap_amt_ap  BIGINT := v_qty_to_ap * v_pl.unit_cost;
        v_ppv_amt_ap BIGINT := v_qty_to_ap * v_pl.unit_cost
                              - v_qty_to_ap * v_inv_unit;
      BEGIN
        IF v_ppv_amt_ap > 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'ppv',
            'document_kind',     'po_return',
            'document_id',       v_doc_id,
            'document_line_id',  v_return_line_id,
            'debit_account_id',  v_ven_ap,
            'credit_account_id', v_var_acct,
            'amount',            v_ppv_amt_ap,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_vendor_id,
            'posted_by',         p_posted_by
          ));
        ELSIF v_ppv_amt_ap < 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'ppv',
            'document_kind',     'po_return',
            'document_id',       v_doc_id,
            'document_line_id',  v_return_line_id,
            'debit_account_id',  v_var_acct,
            'credit_account_id', v_ven_ap,
            'amount',            -v_ppv_amt_ap,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_vendor_id,
            'posted_by',         p_posted_by
          ));
        END IF;

        IF v_inv_amt_ap > 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'po_return_to_vendor',
            'document_kind',     'po_return',
            'document_id',       v_doc_id,
            'document_line_id',  v_return_line_id,
            'debit_account_id',  v_ven_ap,
            'credit_account_id', v_val_acct,
            'amount',            v_inv_amt_ap,
            'qty',               v_qty_to_ap,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_vendor_id,
            'posted_by',         p_posted_by
          ));
        END IF;
      END;
    END IF;
  END LOOP;

  PERFORM post_transfers(v_batch, p_override_closed_period);

  RETURN v_doc_id;
END;
$$;

COMMENT ON FUNCTION post_po_return(UUID, JSONB, DATE, UUID, UUID, TEXT, BOOLEAN) IS
  'Vendor return / debit memo. State-aware routing: per po_line, '
  'splits qty between ap_unsettled (un-billed portion) and ap '
  '(billed portion); PPV reversal follows the same split. '
  'qty_to_ap_unsettled / qty_to_ap recorded on po_return_lines. '
  'p_override_closed_period passes through to post_transfers (for '
  'reopened-period back-posts). Strict over-return P0047.';

-- ============================================================
-- 4. post_customer_return — state-aware + p_override_closed_period
-- ============================================================

DROP FUNCTION IF EXISTS post_customer_return(UUID, JSONB, DATE, UUID, UUID, TEXT);

CREATE FUNCTION post_customer_return(
  p_customer_id            UUID,
  p_lines                  JSONB,
  p_business_date          DATE,
  p_posted_by              UUID,
  p_idempotency_key        UUID,
  p_notes                  TEXT DEFAULT NULL,
  p_override_closed_period BOOLEAN DEFAULT FALSE
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id        UUID;
  v_doc_id             UUID;
  v_customer_check     UUID;
  v_n                  INT;
  v_idx                INT;
  v_line               JSONB;
  v_ship_line_id       UUID;
  v_qty_returned       BIGINT;
  v_disposition        return_disposition;
  v_sl                 RECORD;
  v_so_line_id         UUID;
  v_total_shipped      BIGINT;
  v_total_invoiced     BIGINT;
  v_prior_to_unsettled BIGINT;
  v_prior_to_ar        BIGINT;
  v_unsettled_rem      BIGINT;
  v_ar_rem             BIGINT;
  v_qty_to_unsettled   BIGINT;
  v_qty_to_ar          BIGINT;
  v_tax_pro            BIGINT;
  v_qty_dr_acct        BIGINT;
  v_val_dr_acct        BIGINT;
  v_cust_qty           BIGINT;
  v_cust_unsettled     BIGINT;
  v_cust_ar            BIGINT;
  v_cogs_acct          BIGINT;
  v_revenue_acct       BIGINT;
  v_tax_acct           BIGINT;
  v_var_scrap          BIGINT;
  v_return_line_id     UUID;
  v_batch              JSONB := '[]'::JSONB;
BEGIN
  -- Fast-path replay.
  SELECT id INTO v_existing_id FROM customer_returns
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

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

    SELECT
      ssl.id              AS ship_line_id,
      ssl.so_line_id      AS so_line_id,
      ssl.qty_shipped     AS qty_shipped,
      ssl.unit_cost       AS unit_cost,
      ssl.unit_price      AS unit_price,
      ssl.tax_amount      AS tax_amount,
      sl.sku_id           AS sku_id,
      sl.ship_location_id AS ship_location_id,
      sl.currency         AS currency,
      so.customer_id      AS customer_id
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

    v_so_line_id := v_sl.so_line_id;

    -- Lock the so_line row so cumulative reads serialize against
    -- concurrent shipments / invoices / returns (acct-du2.9 mirror).
    PERFORM 1 FROM sales_order_lines WHERE id = v_so_line_id FOR UPDATE;

    -- Cumulative state per so_line.
    SELECT COALESCE(SUM(qty_shipped), 0)
      INTO v_total_shipped
      FROM so_shipment_lines
     WHERE so_line_id = v_so_line_id;

    SELECT COALESCE(SUM(qty), 0)
      INTO v_total_invoiced
      FROM customer_invoice_lines
     WHERE so_line_id = v_so_line_id AND kind = 'so_match';

    SELECT
      COALESCE(SUM(crl.qty_to_ar_unsettled), 0),
      COALESCE(SUM(crl.qty_to_ar), 0)
      INTO v_prior_to_unsettled, v_prior_to_ar
      FROM customer_return_lines crl
      JOIN so_shipment_lines     ssl ON ssl.id = crl.ship_line_id
      JOIN customer_returns      cr  ON cr.id  = crl.return_id
     WHERE ssl.so_line_id = v_so_line_id
       AND cr.id <> v_doc_id;

    v_unsettled_rem := v_total_shipped - v_total_invoiced - v_prior_to_unsettled;
    v_ar_rem        := v_total_invoiced - v_prior_to_ar;

    v_qty_to_unsettled := LEAST(v_qty_returned, GREATEST(v_unsettled_rem, 0));
    v_qty_to_ar        := v_qty_returned - v_qty_to_unsettled;

    IF v_qty_to_ar > v_ar_rem THEN
      RAISE EXCEPTION
        'customer_return_overreturned: so_line % cumulative would exceed invoiced-not-returned + uninvoiced-not-returned (shipped=%, invoiced=%, prior_to_unsettled=%, prior_to_ar=%, requested=%)',
        v_so_line_id, v_total_shipped, v_total_invoiced,
        v_prior_to_unsettled, v_prior_to_ar, v_qty_returned
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
    ELSE
      SELECT id INTO v_var_scrap FROM accounts
       WHERE kind='variance_scrap' AND currency=v_sl.currency AND NOT is_closed;
      IF v_var_scrap IS NULL THEN
        RAISE EXCEPTION 'no open variance_scrap for ccy=%',
                        v_sl.currency USING ERRCODE = 'P0010';
      END IF;
      v_val_dr_acct := v_var_scrap;
    END IF;

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

    -- ar_unsettled is the route for revenue's un-invoiced portion AND
    -- always the route for tax (since post_customer_invoice doesn't
    -- migrate tax for so_match lines — ship-side tax stays parked).
    IF v_qty_to_unsettled > 0 OR v_tax_pro > 0 THEN
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
    END IF;

    IF v_qty_to_ar > 0 THEN
      SELECT id INTO v_cust_ar FROM accounts
       WHERE kind='ar' AND counterparty_id=p_customer_id
         AND currency=v_sl.currency AND NOT is_closed;
      IF v_cust_ar IS NULL THEN
        RAISE EXCEPTION 'no open ar for customer=% ccy=%',
                        p_customer_id, v_sl.currency USING ERRCODE = 'P0010';
      END IF;
    END IF;

    INSERT INTO customer_return_lines (
      return_id, line_no, ship_line_id, qty_returned, disposition,
      unit_cost, unit_price, tax_amount,
      qty_to_ar_unsettled, qty_to_ar
    ) VALUES (
      v_doc_id, v_idx, v_ship_line_id, v_qty_returned, v_disposition,
      v_sl.unit_cost, v_sl.unit_price, v_tax_pro,
      v_qty_to_unsettled, v_qty_to_ar
    )
    RETURNING id INTO v_return_line_id;

    -- Event 1: qty leg (single, full qty_returned).
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

    -- Event 2: value leg (cogs reversal, single — full qty regardless of route).
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

    -- Event 3a: revenue reversal — un-invoiced portion routes to ar_unsettled.
    IF v_qty_to_unsettled > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'customer_return',
        'document_kind',     'customer_return',
        'document_id',       v_doc_id,
        'document_line_id',  v_return_line_id,
        'debit_account_id',  v_revenue_acct,
        'credit_account_id', v_cust_unsettled,
        'amount',            v_qty_to_unsettled * v_sl.unit_price,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_customer_id,
        'posted_by',         p_posted_by
      ));
    END IF;

    -- Event 3b: revenue reversal — invoiced portion routes to ar.
    IF v_qty_to_ar > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'customer_return',
        'document_kind',     'customer_return',
        'document_id',       v_doc_id,
        'document_line_id',  v_return_line_id,
        'debit_account_id',  v_revenue_acct,
        'credit_account_id', v_cust_ar,
        'amount',            v_qty_to_ar * v_sl.unit_price,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_customer_id,
        'posted_by',         p_posted_by
      ));
    END IF;

    -- Event 4: tax reversal — always credits ar_unsettled (ship-side
    -- tax parking, untouched by invoice on so_match lines).
    IF v_tax_pro > 0 THEN
      SELECT id INTO v_tax_acct FROM accounts
       WHERE kind='sales_tax_payable' AND currency=v_sl.currency
         AND NOT is_closed;
      IF v_tax_acct IS NULL THEN
        RAISE EXCEPTION 'no open sales_tax_payable for ccy=%',
                        v_sl.currency USING ERRCODE = 'P0010';
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

  PERFORM post_transfers(v_batch, p_override_closed_period);

  RETURN v_doc_id;
END;
$$;

COMMENT ON FUNCTION post_customer_return(UUID, JSONB, DATE, UUID, UUID, TEXT, BOOLEAN) IS
  'Customer return / credit memo. State-aware routing: per so_line, '
  'splits revenue between ar_unsettled (un-invoiced) and ar '
  '(invoiced). qty_to_ar_unsettled / qty_to_ar recorded on '
  'customer_return_lines. Tax always reverses against ar_unsettled '
  '(invoice does not migrate tax for so_match). '
  'p_override_closed_period passes through to post_transfers. '
  'Strict over-return P0045.';

-- ============================================================
-- 5. post_ap_bill — subtract returns_to_ap_unsettled from v_avail
-- ============================================================
--
-- Round-trip correctness: receive 10 → return 3 to ap_unsettled →
-- the bill availability for that po_line is 7, not 10. Without this
-- fix, a billed-10 attempt would pass the v_avail gate and trip
-- accounts_check on ap_unsettled (only 7 credit available, can't
-- drain 10).

CREATE OR REPLACE FUNCTION post_ap_bill(
  p_vendor_id       UUID,
  p_currency        CHAR(3),
  p_lines           JSONB,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id    UUID;
  v_doc_id         UUID;
  v_vendor_check   UUID;
  v_n              INT;
  v_idx            INT;
  v_line           JSONB;
  v_kind           TEXT;
  v_po_line_id     UUID;
  v_qty            BIGINT;
  v_unit_cost      BIGINT;
  v_amount         BIGINT;
  v_expense_acct   BIGINT;
  v_pl             RECORD;
  v_total_received BIGINT;
  v_total_billed   BIGINT;
  v_returns_to_us  BIGINT;
  v_avail          BIGINT;
  v_ven_unsettled  BIGINT;
  v_ven_ap         BIGINT;
  v_exp_acct       accounts%ROWTYPE;
  v_bill_line_id   UUID;
  v_batch          JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM vendor_bills
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT id INTO v_vendor_check FROM vendors WHERE id = p_vendor_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'ap_bill_invalid_line: vendor % not found', p_vendor_id
      USING ERRCODE = 'P0025';
  END IF;

  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'ap_bill_invalid_line: empty bill for vendor %', p_vendor_id
      USING ERRCODE = 'P0025';
  END IF;

  SELECT id INTO v_ven_ap FROM accounts
   WHERE kind='ap' AND counterparty_id=p_vendor_id
     AND currency=p_currency AND NOT is_closed;
  IF v_ven_ap IS NULL THEN
    RAISE EXCEPTION 'no open ap account for vendor=% ccy=%',
                    p_vendor_id, p_currency USING ERRCODE = 'P0010';
  END IF;

  INSERT INTO vendor_bills (
    vendor_id, currency, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_vendor_id, p_currency, p_business_date, p_posted_by,
    p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM vendor_bills WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  FOR v_idx IN 1..v_n LOOP
    v_line   := p_lines -> (v_idx - 1);
    v_kind   := v_line->>'kind';
    v_amount := (v_line->>'amount')::BIGINT;

    IF v_kind = 'po_match' THEN
      v_po_line_id := (v_line->>'po_line_id')::UUID;
      v_qty        := (v_line->>'qty')::BIGINT;
      v_unit_cost  := (v_line->>'unit_cost')::BIGINT;

      SELECT pl.po_id, pl.unit_cost, pl.currency, po.vendor_id
        INTO v_pl
        FROM purchase_order_lines pl
        JOIN purchase_orders po ON po.id = pl.po_id
       WHERE pl.id = v_po_line_id
         FOR UPDATE OF pl;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'ap_bill_invalid_line: line % po_line % not found',
                        v_idx, v_po_line_id USING ERRCODE = 'P0025';
      END IF;
      IF v_pl.vendor_id IS DISTINCT FROM p_vendor_id THEN
        RAISE EXCEPTION
          'ap_bill_invalid_line: line % po_line % belongs to vendor % '
          'but bill is for vendor %',
          v_idx, v_po_line_id, v_pl.vendor_id, p_vendor_id
          USING ERRCODE = 'P0025';
      END IF;
      IF v_pl.currency <> p_currency THEN
        RAISE EXCEPTION
          'ap_bill_invalid_line: line % po_line currency=% but bill currency=%',
          v_idx, v_pl.currency, p_currency USING ERRCODE = 'P0025';
      END IF;

      IF v_unit_cost <> v_pl.unit_cost THEN
        RAISE EXCEPTION
          'ap_bill_three_way_mismatch: line % unit_cost % does not match '
          'po_line.unit_cost %',
          v_idx, v_unit_cost, v_pl.unit_cost
          USING ERRCODE = 'P0024';
      END IF;
      IF v_amount <> v_qty * v_unit_cost THEN
        RAISE EXCEPTION
          'ap_bill_three_way_mismatch: line % amount % <> qty % × unit_cost %',
          v_idx, v_amount, v_qty, v_unit_cost
          USING ERRCODE = 'P0024';
      END IF;

      SELECT COALESCE(SUM(qty_received), 0) INTO v_total_received
        FROM po_receipt_lines WHERE po_line_id = v_po_line_id;
      SELECT COALESCE(SUM(qty), 0) INTO v_total_billed
        FROM vendor_bill_lines
       WHERE po_line_id = v_po_line_id AND kind = 'po_match';
      SELECT COALESCE(SUM(prl.qty_to_ap_unsettled), 0) INTO v_returns_to_us
        FROM po_return_lines prl
        JOIN po_receipt_lines rcl ON rcl.id = prl.recv_line_id
       WHERE rcl.po_line_id = v_po_line_id;
      v_avail := v_total_received - v_total_billed - v_returns_to_us;
      IF v_qty > v_avail THEN
        RAISE EXCEPTION
          'ap_bill_three_way_mismatch: line % qty % exceeds received-not-'
          'billed-not-returned remainder % for po_line % (received=%, '
          'already billed=%, prior returns to ap_unsettled=%)',
          v_idx, v_qty, v_avail, v_po_line_id, v_total_received,
          v_total_billed, v_returns_to_us
          USING ERRCODE = 'P0024';
      END IF;

      SELECT id INTO v_ven_unsettled FROM accounts
       WHERE kind='ap_unsettled' AND counterparty_id=p_vendor_id
         AND currency=p_currency AND NOT is_closed;
      IF v_ven_unsettled IS NULL THEN
        RAISE EXCEPTION 'no open ap_unsettled account for vendor=% ccy=%',
                        p_vendor_id, p_currency USING ERRCODE = 'P0010';
      END IF;

      INSERT INTO vendor_bill_lines (
        bill_id, line_no, kind, po_line_id, qty, unit_cost, amount
      ) VALUES (
        v_doc_id, v_idx, 'po_match', v_po_line_id, v_qty, v_unit_cost, v_amount
      ) RETURNING id INTO v_bill_line_id;

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ap_bill',
        'document_kind',     'vendor_bill',
        'document_id',       v_doc_id,
        'document_line_id',  v_bill_line_id,
        'debit_account_id',  v_ven_unsettled,
        'credit_account_id', v_ven_ap,
        'amount',            v_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_vendor_id,
        'posted_by',         p_posted_by
      ));

    ELSIF v_kind = 'service' THEN
      v_expense_acct := (v_line->>'expense_account_id')::BIGINT;

      SELECT * INTO v_exp_acct FROM accounts WHERE id = v_expense_acct;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'ap_bill_invalid_line: line % expense_account_id % not found',
                        v_idx, v_expense_acct USING ERRCODE = 'P0025';
      END IF;
      IF v_exp_acct.is_closed THEN
        RAISE EXCEPTION 'ap_bill_invalid_line: line % expense account % is closed',
                        v_idx, v_expense_acct USING ERRCODE = 'P0025';
      END IF;
      IF v_exp_acct.ledger_kind <> 'value' THEN
        RAISE EXCEPTION 'ap_bill_invalid_line: line % expense account % is %, expected value',
                        v_idx, v_expense_acct, v_exp_acct.ledger_kind
          USING ERRCODE = 'P0025';
      END IF;
      IF v_exp_acct.currency <> p_currency THEN
        RAISE EXCEPTION 'ap_bill_invalid_line: line % expense account ccy=% but bill ccy=%',
                        v_idx, v_exp_acct.currency, p_currency
          USING ERRCODE = 'P0025';
      END IF;

      INSERT INTO vendor_bill_lines (
        bill_id, line_no, kind, expense_account_id, amount
      ) VALUES (
        v_doc_id, v_idx, 'service', v_expense_acct, v_amount
      ) RETURNING id INTO v_bill_line_id;

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ap_bill',
        'document_kind',     'vendor_bill',
        'document_id',       v_doc_id,
        'document_line_id',  v_bill_line_id,
        'debit_account_id',  v_expense_acct,
        'credit_account_id', v_ven_ap,
        'amount',            v_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_vendor_id,
        'posted_by',         p_posted_by
      ));

    ELSE
      RAISE EXCEPTION 'ap_bill_invalid_line: line % unknown kind %',
                      v_idx, v_kind USING ERRCODE = 'P0025';
    END IF;
  END LOOP;

  PERFORM post_transfers(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;

-- ============================================================
-- 6. post_customer_invoice — subtract returns_to_ar_unsettled from v_avail
-- ============================================================
--
-- Mirror of post_ap_bill fix: ship 10 → return 3 to ar_unsettled →
-- invoice availability for that so_line is 7, not 10.

CREATE OR REPLACE FUNCTION post_customer_invoice(
  p_customer_id     UUID,
  p_currency        CHAR(3),
  p_lines           JSONB,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id     UUID;
  v_doc_id          UUID;
  v_customer_check  UUID;
  v_n               INT;
  v_idx             INT;
  v_line            JSONB;
  v_kind            TEXT;
  v_so_line_id      UUID;
  v_qty             BIGINT;
  v_unit_price      BIGINT;
  v_amount          BIGINT;
  v_tax_amount      BIGINT;
  v_revenue_acct_id BIGINT;
  v_sl              RECORD;
  v_total_shipped   BIGINT;
  v_total_invoiced  BIGINT;
  v_returns_to_us   BIGINT;
  v_avail           BIGINT;
  v_cust_unsettled  BIGINT;
  v_cust_ar         BIGINT;
  v_cust_tax        BIGINT;
  v_rev_acct        accounts%ROWTYPE;
  v_inv_line_id     UUID;
  v_batch           JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM customer_invoices
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT id INTO v_customer_check FROM customers WHERE id = p_customer_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'customer_invoice_invalid_line: customer % not found',
                    p_customer_id USING ERRCODE = 'P0041';
  END IF;

  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'customer_invoice_invalid_line: empty invoice for customer %',
                    p_customer_id USING ERRCODE = 'P0041';
  END IF;

  SELECT id INTO v_cust_ar FROM accounts
   WHERE kind='ar' AND counterparty_id=p_customer_id
     AND currency=p_currency AND NOT is_closed;
  IF v_cust_ar IS NULL THEN
    RAISE EXCEPTION 'no open ar account for customer=% ccy=%',
                    p_customer_id, p_currency USING ERRCODE = 'P0010';
  END IF;

  INSERT INTO customer_invoices (
    customer_id, currency, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_customer_id, p_currency, p_business_date, p_posted_by,
    p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM customer_invoices
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  FOR v_idx IN 1..v_n LOOP
    v_line       := p_lines -> (v_idx - 1);
    v_kind       := v_line->>'kind';
    v_amount     := (v_line->>'amount')::BIGINT;
    v_tax_amount := COALESCE((v_line->>'tax_amount')::BIGINT, 0);

    IF v_kind = 'so_match' THEN
      v_so_line_id := (v_line->>'so_line_id')::UUID;
      v_qty        := (v_line->>'qty')::BIGINT;
      v_unit_price := (v_line->>'unit_price')::BIGINT;

      SELECT sl.so_id, sl.unit_price, sl.currency, so.customer_id
        INTO v_sl
        FROM sales_order_lines sl
        JOIN sales_orders so ON so.id = sl.so_id
       WHERE sl.id = v_so_line_id
         FOR UPDATE OF sl;
      IF NOT FOUND THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % so_line % not found',
          v_idx, v_so_line_id USING ERRCODE = 'P0041';
      END IF;
      IF v_sl.customer_id IS DISTINCT FROM p_customer_id THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % so_line % belongs to '
          'customer % but invoice is for customer %',
          v_idx, v_so_line_id, v_sl.customer_id, p_customer_id
          USING ERRCODE = 'P0041';
      END IF;
      IF v_sl.currency <> p_currency THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % so_line currency=% but invoice currency=%',
          v_idx, v_sl.currency, p_currency USING ERRCODE = 'P0041';
      END IF;

      IF v_unit_price <> v_sl.unit_price THEN
        RAISE EXCEPTION
          'customer_invoice_three_way_mismatch: line % unit_price % does '
          'not match so_line.unit_price %',
          v_idx, v_unit_price, v_sl.unit_price USING ERRCODE = 'P0040';
      END IF;
      IF v_amount <> v_qty * v_unit_price THEN
        RAISE EXCEPTION
          'customer_invoice_three_way_mismatch: line % amount % <> qty % × unit_price %',
          v_idx, v_amount, v_qty, v_unit_price USING ERRCODE = 'P0040';
      END IF;

      SELECT COALESCE(SUM(qty_shipped), 0) INTO v_total_shipped
        FROM so_shipment_lines WHERE so_line_id = v_so_line_id;
      SELECT COALESCE(SUM(qty), 0) INTO v_total_invoiced
        FROM customer_invoice_lines
       WHERE so_line_id = v_so_line_id AND kind = 'so_match';
      SELECT COALESCE(SUM(crl.qty_to_ar_unsettled), 0) INTO v_returns_to_us
        FROM customer_return_lines crl
        JOIN so_shipment_lines     ssl ON ssl.id = crl.ship_line_id
       WHERE ssl.so_line_id = v_so_line_id;
      v_avail := v_total_shipped - v_total_invoiced - v_returns_to_us;
      IF v_qty > v_avail THEN
        RAISE EXCEPTION
          'customer_invoice_three_way_mismatch: line % qty % exceeds '
          'shipped-not-invoiced-not-returned remainder % for so_line % '
          '(shipped=%, already invoiced=%, prior returns to ar_unsettled=%)',
          v_idx, v_qty, v_avail, v_so_line_id, v_total_shipped,
          v_total_invoiced, v_returns_to_us
          USING ERRCODE = 'P0040';
      END IF;

      SELECT id INTO v_cust_unsettled FROM accounts
       WHERE kind='ar_unsettled' AND counterparty_id=p_customer_id
         AND currency=p_currency AND NOT is_closed;
      IF v_cust_unsettled IS NULL THEN
        RAISE EXCEPTION 'no open ar_unsettled account for customer=% ccy=%',
                        p_customer_id, p_currency USING ERRCODE = 'P0010';
      END IF;

      INSERT INTO customer_invoice_lines (
        invoice_id, line_no, kind, so_line_id, qty, unit_price, amount, tax_amount
      ) VALUES (
        v_doc_id, v_idx, 'so_match', v_so_line_id, v_qty, v_unit_price,
        v_amount, v_tax_amount
      ) RETURNING id INTO v_inv_line_id;

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ar_invoice',
        'document_kind',     'customer_invoice',
        'document_id',       v_doc_id,
        'document_line_id',  v_inv_line_id,
        'debit_account_id',  v_cust_ar,
        'credit_account_id', v_cust_unsettled,
        'amount',            v_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_customer_id,
        'posted_by',         p_posted_by
      ));

    ELSIF v_kind = 'service' THEN
      v_revenue_acct_id := (v_line->>'revenue_account_id')::BIGINT;

      SELECT * INTO v_rev_acct FROM accounts WHERE id = v_revenue_acct_id;
      IF NOT FOUND THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % revenue_account_id % not found',
          v_idx, v_revenue_acct_id USING ERRCODE = 'P0041';
      END IF;
      IF v_rev_acct.is_closed THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % revenue account % is closed',
          v_idx, v_revenue_acct_id USING ERRCODE = 'P0041';
      END IF;
      IF v_rev_acct.ledger_kind <> 'value' THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % revenue account % is %, expected value',
          v_idx, v_revenue_acct_id, v_rev_acct.ledger_kind
          USING ERRCODE = 'P0041';
      END IF;
      IF v_rev_acct.currency <> p_currency THEN
        RAISE EXCEPTION
          'customer_invoice_invalid_line: line % revenue account ccy=% but invoice ccy=%',
          v_idx, v_rev_acct.currency, p_currency USING ERRCODE = 'P0041';
      END IF;

      INSERT INTO customer_invoice_lines (
        invoice_id, line_no, kind, revenue_account_id, amount, tax_amount
      ) VALUES (
        v_doc_id, v_idx, 'service', v_revenue_acct_id, v_amount, v_tax_amount
      ) RETURNING id INTO v_inv_line_id;

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ar_invoice',
        'document_kind',     'customer_invoice',
        'document_id',       v_doc_id,
        'document_line_id',  v_inv_line_id,
        'debit_account_id',  v_cust_ar,
        'credit_account_id', v_revenue_acct_id,
        'amount',            v_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_customer_id,
        'posted_by',         p_posted_by
      ));

      IF v_tax_amount > 0 THEN
        SELECT id INTO v_cust_tax FROM accounts
         WHERE kind='sales_tax_payable' AND ledger_kind='value'
           AND currency=p_currency AND NOT is_closed;
        IF v_cust_tax IS NULL THEN
          RAISE EXCEPTION 'no open sales_tax_payable account for ccy=%',
                          p_currency USING ERRCODE = 'P0010';
        END IF;
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'ar_invoice',
          'document_kind',     'customer_invoice',
          'document_id',       v_doc_id,
          'document_line_id',  v_inv_line_id,
          'debit_account_id',  v_cust_ar,
          'credit_account_id', v_cust_tax,
          'amount',            v_tax_amount,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'counterparty_id',   p_customer_id,
          'posted_by',         p_posted_by
        ));
      END IF;

    ELSE
      RAISE EXCEPTION 'customer_invoice_invalid_line: line % unknown kind %',
                      v_idx, v_kind USING ERRCODE = 'P0041';
    END IF;
  END LOOP;

  PERFORM post_transfers(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;
