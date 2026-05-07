-- acct-tae + acct-b6e — standalone customer credit memo / vendor debit memo.
--
-- post_customer_return (mig 0084) requires every line to reference a
-- so_shipment_lines row; post_po_return (mig 0085) requires every line
-- to reference a po_receipt_lines row. The NOT NULL FKs are load-bearing
-- for three-way auditability of returns-tied-to-original.
--
-- Standalone memos cover the cases where there's no original to point to:
--
--   AR side (customer credit memo):
--     - Goodwill credit (no goods returning)
--     - Dispute settlement (financial only)
--     - Volume / promotional rebate
--     - Stale-SO return (goods come back; original SO archived)
--
--   AP side (vendor debit memo):
--     - Disputed invoice (no goods returning)
--     - Damage-in-transit allowance (kept the goods at reduced cost)
--     - Vendor allowance / rebate (financial only)
--     - Stale-PO return
--     - Duplicate-bill clearance
--
-- Mainstream-ERP convention (SAP / Oracle / NetSuite / D365) treats
-- credit memo / debit memo as separate document types from sales-
-- return / purchase-return. We mirror that.
--
-- Routing: standalone memos always post against the CLEARED account
-- (ar / ap), not the staging account. Standalone means there's no
-- known link to a specific shipment/receipt, so the state-aware
-- routing of acct-tk7 doesn't apply. If the caller needs pre-invoice
-- or pre-bill routing, they should use post_customer_return /
-- post_po_return (which carries a line reference).
--
-- For 'financial' line kind, the caller specifies the offsetting
-- account_id (revenue_account_id on AR — the debit side; or
-- expense_account_id on AP — the credit side); the function adds the
-- ar/ap leg.
--
-- For 'goods_return' line kind without a line reference, the function
-- reverses qty + value + ar/ap exactly like the corresponding
-- per-line function but uses caller-supplied unit_cost. PPV is NOT
-- computed for standalone goods returns (no original po_line.unit_cost
-- to compare against; documented limitation).
--
-- New error codes:
--   P0048 customer_credit_memo_invalid
--   P0049 vendor_debit_memo_invalid

-- ============================================================
-- 1. customer_credit_memos + customer_credit_memo_lines
-- ============================================================

CREATE TABLE customer_credit_memos (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  customer_id     UUID NOT NULL REFERENCES customers(id),
  currency        CHAR(3) NOT NULL,
  business_date   DATE NOT NULL,
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID NOT NULL UNIQUE,
  notes           TEXT
);

CREATE INDEX customer_credit_memos_customer  ON customer_credit_memos (customer_id);
CREATE INDEX customer_credit_memos_posted_at ON customer_credit_memos (posted_at);

COMMENT ON TABLE customer_credit_memos IS
  'Standalone AR credit memo header. Distinct from customer_returns '
  '(which references a so_shipment_line). Used for goodwill credit, '
  'dispute settlement, volume rebate, stale-SO return.';

CREATE TABLE customer_credit_memo_lines (
  id                 UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  memo_id            UUID NOT NULL REFERENCES customer_credit_memos(id),
  line_no            INT NOT NULL,
  kind               TEXT NOT NULL CHECK (kind IN ('financial', 'goods_return')),
  -- 'financial' fields
  revenue_account_id BIGINT REFERENCES accounts(id),
  -- 'goods_return' fields
  sku_id             UUID REFERENCES skus(id),
  location_id        UUID REFERENCES locations(id),
  qty                BIGINT,
  unit_cost          BIGINT,
  unit_price         BIGINT,
  disposition        return_disposition,
  -- Always-present
  amount             BIGINT NOT NULL,
  tax_amount         BIGINT NOT NULL DEFAULT 0,
  UNIQUE (memo_id, line_no),
  CHECK (
    (kind = 'financial'
       AND revenue_account_id IS NOT NULL
       AND sku_id IS NULL AND location_id IS NULL
       AND qty IS NULL AND unit_cost IS NULL AND disposition IS NULL)
    OR
    (kind = 'goods_return'
       AND revenue_account_id IS NULL
       AND sku_id IS NOT NULL AND location_id IS NOT NULL
       AND qty IS NOT NULL AND qty > 0
       AND unit_cost IS NOT NULL AND unit_cost >= 0
       AND unit_price IS NOT NULL AND unit_price >= 0
       AND disposition IS NOT NULL)
  ),
  CHECK (amount >= 0 AND tax_amount >= 0)
);

CREATE INDEX customer_credit_memo_lines_sku ON customer_credit_memo_lines (sku_id);

COMMENT ON TABLE customer_credit_memo_lines IS
  'Per-line credit memo detail. kind=financial for pure financial '
  'credit (revenue_account_id specifies the debit side). '
  'kind=goods_return for goods returning without a ship reference '
  '(unit_cost / unit_price caller-supplied; PPV not computed).';

-- ============================================================
-- 2. post_customer_credit_memo
-- ============================================================

CREATE FUNCTION post_customer_credit_memo(
  p_customer_id            UUID,
  p_currency               CHAR(3),
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
  v_existing_id      UUID;
  v_doc_id           UUID;
  v_customer_check   UUID;
  v_n                INT;
  v_idx              INT;
  v_line             JSONB;
  v_kind             TEXT;
  v_amount           BIGINT;
  v_tax_amount       BIGINT;
  v_revenue_acct_id  BIGINT;
  v_rev_acct         accounts%ROWTYPE;
  v_sku_id           UUID;
  v_location_id      UUID;
  v_qty              BIGINT;
  v_unit_cost        BIGINT;
  v_unit_price       BIGINT;
  v_disposition      return_disposition;
  v_qty_dr_acct      BIGINT;
  v_val_dr_acct      BIGINT;
  v_var_scrap        BIGINT;
  v_cust_qty         BIGINT;
  v_cust_ar          BIGINT;
  v_cogs_acct        BIGINT;
  v_tax_acct         BIGINT;
  v_memo_line_id     UUID;
  v_batch            JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM customer_credit_memos
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT id INTO v_customer_check FROM customers WHERE id = p_customer_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'customer_credit_memo_invalid: customer % not found',
                    p_customer_id USING ERRCODE = 'P0048';
  END IF;

  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'customer_credit_memo_invalid: empty lines for customer %',
                    p_customer_id USING ERRCODE = 'P0048';
  END IF;

  -- Resolve customer ar (the always-credited account for standalone memos).
  SELECT id INTO v_cust_ar FROM accounts
   WHERE kind='ar' AND counterparty_id=p_customer_id
     AND currency=p_currency AND NOT is_closed;
  IF v_cust_ar IS NULL THEN
    RAISE EXCEPTION 'no open ar account for customer=% ccy=%',
                    p_customer_id, p_currency USING ERRCODE = 'P0010';
  END IF;

  INSERT INTO customer_credit_memos (
    customer_id, currency, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_customer_id, p_currency, p_business_date, p_posted_by,
    p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM customer_credit_memos
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  FOR v_idx IN 1..v_n LOOP
    v_line       := p_lines -> (v_idx - 1);
    v_kind       := v_line->>'kind';
    v_amount     := (v_line->>'amount')::BIGINT;
    v_tax_amount := COALESCE((v_line->>'tax_amount')::BIGINT, 0);

    IF v_amount IS NULL OR v_amount < 0 THEN
      RAISE EXCEPTION 'customer_credit_memo_invalid: line % amount must be >= 0',
                      v_idx USING ERRCODE = 'P0048';
    END IF;

    IF v_kind = 'financial' THEN
      v_revenue_acct_id := (v_line->>'revenue_account_id')::BIGINT;

      SELECT * INTO v_rev_acct FROM accounts WHERE id = v_revenue_acct_id;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'customer_credit_memo_invalid: line % revenue_account_id % not found',
                        v_idx, v_revenue_acct_id USING ERRCODE = 'P0048';
      END IF;
      IF v_rev_acct.is_closed THEN
        RAISE EXCEPTION 'customer_credit_memo_invalid: line % revenue_account_id % is closed',
                        v_idx, v_revenue_acct_id USING ERRCODE = 'P0048';
      END IF;
      IF v_rev_acct.ledger_kind <> 'value' THEN
        RAISE EXCEPTION 'customer_credit_memo_invalid: line % revenue_account_id % is %, expected value',
                        v_idx, v_revenue_acct_id, v_rev_acct.ledger_kind
          USING ERRCODE = 'P0048';
      END IF;
      IF v_rev_acct.currency <> p_currency THEN
        RAISE EXCEPTION 'customer_credit_memo_invalid: line % revenue ccy=% but memo ccy=%',
                        v_idx, v_rev_acct.currency, p_currency USING ERRCODE = 'P0048';
      END IF;

      INSERT INTO customer_credit_memo_lines (
        memo_id, line_no, kind, revenue_account_id, amount, tax_amount
      ) VALUES (
        v_doc_id, v_idx, 'financial', v_revenue_acct_id, v_amount, v_tax_amount
      ) RETURNING id INTO v_memo_line_id;

      -- Revenue (or expense) DR / ar CR.
      IF v_amount > 0 THEN
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'customer_return',
          'document_kind',     'customer_credit_memo',
          'document_id',       v_doc_id,
          'document_line_id',  v_memo_line_id,
          'debit_account_id',  v_revenue_acct_id,
          'credit_account_id', v_cust_ar,
          'amount',            v_amount,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'counterparty_id',   p_customer_id,
          'posted_by',         p_posted_by
        ));
      END IF;

      -- Tax leg (sales_tax_payable DR / ar CR) if asserted.
      IF v_tax_amount > 0 THEN
        SELECT id INTO v_tax_acct FROM accounts
         WHERE kind='sales_tax_payable' AND ledger_kind='value'
           AND currency=p_currency AND NOT is_closed;
        IF v_tax_acct IS NULL THEN
          RAISE EXCEPTION 'no open sales_tax_payable for ccy=%',
                          p_currency USING ERRCODE = 'P0010';
        END IF;
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'customer_return',
          'document_kind',     'customer_credit_memo',
          'document_id',       v_doc_id,
          'document_line_id',  v_memo_line_id,
          'debit_account_id',  v_tax_acct,
          'credit_account_id', v_cust_ar,
          'amount',            v_tax_amount,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'counterparty_id',   p_customer_id,
          'posted_by',         p_posted_by
        ));
      END IF;

    ELSIF v_kind = 'goods_return' THEN
      v_sku_id      := (v_line->>'sku_id')::UUID;
      v_location_id := (v_line->>'location_id')::UUID;
      v_qty         := (v_line->>'qty')::BIGINT;
      v_unit_cost   := (v_line->>'unit_cost')::BIGINT;
      v_unit_price  := (v_line->>'unit_price')::BIGINT;
      v_disposition := (v_line->>'disposition')::return_disposition;

      IF v_qty IS NULL OR v_qty <= 0 THEN
        RAISE EXCEPTION 'customer_credit_memo_invalid: line % qty must be > 0',
                        v_idx USING ERRCODE = 'P0048';
      END IF;
      IF v_amount <> v_qty * v_unit_price THEN
        RAISE EXCEPTION 'customer_credit_memo_invalid: line % amount=% but qty=% × unit_price=%',
                        v_idx, v_amount, v_qty, v_unit_price USING ERRCODE = 'P0048';
      END IF;

      -- Resolve qty-side debit account by disposition.
      IF v_disposition = 'restock' THEN
        SELECT id INTO v_qty_dr_acct FROM accounts
         WHERE kind='stock_available' AND sku_id=v_sku_id
           AND location_id=v_location_id AND NOT is_closed;
        IF v_qty_dr_acct IS NULL THEN
          RAISE EXCEPTION 'no open stock_available for sku=% loc=%',
                          v_sku_id, v_location_id USING ERRCODE = 'P0010';
        END IF;
      ELSIF v_disposition = 'scrap' THEN
        SELECT id INTO v_qty_dr_acct FROM accounts
         WHERE kind='stock_scrap' AND sku_id=v_sku_id AND NOT is_closed;
        IF v_qty_dr_acct IS NULL THEN
          RAISE EXCEPTION 'no open stock_scrap for sku=%',
                          v_sku_id USING ERRCODE = 'P0010';
        END IF;
      ELSIF v_disposition = 'repair' THEN
        SELECT id INTO v_qty_dr_acct FROM accounts
         WHERE kind='stock_quarantine' AND sku_id=v_sku_id
           AND location_id=v_location_id AND NOT is_closed;
        IF v_qty_dr_acct IS NULL THEN
          RAISE EXCEPTION 'no open stock_quarantine for sku=% loc=%',
                          v_sku_id, v_location_id USING ERRCODE = 'P0010';
        END IF;
      END IF;

      -- Resolve value-side debit account.
      IF v_disposition IN ('restock', 'repair') THEN
        SELECT id INTO v_val_dr_acct FROM accounts
         WHERE kind='inv_value_fg' AND sku_id=v_sku_id
           AND location_id=v_location_id AND currency=p_currency
           AND NOT is_closed;
        IF v_val_dr_acct IS NULL THEN
          RAISE EXCEPTION 'no open inv_value_fg for sku=% loc=% ccy=%',
                          v_sku_id, v_location_id, p_currency USING ERRCODE = 'P0010';
        END IF;
      ELSE
        SELECT id INTO v_var_scrap FROM accounts
         WHERE kind='variance_scrap' AND currency=p_currency AND NOT is_closed;
        IF v_var_scrap IS NULL THEN
          RAISE EXCEPTION 'no open variance_scrap for ccy=%',
                          p_currency USING ERRCODE = 'P0010';
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
       WHERE kind='cogs' AND currency=p_currency AND NOT is_closed;
      IF v_cogs_acct IS NULL THEN
        RAISE EXCEPTION 'no open cogs for ccy=%',
                        p_currency USING ERRCODE = 'P0010';
      END IF;

      INSERT INTO customer_credit_memo_lines (
        memo_id, line_no, kind, sku_id, location_id, qty,
        unit_cost, unit_price, disposition, amount, tax_amount
      ) VALUES (
        v_doc_id, v_idx, 'goods_return', v_sku_id, v_location_id, v_qty,
        v_unit_cost, v_unit_price, v_disposition, v_amount, v_tax_amount
      ) RETURNING id INTO v_memo_line_id;

      -- qty leg.
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'customer_return',
        'document_kind',     'customer_credit_memo',
        'document_id',       v_doc_id,
        'document_line_id',  v_memo_line_id,
        'debit_account_id',  v_qty_dr_acct,
        'credit_account_id', v_cust_qty,
        'amount',            v_qty,
        'qty',               v_qty,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_customer_id,
        'posted_by',         p_posted_by
      ));

      -- value leg (cogs reversal).
      IF v_qty * v_unit_cost > 0 THEN
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'customer_return',
          'document_kind',     'customer_credit_memo',
          'document_id',       v_doc_id,
          'document_line_id',  v_memo_line_id,
          'debit_account_id',  v_val_dr_acct,
          'credit_account_id', v_cogs_acct,
          'amount',            v_qty * v_unit_cost,
          'qty',               v_qty,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'counterparty_id',   p_customer_id,
          'posted_by',         p_posted_by
        ));
      END IF;

      -- revenue reversal (revenue DR / ar CR).
      IF v_amount > 0 THEN
        SELECT id INTO v_revenue_acct_id FROM accounts
         WHERE kind='revenue' AND currency=p_currency AND NOT is_closed;
        IF v_revenue_acct_id IS NULL THEN
          RAISE EXCEPTION 'no open revenue for ccy=%',
                          p_currency USING ERRCODE = 'P0010';
        END IF;
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'customer_return',
          'document_kind',     'customer_credit_memo',
          'document_id',       v_doc_id,
          'document_line_id',  v_memo_line_id,
          'debit_account_id',  v_revenue_acct_id,
          'credit_account_id', v_cust_ar,
          'amount',            v_amount,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'counterparty_id',   p_customer_id,
          'posted_by',         p_posted_by
        ));
      END IF;

      -- tax leg (caller-supplied).
      IF v_tax_amount > 0 THEN
        SELECT id INTO v_tax_acct FROM accounts
         WHERE kind='sales_tax_payable' AND currency=p_currency
           AND NOT is_closed;
        IF v_tax_acct IS NULL THEN
          RAISE EXCEPTION 'no open sales_tax_payable for ccy=%',
                          p_currency USING ERRCODE = 'P0010';
        END IF;
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'customer_return',
          'document_kind',     'customer_credit_memo',
          'document_id',       v_doc_id,
          'document_line_id',  v_memo_line_id,
          'debit_account_id',  v_tax_acct,
          'credit_account_id', v_cust_ar,
          'amount',            v_tax_amount,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'counterparty_id',   p_customer_id,
          'posted_by',         p_posted_by
        ));
      END IF;

    ELSE
      RAISE EXCEPTION 'customer_credit_memo_invalid: line % unknown kind %',
                      v_idx, v_kind USING ERRCODE = 'P0048';
    END IF;
  END LOOP;

  PERFORM post_transfers(v_batch, p_override_closed_period);
  RETURN v_doc_id;
END;
$$;

COMMENT ON FUNCTION post_customer_credit_memo(UUID, CHAR, JSONB, DATE, UUID, UUID, TEXT, BOOLEAN) IS
  'Standalone AR credit memo (no shipment reference). financial '
  'lines post caller-supplied account DR / ar CR. goods_return '
  'lines reverse qty + cogs + revenue against ar (no PPV; '
  'caller-supplied unit_cost). Always routes to ar (cleared); '
  'pre-invoice routing requires post_customer_return.';

-- ============================================================
-- 3. vendor_debit_memos + vendor_debit_memo_lines
-- ============================================================

CREATE TABLE vendor_debit_memos (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  vendor_id       UUID NOT NULL REFERENCES vendors(id),
  currency        CHAR(3) NOT NULL,
  business_date   DATE NOT NULL,
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID NOT NULL UNIQUE,
  notes           TEXT
);

CREATE INDEX vendor_debit_memos_vendor    ON vendor_debit_memos (vendor_id);
CREATE INDEX vendor_debit_memos_posted_at ON vendor_debit_memos (posted_at);

COMMENT ON TABLE vendor_debit_memos IS
  'Standalone AP debit memo header. Distinct from po_returns. Used '
  'for disputed invoice, damage-in-transit allowance, vendor '
  'allowance / rebate, stale-PO return, duplicate-bill clearance.';

CREATE TABLE vendor_debit_memo_lines (
  id                 UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  memo_id            UUID NOT NULL REFERENCES vendor_debit_memos(id),
  line_no            INT NOT NULL,
  kind               TEXT NOT NULL CHECK (kind IN ('financial', 'goods_return')),
  expense_account_id BIGINT REFERENCES accounts(id),
  sku_id             UUID REFERENCES skus(id),
  location_id        UUID REFERENCES locations(id),
  qty                BIGINT,
  unit_cost          BIGINT,
  amount             BIGINT NOT NULL,
  UNIQUE (memo_id, line_no),
  CHECK (
    (kind = 'financial'
       AND expense_account_id IS NOT NULL
       AND sku_id IS NULL AND location_id IS NULL
       AND qty IS NULL AND unit_cost IS NULL)
    OR
    (kind = 'goods_return'
       AND expense_account_id IS NULL
       AND sku_id IS NOT NULL AND location_id IS NOT NULL
       AND qty IS NOT NULL AND qty > 0
       AND unit_cost IS NOT NULL AND unit_cost >= 0)
  ),
  CHECK (amount >= 0)
);

CREATE INDEX vendor_debit_memo_lines_sku ON vendor_debit_memo_lines (sku_id);

COMMENT ON TABLE vendor_debit_memo_lines IS
  'Per-line debit memo detail. kind=financial: caller specifies '
  'credit-side account (e.g., expense GL or inv_value_raw to reduce '
  'inventory cost without physical return). kind=goods_return: qty '
  'flows out via vendor_pool / stock_available; value flows out via '
  'inv_value_raw at caller-supplied unit_cost (no PPV computed).';

-- ============================================================
-- 4. post_vendor_debit_memo
-- ============================================================

CREATE FUNCTION post_vendor_debit_memo(
  p_vendor_id              UUID,
  p_currency               CHAR(3),
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
  v_existing_id      UUID;
  v_doc_id           UUID;
  v_vendor_check     UUID;
  v_n                INT;
  v_idx              INT;
  v_line             JSONB;
  v_kind             TEXT;
  v_amount           BIGINT;
  v_expense_acct_id  BIGINT;
  v_exp_acct         accounts%ROWTYPE;
  v_sku_id           UUID;
  v_location_id      UUID;
  v_qty              BIGINT;
  v_unit_cost        BIGINT;
  v_qty_acct         BIGINT;
  v_val_acct         BIGINT;
  v_ven_qty          BIGINT;
  v_ven_ap           BIGINT;
  v_memo_line_id     UUID;
  v_batch            JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM vendor_debit_memos
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT id INTO v_vendor_check FROM vendors WHERE id = p_vendor_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'vendor_debit_memo_invalid: vendor % not found',
                    p_vendor_id USING ERRCODE = 'P0049';
  END IF;

  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'vendor_debit_memo_invalid: empty lines for vendor %',
                    p_vendor_id USING ERRCODE = 'P0049';
  END IF;

  -- Resolve vendor ap (the always-debited account for standalone memos).
  SELECT id INTO v_ven_ap FROM accounts
   WHERE kind='ap' AND counterparty_id=p_vendor_id
     AND currency=p_currency AND NOT is_closed;
  IF v_ven_ap IS NULL THEN
    RAISE EXCEPTION 'no open ap account for vendor=% ccy=%',
                    p_vendor_id, p_currency USING ERRCODE = 'P0010';
  END IF;

  INSERT INTO vendor_debit_memos (
    vendor_id, currency, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_vendor_id, p_currency, p_business_date, p_posted_by,
    p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM vendor_debit_memos
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  FOR v_idx IN 1..v_n LOOP
    v_line   := p_lines -> (v_idx - 1);
    v_kind   := v_line->>'kind';
    v_amount := (v_line->>'amount')::BIGINT;

    IF v_amount IS NULL OR v_amount < 0 THEN
      RAISE EXCEPTION 'vendor_debit_memo_invalid: line % amount must be >= 0',
                      v_idx USING ERRCODE = 'P0049';
    END IF;

    IF v_kind = 'financial' THEN
      v_expense_acct_id := (v_line->>'expense_account_id')::BIGINT;

      SELECT * INTO v_exp_acct FROM accounts WHERE id = v_expense_acct_id;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'vendor_debit_memo_invalid: line % expense_account_id % not found',
                        v_idx, v_expense_acct_id USING ERRCODE = 'P0049';
      END IF;
      IF v_exp_acct.is_closed THEN
        RAISE EXCEPTION 'vendor_debit_memo_invalid: line % expense_account_id % is closed',
                        v_idx, v_expense_acct_id USING ERRCODE = 'P0049';
      END IF;
      IF v_exp_acct.ledger_kind <> 'value' THEN
        RAISE EXCEPTION 'vendor_debit_memo_invalid: line % expense account % is %, expected value',
                        v_idx, v_expense_acct_id, v_exp_acct.ledger_kind
          USING ERRCODE = 'P0049';
      END IF;
      IF v_exp_acct.currency <> p_currency THEN
        RAISE EXCEPTION 'vendor_debit_memo_invalid: line % expense ccy=% but memo ccy=%',
                        v_idx, v_exp_acct.currency, p_currency USING ERRCODE = 'P0049';
      END IF;

      INSERT INTO vendor_debit_memo_lines (
        memo_id, line_no, kind, expense_account_id, amount
      ) VALUES (
        v_doc_id, v_idx, 'financial', v_expense_acct_id, v_amount
      ) RETURNING id INTO v_memo_line_id;

      -- ap DR / expense (or whatever caller chose) CR.
      IF v_amount > 0 THEN
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'po_return_to_vendor',
          'document_kind',     'vendor_debit_memo',
          'document_id',       v_doc_id,
          'document_line_id',  v_memo_line_id,
          'debit_account_id',  v_ven_ap,
          'credit_account_id', v_expense_acct_id,
          'amount',            v_amount,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'counterparty_id',   p_vendor_id,
          'posted_by',         p_posted_by
        ));
      END IF;

    ELSIF v_kind = 'goods_return' THEN
      v_sku_id      := (v_line->>'sku_id')::UUID;
      v_location_id := (v_line->>'location_id')::UUID;
      v_qty         := (v_line->>'qty')::BIGINT;
      v_unit_cost   := (v_line->>'unit_cost')::BIGINT;

      IF v_qty IS NULL OR v_qty <= 0 THEN
        RAISE EXCEPTION 'vendor_debit_memo_invalid: line % qty must be > 0',
                        v_idx USING ERRCODE = 'P0049';
      END IF;
      IF v_amount <> v_qty * v_unit_cost THEN
        RAISE EXCEPTION 'vendor_debit_memo_invalid: line % amount=% but qty=% × unit_cost=%',
                        v_idx, v_amount, v_qty, v_unit_cost USING ERRCODE = 'P0049';
      END IF;

      SELECT id INTO v_qty_acct FROM accounts
       WHERE kind='stock_available' AND sku_id=v_sku_id
         AND location_id=v_location_id AND NOT is_closed;
      IF v_qty_acct IS NULL THEN
        RAISE EXCEPTION 'no open stock_available for sku=% loc=%',
                        v_sku_id, v_location_id USING ERRCODE = 'P0010';
      END IF;

      SELECT id INTO v_val_acct FROM accounts
       WHERE kind='inv_value_raw' AND sku_id=v_sku_id
         AND location_id=v_location_id AND currency=p_currency
         AND NOT is_closed;
      IF v_val_acct IS NULL THEN
        RAISE EXCEPTION 'no open inv_value_raw for sku=% loc=% ccy=%',
                        v_sku_id, v_location_id, p_currency USING ERRCODE = 'P0010';
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

      INSERT INTO vendor_debit_memo_lines (
        memo_id, line_no, kind, sku_id, location_id, qty, unit_cost, amount
      ) VALUES (
        v_doc_id, v_idx, 'goods_return', v_sku_id, v_location_id, v_qty,
        v_unit_cost, v_amount
      ) RETURNING id INTO v_memo_line_id;

      -- qty leg: vendor_pool DR / stock_available CR.
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'po_return_to_vendor',
        'document_kind',     'vendor_debit_memo',
        'document_id',       v_doc_id,
        'document_line_id',  v_memo_line_id,
        'debit_account_id',  v_ven_qty,
        'credit_account_id', v_qty_acct,
        'amount',            v_qty,
        'qty',               v_qty,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_vendor_id,
        'posted_by',         p_posted_by
      ));

      -- value leg: ap DR / inv_value_raw CR (no PPV — caller-supplied unit_cost).
      IF v_amount > 0 THEN
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'po_return_to_vendor',
          'document_kind',     'vendor_debit_memo',
          'document_id',       v_doc_id,
          'document_line_id',  v_memo_line_id,
          'debit_account_id',  v_ven_ap,
          'credit_account_id', v_val_acct,
          'amount',            v_amount,
          'qty',               v_qty,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'counterparty_id',   p_vendor_id,
          'posted_by',         p_posted_by
        ));
      END IF;

    ELSE
      RAISE EXCEPTION 'vendor_debit_memo_invalid: line % unknown kind %',
                      v_idx, v_kind USING ERRCODE = 'P0049';
    END IF;
  END LOOP;

  PERFORM post_transfers(v_batch, p_override_closed_period);
  RETURN v_doc_id;
END;
$$;

COMMENT ON FUNCTION post_vendor_debit_memo(UUID, CHAR, JSONB, DATE, UUID, UUID, TEXT, BOOLEAN) IS
  'Standalone AP debit memo (no receipt reference). financial '
  'lines post ap DR / caller-supplied account CR. goods_return '
  'lines flow qty out vendor_pool / stock_available and value out '
  'ap / inv_value_raw at caller-supplied unit_cost (no PPV). Always '
  'routes to ap (cleared); pre-bill routing requires post_po_return.';
