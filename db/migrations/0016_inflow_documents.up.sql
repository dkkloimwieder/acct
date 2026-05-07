-- Slice A inflow cycle + Phase 1 inventory-adjustment workflows.
--
-- Consolidates archive migs:
--   0022 (acct-dj8 / sb6)      — inventory_adjustments + post_inventory_adjustment
--   0024 (acct-14m / sb6)      — inventory_cost_adjustments + post_cost_adjustment
--   0028 (acct-hlr)            — inventory_standard_cost_rolls + post_standard_cost_roll
--   0031 (acct-9tw)            — post_inventory_adjustment wac_retroactive branch
--   0034 (acct-7mg)            — Slice A schema (vendors/PO/receipts/bills)
--   0036 (acct-397)            — supplier→vendor rename (already baked into 0006)
--   0069 (acct-fii)            — post_cost_adjustment per-class qty divisor
--   0078 (acct-bru)            — post_wip_material_revaluation companion
--   0082 (acct-bvh)            — ap_payments + post_ap_payment
--   0085 (acct-quk)            — po_returns + post_po_return
--   0086 (acct-tk7 / dso)      — return-routing split columns
--   0087 (acct-6d8)            — cost_method_at_receipt snapshot
--   0089 (acct-b8n)            — variance_ppv_prior_period_adj routing
--   0090 (acct-7mc)            — three-way match tolerance windows
--   0091 (acct-quca)           — post_standard_cost_roll WIP lock-set
--   0092 (acct-3xcg)           — post_ap_payment cross-currency settlement
--
-- Naming unifications baked in:
--   transfers → posting_lines (body references)
--   post_transfers → post_posting_lines (call sites)
--   resolve_standard_cost_at → _resolve_standard_cost_at
--   transfers_provisional → posting_lines_provisional
--   transfer_id → posting_line_id
--   document_kind 'inventory_adjustment' → 'inventory_adjustment_doc'
--   document_kind 'inventory_cost_adjustment' → 'cost_adjustment'
--
-- Forward-deferred to 0017 (after wo_events + wo_by_products exist):
--   post_ap_bill 'disposal_match' line kind
--   vendor_bill_lines.disposal_wo_event_id / .by_product_no columns
--
-- ============================================================
-- AP/AR partition unique keys.
-- ============================================================

CREATE UNIQUE INDEX accounts_ap_partitioned_uk
  ON accounts (kind, counterparty_id, currency)
  WHERE kind IN ('ap', 'ap_unsettled')
    AND counterparty_id IS NOT NULL
    AND NOT is_closed;

CREATE UNIQUE INDEX accounts_vendor_pool_uk
  ON accounts (counterparty_id)
  WHERE kind = 'vendor_pool'
    AND counterparty_id IS NOT NULL
    AND NOT is_closed;

CREATE UNIQUE INDEX accounts_ar_partitioned_uk
  ON accounts (kind, counterparty_id, currency)
  WHERE kind IN ('ar', 'ar_unsettled')
    AND counterparty_id IS NOT NULL
    AND NOT is_closed;

CREATE UNIQUE INDEX accounts_customer_pool_uk
  ON accounts (counterparty_id)
  WHERE kind = 'customer_pool'
    AND counterparty_id IS NOT NULL
    AND NOT is_closed;

-- ============================================================
-- Schema: PO / receipts / vendor_bills.
-- ============================================================

CREATE TABLE purchase_order_lines (
  id          UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  po_id       UUID NOT NULL REFERENCES purchase_orders(id),
  line_no     INT  NOT NULL,
  sku_id      UUID NOT NULL REFERENCES skus(id),
  location_id UUID NOT NULL REFERENCES locations(id),
  qty_ordered BIGINT NOT NULL CHECK (qty_ordered > 0),
  unit_cost   BIGINT NOT NULL CHECK (unit_cost >= 0),
  currency    CHAR(3) NOT NULL,
  UNIQUE (po_id, line_no)
);

CREATE INDEX po_lines_po       ON purchase_order_lines (po_id);
CREATE INDEX po_lines_sku_loc  ON purchase_order_lines (sku_id, location_id);

CREATE TABLE po_receipts (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  po_id           UUID NOT NULL REFERENCES purchase_orders(id),
  business_date   DATE NOT NULL,
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID NOT NULL UNIQUE,
  notes           TEXT
);

CREATE INDEX po_receipts_po         ON po_receipts (po_id);
CREATE INDEX po_receipts_posted_at  ON po_receipts (posted_at);

CREATE TABLE po_receipt_lines (
  id                     UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  receipt_id             UUID NOT NULL REFERENCES po_receipts(id),
  po_line_id             UUID NOT NULL REFERENCES purchase_order_lines(id),
  qty_received           BIGINT NOT NULL CHECK (qty_received > 0),
  cost_method_at_receipt cost_method NOT NULL DEFAULT 'standard',
  UNIQUE (receipt_id, po_line_id)
);

CREATE INDEX po_receipt_lines_po_line ON po_receipt_lines (po_line_id);

COMMENT ON COLUMN po_receipt_lines.cost_method_at_receipt IS
  'SKU cost_method captured at receipt-post time. post_po_return '
  'dispatches on this snapshot to keep PPV math consistent with the '
  'original receipt even if the SKU later changes cost_method.';

CREATE TABLE vendor_bills (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  vendor_id       UUID NOT NULL REFERENCES vendors(id),
  currency        CHAR(3) NOT NULL,
  business_date   DATE NOT NULL,
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID NOT NULL UNIQUE,
  notes           TEXT
);

CREATE INDEX vendor_bills_vendor    ON vendor_bills (vendor_id);
CREATE INDEX vendor_bills_posted_at ON vendor_bills (posted_at);

CREATE TABLE vendor_bill_lines (
  id                 UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  bill_id            UUID NOT NULL REFERENCES vendor_bills(id),
  line_no            INT  NOT NULL,
  kind               TEXT NOT NULL,
  po_line_id         UUID REFERENCES purchase_order_lines(id),
  expense_account_id BIGINT REFERENCES accounts(id),
  qty                BIGINT,
  unit_cost          BIGINT,
  amount             BIGINT NOT NULL CHECK (amount > 0),
  UNIQUE (bill_id, line_no),
  CONSTRAINT vendor_bill_lines_kind_check
    CHECK (kind IN ('po_match', 'service')),
  CONSTRAINT vendor_bill_lines_check CHECK (
    (kind = 'po_match'
     AND po_line_id IS NOT NULL
     AND expense_account_id IS NULL
     AND qty IS NOT NULL AND qty > 0
     AND unit_cost IS NOT NULL AND unit_cost >= 0)
    OR
    (kind = 'service'
     AND po_line_id IS NULL
     AND expense_account_id IS NOT NULL
     AND qty IS NULL
     AND unit_cost IS NULL)
  )
);

CREATE INDEX vendor_bill_lines_bill    ON vendor_bill_lines (bill_id);
CREATE INDEX vendor_bill_lines_po_line ON vendor_bill_lines (po_line_id)
  WHERE po_line_id IS NOT NULL;

-- ============================================================
-- Schema: ap_payments.
-- ============================================================

CREATE TABLE ap_payments (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  vendor_id       UUID NOT NULL REFERENCES vendors(id),
  currency        CHAR(3) NOT NULL,
  amount          BIGINT NOT NULL CHECK (amount > 0),
  business_date   DATE NOT NULL,
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID NOT NULL UNIQUE,
  notes           TEXT
);

CREATE INDEX ap_payments_vendor    ON ap_payments (vendor_id);
CREATE INDEX ap_payments_posted_at ON ap_payments (posted_at);

-- ============================================================
-- Schema: po_returns + po_return_lines.
-- ============================================================

CREATE TABLE po_returns (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  vendor_id       UUID NOT NULL REFERENCES vendors(id),
  business_date   DATE NOT NULL,
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID NOT NULL UNIQUE,
  notes           TEXT
);

CREATE INDEX po_returns_vendor    ON po_returns (vendor_id);
CREATE INDEX po_returns_posted_at ON po_returns (posted_at);

CREATE TABLE po_return_lines (
  id                  UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  return_id           UUID NOT NULL REFERENCES po_returns(id),
  line_no             INT  NOT NULL,
  recv_line_id        UUID NOT NULL REFERENCES po_receipt_lines(id),
  qty_returned        BIGINT NOT NULL CHECK (qty_returned > 0),
  unit_cost           BIGINT NOT NULL CHECK (unit_cost >= 0),
  qty_to_ap_unsettled BIGINT NOT NULL DEFAULT 0,
  qty_to_ap           BIGINT NOT NULL DEFAULT 0,
  UNIQUE (return_id, line_no),
  CONSTRAINT po_return_lines_split_check
    CHECK (qty_to_ap_unsettled >= 0
           AND qty_to_ap >= 0
           AND qty_to_ap_unsettled + qty_to_ap = qty_returned)
);

CREATE INDEX po_return_lines_recv_line ON po_return_lines (recv_line_id);

-- ============================================================
-- Schema: inventory_adjustments + inventory_cost_adjustments
--         + inventory_standard_cost_rolls.
-- ============================================================

CREATE TABLE inventory_adjustments (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  sku_id          UUID NOT NULL REFERENCES skus(id),
  location_id     UUID NOT NULL REFERENCES locations(id),
  qty_delta       BIGINT NOT NULL CHECK (qty_delta <> 0),
  unit_cost       BIGINT NOT NULL CHECK (unit_cost >= 0),
  currency        TEXT   NOT NULL,
  inventory_class TEXT   NOT NULL CHECK (inventory_class IN ('raw','fg')),
  business_date   DATE   NOT NULL,
  posted_by       UUID   NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID   NOT NULL UNIQUE,
  notes           TEXT
);

CREATE INDEX inv_adj_sku_loc    ON inventory_adjustments (sku_id, location_id);
CREATE INDEX inv_adj_posted_at  ON inventory_adjustments (posted_at);

CREATE TABLE inventory_cost_adjustments (
  id               UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  sku_id           UUID NOT NULL REFERENCES skus(id),
  location_id      UUID NOT NULL REFERENCES locations(id),
  currency         TEXT NOT NULL,
  inventory_class  TEXT NOT NULL CHECK (inventory_class IN ('raw','fg')),
  prior_unit_cost  BIGINT NOT NULL,
  target_unit_cost BIGINT NOT NULL CHECK (target_unit_cost >= 0),
  delta_value      BIGINT NOT NULL,
  pool_qty         BIGINT NOT NULL CHECK (pool_qty > 0),
  business_date    DATE   NOT NULL,
  posted_by        UUID   NOT NULL,
  posted_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key  UUID   NOT NULL UNIQUE,
  notes            TEXT
);

CREATE INDEX inv_cost_adj_sku_loc    ON inventory_cost_adjustments (sku_id, location_id);
CREATE INDEX inv_cost_adj_posted_at  ON inventory_cost_adjustments (posted_at);

CREATE TABLE inventory_standard_cost_rolls (
  id                    UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  sku_id                UUID NOT NULL REFERENCES skus(id),
  prior_standard_cost   BIGINT,
  target_standard_cost  BIGINT NOT NULL CHECK (target_standard_cost >= 0),
  effective_at          DATE NOT NULL,
  total_delta_value     BIGINT NOT NULL,
  pool_qty              BIGINT NOT NULL CHECK (pool_qty >= 0),
  business_date         DATE NOT NULL,
  posted_by             UUID NOT NULL,
  posted_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key       UUID NOT NULL UNIQUE,
  notes                 TEXT
);

CREATE INDEX inv_std_roll_sku        ON inventory_standard_cost_rolls (sku_id);
CREATE INDEX inv_std_roll_posted_at  ON inventory_standard_cost_rolls (posted_at);

-- ============================================================
-- post_inventory_adjustment
-- ============================================================

CREATE OR REPLACE FUNCTION post_inventory_adjustment(
  p_sku_id          UUID,
  p_location_id     UUID,
  p_qty_delta       BIGINT,
  p_unit_cost       BIGINT,
  p_currency        TEXT,
  p_inventory_class TEXT,
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
  v_cost_method      cost_method;
  v_qty_acct         BIGINT;
  v_val_acct         BIGINT;
  v_void_qty         BIGINT;
  v_void_val         BIGINT;
  v_value_kind       TEXT;
  v_lock_first       BIGINT;
  v_lock_second      BIGINT;
  v_qty_balance      BIGINT;
  v_val_balance      BIGINT;
  v_effective_uc     BIGINT;
  v_qty_amount       BIGINT;
  v_val_amount       BIGINT;
  v_qty_debit        BIGINT;
  v_qty_credit       BIGINT;
  v_val_debit        BIGINT;
  v_val_credit       BIGINT;
  v_batch            JSONB;
  v_needs_provisional_method TEXT := NULL;
  v_value_posting_line_id BIGINT;
  v_period_id        BIGINT;
BEGIN
  SELECT id INTO v_existing_id FROM inventory_adjustments WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT cost_method INTO v_cost_method FROM skus WHERE id = p_sku_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'sku % not found', p_sku_id USING ERRCODE = 'P0010'; END IF;

  IF v_cost_method IN ('wac_periodic', 'wac_retroactive') AND p_inventory_class = 'wip' THEN
    RAISE EXCEPTION
      '% adjustment on inv_value_wip class not supported in Phase 1 '
      '(see acct-p7v Phase 2 Epic J: wac across WIP pools); sku=%',
      v_cost_method, p_sku_id USING ERRCODE = 'P0006';
  END IF;

  SELECT id INTO v_qty_acct FROM accounts
   WHERE kind = 'stock_available' AND sku_id = p_sku_id AND location_id = p_location_id AND NOT is_closed;
  IF v_qty_acct IS NULL THEN
    RAISE EXCEPTION 'no open stock_available account for sku=% loc=%', p_sku_id, p_location_id USING ERRCODE = 'P0010';
  END IF;

  v_value_kind := 'inv_value_' || p_inventory_class;
  EXECUTE format('SELECT id FROM accounts WHERE kind = %L AND sku_id = $1 AND location_id = $2 AND currency = $3 AND NOT is_closed', v_value_kind)
    INTO v_val_acct USING p_sku_id, p_location_id, p_currency;
  IF v_val_acct IS NULL THEN
    RAISE EXCEPTION 'no open % account for sku=% loc=% ccy=%', v_value_kind, p_sku_id, p_location_id, p_currency USING ERRCODE = 'P0010';
  END IF;

  SELECT id INTO v_void_qty FROM accounts WHERE kind = 'creation_void' AND ledger_kind = 'qty' AND NOT is_closed;
  IF v_void_qty IS NULL THEN RAISE EXCEPTION 'no creation_void(qty) account configured' USING ERRCODE = 'P0010'; END IF;

  SELECT id INTO v_void_val FROM accounts WHERE kind = 'inv_adj_expense' AND ledger_kind = 'value' AND currency = p_currency AND NOT is_closed;
  IF v_void_val IS NULL THEN RAISE EXCEPTION 'no inv_adj_expense(value, ccy=%) account configured', p_currency USING ERRCODE = 'P0010'; END IF;

  CASE v_cost_method
  WHEN 'standard' THEN
    IF p_unit_cost IS NOT NULL THEN
      RAISE EXCEPTION 'standard SKU % has a fixed standard cost; do not pass p_unit_cost (got %)', p_sku_id, p_unit_cost USING ERRCODE = 'P0011';
    END IF;
    v_effective_uc := _resolve_standard_cost_at(p_sku_id, p_business_date);

  WHEN 'wac_perpetual' THEN
    v_lock_first := LEAST(v_qty_acct, v_val_acct);
    v_lock_second := GREATEST(v_qty_acct, v_val_acct);
    PERFORM 1 FROM accounts WHERE id = v_lock_first FOR UPDATE;
    PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;
    SELECT COALESCE(SUM(CASE WHEN t.debit_account_id = v_val_acct THEN t.qty WHEN t.credit_account_id = v_val_acct THEN -t.qty END), 0)
      INTO v_qty_balance FROM posting_lines t
     WHERE v_val_acct IN (t.debit_account_id, t.credit_account_id) AND t.qty IS NOT NULL;
    SELECT debits_total - credits_total INTO v_val_balance FROM accounts WHERE id = v_val_acct;
    IF p_qty_delta > 0 THEN
      IF p_unit_cost IS NULL THEN
        IF v_qty_balance <= 0 THEN
          RAISE EXCEPTION 'wac_perpetual SKU % at sku=% loc=% has empty pool (qty_balance=%); caller must pass p_unit_cost on first adjustment-in to seed', p_sku_id, p_sku_id, p_location_id, v_qty_balance USING ERRCODE = 'P0011';
        END IF;
        v_effective_uc := v_val_balance / v_qty_balance;
      ELSE
        v_effective_uc := p_unit_cost;
      END IF;
    ELSE
      IF p_unit_cost IS NOT NULL THEN
        RAISE EXCEPTION 'wac_perpetual depletion does not accept asserted unit_cost (got % on sku=% loc=%); use lot cost_method (acct-8gg) for asserted-cost-per-transaction needs', p_unit_cost, p_sku_id, p_location_id USING ERRCODE = 'P0011';
      END IF;
      IF v_qty_balance <= 0 THEN
        RAISE EXCEPTION 'wac_perpetual SKU % at sku=% loc=% has empty pool; cannot deplete', p_sku_id, p_sku_id, p_location_id USING ERRCODE = 'P0010';
      END IF;
      v_effective_uc := v_val_balance / v_qty_balance;
    END IF;

  WHEN 'wac_periodic', 'wac_retroactive' THEN
    v_lock_first := LEAST(v_qty_acct, v_val_acct);
    v_lock_second := GREATEST(v_qty_acct, v_val_acct);
    PERFORM 1 FROM accounts WHERE id = v_lock_first FOR UPDATE;
    PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;
    SELECT COALESCE(SUM(CASE WHEN t.debit_account_id = v_val_acct THEN t.qty WHEN t.credit_account_id = v_val_acct THEN -t.qty END), 0)
      INTO v_qty_balance FROM posting_lines t
     WHERE v_val_acct IN (t.debit_account_id, t.credit_account_id) AND t.qty IS NOT NULL;
    SELECT debits_total - credits_total INTO v_val_balance FROM accounts WHERE id = v_val_acct;
    IF p_qty_delta > 0 THEN
      IF p_unit_cost IS NULL THEN
        IF v_qty_balance <= 0 THEN
          RAISE EXCEPTION '% SKU % at sku=% loc=% has empty pool (qty_balance=%); caller must pass p_unit_cost on first adjustment-in to seed',
                          v_cost_method, p_sku_id, p_sku_id, p_location_id, v_qty_balance USING ERRCODE = 'P0011';
        END IF;
        v_effective_uc := v_val_balance / v_qty_balance;
      ELSE
        v_effective_uc := p_unit_cost;
      END IF;
    ELSE
      IF p_unit_cost IS NOT NULL THEN
        RAISE EXCEPTION '% depletion does not accept asserted unit_cost (got % on sku=% loc=%); use lot cost_method (acct-8gg) for asserted-cost-per-transaction needs',
                        v_cost_method, p_unit_cost, p_sku_id, p_location_id USING ERRCODE = 'P0011';
      END IF;
      IF v_qty_balance <= 0 THEN
        RAISE EXCEPTION '% SKU % at sku=% loc=% has empty pool; cannot deplete', v_cost_method, p_sku_id, p_sku_id, p_location_id USING ERRCODE = 'P0010';
      END IF;
      v_effective_uc := v_val_balance / v_qty_balance;
      v_needs_provisional_method := v_cost_method::TEXT;
    END IF;

  WHEN 'fifo', 'lot' THEN
    RAISE EXCEPTION 'cost_method_not_implemented: % (sku=%); see acct-8gg', v_cost_method, p_sku_id USING ERRCODE = 'P0006';

  ELSE
    RAISE EXCEPTION 'unknown cost_method % for sku=%', v_cost_method, p_sku_id USING ERRCODE = 'P0011';
  END CASE;

  v_qty_amount := abs(p_qty_delta);
  v_val_amount := v_qty_amount * v_effective_uc;

  IF p_qty_delta > 0 THEN
    v_qty_debit := v_qty_acct; v_qty_credit := v_void_qty;
    v_val_debit := v_val_acct; v_val_credit := v_void_val;
  ELSE
    v_qty_debit := v_void_qty; v_qty_credit := v_qty_acct;
    v_val_debit := v_void_val; v_val_credit := v_val_acct;
  END IF;

  INSERT INTO inventory_adjustments (
    sku_id, location_id, qty_delta, unit_cost, currency,
    inventory_class, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_sku_id, p_location_id, p_qty_delta, v_effective_uc, p_currency,
    p_inventory_class, p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM inventory_adjustments WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  IF v_val_amount > 0 THEN
    v_batch := jsonb_build_array(
      jsonb_build_object('reason','inventory_adjustment','document_kind','inventory_adjustment_doc','document_id',v_doc_id,'debit_account_id',v_qty_debit,'credit_account_id',v_qty_credit,'amount',v_qty_amount,'qty',v_qty_amount,'business_date',p_business_date,'idempotency_key',gen_random_uuid(),'posted_by',p_posted_by),
      jsonb_build_object('reason','inventory_adjustment','document_kind','inventory_adjustment_doc','document_id',v_doc_id,'debit_account_id',v_val_debit,'credit_account_id',v_val_credit,'amount',v_val_amount,'qty',v_qty_amount,'business_date',p_business_date,'idempotency_key',gen_random_uuid(),'posted_by',p_posted_by)
    );
  ELSE
    v_batch := jsonb_build_array(
      jsonb_build_object('reason','inventory_adjustment','document_kind','inventory_adjustment_doc','document_id',v_doc_id,'debit_account_id',v_qty_debit,'credit_account_id',v_qty_credit,'amount',v_qty_amount,'qty',v_qty_amount,'business_date',p_business_date,'idempotency_key',gen_random_uuid(),'posted_by',p_posted_by)
    );
  END IF;

  PERFORM post_posting_lines(v_batch, FALSE);

  IF v_needs_provisional_method IS NOT NULL THEN
    SELECT id INTO v_value_posting_line_id FROM posting_lines WHERE document_id = v_doc_id AND reason = 'inventory_adjustment' AND credit_account_id = v_val_acct;
    SELECT id INTO v_period_id FROM periods WHERE opens_at <= p_business_date AND closes_at >= p_business_date;
    INSERT INTO posting_lines_provisional (posting_line_id, period_id, cost_method, qty)
    VALUES (v_value_posting_line_id, v_period_id, v_needs_provisional_method::cost_method, v_qty_amount);
  END IF;

  RETURN v_doc_id;
END;
$$;

-- ============================================================
-- post_cost_adjustment (per-class qty divisor)
-- ============================================================

CREATE OR REPLACE FUNCTION post_cost_adjustment(
  p_sku_id           UUID,
  p_location_id      UUID,
  p_currency         TEXT,
  p_inventory_class  TEXT,
  p_target_unit_cost BIGINT,
  p_business_date    DATE,
  p_posted_by        UUID,
  p_idempotency_key  UUID,
  p_notes            TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id  UUID;
  v_doc_id       UUID;
  v_cost_method  cost_method;
  v_qty_acct     BIGINT;
  v_val_acct     BIGINT;
  v_var_acct     BIGINT;
  v_value_kind   TEXT;
  v_lock_first   BIGINT;
  v_lock_second  BIGINT;
  v_pool_qty     BIGINT;
  v_pool_value   BIGINT;
  v_prior_unit   BIGINT;
  v_new_value    BIGINT;
  v_delta        BIGINT;
  v_amount       BIGINT;
  v_debit        BIGINT;
  v_credit       BIGINT;
  v_batch        JSONB;
BEGIN
  SELECT id INTO v_existing_id
    FROM inventory_cost_adjustments
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN
    RETURN v_existing_id;
  END IF;

  IF p_target_unit_cost < 0 THEN
    RAISE EXCEPTION 'p_target_unit_cost must be >= 0 (got %)', p_target_unit_cost
      USING ERRCODE = '23514';
  END IF;

  SELECT cost_method INTO v_cost_method FROM skus WHERE id = p_sku_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'sku % not found', p_sku_id USING ERRCODE = 'P0010';
  END IF;

  CASE v_cost_method
  WHEN 'standard' THEN
    RAISE EXCEPTION
      'cost_adjustment not applicable to standard SKU % — to change a '
      'standard SKU''s cost, use post_standard_cost_roll',
      p_sku_id USING ERRCODE = 'P0011';
  WHEN 'wac_perpetual' THEN
    NULL;
  WHEN 'wac_periodic' THEN
    RAISE EXCEPTION
      'cost_method_not_implemented: cost_adjustment on wac_periodic '
      'requires period-close machinery; sku=%',
      p_sku_id USING ERRCODE = 'P0006';
  WHEN 'wac_retroactive' THEN
    RAISE EXCEPTION
      'cost_method_not_implemented: cost_adjustment on wac_retroactive '
      'requires period-close machinery; sku=%',
      p_sku_id USING ERRCODE = 'P0006';
  WHEN 'fifo', 'lot' THEN
    RAISE EXCEPTION
      'cost_method_not_implemented: cost_adjustment on % SKU; see acct-8gg; sku=%',
      v_cost_method, p_sku_id USING ERRCODE = 'P0006';
  ELSE
    RAISE EXCEPTION 'unknown cost_method % for sku=%', v_cost_method, p_sku_id
      USING ERRCODE = 'P0011';
  END CASE;

  SELECT id INTO v_qty_acct
    FROM accounts
   WHERE kind = 'stock_available' AND sku_id = p_sku_id
     AND location_id = p_location_id AND NOT is_closed;
  IF v_qty_acct IS NULL THEN
    RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                    p_sku_id, p_location_id USING ERRCODE = 'P0010';
  END IF;

  v_value_kind := 'inv_value_' || p_inventory_class;
  EXECUTE format(
    'SELECT id FROM accounts WHERE kind = %L AND sku_id = $1 AND '
    'location_id = $2 AND currency = $3 AND NOT is_closed',
    v_value_kind
  ) INTO v_val_acct USING p_sku_id, p_location_id, p_currency;
  IF v_val_acct IS NULL THEN
    RAISE EXCEPTION 'no open % account for sku=% loc=% ccy=%',
                    v_value_kind, p_sku_id, p_location_id, p_currency
      USING ERRCODE = 'P0010';
  END IF;

  SELECT id INTO v_var_acct FROM accounts
   WHERE kind = 'variance_cost_adjustment' AND ledger_kind = 'value'
     AND currency = p_currency AND NOT is_closed;
  IF v_var_acct IS NULL THEN
    RAISE EXCEPTION 'no variance_cost_adjustment(value, ccy=%) account configured',
                    p_currency USING ERRCODE = 'P0010';
  END IF;

  v_lock_first  := LEAST(v_qty_acct, v_val_acct);
  v_lock_second := GREATEST(v_qty_acct, v_val_acct);
  PERFORM 1 FROM accounts WHERE id = v_lock_first  FOR UPDATE;
  PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;

  -- R1: per-class qty divisor from signed SUM on the value pool's
  -- posting_lines.qty (NOT stock_available; cross-class for raw + fg
  -- same-location).
  SELECT COALESCE(SUM(
    CASE
      WHEN t.debit_account_id  = v_val_acct THEN  t.qty
      WHEN t.credit_account_id = v_val_acct THEN -t.qty
    END
  ), 0) INTO v_pool_qty
    FROM posting_lines t
   WHERE v_val_acct IN (t.debit_account_id, t.credit_account_id)
     AND t.qty IS NOT NULL;

  SELECT debits_total - credits_total INTO v_pool_value
    FROM accounts WHERE id = v_val_acct;

  IF v_pool_qty <= 0 THEN
    RAISE EXCEPTION
      'cost_adjustment requires non-empty class pool; sku=% loc=% class=% has per-class qty=%',
      p_sku_id, p_location_id, p_inventory_class, v_pool_qty
      USING ERRCODE = 'P0010';
  END IF;

  v_prior_unit := v_pool_value / v_pool_qty;
  v_new_value  := p_target_unit_cost * v_pool_qty;
  v_delta      := v_new_value - v_pool_value;

  INSERT INTO inventory_cost_adjustments (
    sku_id, location_id, currency, inventory_class,
    prior_unit_cost, target_unit_cost, delta_value, pool_qty,
    business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_sku_id, p_location_id, p_currency, p_inventory_class,
    v_prior_unit, p_target_unit_cost, v_delta, v_pool_qty,
    p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM inventory_cost_adjustments
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  IF v_delta = 0 THEN
    RETURN v_doc_id;
  END IF;

  IF v_delta > 0 THEN
    v_debit := v_val_acct; v_credit := v_var_acct; v_amount := v_delta;
  ELSE
    v_debit := v_var_acct; v_credit := v_val_acct; v_amount := -v_delta;
  END IF;

  v_batch := jsonb_build_array(
    jsonb_build_object(
      'reason','cost_adjustment',
      'document_kind','cost_adjustment',
      'document_id',v_doc_id,
      'debit_account_id',v_debit,
      'credit_account_id',v_credit,
      'amount',v_amount,
      'business_date',p_business_date,
      'idempotency_key',gen_random_uuid(),
      'posted_by',p_posted_by
    )
  );

  PERFORM post_posting_lines(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;

-- ============================================================
-- post_cost_adjustment_retroactive (queue entry function)
-- ============================================================

CREATE OR REPLACE FUNCTION post_cost_adjustment_retroactive(
  p_target_period_id BIGINT,
  p_sku_id           UUID,
  p_location_id      UUID,
  p_currency         TEXT,
  p_inventory_class  TEXT,
  p_target_avg       BIGINT,
  p_business_date    DATE,
  p_posted_by        UUID,
  p_idempotency_key  UUID,
  p_notes            TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id    UUID;
  v_period_opens   DATE;
  v_period_closes  DATE;
  v_period_closed  TIMESTAMPTZ;
  v_period_code    TEXT;
  v_value_kind     TEXT;
  v_val_acct       BIGINT;
  v_doc_id         UUID;
BEGIN
  SELECT id INTO v_existing_id
    FROM inventory_cost_adjustments_retroactive
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN
    RETURN v_existing_id;
  END IF;

  IF p_inventory_class = 'wip' THEN
    RAISE EXCEPTION
      'cost_method_not_implemented: cost_adjustment_retroactive on '
      'inv_value_wip class not supported in Phase 1 (see acct-p7v); sku=%',
      p_sku_id USING ERRCODE = 'P0006';
  END IF;

  IF p_target_avg < 0 THEN
    RAISE EXCEPTION 'p_target_avg must be >= 0 (got %)', p_target_avg
      USING ERRCODE = '23514';
  END IF;

  SELECT opens_at, closes_at, closed_at, code
    INTO v_period_opens, v_period_closes, v_period_closed, v_period_code
    FROM periods WHERE id = p_target_period_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'target period id=% not found', p_target_period_id
      USING ERRCODE = 'P0014';
  END IF;
  IF v_period_closed IS NOT NULL THEN
    RAISE EXCEPTION
      'target_period_closed: period % (id=%) closed at %; '
      'reopen the period first (workflow tracked as acct-7h4 Phase 2 Epic K)',
      v_period_code, p_target_period_id, v_period_closed
      USING ERRCODE = 'P0021';
  END IF;

  IF p_business_date < v_period_opens OR p_business_date > v_period_closes THEN
    RAISE EXCEPTION
      'business_date % out of target period % bounds (%..%)',
      p_business_date, v_period_code, v_period_opens, v_period_closes
      USING ERRCODE = 'P0004';
  END IF;

  PERFORM 1 FROM skus WHERE id = p_sku_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'sku % not found', p_sku_id USING ERRCODE = 'P0010';
  END IF;

  v_value_kind := 'inv_value_' || p_inventory_class;
  EXECUTE format(
    'SELECT id FROM accounts WHERE kind = %L AND sku_id = $1 AND '
    'location_id = $2 AND currency = $3 AND NOT is_closed',
    v_value_kind
  ) INTO v_val_acct USING p_sku_id, p_location_id, p_currency;
  IF v_val_acct IS NULL THEN
    RAISE EXCEPTION
      'no open % account for sku=% loc=% ccy=%',
      v_value_kind, p_sku_id, p_location_id, p_currency
      USING ERRCODE = 'P0010';
  END IF;

  INSERT INTO inventory_cost_adjustments_retroactive (
    sku_id, location_id, currency, inventory_class, target_period_id,
    target_avg, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_sku_id, p_location_id, p_currency, p_inventory_class, p_target_period_id,
    p_target_avg, p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id
      FROM inventory_cost_adjustments_retroactive
     WHERE idempotency_key = p_idempotency_key;
  END IF;

  RETURN v_doc_id;
END;
$$;

-- ============================================================
-- post_standard_cost_roll (with WIP revaluation companion)
-- ============================================================

CREATE OR REPLACE FUNCTION post_standard_cost_roll(
  p_sku_id            UUID,
  p_new_cost          BIGINT,
  p_effective_at      DATE,
  p_business_date     DATE,
  p_posted_by         UUID,
  p_idempotency_key   UUID,
  p_notes             TEXT    DEFAULT NULL,
  p_expected_old_cost BIGINT  DEFAULT NULL,
  p_revalue_wip       BOOLEAN DEFAULT FALSE
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id      UUID;
  v_doc_id           UUID;
  v_cost_method      cost_method;
  v_max_effective    DATE;
  v_prior            BIGINT;
  v_wip_count        BIGINT;
  v_var_acct         BIGINT;
  v_wip_var_acct     BIGINT;
  v_pool_record      RECORD;
  v_wip_record       RECORD;
  v_pool_qty         BIGINT;
  v_total_qty        BIGINT := 0;
  v_total_delta      BIGINT := 0;
  v_delta            BIGINT;
  v_amount           BIGINT;
  v_debit            BIGINT;
  v_credit           BIGINT;
  v_lock_ids         BIGINT[];
  v_lock_id          BIGINT;
  v_batch            JSONB := '[]'::JSONB;
  v_future_dated     BOOLEAN;
BEGIN
  SELECT id INTO v_existing_id
    FROM inventory_standard_cost_rolls
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  IF p_new_cost < 0 THEN
    RAISE EXCEPTION 'p_new_cost must be >= 0 (got %)', p_new_cost
      USING ERRCODE = '23514';
  END IF;

  SELECT cost_method INTO v_cost_method
    FROM skus WHERE id = p_sku_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'sku % not found', p_sku_id USING ERRCODE = 'P0010';
  END IF;

  IF v_cost_method <> 'standard' THEN
    IF v_cost_method IN ('wac_perpetual', 'wac_periodic', 'wac_retroactive') THEN
      RAISE EXCEPTION
        'standard_cost_roll not applicable to % SKU % — use post_cost_adjustment for WAC pools',
        v_cost_method, p_sku_id USING ERRCODE = 'P0011';
    ELSE
      RAISE EXCEPTION
        'cost_method_not_implemented: standard_cost_roll on % SKU %; see acct-8gg',
        v_cost_method, p_sku_id USING ERRCODE = 'P0006';
    END IF;
  END IF;

  SELECT MAX(effective_at) INTO v_max_effective
    FROM standard_costs WHERE sku_id = p_sku_id;
  IF v_max_effective IS NOT NULL AND p_effective_at <= v_max_effective THEN
    RAISE EXCEPTION
      'retroactive_std_cost_roll_blocked: sku=% has standard_costs row at '
      'effective_at=%; p_effective_at=% must be strictly greater. '
      'Retroactive standard cost corrections are not supported in Phase 1.',
      p_sku_id, v_max_effective, p_effective_at
      USING ERRCODE = 'P0019';
  END IF;

  BEGIN
    v_prior := _resolve_standard_cost_at(p_sku_id, p_business_date);
  EXCEPTION WHEN SQLSTATE 'P0018' THEN
    v_prior := NULL;
  END;

  IF p_expected_old_cost IS DISTINCT FROM v_prior THEN
    RAISE EXCEPTION
      'optimistic_concurrency_violation: caller expected prior=%, actual prior=%',
      p_expected_old_cost, v_prior
      USING ERRCODE = 'P0017';
  END IF;

  IF NOT p_revalue_wip THEN
    SELECT COUNT(*) INTO v_wip_count
      FROM accounts
     WHERE kind = 'inv_value_wip'
       AND sku_id = p_sku_id
       AND NOT is_closed
       AND (debits_total - credits_total) > 0;
    IF v_wip_count > 0 THEN
      RAISE EXCEPTION
        'wip_present_blocks_std_cost_roll: sku=% has % open inv_value_wip pool(s) '
        'with non-zero balance. Pass p_revalue_wip=TRUE to invoke the WIP '
        'material revaluation companion (acct-bru), or close out WIP via '
        'wo_complete + scrap before rolling.',
        p_sku_id, v_wip_count USING ERRCODE = 'P0006';
    END IF;
  END IF;

  v_future_dated := (p_effective_at > p_business_date);

  INSERT INTO standard_costs (
    sku_id, cost, effective_at, posted_by, idempotency_key, notes
  ) VALUES (
    p_sku_id, p_new_cost, p_effective_at, p_posted_by,
    gen_random_uuid(), p_notes
  );

  IF NOT v_future_dated AND v_prior IS NOT NULL AND v_prior <> p_new_cost THEN
    -- R4: lock raw/fg pools (and stock_wip qty pools when revaluing WIP).
    IF p_revalue_wip THEN
      SELECT array_agg(id ORDER BY id) INTO v_lock_ids
        FROM (
          SELECT id FROM accounts
           WHERE kind IN ('inv_value_raw', 'inv_value_fg', 'inv_value_wip')
             AND sku_id = p_sku_id AND NOT is_closed
          UNION
          SELECT s.id FROM accounts s
           WHERE s.kind = 'stock_wip'
             AND s.sku_id = p_sku_id
             AND NOT s.is_closed
             AND EXISTS (
               SELECT 1 FROM accounts v
                WHERE v.kind = 'inv_value_wip'
                  AND v.sku_id = p_sku_id
                  AND NOT v.is_closed
                  AND v.routing_op = s.routing_op
             )
        ) sub;
    ELSE
      SELECT array_agg(id ORDER BY id) INTO v_lock_ids
        FROM accounts
       WHERE kind IN ('inv_value_raw', 'inv_value_fg')
         AND sku_id = p_sku_id AND NOT is_closed;
    END IF;

    IF v_lock_ids IS NOT NULL THEN
      FOREACH v_lock_id IN ARRAY v_lock_ids LOOP
        PERFORM 1 FROM accounts WHERE id = v_lock_id FOR UPDATE;
      END LOOP;
    END IF;

    -- Raw/fg revaluation.
    FOR v_pool_record IN
      SELECT v.id AS val_acct, v.currency AS currency, v.location_id AS location_id
        FROM accounts v
       WHERE v.kind IN ('inv_value_raw', 'inv_value_fg')
         AND v.sku_id = p_sku_id AND NOT v.is_closed
       ORDER BY v.id
    LOOP
      SELECT COALESCE(SUM(
        CASE
          WHEN t.debit_account_id  = v_pool_record.val_acct THEN  t.qty
          WHEN t.credit_account_id = v_pool_record.val_acct THEN -t.qty
        END
      ), 0) INTO v_pool_qty
        FROM posting_lines t
       WHERE v_pool_record.val_acct IN (t.debit_account_id, t.credit_account_id)
         AND t.qty IS NOT NULL;
      IF v_pool_qty IS NULL OR v_pool_qty = 0 THEN CONTINUE; END IF;

      v_delta := v_pool_qty * (p_new_cost - v_prior);
      IF v_delta = 0 THEN CONTINUE; END IF;

      v_total_qty   := v_total_qty + v_pool_qty;
      v_total_delta := v_total_delta + v_delta;

      SELECT id INTO v_var_acct FROM accounts
       WHERE kind = 'variance_std_cost_roll' AND ledger_kind = 'value'
         AND currency = v_pool_record.currency AND NOT is_closed;
      IF v_var_acct IS NULL THEN
        RAISE EXCEPTION 'no variance_std_cost_roll(value, ccy=%) account configured',
                        v_pool_record.currency USING ERRCODE = 'P0010';
      END IF;

      IF v_delta > 0 THEN
        v_debit := v_pool_record.val_acct; v_credit := v_var_acct; v_amount := v_delta;
      ELSE
        v_debit := v_var_acct; v_credit := v_pool_record.val_acct; v_amount := -v_delta;
      END IF;

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason','standard_cost_roll',
        'document_kind','inventory_standard_cost_roll',
        'document_id', NULL,
        'debit_account_id', v_debit, 'credit_account_id', v_credit,
        'amount', v_amount, 'business_date', p_business_date,
        'idempotency_key', gen_random_uuid(), 'posted_by', p_posted_by
      ));
    END LOOP;

    -- WIP revaluation (acct-bru). Reads pool_qty from paired stock_wip
    -- account (R4: locked above). Variance routes through
    -- variance_wip_revaluation.
    IF p_revalue_wip THEN
      FOR v_wip_record IN
        SELECT v.id AS val_acct, v.currency AS currency, v.routing_op AS routing_op,
               s.id AS qty_acct
          FROM accounts v
          JOIN accounts s ON s.kind = 'stock_wip' AND s.sku_id = v.sku_id
                         AND s.routing_op = v.routing_op AND NOT s.is_closed
         WHERE v.kind = 'inv_value_wip' AND v.sku_id = p_sku_id AND NOT v.is_closed
         ORDER BY v.id
      LOOP
        SELECT debits_total - credits_total INTO v_pool_qty
          FROM accounts WHERE id = v_wip_record.qty_acct;
        IF v_pool_qty IS NULL OR v_pool_qty = 0 THEN CONTINUE; END IF;

        v_delta := v_pool_qty * (p_new_cost - v_prior);
        IF v_delta = 0 THEN CONTINUE; END IF;

        v_total_qty   := v_total_qty + v_pool_qty;
        v_total_delta := v_total_delta + v_delta;

        SELECT id INTO v_wip_var_acct FROM accounts
         WHERE kind = 'variance_wip_revaluation' AND ledger_kind = 'value'
           AND currency = v_wip_record.currency AND NOT is_closed;
        IF v_wip_var_acct IS NULL THEN
          RAISE EXCEPTION 'no variance_wip_revaluation(value, ccy=%) account configured',
                          v_wip_record.currency USING ERRCODE = 'P0010';
        END IF;

        IF v_delta > 0 THEN
          v_debit := v_wip_record.val_acct; v_credit := v_wip_var_acct; v_amount := v_delta;
        ELSE
          v_debit := v_wip_var_acct; v_credit := v_wip_record.val_acct; v_amount := -v_delta;
        END IF;

        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason','standard_cost_roll',
          'document_kind','inventory_standard_cost_roll',
          'document_id', NULL,
          'debit_account_id', v_debit, 'credit_account_id', v_credit,
          'amount', v_amount, 'business_date', p_business_date,
          'idempotency_key', gen_random_uuid(), 'posted_by', p_posted_by
        ));
      END LOOP;
    END IF;
  END IF;

  INSERT INTO inventory_standard_cost_rolls (
    sku_id, prior_standard_cost, target_standard_cost, effective_at,
    total_delta_value, pool_qty, business_date, posted_by,
    idempotency_key, notes
  ) VALUES (
    p_sku_id, v_prior, p_new_cost, p_effective_at,
    v_total_delta, v_total_qty, p_business_date, p_posted_by,
    p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM inventory_standard_cost_rolls
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  IF jsonb_array_length(v_batch) > 0 THEN
    SELECT jsonb_agg(jsonb_set(ev, '{document_id}', to_jsonb(v_doc_id::TEXT)))
      INTO v_batch FROM jsonb_array_elements(v_batch) ev;
    PERFORM post_posting_lines(v_batch, FALSE);
  END IF;

  RETURN v_doc_id;
END;
$$;

-- ============================================================
-- post_po_receipt (cost_method snapshot at receipt)
-- ============================================================

CREATE OR REPLACE FUNCTION post_po_receipt(
  p_po_id           UUID,
  p_lines           JSONB,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id   UUID;
  v_doc_id        UUID;
  v_vendor_id     UUID;
  v_n             INT;
  v_idx           INT;
  v_line          JSONB;
  v_po_line_id    UUID;
  v_qty_received  BIGINT;
  v_pl            RECORD;
  v_already_recv  BIGINT;
  v_cost_method   cost_method;
  v_std_cost      BIGINT;
  v_qty_acct      BIGINT;
  v_val_acct      BIGINT;
  v_ven_qty       BIGINT;
  v_ven_val       BIGINT;
  v_var_acct      BIGINT;
  v_val_unit      BIGINT;
  v_val_amount    BIGINT;
  v_ppv_amount    BIGINT;
  v_recv_line_id  UUID;
  v_batch         JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM po_receipts
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT vendor_id INTO v_vendor_id FROM purchase_orders WHERE id = p_po_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'po_receipt_invalid: PO % not found', p_po_id
      USING ERRCODE = 'P0022';
  END IF;
  IF v_vendor_id IS NULL THEN
    RAISE EXCEPTION 'po_receipt_invalid: PO % has no vendor_id', p_po_id
      USING ERRCODE = 'P0022';
  END IF;

  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'po_receipt_invalid: empty lines for PO %', p_po_id
      USING ERRCODE = 'P0022';
  END IF;

  INSERT INTO po_receipts (po_id, business_date, posted_by, idempotency_key, notes)
  VALUES (p_po_id, p_business_date, p_posted_by, p_idempotency_key, p_notes)
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM po_receipts WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  FOR v_idx IN 1..v_n LOOP
    v_line         := p_lines -> (v_idx - 1);
    v_po_line_id   := (v_line->>'po_line_id')::UUID;
    v_qty_received := (v_line->>'qty_received')::BIGINT;

    IF v_qty_received IS NULL OR v_qty_received <= 0 THEN
      RAISE EXCEPTION 'po_receipt_invalid: line % qty_received must be > 0',
                      v_idx USING ERRCODE = 'P0022';
    END IF;

    SELECT po_id, sku_id, location_id, qty_ordered, unit_cost, currency
      INTO v_pl
      FROM purchase_order_lines WHERE id = v_po_line_id FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'po_receipt_invalid: po_line % not found', v_po_line_id
        USING ERRCODE = 'P0022';
    END IF;
    IF v_pl.po_id <> p_po_id THEN
      RAISE EXCEPTION 'po_receipt_invalid: po_line % belongs to PO % not %',
                      v_po_line_id, v_pl.po_id, p_po_id USING ERRCODE = 'P0022';
    END IF;

    SELECT COALESCE(SUM(qty_received), 0) INTO v_already_recv
      FROM po_receipt_lines WHERE po_line_id = v_po_line_id;
    IF v_already_recv + v_qty_received > v_pl.qty_ordered THEN
      RAISE EXCEPTION
        'po_line_overreceived: po_line %: ordered=%, already received=%, '
        'this receipt=%; cumulative would exceed qty_ordered',
        v_po_line_id, v_pl.qty_ordered, v_already_recv, v_qty_received
        USING ERRCODE = 'P0023';
    END IF;

    SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_pl.sku_id;
    IF v_cost_method IN ('fifo', 'lot') THEN
      RAISE EXCEPTION
        'cost_method_not_implemented: % for po_receipt (sku=%); see acct-8gg',
        v_cost_method, v_pl.sku_id USING ERRCODE = 'P0006';
    END IF;

    SELECT id INTO v_qty_acct FROM accounts
     WHERE kind='stock_available' AND sku_id=v_pl.sku_id
       AND location_id=v_pl.location_id AND NOT is_closed;
    IF v_qty_acct IS NULL THEN
      RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                      v_pl.sku_id, v_pl.location_id USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_val_acct FROM accounts
     WHERE kind='inv_value_raw' AND sku_id=v_pl.sku_id
       AND location_id=v_pl.location_id
       AND currency=v_pl.currency AND NOT is_closed;
    IF v_val_acct IS NULL THEN
      RAISE EXCEPTION 'no open inv_value_raw account for sku=% loc=% ccy=%',
                      v_pl.sku_id, v_pl.location_id, v_pl.currency
        USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_ven_qty FROM accounts
     WHERE kind='vendor_pool' AND counterparty_id=v_vendor_id
       AND ledger_kind='qty' AND NOT is_closed;
    IF v_ven_qty IS NULL THEN
      RAISE EXCEPTION 'no open vendor_pool(qty) account for vendor=%',
                      v_vendor_id USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_ven_val FROM accounts
     WHERE kind='ap_unsettled' AND counterparty_id=v_vendor_id
       AND currency=v_pl.currency AND NOT is_closed;
    IF v_ven_val IS NULL THEN
      RAISE EXCEPTION 'no open ap_unsettled account for vendor=% ccy=%',
                      v_vendor_id, v_pl.currency USING ERRCODE = 'P0010';
    END IF;

    IF v_cost_method = 'standard' THEN
      v_std_cost   := _resolve_standard_cost_at(v_pl.sku_id, p_business_date);
      v_val_unit   := v_std_cost;
      v_val_amount := v_qty_received * v_std_cost;
      v_ppv_amount := v_qty_received * (v_pl.unit_cost - v_std_cost);
    ELSE
      v_val_unit   := v_pl.unit_cost;
      v_val_amount := v_qty_received * v_pl.unit_cost;
      v_ppv_amount := 0;
    END IF;

    IF v_ppv_amount <> 0 THEN
      SELECT id INTO v_var_acct FROM accounts
       WHERE kind='variance_ppv' AND ledger_kind='value'
         AND currency=v_pl.currency AND NOT is_closed;
      IF v_var_acct IS NULL THEN
        RAISE EXCEPTION 'no open variance_ppv account for ccy=%',
                        v_pl.currency USING ERRCODE = 'P0010';
      END IF;
    END IF;

    INSERT INTO po_receipt_lines (
      receipt_id, po_line_id, qty_received, cost_method_at_receipt
    ) VALUES (v_doc_id, v_po_line_id, v_qty_received, v_cost_method)
    RETURNING id INTO v_recv_line_id;

    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason','po_receipt','document_kind','po_receipt',
      'document_id',v_doc_id,'document_line_id',v_recv_line_id,
      'debit_account_id',v_qty_acct,'credit_account_id',v_ven_qty,
      'amount',v_qty_received,'qty',v_qty_received,
      'business_date',p_business_date,'idempotency_key',gen_random_uuid(),
      'counterparty_id',v_vendor_id,'posted_by',p_posted_by
    ));

    IF v_val_amount > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason','po_receipt','document_kind','po_receipt',
        'document_id',v_doc_id,'document_line_id',v_recv_line_id,
        'debit_account_id',v_val_acct,'credit_account_id',v_ven_val,
        'amount',v_val_amount,'qty',v_qty_received,
        'business_date',p_business_date,'idempotency_key',gen_random_uuid(),
        'counterparty_id',v_vendor_id,'posted_by',p_posted_by
      ));
    END IF;

    IF v_ppv_amount > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason','ppv','document_kind','po_receipt',
        'document_id',v_doc_id,'document_line_id',v_recv_line_id,
        'debit_account_id',v_var_acct,'credit_account_id',v_ven_val,
        'amount',v_ppv_amount,'business_date',p_business_date,
        'idempotency_key',gen_random_uuid(),
        'counterparty_id',v_vendor_id,'posted_by',p_posted_by
      ));
    ELSIF v_ppv_amount < 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason','ppv','document_kind','po_receipt',
        'document_id',v_doc_id,'document_line_id',v_recv_line_id,
        'debit_account_id',v_ven_val,'credit_account_id',v_var_acct,
        'amount',-v_ppv_amount,'business_date',p_business_date,
        'idempotency_key',gen_random_uuid(),
        'counterparty_id',v_vendor_id,'posted_by',p_posted_by
      ));
    END IF;
  END LOOP;

  PERFORM post_posting_lines(v_batch, FALSE);
  RETURN v_doc_id;
END;
$$;

-- ============================================================
-- post_ap_bill (with three-way match tolerance windows)
-- 0017 will CREATE OR REPLACE this to add 'disposal_match' kind.
-- ============================================================

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
  v_existing_id      UUID;
  v_doc_id           UUID;
  v_vendor_check     UUID;
  v_tolerance_pct    NUMERIC(5,2);
  v_n                INT;
  v_idx              INT;
  v_line             JSONB;
  v_kind             TEXT;
  v_po_line_id       UUID;
  v_qty              BIGINT;
  v_unit_cost        BIGINT;
  v_amount           BIGINT;
  v_expense_acct     BIGINT;
  v_pl               RECORD;
  v_total_received   BIGINT;
  v_total_billed     BIGINT;
  v_returns_to_us    BIGINT;
  v_avail            BIGINT;
  v_ven_unsettled    BIGINT;
  v_ven_ap           BIGINT;
  v_match_tol_acct   BIGINT;
  v_exp_acct         accounts%ROWTYPE;
  v_bill_line_id     UUID;
  v_diff_total       BIGINT;
  v_diff_pct         NUMERIC(10,4);
  v_amount_at_po     BIGINT;
  v_batch            JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM vendor_bills
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT id, unit_cost_tolerance_pct INTO v_vendor_check, v_tolerance_pct
    FROM vendors WHERE id = p_vendor_id;
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
       WHERE pl.id = v_po_line_id FOR UPDATE OF pl;
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
        IF v_tolerance_pct = 0 THEN
          RAISE EXCEPTION
            'ap_bill_three_way_mismatch: line % unit_cost % does not match '
            'po_line.unit_cost %',
            v_idx, v_unit_cost, v_pl.unit_cost USING ERRCODE = 'P0024';
        END IF;
        IF v_pl.unit_cost = 0 THEN
          RAISE EXCEPTION
            'ap_bill_three_way_mismatch: line % po_line.unit_cost is 0 '
            'but bill unit_cost is % (zero-baseline; out of tolerance '
            'by definition, vendor tolerance %%%)',
            v_idx, v_unit_cost, v_tolerance_pct USING ERRCODE = 'P0024';
        END IF;
        v_diff_pct := ABS(v_unit_cost - v_pl.unit_cost) * 100.0 / v_pl.unit_cost;
        IF v_diff_pct > v_tolerance_pct THEN
          RAISE EXCEPTION
            'ap_bill_three_way_mismatch: line % unit_cost % differs from '
            'po_line.unit_cost % by %%% (vendor tolerance %%%)',
            v_idx, v_unit_cost, v_pl.unit_cost, v_diff_pct, v_tolerance_pct
            USING ERRCODE = 'P0024';
        END IF;
      END IF;
      IF v_amount <> v_qty * v_unit_cost THEN
        RAISE EXCEPTION
          'ap_bill_three_way_mismatch: line % amount % <> qty % × unit_cost %',
          v_idx, v_amount, v_qty, v_unit_cost USING ERRCODE = 'P0024';
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
          v_total_billed, v_returns_to_us USING ERRCODE = 'P0024';
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

      v_amount_at_po := v_qty * v_pl.unit_cost;
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason','ap_bill','document_kind','vendor_bill',
        'document_id',v_doc_id,'document_line_id',v_bill_line_id,
        'debit_account_id',v_ven_unsettled,'credit_account_id',v_ven_ap,
        'amount',v_amount_at_po,'business_date',p_business_date,
        'idempotency_key',gen_random_uuid(),
        'counterparty_id',p_vendor_id,'posted_by',p_posted_by
      ));

      v_diff_total := v_amount - v_amount_at_po;
      IF v_diff_total <> 0 THEN
        SELECT id INTO v_match_tol_acct FROM accounts
         WHERE kind='variance_match_tolerance' AND ledger_kind='value'
           AND currency=p_currency AND NOT is_closed;
        IF v_match_tol_acct IS NULL THEN
          RAISE EXCEPTION 'no open variance_match_tolerance account for ccy=%',
                          p_currency USING ERRCODE = 'P0010';
        END IF;
        IF v_diff_total > 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason','ap_bill','document_kind','vendor_bill',
            'document_id',v_doc_id,'document_line_id',v_bill_line_id,
            'debit_account_id',v_match_tol_acct,'credit_account_id',v_ven_ap,
            'amount',v_diff_total,'business_date',p_business_date,
            'idempotency_key',gen_random_uuid(),
            'counterparty_id',p_vendor_id,'posted_by',p_posted_by
          ));
        ELSE
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason','ap_bill','document_kind','vendor_bill',
            'document_id',v_doc_id,'document_line_id',v_bill_line_id,
            'debit_account_id',v_ven_ap,'credit_account_id',v_match_tol_acct,
            'amount',-v_diff_total,'business_date',p_business_date,
            'idempotency_key',gen_random_uuid(),
            'counterparty_id',p_vendor_id,'posted_by',p_posted_by
          ));
        END IF;
      END IF;

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
        'reason','ap_bill','document_kind','vendor_bill',
        'document_id',v_doc_id,'document_line_id',v_bill_line_id,
        'debit_account_id',v_expense_acct,'credit_account_id',v_ven_ap,
        'amount',v_amount,'business_date',p_business_date,
        'idempotency_key',gen_random_uuid(),
        'counterparty_id',p_vendor_id,'posted_by',p_posted_by
      ));

    ELSE
      RAISE EXCEPTION 'ap_bill_invalid_line: line % unknown kind %',
                      v_idx, v_kind USING ERRCODE = 'P0025';
    END IF;
  END LOOP;

  PERFORM post_posting_lines(v_batch, FALSE);
  RETURN v_doc_id;
END;
$$;

-- ============================================================
-- post_ap_payment (cross-currency settlement)
-- ============================================================

CREATE OR REPLACE FUNCTION post_ap_payment(
  p_vendor_id       UUID,
  p_currency        CHAR(3),
  p_amount          BIGINT,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT     DEFAULT NULL,
  p_cash_currency   CHAR(3)  DEFAULT NULL,
  p_cash_amount     BIGINT   DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id   UUID;
  v_doc_id        UUID;
  v_vendor_check  UUID;
  v_cash_acct     BIGINT;
  v_vend_ap       BIGINT;
  v_fx_clr_ccy    BIGINT;
  v_fx_clr_cash   BIGINT;
  v_fx_gain_acct  BIGINT;
  v_fx_loss_acct  BIGINT;
  v_rate          NUMERIC(20, 10);
  v_expected      BIGINT;
  v_delta         BIGINT;
  v_cross_ccy     BOOLEAN;
  v_batch         JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM ap_payments
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  IF p_amount IS NULL OR p_amount <= 0 THEN
    RAISE EXCEPTION 'ap_payment_invalid: amount must be > 0 (got %)', p_amount
      USING ERRCODE = 'P0042';
  END IF;

  SELECT id INTO v_vendor_check FROM vendors WHERE id = p_vendor_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'ap_payment_invalid: vendor % not found', p_vendor_id
      USING ERRCODE = 'P0042';
  END IF;

  v_cross_ccy := p_cash_currency IS NOT NULL AND p_cash_currency <> p_currency;

  IF v_cross_ccy THEN
    IF p_cash_amount IS NULL OR p_cash_amount <= 0 THEN
      RAISE EXCEPTION
        'ap_payment_invalid: p_cash_amount required and > 0 when '
        'p_cash_currency (%) differs from p_currency (%)',
        p_cash_currency, p_currency USING ERRCODE = 'P0042';
    END IF;
  ELSE
    IF p_cash_amount IS NOT NULL AND p_cash_amount <> p_amount THEN
      RAISE EXCEPTION
        'ap_payment_invalid: same-currency settlement requires '
        'p_cash_amount (%) = p_amount (%) (or NULL)',
        p_cash_amount, p_amount USING ERRCODE = 'P0042';
    END IF;
  END IF;

  SELECT id INTO v_vend_ap FROM accounts
   WHERE kind='ap' AND counterparty_id=p_vendor_id
     AND currency=p_currency AND NOT is_closed;
  IF v_vend_ap IS NULL THEN
    RAISE EXCEPTION 'no open ap account for vendor=% ccy=%',
                    p_vendor_id, p_currency USING ERRCODE = 'P0010';
  END IF;

  SELECT id INTO v_cash_acct FROM accounts
   WHERE kind='cash' AND ledger_kind='value'
     AND currency=COALESCE(p_cash_currency, p_currency) AND NOT is_closed;
  IF v_cash_acct IS NULL THEN
    RAISE EXCEPTION 'no open cash account for ccy=%',
                    COALESCE(p_cash_currency, p_currency) USING ERRCODE = 'P0010';
  END IF;

  IF v_cross_ccy THEN
    SELECT id INTO v_fx_clr_ccy FROM accounts
     WHERE kind='fx_clearing' AND ledger_kind='value'
       AND currency=p_currency AND NOT is_closed;
    IF v_fx_clr_ccy IS NULL THEN
      RAISE EXCEPTION 'no open fx_clearing account for ccy=%',
                      p_currency USING ERRCODE = 'P0010';
    END IF;
    SELECT id INTO v_fx_clr_cash FROM accounts
     WHERE kind='fx_clearing' AND ledger_kind='value'
       AND currency=p_cash_currency AND NOT is_closed;
    IF v_fx_clr_cash IS NULL THEN
      RAISE EXCEPTION 'no open fx_clearing account for ccy=%',
                      p_cash_currency USING ERRCODE = 'P0010';
    END IF;

    SELECT rate INTO v_rate
      FROM fx_rates
     WHERE from_currency = p_currency
       AND to_currency   = p_cash_currency
       AND effective_at::DATE <= p_business_date
     ORDER BY effective_at DESC LIMIT 1;
    IF v_rate IS NULL THEN
      RAISE EXCEPTION
        'missing_fx_rate: no fx_rates row found for % → % effective_at <= %',
        p_currency, p_cash_currency, p_business_date USING ERRCODE = 'P0050';
    END IF;

    v_expected := (p_amount::NUMERIC * v_rate)::BIGINT;
    v_delta    := p_cash_amount - v_expected;

    IF v_delta > 0 THEN
      SELECT id INTO v_fx_loss_acct FROM accounts
       WHERE kind='realized_fx_loss' AND ledger_kind='value'
         AND currency=p_cash_currency AND NOT is_closed;
      IF v_fx_loss_acct IS NULL THEN
        RAISE EXCEPTION 'no open realized_fx_loss account for ccy=%',
                        p_cash_currency USING ERRCODE = 'P0010';
      END IF;
    ELSIF v_delta < 0 THEN
      SELECT id INTO v_fx_gain_acct FROM accounts
       WHERE kind='realized_fx_gain' AND ledger_kind='value'
         AND currency=p_cash_currency AND NOT is_closed;
      IF v_fx_gain_acct IS NULL THEN
        RAISE EXCEPTION 'no open realized_fx_gain account for ccy=%',
                        p_cash_currency USING ERRCODE = 'P0010';
      END IF;
    END IF;
  END IF;

  INSERT INTO ap_payments (
    vendor_id, currency, amount, business_date, posted_by,
    idempotency_key, notes
  ) VALUES (
    p_vendor_id, p_currency, p_amount, p_business_date, p_posted_by,
    p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM ap_payments
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  IF v_cross_ccy THEN
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason','ap_payment','document_kind','ap_payment',
      'document_id',v_doc_id,
      'debit_account_id',v_vend_ap,'credit_account_id',v_fx_clr_ccy,
      'amount',p_amount,'business_date',p_business_date,
      'idempotency_key',gen_random_uuid(),
      'counterparty_id',p_vendor_id,'posted_by',p_posted_by
    ));
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason','ap_payment','document_kind','ap_payment',
      'document_id',v_doc_id,
      'debit_account_id',v_fx_clr_cash,'credit_account_id',v_cash_acct,
      'amount',p_cash_amount,'business_date',p_business_date,
      'idempotency_key',gen_random_uuid(),
      'counterparty_id',p_vendor_id,'posted_by',p_posted_by
    ));
    IF v_delta > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason','ap_payment','document_kind','ap_payment',
        'document_id',v_doc_id,
        'debit_account_id',v_fx_loss_acct,'credit_account_id',v_fx_clr_cash,
        'amount',v_delta,'business_date',p_business_date,
        'idempotency_key',gen_random_uuid(),
        'counterparty_id',p_vendor_id,'posted_by',p_posted_by
      ));
    ELSIF v_delta < 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason','ap_payment','document_kind','ap_payment',
        'document_id',v_doc_id,
        'debit_account_id',v_fx_clr_cash,'credit_account_id',v_fx_gain_acct,
        'amount',-v_delta,'business_date',p_business_date,
        'idempotency_key',gen_random_uuid(),
        'counterparty_id',p_vendor_id,'posted_by',p_posted_by
      ));
    END IF;
  ELSE
    v_batch := jsonb_build_array(jsonb_build_object(
      'reason','ap_payment','document_kind','ap_payment',
      'document_id',v_doc_id,
      'debit_account_id',v_vend_ap,'credit_account_id',v_cash_acct,
      'amount',p_amount,'business_date',p_business_date,
      'idempotency_key',gen_random_uuid(),
      'counterparty_id',p_vendor_id,'posted_by',p_posted_by
    ));
  END IF;

  PERFORM post_posting_lines(v_batch, FALSE);
  RETURN v_doc_id;
END;
$$;

-- ============================================================
-- post_po_return (state-aware routing + prior-period adjustment + cost_method snapshot)
-- ============================================================

CREATE OR REPLACE FUNCTION post_po_return(
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
  v_var_kind           account_kind;
  v_recv_period_closed BOOLEAN;
  v_return_line_id     UUID;
  v_batch              JSONB := '[]'::JSONB;
BEGIN
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

    SELECT
      prl.id                     AS recv_line_id,
      prl.po_line_id             AS po_line_id,
      prl.cost_method_at_receipt AS cost_method_snap,
      pol.sku_id                 AS sku_id,
      pol.location_id            AS location_id,
      pol.unit_cost              AS unit_cost,
      pol.currency               AS currency,
      po.vendor_id               AS vendor_id,
      pr.business_date           AS recv_business_date
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
        v_recv_line_id, v_pl.vendor_id, p_vendor_id USING ERRCODE = 'P0046';
    END IF;

    v_po_line_id := v_pl.po_line_id;
    PERFORM 1 FROM purchase_order_lines WHERE id = v_po_line_id FOR UPDATE;

    SELECT COALESCE(SUM(qty_received), 0) INTO v_total_recv
      FROM po_receipt_lines WHERE po_line_id = v_po_line_id;

    SELECT COALESCE(SUM(qty), 0) INTO v_total_billed
      FROM vendor_bill_lines
     WHERE po_line_id = v_po_line_id AND kind = 'po_match';

    SELECT
      COALESCE(SUM(prl.qty_to_ap_unsettled), 0),
      COALESCE(SUM(prl.qty_to_ap), 0)
      INTO v_prior_to_unsettled, v_prior_to_ap
      FROM po_return_lines prl
      JOIN po_receipt_lines rcl ON rcl.id = prl.recv_line_id
      JOIN po_returns       pr  ON pr.id  = prl.return_id
     WHERE rcl.po_line_id = v_po_line_id AND pr.id <> v_doc_id;

    v_unsettled_rem := v_total_recv - v_total_billed - v_prior_to_unsettled;
    v_ap_rem        := v_total_billed - v_prior_to_ap;
    v_qty_to_unsettled := LEAST(v_qty_returned, GREATEST(v_unsettled_rem, 0));
    v_qty_to_ap        := v_qty_returned - v_qty_to_unsettled;

    IF v_qty_to_ap > v_ap_rem THEN
      RAISE EXCEPTION
        'po_return_overreturned: po_line % cumulative would exceed billed-not-returned + unsettled-not-returned (recv=%, billed=%, prior_to_unsettled=%, prior_to_ap=%, requested=%)',
        v_po_line_id, v_total_recv, v_total_billed,
        v_prior_to_unsettled, v_prior_to_ap, v_qty_returned
        USING ERRCODE = 'P0047';
    END IF;

    v_cost_method := v_pl.cost_method_snap;
    IF v_cost_method = 'standard' THEN
      v_inv_unit := _resolve_standard_cost_at(v_pl.sku_id, p_business_date);
    ELSIF v_cost_method IN ('wac_perpetual', 'wac_periodic', 'wac_retroactive') THEN
      v_inv_unit := v_pl.unit_cost;
    ELSE
      RAISE EXCEPTION 'cost_method_not_implemented: % for po_return',
                      v_cost_method USING ERRCODE = 'P0006';
    END IF;

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
     WHERE kind='vendor_pool' AND counterparty_id=p_vendor_id AND NOT is_closed;
    IF v_ven_qty IS NULL THEN
      SELECT id INTO v_ven_qty FROM accounts
       WHERE kind='vendor_pool' AND counterparty_id IS NULL AND NOT is_closed;
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

    v_recv_period_closed := FALSE;
    SELECT (closed_at IS NOT NULL) INTO v_recv_period_closed
      FROM periods
     WHERE v_pl.recv_business_date BETWEEN opens_at AND closes_at LIMIT 1;
    v_recv_period_closed := COALESCE(v_recv_period_closed, FALSE);

    IF v_recv_period_closed THEN
      v_var_kind := 'variance_ppv_prior_period_adj';
    ELSE
      v_var_kind := 'variance_ppv';
    END IF;

    IF v_cost_method = 'standard' AND v_pl.unit_cost <> v_inv_unit THEN
      SELECT id INTO v_var_acct FROM accounts
       WHERE kind = v_var_kind AND currency=v_pl.currency AND NOT is_closed;
      IF v_var_acct IS NULL THEN
        RAISE EXCEPTION 'no open % for ccy=%',
                        v_var_kind, v_pl.currency USING ERRCODE = 'P0010';
      END IF;
    END IF;

    INSERT INTO po_return_lines (
      return_id, line_no, recv_line_id, qty_returned, unit_cost,
      qty_to_ap_unsettled, qty_to_ap
    ) VALUES (
      v_doc_id, v_idx, v_recv_line_id, v_qty_returned, v_pl.unit_cost,
      v_qty_to_unsettled, v_qty_to_ap
    ) RETURNING id INTO v_return_line_id;

    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason','po_return_to_vendor','document_kind','po_return',
      'document_id',v_doc_id,'document_line_id',v_return_line_id,
      'debit_account_id',v_ven_qty,'credit_account_id',v_qty_acct,
      'amount',v_qty_returned,'qty',v_qty_returned,
      'business_date',p_business_date,'idempotency_key',gen_random_uuid(),
      'counterparty_id',p_vendor_id,'posted_by',p_posted_by
    ));

    IF v_qty_to_unsettled > 0 THEN
      DECLARE
        v_inv_amt_us BIGINT := v_qty_to_unsettled * v_inv_unit;
        v_ppv_amt_us BIGINT := v_qty_to_unsettled * v_pl.unit_cost
                              - v_qty_to_unsettled * v_inv_unit;
      BEGIN
        IF v_ppv_amt_us > 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason','ppv','document_kind','po_return',
            'document_id',v_doc_id,'document_line_id',v_return_line_id,
            'debit_account_id',v_ven_unsettled,'credit_account_id',v_var_acct,
            'amount',v_ppv_amt_us,'business_date',p_business_date,
            'idempotency_key',gen_random_uuid(),
            'counterparty_id',p_vendor_id,'posted_by',p_posted_by
          ));
        ELSIF v_ppv_amt_us < 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason','ppv','document_kind','po_return',
            'document_id',v_doc_id,'document_line_id',v_return_line_id,
            'debit_account_id',v_var_acct,'credit_account_id',v_ven_unsettled,
            'amount',-v_ppv_amt_us,'business_date',p_business_date,
            'idempotency_key',gen_random_uuid(),
            'counterparty_id',p_vendor_id,'posted_by',p_posted_by
          ));
        END IF;
        IF v_inv_amt_us > 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason','po_return_to_vendor','document_kind','po_return',
            'document_id',v_doc_id,'document_line_id',v_return_line_id,
            'debit_account_id',v_ven_unsettled,'credit_account_id',v_val_acct,
            'amount',v_inv_amt_us,'qty',v_qty_to_unsettled,
            'business_date',p_business_date,'idempotency_key',gen_random_uuid(),
            'counterparty_id',p_vendor_id,'posted_by',p_posted_by
          ));
        END IF;
      END;
    END IF;

    IF v_qty_to_ap > 0 THEN
      DECLARE
        v_inv_amt_ap BIGINT := v_qty_to_ap * v_inv_unit;
        v_ppv_amt_ap BIGINT := v_qty_to_ap * v_pl.unit_cost
                              - v_qty_to_ap * v_inv_unit;
      BEGIN
        IF v_ppv_amt_ap > 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason','ppv','document_kind','po_return',
            'document_id',v_doc_id,'document_line_id',v_return_line_id,
            'debit_account_id',v_ven_ap,'credit_account_id',v_var_acct,
            'amount',v_ppv_amt_ap,'business_date',p_business_date,
            'idempotency_key',gen_random_uuid(),
            'counterparty_id',p_vendor_id,'posted_by',p_posted_by
          ));
        ELSIF v_ppv_amt_ap < 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason','ppv','document_kind','po_return',
            'document_id',v_doc_id,'document_line_id',v_return_line_id,
            'debit_account_id',v_var_acct,'credit_account_id',v_ven_ap,
            'amount',-v_ppv_amt_ap,'business_date',p_business_date,
            'idempotency_key',gen_random_uuid(),
            'counterparty_id',p_vendor_id,'posted_by',p_posted_by
          ));
        END IF;
        IF v_inv_amt_ap > 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason','po_return_to_vendor','document_kind','po_return',
            'document_id',v_doc_id,'document_line_id',v_return_line_id,
            'debit_account_id',v_ven_ap,'credit_account_id',v_val_acct,
            'amount',v_inv_amt_ap,'qty',v_qty_to_ap,
            'business_date',p_business_date,'idempotency_key',gen_random_uuid(),
            'counterparty_id',p_vendor_id,'posted_by',p_posted_by
          ));
        END IF;
      END;
    END IF;
  END LOOP;

  PERFORM post_posting_lines(v_batch, p_override_closed_period);
  RETURN v_doc_id;
END;
$$;
