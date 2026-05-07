-- acct-quk — post_po_return / vendor return workflow.
--
-- Inverse of post_po_receipt + post_ap_bill on the AP side. Mirror
-- of post_customer_return (acct-ari, mig 0084) on the inflow side.
-- Vendor returns goods we received earlier; we issue a credit memo
-- against ap (mainstream-ERP convention; assumes the bill has
-- already cleared ap_unsettled → ap).
--
-- Per line, post_po_return emits the inverse of post_po_receipt's
-- legs:
--
--   qty:    vendor_pool(vendor)              DR / stock_available(sku, loc) CR
--   value:  ap(vendor, ccy)                  DR / inv_value_raw(sku, loc, ccy) CR
--   PPV:    standard SKUs with po ≠ std post the variance leg.
--           If po > std: ap(vendor, ccy)     DR / variance_ppv(ccy)         CR
--           If po < std: variance_ppv(ccy)   DR / ap(vendor, ccy)           CR
--           (Mirror of mig 0036's post_po_receipt PPV split.)
--
-- All four legs use reason='po_return_to_vendor' for qty + value;
-- PPV reversal uses reason='ppv'. The transfer reason
-- 'po_return_to_vendor' has been in the enum since mig 0002 and the
-- A1 conformance fixture exercises the qty leg with reason 'po_return_to_vendor';
-- this migration is the document-wrapper layer.
--
-- Pre-bill returns (return BEFORE the bill clears ap_unsettled →
-- ap) are out of scope — they would route the credit memo to
-- ap_unsettled instead of ap. A future "void receipt" workflow
-- covers that path.
--
-- Cost dispatch mirrors post_po_receipt:
--   * standard:    inv_value_raw at standard cost; PPV absorbs po − std diff.
--   * wac_*:       inv_value_raw at po_unit_cost; no PPV.
--   * fifo / lot:  P0006 (cost_method_not_implemented).
--
-- New error codes:
--   P0046 po_return_invalid
--   P0047 po_return_overreturned

-- ============================================================
-- po_returns + po_return_lines
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

COMMENT ON TABLE po_returns IS
  'Vendor return / debit memo header. One row per post_po_return '
  'call. Reverses prior po_receipt lines (qty back to vendor_pool, '
  'value back from inv_value_raw, ap reduced).';

CREATE TABLE po_return_lines (
  id           UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  return_id    UUID NOT NULL REFERENCES po_returns(id),
  line_no      INT  NOT NULL,
  recv_line_id UUID NOT NULL REFERENCES po_receipt_lines(id),
  qty_returned BIGINT NOT NULL CHECK (qty_returned > 0),
  unit_cost    BIGINT NOT NULL CHECK (unit_cost >= 0),
  UNIQUE (return_id, line_no)
);

CREATE INDEX po_return_lines_recv_line ON po_return_lines (recv_line_id);

COMMENT ON TABLE po_return_lines IS
  'Per-line return detail. recv_line_id pins the originating receipt '
  'line; qty_returned must respect cumulative not-yet-returned '
  'remainder per recv_line. unit_cost snapshotted from the po_line '
  'at return-post time (audit-trail integrity over current-WAC '
  'accuracy).';

-- ============================================================
-- post_po_return
-- ============================================================

CREATE OR REPLACE FUNCTION post_po_return(
  p_vendor_id       UUID,
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
  v_n                INT;
  v_idx              INT;
  v_line             JSONB;
  v_recv_line_id     UUID;
  v_qty_returned     BIGINT;
  v_pl               RECORD;          -- po_receipt_lines + po_line + po cols
  v_already_returned BIGINT;
  v_remaining        BIGINT;
  v_cost_method      cost_method;
  v_inv_unit         BIGINT;          -- inv-side per-unit cost basis
  v_inv_amount       BIGINT;          -- qty * v_inv_unit
  v_ap_amount        BIGINT;          -- qty * po_unit_cost
  v_ppv_amount       BIGINT;          -- v_ap_amount − v_inv_amount
  v_qty_acct         BIGINT;
  v_val_acct         BIGINT;
  v_ven_qty          BIGINT;
  v_ven_ap           BIGINT;
  v_var_acct         BIGINT;
  v_return_line_id   UUID;
  v_batch            JSONB := '[]'::JSONB;
BEGIN
  -- Fast-path replay.
  SELECT id INTO v_existing_id FROM po_returns
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  -- Vendor existence.
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

  -- Reserve doc id (race-safe via UNIQUE on idempotency_key).
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

    -- po_receipt_line + po_line + po (vendor ownership check).
    SELECT
      prl.id           AS recv_line_id,
      prl.qty_received AS qty_received,
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

    -- Cumulative-not-yet-returned check.
    SELECT COALESCE(SUM(prl.qty_returned), 0)
      INTO v_already_returned
      FROM po_return_lines prl
      JOIN po_returns      pr ON pr.id = prl.return_id
     WHERE prl.recv_line_id = v_recv_line_id
       AND pr.id <> v_doc_id;

    v_remaining := v_pl.qty_received - v_already_returned;
    IF v_qty_returned > v_remaining THEN
      RAISE EXCEPTION
        'po_return_overreturned: recv_line % received=% already_returned=% requested=% remaining=%',
        v_recv_line_id, v_pl.qty_received, v_already_returned, v_qty_returned, v_remaining
        USING ERRCODE = 'P0047';
    END IF;

    -- Cost dispatch (mirror of post_po_receipt's branches).
    SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_pl.sku_id;
    IF v_cost_method = 'standard' THEN
      v_inv_unit := resolve_standard_cost_at(v_pl.sku_id, p_business_date);
    ELSIF v_cost_method IN ('wac_perpetual', 'wac_periodic', 'wac_retroactive') THEN
      v_inv_unit := v_pl.unit_cost;
    ELSE
      RAISE EXCEPTION 'cost_method_not_implemented: % for po_return',
                      v_cost_method USING ERRCODE = 'P0006';
    END IF;

    v_inv_amount := v_qty_returned * v_inv_unit;
    v_ap_amount  := v_qty_returned * v_pl.unit_cost;
    v_ppv_amount := v_ap_amount - v_inv_amount;

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

    SELECT id INTO v_ven_ap FROM accounts
     WHERE kind='ap' AND counterparty_id=p_vendor_id
       AND currency=v_pl.currency AND NOT is_closed;
    IF v_ven_ap IS NULL THEN
      RAISE EXCEPTION 'no open ap for vendor=% ccy=%',
                      p_vendor_id, v_pl.currency USING ERRCODE = 'P0010';
    END IF;

    IF v_ppv_amount <> 0 THEN
      SELECT id INTO v_var_acct FROM accounts
       WHERE kind='variance_ppv' AND currency=v_pl.currency AND NOT is_closed;
      IF v_var_acct IS NULL THEN
        RAISE EXCEPTION 'no open variance_ppv for ccy=%',
                        v_pl.currency USING ERRCODE = 'P0010';
      END IF;
    END IF;

    INSERT INTO po_return_lines (
      return_id, line_no, recv_line_id, qty_returned, unit_cost
    ) VALUES (
      v_doc_id, v_idx, v_recv_line_id, v_qty_returned, v_pl.unit_cost
    )
    RETURNING id INTO v_return_line_id;

    -- Event 1: qty leg. vendor_pool DR / stock_available CR.
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

    -- Event 2: PPV reversal — emitted BEFORE the value leg so the
    -- credit-normal accounts_check on ap stays satisfied at every
    -- intermediate row state. For po > std the inv leg's debit is
    -- absorbable; for po < std the PPV event must add credit to ap
    -- first, then the inv leg's larger debit balances out.
    IF v_ppv_amount > 0 THEN
      -- po > std: extra ap debit, variance_ppv credit.
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ppv',
        'document_kind',     'po_return',
        'document_id',       v_doc_id,
        'document_line_id',  v_return_line_id,
        'debit_account_id',  v_ven_ap,
        'credit_account_id', v_var_acct,
        'amount',            v_ppv_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_vendor_id,
        'posted_by',         p_posted_by
      ));
    ELSIF v_ppv_amount < 0 THEN
      -- po < std: variance_ppv debit, ap credit.
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ppv',
        'document_kind',     'po_return',
        'document_id',       v_doc_id,
        'document_line_id',  v_return_line_id,
        'debit_account_id',  v_var_acct,
        'credit_account_id', v_ven_ap,
        'amount',            -v_ppv_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_vendor_id,
        'posted_by',         p_posted_by
      ));
    END IF;

    -- Event 3: value leg. ap DR / inv_value_raw CR (drains inventory
    -- at standard for standard SKUs, at po for WAC).
    IF v_inv_amount > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'po_return_to_vendor',
        'document_kind',     'po_return',
        'document_id',       v_doc_id,
        'document_line_id',  v_return_line_id,
        'debit_account_id',  v_ven_ap,
        'credit_account_id', v_val_acct,
        'amount',            v_inv_amount,
        'qty',               v_qty_returned,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_vendor_id,
        'posted_by',         p_posted_by
      ));
    END IF;
  END LOOP;

  PERFORM post_transfers(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;

COMMENT ON FUNCTION post_po_return(UUID, JSONB, DATE, UUID, UUID, TEXT) IS
  'Vendor return / debit memo. Reverses prior po_receipt lines: qty '
  'back from stock_available to vendor_pool; value out of '
  'inv_value_raw; ap reduced by qty * po_unit_cost. Standard SKUs '
  'reverse PPV symmetrically with mig 0036 post_po_receipt. Strict '
  'over-return rejection (P0047). Pre-bill returns out of scope; '
  'caller must bill before returning under MVP.';
