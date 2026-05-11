-- acct-3aak: batch post_customer_invoice three-way-match aggregates outside
-- the per-line FOR LOOP.
--
-- Body sources: db/migrations/0068_wrapper_instrumentation.up.sql lines
-- 523-855 (verbatim post_customer_invoice with h73o instrumentation).
-- Surgical changes only:
--   (1) 5 new DECLAREs for aggregate hash maps + in-flight tracker.
--   (2) Pre-LOOP block: collect distinct so_line_ids; jsonb_object_agg the
--       three aggregates ONCE per invoice instead of 3 scans per line.
--   (3) In-LOOP: the 3 SELECT SUMs replaced with JSONB hash lookups + an
--       in-flight tracker that captures cumulative invoiced qty for the
--       SAME so_line_id across iterations of THIS invoice (so the
--       two-lines-on-one-so_line cumulative check still catches an
--       over-invoice within a single call).
-- Preserved verbatim:
--   - FOR UPDATE OF sl per-line lock (load-bearing for the tolerance check
--     against contemporaneous SO-line edits per ERP scope; see saved
--     feedback memory feedback_defensive_guards_in_erp_scope).
--   - h73o v_t0/v_t1/v_t2/v_t3 timing wrappers + INSERT INTO
--     _wrapper_section_timings (load-bearing for 3aak's own pre/post
--     measurement).
--   - All other validation, ledger-building, and service-line branch.

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
  v_tolerance_pct   NUMERIC(5,2);
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
  v_match_tol_acct  BIGINT;
  v_rev_acct        accounts%ROWTYPE;
  v_inv_line_id     UUID;
  v_diff_total      BIGINT;
  v_diff_pct        NUMERIC(10,4);
  v_amount_at_so    BIGINT;
  v_batch           JSONB := '[]'::JSONB;
  -- acct-3aak: aggregate batching + in-flight tracker.
  v_so_line_ids       UUID[];
  v_shipped_by_line   JSONB := '{}'::JSONB;
  v_invoiced_by_line  JSONB := '{}'::JSONB;
  v_returned_by_line  JSONB := '{}'::JSONB;
  v_inflight_invoiced JSONB := '{}'::JSONB;
  -- acct-h73o instrumentation:
  v_t0              TIMESTAMPTZ;
  v_t1              TIMESTAMPTZ;
  v_t2              TIMESTAMPTZ;
  v_t3              TIMESTAMPTZ;
BEGIN
  v_t0 := clock_timestamp();

  SELECT id INTO v_existing_id FROM customer_invoices
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT id, unit_price_tolerance_pct INTO v_customer_check, v_tolerance_pct
    FROM customers WHERE id = p_customer_id;
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

  -- acct-3aak: aggregate-batch the three so_match three-way-match scans
  -- over the union of distinct so_line_ids in this invoice. The per-line
  -- SUMs inside the LOOP would otherwise re-scan so_shipment_lines /
  -- customer_invoice_lines / customer_return_lines once per so_match
  -- line, which dominates setup p99 under 32-writer contention. The
  -- in-flight tracker (v_inflight_invoiced) handles same-invoice
  -- cumulative invoiced qty for repeated so_line_id within p_lines.
  SELECT array_agg(DISTINCT (elem->>'so_line_id')::UUID)
    INTO v_so_line_ids
    FROM jsonb_array_elements(p_lines) elem
   WHERE elem->>'kind' = 'so_match'
     AND elem->>'so_line_id' IS NOT NULL;

  IF v_so_line_ids IS NOT NULL THEN
    SELECT COALESCE(jsonb_object_agg(so_line_id::TEXT, total), '{}'::JSONB)
      INTO v_shipped_by_line
      FROM (
        SELECT so_line_id, COALESCE(SUM(qty_shipped), 0)::BIGINT AS total
          FROM so_shipment_lines
         WHERE so_line_id = ANY(v_so_line_ids)
         GROUP BY so_line_id
      ) s;

    SELECT COALESCE(jsonb_object_agg(so_line_id::TEXT, total), '{}'::JSONB)
      INTO v_invoiced_by_line
      FROM (
        SELECT so_line_id, COALESCE(SUM(qty), 0)::BIGINT AS total
          FROM customer_invoice_lines
         WHERE so_line_id = ANY(v_so_line_ids)
           AND kind = 'so_match'
         GROUP BY so_line_id
      ) i;

    SELECT COALESCE(jsonb_object_agg(so_line_id::TEXT, total), '{}'::JSONB)
      INTO v_returned_by_line
      FROM (
        SELECT ssl.so_line_id,
               COALESCE(SUM(crl.qty_to_ar_unsettled), 0)::BIGINT AS total
          FROM customer_return_lines crl
          JOIN so_shipment_lines     ssl ON ssl.id = crl.ship_line_id
         WHERE ssl.so_line_id = ANY(v_so_line_ids)
         GROUP BY ssl.so_line_id
      ) r;
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
        IF v_tolerance_pct = 0 THEN
          RAISE EXCEPTION
            'customer_invoice_three_way_mismatch: line % unit_price % does '
            'not match so_line.unit_price %',
            v_idx, v_unit_price, v_sl.unit_price USING ERRCODE = 'P0040';
        END IF;
        IF v_sl.unit_price = 0 THEN
          RAISE EXCEPTION
            'customer_invoice_three_way_mismatch: line % so_line.unit_price '
            'is 0 but invoice unit_price is % (zero-baseline; out of '
            'tolerance, customer tolerance %%%)',
            v_idx, v_unit_price, v_tolerance_pct
            USING ERRCODE = 'P0040';
        END IF;
        v_diff_pct := ABS(v_unit_price - v_sl.unit_price) * 100.0 / v_sl.unit_price;
        IF v_diff_pct > v_tolerance_pct THEN
          RAISE EXCEPTION
            'customer_invoice_three_way_mismatch: line % unit_price % differs from '
            'so_line.unit_price % by %%% (customer tolerance %%%)',
            v_idx, v_unit_price, v_sl.unit_price, v_diff_pct, v_tolerance_pct
            USING ERRCODE = 'P0040';
        END IF;
      END IF;
      IF v_amount <> v_qty * v_unit_price THEN
        RAISE EXCEPTION
          'customer_invoice_three_way_mismatch: line % amount % <> qty % × unit_price %',
          v_idx, v_amount, v_qty, v_unit_price USING ERRCODE = 'P0040';
      END IF;

      -- acct-3aak: hash-lookup the per-invoice batched aggregates +
      -- add cumulative in-flight invoiced for SAME so_line_id within
      -- this invoice call. (Was 3 SELECT SUM per line; now constant-
      -- time JSONB lookups against the pre-LOOP aggregate.)
      v_total_shipped  := COALESCE((v_shipped_by_line  ->> v_so_line_id::TEXT)::BIGINT, 0);
      v_total_invoiced := COALESCE((v_invoiced_by_line ->> v_so_line_id::TEXT)::BIGINT, 0)
                        + COALESCE((v_inflight_invoiced ->> v_so_line_id::TEXT)::BIGINT, 0);
      v_returns_to_us  := COALESCE((v_returned_by_line ->> v_so_line_id::TEXT)::BIGINT, 0);
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

      -- acct-3aak: stamp the in-flight tracker BEFORE the INSERT so the
      -- next iteration with the same so_line_id sees this line as
      -- already-cumulative.
      v_inflight_invoiced := jsonb_set(
        v_inflight_invoiced,
        ARRAY[v_so_line_id::TEXT],
        to_jsonb(COALESCE((v_inflight_invoiced->>v_so_line_id::TEXT)::BIGINT, 0) + v_qty)
      );

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

      v_amount_at_so := v_qty * v_sl.unit_price;
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ar_invoice',
        'document_kind',     'customer_invoice',
        'document_id',       v_doc_id,
        'document_line_id',  v_inv_line_id,
        'debit_account_id',  v_cust_ar,
        'credit_account_id', v_cust_unsettled,
        'amount',            v_amount_at_so,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_customer_id,
        'posted_by',         p_posted_by
      ));

      v_diff_total := v_amount - v_amount_at_so;
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
            'reason',            'ar_invoice',
            'document_kind',     'customer_invoice',
            'document_id',       v_doc_id,
            'document_line_id',  v_inv_line_id,
            'debit_account_id',  v_cust_ar,
            'credit_account_id', v_match_tol_acct,
            'amount',            v_diff_total,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_customer_id,
            'posted_by',         p_posted_by
          ));
        ELSE
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'ar_invoice',
            'document_kind',     'customer_invoice',
            'document_id',       v_doc_id,
            'document_line_id',  v_inv_line_id,
            'debit_account_id',  v_match_tol_acct,
            'credit_account_id', v_cust_ar,
            'amount',            -v_diff_total,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_customer_id,
            'posted_by',         p_posted_by
          ));
        END IF;
      END IF;

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

  v_t1 := clock_timestamp();
  PERFORM post_posting_lines(v_batch, FALSE);
  v_t2 := clock_timestamp();
  v_t3 := v_t2;

  INSERT INTO _wrapper_section_timings (wrapper_name, section, elapsed_us) VALUES
    ('post_customer_invoice', 'setup',              (EXTRACT(EPOCH FROM v_t1 - v_t0) * 1e6)::BIGINT),
    ('post_customer_invoice', 'post_posting_lines', (EXTRACT(EPOCH FROM v_t2 - v_t1) * 1e6)::BIGINT),
    ('post_customer_invoice', 'followup',           (EXTRACT(EPOCH FROM v_t3 - v_t2) * 1e6)::BIGINT);

  RETURN v_doc_id;
END;
$$;
