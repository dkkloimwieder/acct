-- Down: restore mig 0087 / 0078 / 0090 prior bodies (acct-5prc /
-- acct-quca / acct-nuw7 fixes reverted).

CREATE OR REPLACE FUNCTION post_so_ship(
  p_so_id           UUID,
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
  v_customer_id    UUID;
  v_n              INT;
  v_idx            INT;
  v_line           JSONB;
  v_so_line_id     UUID;
  v_qty_shipped    BIGINT;
  v_unit_price     BIGINT;
  v_tax_amount     BIGINT;
  v_sl             RECORD;
  v_already_ship   BIGINT;
  v_cost_method    cost_method;
  v_unit_cost      BIGINT;
  v_qty_acct       BIGINT;
  v_val_acct       BIGINT;
  v_cust_qty       BIGINT;
  v_cust_unsettled BIGINT;
  v_revenue_acct   BIGINT;
  v_cogs_acct      BIGINT;
  v_tax_acct       BIGINT;
  v_qty_balance    BIGINT;
  v_value_balance  BIGINT;
  v_ship_line_id   UUID;
  v_batch          JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM so_shipments
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT customer_id INTO v_customer_id FROM sales_orders WHERE id = p_so_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'so_ship_invalid: SO % not found', p_so_id
      USING ERRCODE = 'P0037';
  END IF;
  IF v_customer_id IS NULL THEN
    RAISE EXCEPTION 'so_ship_invalid: SO % has no customer_id', p_so_id
      USING ERRCODE = 'P0037';
  END IF;

  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'so_ship_invalid: empty lines for SO %', p_so_id
      USING ERRCODE = 'P0037';
  END IF;

  INSERT INTO so_shipments (so_id, business_date, posted_by, idempotency_key, notes)
  VALUES (p_so_id, p_business_date, p_posted_by, p_idempotency_key, p_notes)
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM so_shipments WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  FOR v_idx IN 1..v_n LOOP
    v_line        := p_lines -> (v_idx - 1);
    v_so_line_id  := (v_line->>'so_line_id')::UUID;
    v_qty_shipped := (v_line->>'qty_shipped')::BIGINT;

    IF v_qty_shipped IS NULL OR v_qty_shipped <= 0 THEN
      RAISE EXCEPTION 'so_ship_invalid: line % qty_shipped must be > 0',
                      v_idx USING ERRCODE = 'P0037';
    END IF;

    SELECT so_id, sku_id, ship_location_id, qty_ordered, unit_price,
           currency, tax_amount
      INTO v_sl
      FROM sales_order_lines WHERE id = v_so_line_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'so_ship_invalid: so_line % not found', v_so_line_id
        USING ERRCODE = 'P0037';
    END IF;
    IF v_sl.so_id <> p_so_id THEN
      RAISE EXCEPTION 'so_ship_invalid: so_line % belongs to SO % not %',
                      v_so_line_id, v_sl.so_id, p_so_id
        USING ERRCODE = 'P0037';
    END IF;

    SELECT COALESCE(SUM(qty_shipped), 0) INTO v_already_ship
      FROM so_shipment_lines WHERE so_line_id = v_so_line_id;
    IF v_already_ship + v_qty_shipped > v_sl.qty_ordered THEN
      RAISE EXCEPTION
        'so_line_overshipped: so_line %: ordered=%, already shipped=%, '
        'this shipment=%; cumulative would exceed qty_ordered',
        v_so_line_id, v_sl.qty_ordered, v_already_ship, v_qty_shipped
        USING ERRCODE = 'P0038';
    END IF;

    v_unit_price := COALESCE((v_line->>'unit_price')::BIGINT, v_sl.unit_price);
    v_tax_amount := COALESCE((v_line->>'tax_amount')::BIGINT, v_sl.tax_amount);

    SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_sl.sku_id;

    IF v_cost_method IN ('fifo', 'lot') THEN
      RAISE EXCEPTION
        'cost_method_not_implemented: % for so_ship (sku=%); see acct-8gg',
        v_cost_method, v_sl.sku_id USING ERRCODE = 'P0006';
    END IF;

    SELECT id INTO v_qty_acct FROM accounts
     WHERE kind='stock_available' AND sku_id=v_sl.sku_id
       AND location_id=v_sl.ship_location_id AND NOT is_closed;
    IF v_qty_acct IS NULL THEN
      RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                      v_sl.sku_id, v_sl.ship_location_id USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_val_acct FROM accounts
     WHERE kind='inv_value_fg' AND sku_id=v_sl.sku_id
       AND location_id=v_sl.ship_location_id
       AND currency=v_sl.currency AND NOT is_closed;
    IF v_val_acct IS NULL THEN
      RAISE EXCEPTION 'no open inv_value_fg account for sku=% loc=% ccy=%',
                      v_sl.sku_id, v_sl.ship_location_id, v_sl.currency
        USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_cust_qty FROM accounts
     WHERE kind='customer_pool' AND counterparty_id=v_customer_id
       AND ledger_kind='qty' AND NOT is_closed;
    IF v_cust_qty IS NULL THEN
      RAISE EXCEPTION 'no open customer_pool(qty) account for customer=%',
                      v_customer_id USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_cust_unsettled FROM accounts
     WHERE kind='ar_unsettled' AND counterparty_id=v_customer_id
       AND currency=v_sl.currency AND NOT is_closed;
    IF v_cust_unsettled IS NULL THEN
      RAISE EXCEPTION 'no open ar_unsettled account for customer=% ccy=%',
                      v_customer_id, v_sl.currency USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_revenue_acct FROM accounts
     WHERE kind='revenue' AND ledger_kind='value'
       AND currency=v_sl.currency AND NOT is_closed;
    IF v_revenue_acct IS NULL THEN
      RAISE EXCEPTION 'no open revenue account for ccy=%',
                      v_sl.currency USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_cogs_acct FROM accounts
     WHERE kind='cogs' AND ledger_kind='value'
       AND currency=v_sl.currency AND NOT is_closed;
    IF v_cogs_acct IS NULL THEN
      RAISE EXCEPTION 'no open cogs account for ccy=%',
                      v_sl.currency USING ERRCODE = 'P0010';
    END IF;

    IF v_cost_method = 'standard' THEN
      v_unit_cost := resolve_standard_cost_at(v_sl.sku_id, p_business_date);
    ELSE
      SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = v_val_acct THEN  t.qty
                               WHEN t.credit_account_id = v_val_acct THEN -t.qty END), 0)
        INTO v_qty_balance
        FROM transfers t
       WHERE v_val_acct IN (t.debit_account_id, t.credit_account_id)
         AND t.qty IS NOT NULL;

      IF v_qty_balance <= 0 THEN
        RAISE EXCEPTION
          'wac so_ship qty balance is %, cannot price (sku=%, loc=%, ccy=%)',
          v_qty_balance, v_sl.sku_id, v_sl.ship_location_id, v_sl.currency
          USING ERRCODE = 'P0006';
      END IF;
      SELECT debits_total - credits_total INTO v_value_balance
        FROM accounts WHERE id = v_val_acct;
      IF v_value_balance < 0 THEN v_value_balance := 0; END IF;
      v_unit_cost := v_value_balance / v_qty_balance;
    END IF;

    IF v_tax_amount > 0 THEN
      SELECT id INTO v_tax_acct FROM accounts
       WHERE kind='sales_tax_payable' AND ledger_kind='value'
         AND currency=v_sl.currency AND NOT is_closed;
      IF v_tax_acct IS NULL THEN
        RAISE EXCEPTION 'no open sales_tax_payable account for ccy=%',
                        v_sl.currency USING ERRCODE = 'P0010';
      END IF;
    END IF;

    INSERT INTO so_shipment_lines (
      shipment_id, so_line_id, qty_shipped, unit_cost, unit_price, tax_amount,
      cost_method_at_ship
    ) VALUES (
      v_doc_id, v_so_line_id, v_qty_shipped, v_unit_cost, v_unit_price, v_tax_amount,
      v_cost_method
    ) RETURNING id INTO v_ship_line_id;

    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'so_ship',
      'document_kind',     'so_shipment',
      'document_id',       v_doc_id,
      'document_line_id',  v_ship_line_id,
      'debit_account_id',  v_cust_qty,
      'credit_account_id', v_qty_acct,
      'amount',            v_qty_shipped,
      'qty',               v_qty_shipped,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   v_customer_id,
      'posted_by',         p_posted_by
    ));

    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'so_ship',
      'document_kind',     'so_shipment',
      'document_id',       v_doc_id,
      'document_line_id',  v_ship_line_id,
      'debit_account_id',  v_cogs_acct,
      'credit_account_id', v_val_acct,
      'amount',            v_qty_shipped * v_unit_cost,
      'qty',               v_qty_shipped,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   v_customer_id,
      'posted_by',         p_posted_by
    ));

    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'so_ship',
      'document_kind',     'so_shipment',
      'document_id',       v_doc_id,
      'document_line_id',  v_ship_line_id,
      'debit_account_id',  v_cust_unsettled,
      'credit_account_id', v_revenue_acct,
      'amount',            v_qty_shipped * v_unit_price,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   v_customer_id,
      'posted_by',         p_posted_by
    ));

    IF v_tax_amount > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'so_ship',
        'document_kind',     'so_shipment',
        'document_id',       v_doc_id,
        'document_line_id',  v_ship_line_id,
        'debit_account_id',  v_cust_unsettled,
        'credit_account_id', v_tax_acct,
        'amount',            v_tax_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   v_customer_id,
        'posted_by',         p_posted_by
      ));
    END IF;
  END LOOP;

  UPDATE inventory_reservations
     SET status = 'shipped',
         resolved_at = clock_timestamp()
   WHERE so_id = p_so_id
     AND status IN ('active', 'allocated');

  PERFORM post_transfers(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;

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
  IF v_existing_id IS NOT NULL THEN
    RETURN v_existing_id;
  END IF;

  IF p_new_cost < 0 THEN
    RAISE EXCEPTION 'p_new_cost must be >= 0 (got %)', p_new_cost
      USING ERRCODE = '23514';
  END IF;

  SELECT cost_method INTO v_cost_method
    FROM skus WHERE id = p_sku_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'sku % not found', p_sku_id USING ERRCODE = 'P0010';
  END IF;

  CASE v_cost_method
  WHEN 'standard' THEN
    NULL;
  WHEN 'wac_perpetual' THEN
    RAISE EXCEPTION
      'standard_cost_roll not applicable to wac_perpetual SKU % — use '
      'post_cost_adjustment for WAC pools (Epic D / acct-14m)',
      p_sku_id USING ERRCODE = 'P0011';
  WHEN 'wac_periodic' THEN
    RAISE EXCEPTION
      'standard_cost_roll not applicable to wac_periodic SKU % — use '
      'post_cost_adjustment for WAC pools (Epic D / acct-14m)',
      p_sku_id USING ERRCODE = 'P0011';
  WHEN 'wac_retroactive' THEN
    RAISE EXCEPTION
      'standard_cost_roll not applicable to wac_retroactive SKU % — use '
      'post_cost_adjustment for WAC pools (Epic D / acct-14m)',
      p_sku_id USING ERRCODE = 'P0011';
  WHEN 'fifo', 'lot' THEN
    RAISE EXCEPTION
      'cost_method_not_implemented: standard_cost_roll on % SKU %; see acct-8gg',
      v_cost_method, p_sku_id USING ERRCODE = 'P0006';
  ELSE
    RAISE EXCEPTION 'unknown cost_method % for sku=%', v_cost_method, p_sku_id
      USING ERRCODE = 'P0011';
  END CASE;

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
    v_prior := resolve_standard_cost_at(p_sku_id, p_business_date);
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
        p_sku_id, v_wip_count
        USING ERRCODE = 'P0006';
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
    IF p_revalue_wip THEN
      SELECT array_agg(id ORDER BY id) INTO v_lock_ids
        FROM accounts
       WHERE kind IN ('inv_value_raw', 'inv_value_fg', 'inv_value_wip')
         AND sku_id = p_sku_id
         AND NOT is_closed;
    ELSE
      SELECT array_agg(id ORDER BY id) INTO v_lock_ids
        FROM accounts
       WHERE kind IN ('inv_value_raw', 'inv_value_fg')
         AND sku_id = p_sku_id
         AND NOT is_closed;
    END IF;

    IF v_lock_ids IS NOT NULL THEN
      FOREACH v_lock_id IN ARRAY v_lock_ids LOOP
        PERFORM 1 FROM accounts WHERE id = v_lock_id FOR UPDATE;
      END LOOP;
    END IF;

    FOR v_pool_record IN
      SELECT v.id          AS val_acct,
             v.currency    AS currency,
             v.location_id AS location_id
        FROM accounts v
       WHERE v.kind IN ('inv_value_raw', 'inv_value_fg')
         AND v.sku_id = p_sku_id
         AND NOT v.is_closed
       ORDER BY v.id
    LOOP
      SELECT COALESCE(SUM(
        CASE
          WHEN t.debit_account_id  = v_pool_record.val_acct THEN  t.qty
          WHEN t.credit_account_id = v_pool_record.val_acct THEN -t.qty
        END
      ), 0) INTO v_pool_qty
        FROM transfers t
       WHERE v_pool_record.val_acct IN (t.debit_account_id, t.credit_account_id)
         AND t.qty IS NOT NULL;

      IF v_pool_qty IS NULL OR v_pool_qty = 0 THEN
        CONTINUE;
      END IF;

      v_delta := v_pool_qty * (p_new_cost - v_prior);
      IF v_delta = 0 THEN
        CONTINUE;
      END IF;

      v_total_qty   := v_total_qty + v_pool_qty;
      v_total_delta := v_total_delta + v_delta;

      SELECT id INTO v_var_acct
        FROM accounts
       WHERE kind = 'variance_std_cost_roll'
         AND ledger_kind = 'value'
         AND currency = v_pool_record.currency
         AND NOT is_closed;
      IF v_var_acct IS NULL THEN
        RAISE EXCEPTION
          'no variance_std_cost_roll(value, ccy=%) account configured',
          v_pool_record.currency
          USING ERRCODE = 'P0010';
      END IF;

      IF v_delta > 0 THEN
        v_debit  := v_pool_record.val_acct;
        v_credit := v_var_acct;
        v_amount := v_delta;
      ELSE
        v_debit  := v_var_acct;
        v_credit := v_pool_record.val_acct;
        v_amount := -v_delta;
      END IF;

      v_batch := v_batch || jsonb_build_array(
        jsonb_build_object(
          'reason',            'standard_cost_roll',
          'document_kind',     'inventory_standard_cost_roll',
          'document_id',       NULL,
          'debit_account_id',  v_debit,
          'credit_account_id', v_credit,
          'amount',            v_amount,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        )
      );
    END LOOP;

    IF p_revalue_wip THEN
      FOR v_wip_record IN
        SELECT v.id           AS val_acct,
               v.currency     AS currency,
               v.routing_op   AS routing_op,
               s.id           AS qty_acct
          FROM accounts v
          JOIN accounts s ON s.kind        = 'stock_wip'
                         AND s.sku_id      = v.sku_id
                         AND s.routing_op  = v.routing_op
                         AND NOT s.is_closed
         WHERE v.kind = 'inv_value_wip'
           AND v.sku_id = p_sku_id
           AND NOT v.is_closed
         ORDER BY v.id
      LOOP
        SELECT debits_total - credits_total
          INTO v_pool_qty
          FROM accounts
         WHERE id = v_wip_record.qty_acct;

        IF v_pool_qty IS NULL OR v_pool_qty = 0 THEN
          CONTINUE;
        END IF;

        v_delta := v_pool_qty * (p_new_cost - v_prior);
        IF v_delta = 0 THEN
          CONTINUE;
        END IF;

        v_total_qty   := v_total_qty + v_pool_qty;
        v_total_delta := v_total_delta + v_delta;

        SELECT id INTO v_wip_var_acct
          FROM accounts
         WHERE kind = 'variance_wip_revaluation'
           AND ledger_kind = 'value'
           AND currency = v_wip_record.currency
           AND NOT is_closed;
        IF v_wip_var_acct IS NULL THEN
          RAISE EXCEPTION
            'no variance_wip_revaluation(value, ccy=%) account configured',
            v_wip_record.currency
            USING ERRCODE = 'P0010';
        END IF;

        IF v_delta > 0 THEN
          v_debit  := v_wip_record.val_acct;
          v_credit := v_wip_var_acct;
          v_amount := v_delta;
        ELSE
          v_debit  := v_wip_var_acct;
          v_credit := v_wip_record.val_acct;
          v_amount := -v_delta;
        END IF;

        v_batch := v_batch || jsonb_build_array(
          jsonb_build_object(
            'reason',            'standard_cost_roll',
            'document_kind',     'inventory_standard_cost_roll',
            'document_id',       NULL,
            'debit_account_id',  v_debit,
            'credit_account_id', v_credit,
            'amount',            v_amount,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'posted_by',         p_posted_by
          )
        );
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
    SELECT id INTO v_doc_id
      FROM inventory_standard_cost_rolls
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  IF jsonb_array_length(v_batch) > 0 THEN
    SELECT jsonb_agg(jsonb_set(ev, '{document_id}', to_jsonb(v_doc_id::TEXT)))
      INTO v_batch
      FROM jsonb_array_elements(v_batch) ev;
    PERFORM post_transfers(v_batch, FALSE);
  END IF;

  RETURN v_doc_id;
END;
$$;

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
        IF v_tolerance_pct = 0 THEN
          RAISE EXCEPTION
            'ap_bill_three_way_mismatch: line % unit_cost % does not match '
            'po_line.unit_cost %',
            v_idx, v_unit_cost, v_pl.unit_cost
            USING ERRCODE = 'P0024';
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

      v_amount_at_po := v_qty * v_pl.unit_cost;
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ap_bill',
        'document_kind',     'vendor_bill',
        'document_id',       v_doc_id,
        'document_line_id',  v_bill_line_id,
        'debit_account_id',  v_ven_unsettled,
        'credit_account_id', v_ven_ap,
        'amount',            v_amount_at_po,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_vendor_id,
        'posted_by',         p_posted_by
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
            'reason',            'ap_bill',
            'document_kind',     'vendor_bill',
            'document_id',       v_doc_id,
            'document_line_id',  v_bill_line_id,
            'debit_account_id',  v_match_tol_acct,
            'credit_account_id', v_ven_ap,
            'amount',            v_diff_total,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_vendor_id,
            'posted_by',         p_posted_by
          ));
        ELSE
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'ap_bill',
            'document_kind',     'vendor_bill',
            'document_id',       v_doc_id,
            'document_line_id',  v_bill_line_id,
            'debit_account_id',  v_ven_ap,
            'credit_account_id', v_match_tol_acct,
            'amount',            -v_diff_total,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_vendor_id,
            'posted_by',         p_posted_by
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
BEGIN
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

  PERFORM post_transfers(v_batch, FALSE);
  RETURN v_doc_id;
END;
$$;
