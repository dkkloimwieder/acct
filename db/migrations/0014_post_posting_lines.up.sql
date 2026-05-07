-- Core ledger dispatcher and helpers.
--
-- Consolidates archive migs:
--   0021 (acct-uxu)  — _post_transfers_lookup_qty_account
--   0033 (acct-7mg)  — _post_transfers_lock_pre_scan + apply_event extraction
--   0067 (acct-7py)  — credit-first SKU resolution + rm_issue_to_wo flagging
--   0081 (acct-th7)  — Slice C: so_ship non-SKU value-leg passthrough
--   0094 (acct-w0lo) — _post_transfers_compute_amount registry-shell
--   0104 (acct-wb75.1.1) — B1 posting_line_sources extension write
--
-- Naming unifications baked in:
--   transfers → posting_lines (table)
--   transfer_reason → posting_line_reason (enum)
--   transfers_provisional → posting_lines_provisional (extension)
--   transfer_line_sources → posting_line_sources
--   transfer_id → posting_line_id (FK on extension tables)
--   reverses_transfer_id → reverses_posting_line_id
--   _post_transfers_* → _post_posting_lines_*
--   post_transfers → post_posting_lines

-- ============================================================
-- Helper: resolve matching qty-side account for a value-side account.
-- ============================================================

CREATE OR REPLACE FUNCTION _post_posting_lines_lookup_qty_account(
  p_value_acct accounts
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_id BIGINT;
BEGIN
  IF p_value_acct.ledger_kind <> 'value' OR p_value_acct.sku_id IS NULL THEN
    RETURN NULL;
  END IF;

  CASE p_value_acct.kind
    WHEN 'inv_value_raw', 'inv_value_fg' THEN
      IF p_value_acct.location_id IS NULL THEN
        RETURN NULL;
      END IF;
      SELECT id INTO v_id FROM accounts
        WHERE kind        = 'stock_available'
          AND sku_id      = p_value_acct.sku_id
          AND location_id = p_value_acct.location_id
          AND NOT is_closed;
      RETURN v_id;
    WHEN 'inv_value_wip' THEN
      IF p_value_acct.routing_op IS NULL THEN
        RETURN NULL;
      END IF;
      SELECT id INTO v_id FROM accounts
        WHERE kind       = 'stock_wip'
          AND sku_id     = p_value_acct.sku_id
          AND routing_op = p_value_acct.routing_op
          AND NOT is_closed;
      RETURN v_id;
    ELSE
      RETURN NULL;
  END CASE;
END;
$$;

COMMENT ON FUNCTION _post_posting_lines_lookup_qty_account(accounts) IS
  'Maps a value-side account to its matching qty-side account. Returns '
  'NULL if no clean match. Used by the WAC lock pre-scan to ensure aux '
  'qty pools are locked before depletion math reads them.';

-- ============================================================
-- Helper: lock pre-scan.
-- Acquires FOR UPDATE on every account referenced by the event batch
-- (debit + credit) plus aux qty accounts that WAC cost computation
-- needs to read. Empty array for non-WAC batches.
-- ============================================================

CREATE OR REPLACE FUNCTION _post_posting_lines_lock_pre_scan(
  p_events       JSONB,
  p_aux_qty_ids  BIGINT[]
) RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM 1 FROM accounts
   WHERE id IN (
     SELECT (ev->>'debit_account_id')::BIGINT
       FROM jsonb_array_elements(p_events) ev
     UNION
     SELECT (ev->>'credit_account_id')::BIGINT
       FROM jsonb_array_elements(p_events) ev
     UNION
     SELECT unnest(p_aux_qty_ids))
   ORDER BY id FOR UPDATE;
END;
$$;

-- ============================================================
-- Registry-dispatched cost amount for the outbound depletion path.
--
-- Looks up cost_method_strategies(cost_method, 'outbound') from the
-- registry seeded in 0013 and EXECUTEs the registered per-strategy
-- function. P0006 if no strategy is registered (FIFO + lot fall
-- through here pending acct-8gg).
--
-- Pre-strategy work (qty NULL gate + credit-first SKU resolution) is
-- centralized here so per-strategy functions can assume those
-- invariants hold (R2 — credit-first SKU resolution).
-- ============================================================

CREATE OR REPLACE FUNCTION _post_posting_lines_compute_amount(
  p_event        JSONB,
  p_d_acct       accounts,
  p_c_acct       accounts,
  p_cost_method  cost_method,
  p_idx          INT
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_qty       BIGINT;
  v_sku       UUID;
  v_fn_name   TEXT;
  v_amount    BIGINT;
BEGIN
  v_qty := (p_event->>'qty')::BIGINT;
  IF v_qty IS NULL THEN
    RAISE EXCEPTION 'cost_method_not_implemented: cost-relevant value event missing qty at event index %',
                    p_idx
      USING ERRCODE = 'P0006';
  END IF;

  -- R2: credit-first SKU resolution.
  v_sku := COALESCE(p_c_acct.sku_id, p_d_acct.sku_id);
  IF v_sku IS NULL THEN
    RAISE EXCEPTION 'cost_method_not_implemented: sku not resolvable in compute_amount at event index %',
                    p_idx
      USING ERRCODE = 'P0006';
  END IF;

  SELECT compute_fn_name
    INTO v_fn_name
    FROM cost_method_strategies
   WHERE cost_method = p_cost_method
     AND event_kind  = 'outbound';

  IF v_fn_name IS NULL THEN
    RAISE EXCEPTION 'cost_method_not_implemented: no strategy registered for cost_method=% event_kind=outbound at event index % (see acct-8gg for FIFO/lot)',
                    p_cost_method, p_idx
      USING ERRCODE = 'P0006';
  END IF;

  EXECUTE format('SELECT %I($1, $2, $3, $4)', v_fn_name)
    INTO v_amount
    USING p_event, p_d_acct, p_c_acct, p_idx;

  RETURN v_amount;
END;
$$;

-- ============================================================
-- Single-event apply step.
--
-- Validates account state (P0001 closed, P0002 ledger_mismatch, P0003
-- currency_mismatch); resolves and gates period (P0004 missing, P0005
-- closed unless override); resolves qty_for_row from the source rule;
-- UPDATEs balances; INSERTs the posting_line row; optionally INSERTs
-- a posting_lines_provisional row for wac_periodic / wac_retroactive
-- depletions; optionally INSERTs a posting_line_sources row when the
-- caller passed any of the four B1 extension fields.
--
-- Provisional flagging:
--   - Cost-event reasons: canonical (op_move/scrap/wo_complete/so_ship),
--     BOM2 *_v variants (op_move_v/scrap_v/wo_complete_v), and
--     rm_issue_to_wo (acct-7py).
--   - SKU resolution is credit-first (depletion source). Falls back to
--     debit when credit has NULL sku_id.
--
-- Returns the new posting_line id.
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
  v_period_id      BIGINT;
  v_period_closed  TIMESTAMPTZ;
  v_business_date  DATE;
  v_qty_for_row    BIGINT;
  v_reason         posting_line_reason;
  v_idem_key       UUID;
  v_new_id         BIGINT;
  v_event_qty      BIGINT;
  v_resolved_cm    cost_method;
  v_cost_sku       UUID;
  v_reverses_id    BIGINT;
  v_parent_doc     UUID;
  v_ic_pair        UUID;
  v_proc           VARCHAR;
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

  -- qty_for_row source rule:
  --   1. caller-asserted event.qty (cost-relevant value-leg events
  --      always supply this);
  --   2. else qty-leg fallback (both sides ledger_kind='qty' → qty == amount);
  --   3. else NULL (cash, AR, AP, FX).
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
  -- Cost-event reasons: canonical four (op_move/scrap/wo_complete/
  -- so_ship), BOM2 *_v variants (op_move_v/scrap_v/wo_complete_v),
  -- and rm_issue_to_wo. Resolves cost_method on-demand when caller
  -- passes NULL (every reason except canonical four bypasses the
  -- dispatcher's reason list).
  --
  -- R2: credit-first SKU resolution. Credit account is the depletion
  -- source whose cost_method drives flagging.
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

  -- B1 extension write. Insert posting_line_sources only when caller
  -- passed any of the four new fields. Pure-NULL extension rows are
  -- forbidden by CHECK on the table.
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

  RETURN v_new_id;
END;
$$;

-- ============================================================
-- post_posting_lines: append-only ledger primitive.
--
-- Validates a JSONB array of events, applies each as one row in
-- posting_lines + paired UPDATE on accounts.debits_total / credits_total,
-- returns per-event status. Single-pass for non-WAC batches; two-pass
-- for batches that touch a WAC SKU (pre-compute amounts so depletions
-- see a consistent pre-application pool average).
--
-- SKU resolution: credit-first. Cost-event value legs without
-- resolvable SKU (so_ship revenue / tax legs through ar_unsettled /
-- revenue / sales_tax_payable, none of which carry SKU) pass through
-- with caller-supplied amount (per Slice C / acct-th7). Other
-- cost-event reasons (op_move / scrap / wo_complete) MUST resolve an
-- SKU.
-- ============================================================

CREATE OR REPLACE FUNCTION post_posting_lines(
  p_events                 JSONB,
  p_override_closed_period BOOLEAN DEFAULT FALSE
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
  v_results        JSONB := '[]'::JSONB;
  v_n              INT;
  v_idx            INT;
  v_event          JSONB;
  v_d_acct         accounts%ROWTYPE;
  v_c_acct         accounts%ROWTYPE;
  v_d_id           BIGINT;
  v_c_id           BIGINT;
  v_idem_key       UUID;
  v_reason         posting_line_reason;
  v_cost_sku       UUID;
  v_cost_method    cost_method;
  v_amount         BIGINT;
  v_amounts        BIGINT[];
  v_aux_qty_id     BIGINT;
  v_aux_qty_ids    BIGINT[] := '{}';
  v_has_wac        BOOLEAN  := FALSE;
  v_has_cost_event BOOLEAN;
BEGIN
  v_n := jsonb_array_length(p_events);
  IF v_n = 0 THEN RETURN '[]'::JSONB; END IF;

  v_has_cost_event := EXISTS (
    SELECT 1 FROM jsonb_array_elements(p_events) ev
     WHERE (ev->>'reason')::posting_line_reason
           IN ('op_move','scrap','wo_complete','so_ship')
  );

  IF v_has_cost_event THEN
    FOR v_idx IN 1..v_n LOOP
      v_event  := p_events -> (v_idx - 1);
      v_reason := (v_event->>'reason')::posting_line_reason;
      IF v_reason NOT IN ('op_move','scrap','wo_complete','so_ship') THEN
        CONTINUE;
      END IF;
      v_c_id := (v_event->>'credit_account_id')::BIGINT;
      SELECT * INTO v_c_acct FROM accounts WHERE id = v_c_id;
      IF v_c_acct.ledger_kind <> 'value' THEN CONTINUE; END IF;
      v_cost_sku := v_c_acct.sku_id;
      IF v_cost_sku IS NULL THEN
        v_d_id := (v_event->>'debit_account_id')::BIGINT;
        SELECT * INTO v_d_acct FROM accounts WHERE id = v_d_id;
        v_cost_sku := v_d_acct.sku_id;
      END IF;
      IF v_cost_sku IS NULL THEN CONTINUE; END IF;
      SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_cost_sku;
      IF v_cost_method IN ('wac_perpetual', 'wac_periodic', 'wac_retroactive') THEN
        v_has_wac := TRUE;
        v_aux_qty_id := _post_posting_lines_lookup_qty_account(v_c_acct);
        IF v_aux_qty_id IS NOT NULL THEN
          v_aux_qty_ids := array_append(v_aux_qty_ids, v_aux_qty_id);
        END IF;
      END IF;
    END LOOP;
  END IF;

  PERFORM _post_posting_lines_lock_pre_scan(p_events, v_aux_qty_ids);

  IF NOT v_has_wac THEN
    -- Single-pass for non-WAC batches.
    FOR v_idx IN 1..v_n LOOP
      v_event    := p_events -> (v_idx - 1);
      v_idem_key := (v_event->>'idempotency_key')::UUID;
      IF EXISTS (SELECT 1 FROM posting_lines WHERE idempotency_key = v_idem_key) THEN
        v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'exists');
        CONTINUE;
      END IF;
      v_d_id := (v_event->>'debit_account_id')::BIGINT;
      v_c_id := (v_event->>'credit_account_id')::BIGINT;
      v_reason := (v_event->>'reason')::posting_line_reason;
      v_cost_method := NULL;
      SELECT * INTO v_d_acct FROM accounts WHERE id = v_d_id;
      SELECT * INTO v_c_acct FROM accounts WHERE id = v_c_id;
      IF v_reason IN ('op_move','scrap','wo_complete','so_ship') THEN
        v_cost_sku := COALESCE(v_c_acct.sku_id, v_d_acct.sku_id);
        IF v_cost_sku IS NULL THEN
          -- so_ship's revenue / tax legs through ar_unsettled / revenue
          -- / sales_tax_payable carry no SKU on either side. Caller-
          -- supplied amount stands. Other cost-event reasons MUST
          -- resolve an SKU because they're inventory ops.
          IF v_reason = 'so_ship' AND v_d_acct.ledger_kind = 'value' THEN
            v_amount := (v_event->>'amount')::BIGINT;
          ELSE
            RAISE EXCEPTION
              'cost_method_not_implemented: sku not resolvable for reason % at event index %',
              v_reason, v_idx USING ERRCODE = 'P0006';
          END IF;
        ELSE
          SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_cost_sku;
          IF v_d_acct.ledger_kind = 'value' THEN
            v_amount := _post_posting_lines_compute_amount(
              v_event, v_d_acct, v_c_acct, v_cost_method, v_idx
            );
          ELSE
            IF v_cost_method NOT IN
               ('standard', 'wac_perpetual', 'wac_periodic', 'wac_retroactive') THEN
              RAISE EXCEPTION
                'cost_method_not_implemented: % for reason % at event index %',
                v_cost_method, v_reason, v_idx USING ERRCODE = 'P0006';
            END IF;
            v_amount := (v_event->>'amount')::BIGINT;
          END IF;
        END IF;
      ELSE
        v_amount := (v_event->>'amount')::BIGINT;
      END IF;
      PERFORM _post_posting_lines_apply_event(
        v_event, v_idx, v_amount, v_d_acct, v_c_acct,
        v_cost_method, p_override_closed_period
      );
      v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'ok');
    END LOOP;
    RETURN v_results;
  END IF;

  -- Two-pass for WAC batches: pass-1 compute amounts; pass-2 apply.
  v_amounts := array_fill(NULL::BIGINT, ARRAY[v_n]);
  FOR v_idx IN 1..v_n LOOP
    v_event  := p_events -> (v_idx - 1);
    v_reason := (v_event->>'reason')::posting_line_reason;
    IF v_reason NOT IN ('op_move','scrap','wo_complete','so_ship') THEN
      v_amounts[v_idx] := (v_event->>'amount')::BIGINT;
      CONTINUE;
    END IF;
    v_idem_key := (v_event->>'idempotency_key')::UUID;
    IF EXISTS (SELECT 1 FROM posting_lines WHERE idempotency_key = v_idem_key) THEN
      CONTINUE;
    END IF;
    v_d_id := (v_event->>'debit_account_id')::BIGINT;
    v_c_id := (v_event->>'credit_account_id')::BIGINT;
    SELECT * INTO v_d_acct FROM accounts WHERE id = v_d_id;
    SELECT * INTO v_c_acct FROM accounts WHERE id = v_c_id;
    v_cost_sku := COALESCE(v_c_acct.sku_id, v_d_acct.sku_id);
    IF v_cost_sku IS NULL THEN
      IF v_reason = 'so_ship' AND v_d_acct.ledger_kind = 'value' THEN
        v_amounts[v_idx] := (v_event->>'amount')::BIGINT;
      ELSE
        RAISE EXCEPTION
          'cost_method_not_implemented: sku not resolvable for reason % at event index %',
          v_reason, v_idx USING ERRCODE = 'P0006';
      END IF;
    ELSE
      SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_cost_sku;
      IF v_d_acct.ledger_kind = 'value' THEN
        v_amounts[v_idx] := _post_posting_lines_compute_amount(
          v_event, v_d_acct, v_c_acct, v_cost_method, v_idx
        );
      ELSE
        IF v_cost_method NOT IN
           ('standard', 'wac_perpetual', 'wac_periodic', 'wac_retroactive') THEN
          RAISE EXCEPTION
            'cost_method_not_implemented: % for reason % at event index %',
            v_cost_method, v_reason, v_idx USING ERRCODE = 'P0006';
        END IF;
        v_amounts[v_idx] := (v_event->>'amount')::BIGINT;
      END IF;
    END IF;
  END LOOP;

  FOR v_idx IN 1..v_n LOOP
    v_event    := p_events -> (v_idx - 1);
    v_idem_key := (v_event->>'idempotency_key')::UUID;
    IF EXISTS (SELECT 1 FROM posting_lines WHERE idempotency_key = v_idem_key) THEN
      v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'exists');
      CONTINUE;
    END IF;
    v_d_id := (v_event->>'debit_account_id')::BIGINT;
    v_c_id := (v_event->>'credit_account_id')::BIGINT;
    v_reason := (v_event->>'reason')::posting_line_reason;
    v_amount := v_amounts[v_idx];
    v_cost_method := NULL;
    SELECT * INTO v_d_acct FROM accounts WHERE id = v_d_id;
    SELECT * INTO v_c_acct FROM accounts WHERE id = v_c_id;
    IF v_reason IN ('op_move','scrap','wo_complete','so_ship') THEN
      v_cost_sku := COALESCE(v_c_acct.sku_id, v_d_acct.sku_id);
      IF v_cost_sku IS NOT NULL THEN
        SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_cost_sku;
      END IF;
    END IF;
    PERFORM _post_posting_lines_apply_event(
      v_event, v_idx, v_amount, v_d_acct, v_c_acct,
      v_cost_method, p_override_closed_period
    );
    v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'ok');
  END LOOP;
  RETURN v_results;
END;
$$;

COMMENT ON FUNCTION post_posting_lines(JSONB, BOOLEAN) IS
  'Append-only ledger primitive. Validates a JSONB array of events, '
  'applies each as one row in posting_lines + paired UPDATE on '
  'accounts.debits_total/credits_total. Single-pass for non-WAC '
  'batches; two-pass for batches that touch a WAC SKU. Apply step '
  'extracted to _post_posting_lines_apply_event. SKU resolution is '
  'credit-first across the family. Cost-event value legs without '
  'resolvable SKU (so_ship revenue / tax legs) pass through with '
  'caller-supplied amount.';
