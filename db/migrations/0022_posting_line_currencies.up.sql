-- Phase B2 of the posting-lines convergence plan
-- (research/posting-lines-convergence-plan.md §4.B.B2; acct-wb75.1.2).
--
-- Extension table for posting_lines that records the original
-- transactional currency + the fx rate to the legal entity's functional
-- currency at business_date. legal_entities.functional_currency is
-- already a column on legal_entities (consolidated 0005); this mig
-- only adds the extension table + extends the dispatcher to write it.
--
-- B2 invariant (posted-by-construction): amount_transaction =
-- posting_lines.amount. posting_lines.amount remains in the account's
-- transaction currency at this phase — fx_rate_to_functional is
-- informational metadata, recording the rate AT business_date for
-- future-state translation work (Phase D / acct-3gzh multi-entity).
-- The recon does NOT yet enforce amount_transaction × fx_rate ≈ amount;
-- that invariant only kicks in once a future migration translates
-- posting_lines.amount to functional currency.
--
-- Dispatcher behavior:
--   - Skip when ledger_kind != 'value' (qty legs have NULL currency).
--   - Resolve transaction currency from credit account (R2: credit-side
--     depletion source governs; both sides equal anyway by P0003).
--   - Resolve functional currency from legal_entities row of credit
--     account's legal_entity_id.
--   - If equal: skip extension (most postings on the USD-functional
--     baseline get no row).
--   - Else: look up fx_rate from fx_rates effective at business_date;
--     INSERT one extension row. Missing fx rate raises P0050 (reused
--     from 0016/0018 fx_clearing path).
--
-- Backfill: existing posting_lines with non-functional value-leg
-- currency get one extension row each with fx_rate_to_functional = 1.
-- Per plan §4.B.B2: pre-B2 amounts were already in transaction
-- currency with no functional translation, so fx_rate=1 preserves the
-- amount_transaction = amount invariant trivially. The actual market
-- rate is unknown for retro-activated postings; treating them as
-- already-functional is the documented compromise.
--
-- Forward references: this mig is the first to write to this table.
-- Future Phase D / multi-entity migs may extend the body further.

CREATE TABLE posting_line_currencies (
  posting_line_id       BIGINT PRIMARY KEY REFERENCES posting_lines(id),
  amount_transaction    BIGINT NOT NULL,
  currency_transaction  CHAR(3) NOT NULL,
  fx_rate_to_functional NUMERIC(20, 10) NOT NULL,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

  CONSTRAINT posting_line_currencies_fx_rate_positive CHECK (
    fx_rate_to_functional > 0
  )
);

CREATE INDEX posting_line_currencies_by_currency
  ON posting_line_currencies (currency_transaction);

COMMENT ON TABLE posting_line_currencies IS
  'Phase B2 extension table for posting_lines. Records the transactional '
  'currency + fx rate to functional currency at business_date when the '
  'transaction currency differs from the credit account''s legal entity''s '
  'functional currency. Most postings on the single-entity USD-functional '
  'baseline have no extension row.';

-- ============================================================
-- _post_posting_lines_apply_event: extended with B2 extension write.
--
-- Body identical to 0014's version through the B1 extension block,
-- with a new B2 block appended before RETURN.
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

  -- B2 extension write. Insert posting_line_currencies only when the
  -- transaction currency differs from the credit account's legal
  -- entity's functional currency. R2: credit-side governs (P0003 made
  -- the two account currencies equal anyway; legal_entity_id may
  -- differ in cross-entity scenarios, future acct-w1v3).
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

  RETURN v_new_id;
END;
$$;

-- ============================================================
-- Backfill: existing posting_lines whose value-leg credit currency
-- differs from their legal entity's functional currency. fx_rate=1
-- per plan §4.B.B2 (pre-B2 amounts were already in transaction
-- currency with no functional translation; treating them as
-- already-functional preserves amount_transaction = amount trivially).
-- ON CONFLICT DO NOTHING idempotency in case the mig is replayed.
-- ============================================================

INSERT INTO posting_line_currencies (
  posting_line_id, amount_transaction, currency_transaction,
  fx_rate_to_functional
)
SELECT pl.id, pl.amount, ca.currency, 1
  FROM posting_lines pl
  JOIN accounts ca       ON ca.id = pl.credit_account_id
  JOIN legal_entities le ON le.id = ca.legal_entity_id
 WHERE ca.ledger_kind = 'value'
   AND ca.currency <> le.functional_currency
ON CONFLICT (posting_line_id) DO NOTHING;

-- ============================================================
-- Extend run_daily_reconciliation with a third check: B2 currency
-- extension consistency. Every posting_line_currencies row's
-- amount_transaction must equal its paired posting_lines.amount.
--
-- Note on the invariant. The plan §4.B.B2 states the invariant as
-- amount_transaction × fx_rate_to_functional ≈ posting_lines.amount,
-- but at this phase posting_lines.amount stays in transaction
-- currency (no functional translation has happened), so the literal
-- multiplication invariant only holds when fx_rate=1. We instead
-- check the deterministic form: amount_transaction = amount. This
-- catches direct-INSERT drift bypassing the dispatcher; the
-- multiplication form will become meaningful once a future migration
-- (Phase D / acct-3gzh) translates posting_lines.amount to
-- functional currency at posting time.
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

  RETURN v_total;
END;
$$;
