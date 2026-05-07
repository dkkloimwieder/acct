-- acct-3xcg — realized FX gain/loss on cross-currency cash settlement.
--
-- Slice C's post_ar_payment (mig 0081) and post_ap_payment (mig 0082)
-- both reject cash currency ≠ counterparty currency: the single-event
-- post requires both legs in p_currency, so a customer paying USD
-- against an EUR invoice trips P0010 (no cash account for ccy=EUR
-- when caller passes 'USD' — or vice versa). This forces the
-- operator to convert externally and lose the FX gain/loss in the
-- ledger.
--
-- This migration extends both functions with optional p_cash_currency
-- + p_cash_amount parameters. When supplied (and ≠ p_currency), the
-- function emits TWO events to preserve the strict per-(ledger_kind,
-- currency) double-entry invariant (CLAUDE.md: B3 fix; never a
-- single global SUM):
--
--   Event A (counterparty currency): bridges the AR/AP settlement
--     through fx_clearing(counterparty_ccy). For AR: fx_clearing DR /
--     ar CR (settles the ar). For AP: ap DR / fx_clearing CR
--     (settles the ap).
--
--   Event B (cash currency): cash leg + FX recognition leg, bridged
--     through fx_clearing(cash_ccy). For AR: cash DR / fx_clearing
--     CR expected / realized_fx_gain CR delta (gain) or
--     realized_fx_loss DR -delta (loss). For AP: fx_clearing DR
--     expected / cash CR p_cash_amount + realized_fx_loss DR delta
--     (paid more than expected) or realized_fx_gain CR -delta (paid
--     less than expected).
--
-- fx_clearing is a balance-sheet bridge: it ends with paired
-- (counterparty_ccy DR, cash_ccy CR) entries that net to zero in
-- home currency at the rate prevailing at p_business_date. Per-
-- currency DE holds (each event balances within its currency).
-- Realized FX is recognized on Event B's cash side, in cash currency.
--
-- Math (cross-currency only):
--   v_rate     := fx_rate(p_currency → p_cash_currency, p_business_date)
--   v_expected := p_amount * v_rate
--   v_delta    := p_cash_amount - v_expected
--   AR: delta > 0 → realized_fx_gain; delta < 0 → realized_fx_loss
--   AP: delta > 0 → realized_fx_loss; delta < 0 → realized_fx_gain
--   delta == 0 → no FX leg posted (rounding-tight case)
--
-- Same-currency (p_cash_currency NULL or = p_currency): preserves
-- mig 0081/0082 behavior bit-for-bit (single 2-leg event, no FX).
--
-- LIMITATION (documented). Strictly speaking, "realized FX gain/loss"
-- in ASC 830 / IAS 21 is the difference between the ORIGINAL
-- accrual rate (when AR/AP was first booked) and the SETTLEMENT
-- rate. This implementation approximates with current-rate-vs-
-- caller-cash-amount: assumes the caller's p_cash_amount represents
-- the actual cash flow and the rate at p_business_date is the
-- current-period rate. The two coincide IF no intermediate fx_
-- revaluation has run between accrual and settlement, which is the
-- common case until acct-3dz2 ships home-currency reporting + period-
-- end revaluation. acct-3dz2 will refine this to a true original-
-- rate-vs-settlement-rate comparison; the function shape stays
-- compatible because realized_fx_gain / realized_fx_loss accounts
-- and Event A/B structure don't change.
--
-- New error: P0050 (missing_fx_rate at p_business_date).
--
-- Compliance anchors: ASC 830 + IAS 21 (realized FX is a separate
-- P&L line from unrealized; both required under GAAP/IFRS). SAP /
-- Oracle Cloud / D365 / NetSuite all model these as separate G/L
-- accounts and trigger them automatically at clearing.

-- ============================================================
-- 1. New account_kind values.
-- ============================================================

ALTER TYPE account_kind ADD VALUE IF NOT EXISTS 'realized_fx_gain';
ALTER TYPE account_kind ADD VALUE IF NOT EXISTS 'realized_fx_loss';
ALTER TYPE account_kind ADD VALUE IF NOT EXISTS 'fx_clearing';

-- ============================================================
-- 2. post_ar_payment — DROP+CREATE with cross-currency settlement.
-- ============================================================
--
-- DROP+CREATE because we're adding parameters at the end; the
-- pg-forbids-renaming-function-parameters memory applies to renames
-- but adding parameters with DEFAULT also benefits from the cleaner
-- DROP+CREATE shape (mirrors mig 0078's acct-bru pattern adding
-- p_revalue_wip).

DROP FUNCTION IF EXISTS post_ar_payment(
  UUID, CHAR, BIGINT, DATE, UUID, UUID, TEXT
);

CREATE OR REPLACE FUNCTION post_ar_payment(
  p_customer_id     UUID,
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
  v_existing_id    UUID;
  v_doc_id         UUID;
  v_customer_check UUID;
  v_cash_acct      BIGINT;
  v_cust_ar        BIGINT;
  v_fx_clr_ccy     BIGINT;       -- fx_clearing in counterparty currency
  v_fx_clr_cash    BIGINT;       -- fx_clearing in cash currency
  v_fx_gain_acct   BIGINT;
  v_fx_loss_acct   BIGINT;
  v_rate           NUMERIC(20, 10);
  v_expected       BIGINT;
  v_delta          BIGINT;
  v_cross_ccy      BOOLEAN;
  v_batch          JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM ar_payments
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  IF p_amount IS NULL OR p_amount <= 0 THEN
    RAISE EXCEPTION 'ar_payment_invalid: amount must be > 0 (got %)', p_amount
      USING ERRCODE = 'P0039';
  END IF;

  SELECT id INTO v_customer_check FROM customers WHERE id = p_customer_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'ar_payment_invalid: customer % not found', p_customer_id
      USING ERRCODE = 'P0039';
  END IF;

  -- Determine cross-currency mode.
  v_cross_ccy := p_cash_currency IS NOT NULL
             AND p_cash_currency <> p_currency;

  IF v_cross_ccy THEN
    IF p_cash_amount IS NULL OR p_cash_amount <= 0 THEN
      RAISE EXCEPTION
        'ar_payment_invalid: p_cash_amount required and > 0 when '
        'p_cash_currency (%) differs from p_currency (%)',
        p_cash_currency, p_currency
        USING ERRCODE = 'P0039';
    END IF;
  ELSE
    -- Same-currency mode. p_cash_amount, if supplied, must equal
    -- p_amount (caller may pass it for consistency).
    IF p_cash_amount IS NOT NULL AND p_cash_amount <> p_amount THEN
      RAISE EXCEPTION
        'ar_payment_invalid: same-currency settlement requires '
        'p_cash_amount (%) = p_amount (%) (or NULL)',
        p_cash_amount, p_amount
        USING ERRCODE = 'P0039';
    END IF;
  END IF;

  -- Resolve customer ar (counterparty currency).
  SELECT id INTO v_cust_ar FROM accounts
   WHERE kind='ar' AND counterparty_id=p_customer_id
     AND currency=p_currency AND NOT is_closed;
  IF v_cust_ar IS NULL THEN
    RAISE EXCEPTION 'no open ar account for customer=% ccy=%',
                    p_customer_id, p_currency USING ERRCODE = 'P0010';
  END IF;

  -- Resolve cash account in (effective) cash currency.
  SELECT id INTO v_cash_acct FROM accounts
   WHERE kind='cash' AND ledger_kind='value'
     AND currency=COALESCE(p_cash_currency, p_currency) AND NOT is_closed;
  IF v_cash_acct IS NULL THEN
    RAISE EXCEPTION 'no open cash account for ccy=%',
                    COALESCE(p_cash_currency, p_currency)
      USING ERRCODE = 'P0010';
  END IF;

  IF v_cross_ccy THEN
    -- Resolve fx_clearing in both currencies.
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

    -- Look up fx_rate (p_currency → p_cash_currency) effective at
    -- p_business_date. Highest effective_at <= business_date wins.
    SELECT rate INTO v_rate
      FROM fx_rates
     WHERE from_currency = p_currency
       AND to_currency   = p_cash_currency
       AND effective_at::DATE <= p_business_date
     ORDER BY effective_at DESC
     LIMIT 1;
    IF v_rate IS NULL THEN
      RAISE EXCEPTION
        'missing_fx_rate: no fx_rates row found for % → % effective_at <= %',
        p_currency, p_cash_currency, p_business_date
        USING ERRCODE = 'P0050';
    END IF;

    -- Integer truncation: BIGINT cents × NUMERIC rate, FLOOR to BIGINT.
    v_expected := (p_amount::NUMERIC * v_rate)::BIGINT;
    v_delta    := p_cash_amount - v_expected;

    -- Resolve FX P&L accounts (only for non-zero delta).
    IF v_delta > 0 THEN
      SELECT id INTO v_fx_gain_acct FROM accounts
       WHERE kind='realized_fx_gain' AND ledger_kind='value'
         AND currency=p_cash_currency AND NOT is_closed;
      IF v_fx_gain_acct IS NULL THEN
        RAISE EXCEPTION 'no open realized_fx_gain account for ccy=%',
                        p_cash_currency USING ERRCODE = 'P0010';
      END IF;
    ELSIF v_delta < 0 THEN
      SELECT id INTO v_fx_loss_acct FROM accounts
       WHERE kind='realized_fx_loss' AND ledger_kind='value'
         AND currency=p_cash_currency AND NOT is_closed;
      IF v_fx_loss_acct IS NULL THEN
        RAISE EXCEPTION 'no open realized_fx_loss account for ccy=%',
                        p_cash_currency USING ERRCODE = 'P0010';
      END IF;
    END IF;
  END IF;

  INSERT INTO ar_payments (
    customer_id, currency, amount, business_date, posted_by,
    idempotency_key, notes
  ) VALUES (
    p_customer_id, p_currency, p_amount, p_business_date, p_posted_by,
    p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM ar_payments
     WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  IF v_cross_ccy THEN
    -- Event A (counterparty currency): fx_clearing DR p_amount / ar
    -- CR p_amount. Settles AR debt; bridges to cash currency via
    -- fx_clearing(p_currency).
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'ar_payment',
      'document_kind',     'ar_payment',
      'document_id',       v_doc_id,
      'debit_account_id',  v_fx_clr_ccy,
      'credit_account_id', v_cust_ar,
      'amount',            p_amount,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   p_customer_id,
      'posted_by',         p_posted_by
    ));

    -- Event B Part 1 (cash currency): cash DR p_cash_amount /
    -- fx_clearing CR p_cash_amount. Cash leg always reflects actual
    -- cash flow (p_cash_amount, what the customer paid).
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'ar_payment',
      'document_kind',     'ar_payment',
      'document_id',       v_doc_id,
      'debit_account_id',  v_cash_acct,
      'credit_account_id', v_fx_clr_cash,
      'amount',            p_cash_amount,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   p_customer_id,
      'posted_by',         p_posted_by
    ));

    -- Event B Part 2: reconcile fx_clearing(cash_ccy) net to
    -- v_expected (paired against fx_clearing(p_currency)'s p_amount
    -- DR — the two clearing legs net to zero in home currency at the
    -- p_business_date rate). Realized FX gain/loss recognizes the
    -- difference in cash currency.
    --
    --   delta > 0 (gain — customer paid more than rate-implied):
    --     fx_clearing DR v_delta / gain CR v_delta.
    --     Reduces fx_clearing CR balance from p_cash_amount to
    --     v_expected; recognizes v_delta as gain.
    --
    --   delta < 0 (loss — customer paid less):
    --     loss DR -v_delta / fx_clearing CR -v_delta.
    --     Increases fx_clearing CR balance from p_cash_amount to
    --     v_expected (since -v_delta > 0); recognizes -v_delta as loss.
    --
    --   delta = 0: skip (no FX recognition needed).
    IF v_delta > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ar_payment',
        'document_kind',     'ar_payment',
        'document_id',       v_doc_id,
        'debit_account_id',  v_fx_clr_cash,
        'credit_account_id', v_fx_gain_acct,
        'amount',            v_delta,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_customer_id,
        'posted_by',         p_posted_by
      ));
    ELSIF v_delta < 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ar_payment',
        'document_kind',     'ar_payment',
        'document_id',       v_doc_id,
        'debit_account_id',  v_fx_loss_acct,
        'credit_account_id', v_fx_clr_cash,
        'amount',            -v_delta,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_customer_id,
        'posted_by',         p_posted_by
      ));
    END IF;
  ELSE
    -- Same-currency: original mig 0081 single-event shape.
    v_batch := jsonb_build_array(jsonb_build_object(
      'reason',            'ar_payment',
      'document_kind',     'ar_payment',
      'document_id',       v_doc_id,
      'debit_account_id',  v_cash_acct,
      'credit_account_id', v_cust_ar,
      'amount',            p_amount,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   p_customer_id,
      'posted_by',         p_posted_by
    ));
  END IF;

  PERFORM post_transfers(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;

COMMENT ON FUNCTION post_ar_payment(UUID, CHAR, BIGINT, DATE, UUID, UUID, TEXT, CHAR, BIGINT) IS
  'AR payment with optional cross-currency settlement (acct-3xcg). '
  'Same-currency: cash DR / ar CR (mig 0081 behavior, bit-for-bit). '
  'Cross-currency (p_cash_currency != p_currency): emits 2 events '
  'bridged through fx_clearing in both currencies; realized FX '
  'gain/loss recognized on the cash side at p_business_date rate.';

-- ============================================================
-- 3. post_ap_payment — symmetric DROP+CREATE.
-- ============================================================

DROP FUNCTION IF EXISTS post_ap_payment(
  UUID, CHAR, BIGINT, DATE, UUID, UUID, TEXT
);

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

  v_cross_ccy := p_cash_currency IS NOT NULL
             AND p_cash_currency <> p_currency;

  IF v_cross_ccy THEN
    IF p_cash_amount IS NULL OR p_cash_amount <= 0 THEN
      RAISE EXCEPTION
        'ap_payment_invalid: p_cash_amount required and > 0 when '
        'p_cash_currency (%) differs from p_currency (%)',
        p_cash_currency, p_currency
        USING ERRCODE = 'P0042';
    END IF;
  ELSE
    IF p_cash_amount IS NOT NULL AND p_cash_amount <> p_amount THEN
      RAISE EXCEPTION
        'ap_payment_invalid: same-currency settlement requires '
        'p_cash_amount (%) = p_amount (%) (or NULL)',
        p_cash_amount, p_amount
        USING ERRCODE = 'P0042';
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
                    COALESCE(p_cash_currency, p_currency)
      USING ERRCODE = 'P0010';
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
     ORDER BY effective_at DESC
     LIMIT 1;
    IF v_rate IS NULL THEN
      RAISE EXCEPTION
        'missing_fx_rate: no fx_rates row found for % → % effective_at <= %',
        p_currency, p_cash_currency, p_business_date
        USING ERRCODE = 'P0050';
    END IF;

    v_expected := (p_amount::NUMERIC * v_rate)::BIGINT;
    v_delta    := p_cash_amount - v_expected;

    -- AR: customer paying us → delta>0 is gain to us.
    -- AP: us paying vendor → delta>0 means we pay MORE than expected
    --     (loss); delta<0 means we pay LESS (gain).
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
    -- Event A (counterparty currency): ap DR p_amount / fx_clearing
    -- CR p_amount. Settles AP debt; bridges to cash currency via
    -- fx_clearing(p_currency).
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'ap_payment',
      'document_kind',     'ap_payment',
      'document_id',       v_doc_id,
      'debit_account_id',  v_vend_ap,
      'credit_account_id', v_fx_clr_ccy,
      'amount',            p_amount,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   p_vendor_id,
      'posted_by',         p_posted_by
    ));

    -- Event B Part 1 (cash currency): fx_clearing DR p_cash_amount /
    -- cash CR p_cash_amount. Cash leg reflects actual cash flow
    -- (what we paid).
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'ap_payment',
      'document_kind',     'ap_payment',
      'document_id',       v_doc_id,
      'debit_account_id',  v_fx_clr_cash,
      'credit_account_id', v_cash_acct,
      'amount',            p_cash_amount,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   p_vendor_id,
      'posted_by',         p_posted_by
    ));

    -- Event B Part 2: reconcile fx_clearing(cash_ccy) net to
    -- v_expected DR (paired against fx_clearing(p_currency)'s
    -- p_amount CR — the two clearing legs net to zero in home
    -- currency at the p_business_date rate).
    --
    --   delta > 0 (loss — we paid more than rate-implied):
    --     loss DR v_delta / fx_clearing CR v_delta.
    --     Reduces fx_clearing DR balance from p_cash_amount to
    --     v_expected; recognizes v_delta as loss.
    --
    --   delta < 0 (gain — we paid less):
    --     fx_clearing DR -v_delta / gain CR -v_delta.
    --     Increases fx_clearing DR balance from p_cash_amount to
    --     v_expected (since -v_delta > 0); recognizes -v_delta as gain.
    --
    --   delta = 0: skip.
    IF v_delta > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ap_payment',
        'document_kind',     'ap_payment',
        'document_id',       v_doc_id,
        'debit_account_id',  v_fx_loss_acct,
        'credit_account_id', v_fx_clr_cash,
        'amount',            v_delta,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_vendor_id,
        'posted_by',         p_posted_by
      ));
    ELSIF v_delta < 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ap_payment',
        'document_kind',     'ap_payment',
        'document_id',       v_doc_id,
        'debit_account_id',  v_fx_clr_cash,
        'credit_account_id', v_fx_gain_acct,
        'amount',            -v_delta,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_vendor_id,
        'posted_by',         p_posted_by
      ));
    END IF;
  ELSE
    -- Same-currency: original mig 0082 single-event shape.
    v_batch := jsonb_build_array(jsonb_build_object(
      'reason',            'ap_payment',
      'document_kind',     'ap_payment',
      'document_id',       v_doc_id,
      'debit_account_id',  v_vend_ap,
      'credit_account_id', v_cash_acct,
      'amount',            p_amount,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   p_vendor_id,
      'posted_by',         p_posted_by
    ));
  END IF;

  PERFORM post_transfers(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;

COMMENT ON FUNCTION post_ap_payment(UUID, CHAR, BIGINT, DATE, UUID, UUID, TEXT, CHAR, BIGINT) IS
  'AP payment with optional cross-currency settlement (acct-3xcg). '
  'Same-currency: ap DR / cash CR (mig 0082 behavior, bit-for-bit). '
  'Cross-currency: emits 2 events bridged through fx_clearing; '
  'realized FX gain/loss recognized on the cash side at p_business_date rate.';
