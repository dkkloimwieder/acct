-- ============================================================
-- acct-wb75.4.3 — Phase F3: post_expense_report (employee
-- expense reimbursement).
--
-- Per research/posting-lines-convergence-plan.md §4.F.F3.
--
-- Employee submits an expense report; finance posts it as a set
-- of expense_account DR / settlement_account CR pairs. The caller
-- chooses the settlement side per-report:
--   * cash (immediate reimbursement, e.g. petty-cash refund)
--   * ap_employee (deferred — the employee is owed; cleared later
--     by post_ap_payment or a later cash disbursement)
-- The function does NOT enforce the kind of the settlement
-- account; it gates on value-ledger / non-SKU / matching currency
-- and lets reporting downstream distinguish the two settlement
-- modes by joining to accounts.kind.
--
-- Employees are introduced as a first-class entity (vs reusing
-- vendor counterparty_ids) for clean reporting separation:
-- expense reimbursement liability is structurally distinct from
-- vendor AP and shouldn't co-mingle in vendor aging.
--
-- Posts (per line):
--   expense_account DR / settlement_account CR  for amount
--
-- All legs share document_id and document_line_id so the report
-- is traceable from any leg back to expense_report_lines.
--
-- Constraints (enforced before any INSERT):
--   * employee exists, currency-aligned with p_currency
--   * non-empty array of lines
--   * each line amount > 0
--   * settlement account: value-ledger, currency=p_currency,
--     sku_id IS NULL, not closed
--   * expense account (per line): same constraints as settlement
--   * expense_account != settlement_account per line
--
-- Non-goals (deliberate):
--   * no posting_line_inventory write (no qty leg, no SKU leg)
--   * no inventory_movements write ('expense_report' falls to the
--     ELSE NULL branch of _inventory_movement_event_type)
--   * no posting_lines_provisional flagging (reason isn't in the
--     cost-event list at apply_event line 278)
--   * no FX (cross-currency settlement deferred to acct-3xcg /
--     acct-3dz2; if employee has functional currency X but
--     settlement happens in currency Y, that's a future epic)
--
-- Error code: P0046 'expense_report_invalid'.
-- ============================================================

ALTER TYPE account_kind         ADD VALUE IF NOT EXISTS 'ap_employee';
ALTER TYPE posting_line_reason  ADD VALUE IF NOT EXISTS 'expense_report';

CREATE TABLE employees (
  id          UUID    NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  code        TEXT    NOT NULL UNIQUE,
  name        TEXT    NOT NULL,
  currency    CHAR(3) NOT NULL,
  is_active   BOOLEAN NOT NULL DEFAULT TRUE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE expense_reports (
  id              UUID    NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  employee_id     UUID    NOT NULL REFERENCES employees(id),
  currency        CHAR(3) NOT NULL,
  business_date   DATE    NOT NULL,
  posted_by       UUID    NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID    NOT NULL UNIQUE,
  -- The settlement_account_id is the credit-side account that
  -- absorbs the report's total value (cash for immediate; an
  -- ap_employee account for deferred reimbursement). Stored on
  -- the header, not per-line, to keep the report semantically
  -- coherent (an expense report settles via one method).
  settlement_account_id BIGINT NOT NULL REFERENCES accounts(id),
  report_number   TEXT,
  memo            TEXT
);

CREATE INDEX expense_reports_employee_id  ON expense_reports (employee_id);
CREATE INDEX expense_reports_posted_at    ON expense_reports (posted_at);
CREATE INDEX expense_reports_business_date ON expense_reports (business_date);

CREATE TABLE expense_report_lines (
  id                 UUID   NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  expense_report_id  UUID   NOT NULL REFERENCES expense_reports(id),
  line_no            INT    NOT NULL CHECK (line_no >= 1),
  expense_account_id BIGINT NOT NULL REFERENCES accounts(id),
  amount             BIGINT NOT NULL CHECK (amount > 0),
  description        TEXT,
  UNIQUE (expense_report_id, line_no)
);

CREATE INDEX expense_report_lines_er_id ON expense_report_lines (expense_report_id);

CREATE OR REPLACE FUNCTION post_expense_report(
  p_employee_id           UUID,
  p_currency              CHAR(3),
  p_lines                 JSONB,
  p_settlement_account_id BIGINT,
  p_business_date         DATE,
  p_posted_by             UUID,
  p_idempotency_key       UUID,
  p_report_number         TEXT DEFAULT NULL,
  p_memo                  TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id UUID;
  v_doc_id      UUID;
  v_emp         employees%ROWTYPE;
  v_settle      accounts%ROWTYPE;
  v_n           INT;
  v_idx         INT;
  v_line        JSONB;
  v_exp_id      BIGINT;
  v_amount      BIGINT;
  v_desc        TEXT;
  v_exp_acct    accounts%ROWTYPE;
  v_line_id     UUID;
  v_batch       JSONB := '[]'::JSONB;
BEGIN
  -- Idempotent replay (fast path; ON CONFLICT below catches the race).
  SELECT id INTO v_existing_id FROM expense_reports
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN
    RETURN v_existing_id;
  END IF;

  IF p_currency IS NULL OR length(p_currency) <> 3 THEN
    RAISE EXCEPTION 'expense_report_invalid: p_currency required (3-char)'
      USING ERRCODE = 'P0046';
  END IF;

  SELECT * INTO v_emp FROM employees WHERE id = p_employee_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'expense_report_invalid: employee % not found', p_employee_id
      USING ERRCODE = 'P0046';
  END IF;
  IF NOT v_emp.is_active THEN
    RAISE EXCEPTION 'expense_report_invalid: employee % is inactive', p_employee_id
      USING ERRCODE = 'P0046';
  END IF;
  IF v_emp.currency IS DISTINCT FROM p_currency THEN
    RAISE EXCEPTION
      'expense_report_invalid: employee currency=% but report currency=%',
      v_emp.currency, p_currency USING ERRCODE = 'P0046';
  END IF;

  IF p_lines IS NULL OR jsonb_typeof(p_lines) <> 'array' THEN
    RAISE EXCEPTION 'expense_report_invalid: p_lines must be a JSONB array'
      USING ERRCODE = 'P0046';
  END IF;
  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'expense_report_invalid: p_lines must be non-empty'
      USING ERRCODE = 'P0046';
  END IF;

  -- Settlement account: caller chooses cash vs ap_employee. We don't
  -- gate on kind; just on value-ledger / non-SKU / currency / not
  -- closed. The accounts.kind partitioning lets reporting separate
  -- modes downstream.
  SELECT * INTO v_settle FROM accounts WHERE id = p_settlement_account_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'expense_report_invalid: settlement_account_id % not found',
      p_settlement_account_id USING ERRCODE = 'P0046';
  END IF;
  IF v_settle.is_closed THEN
    RAISE EXCEPTION 'expense_report_invalid: settlement account % is closed',
      p_settlement_account_id USING ERRCODE = 'P0046';
  END IF;
  IF v_settle.ledger_kind <> 'value' THEN
    RAISE EXCEPTION
      'expense_report_invalid: settlement account must be value-ledger (got %)',
      v_settle.ledger_kind USING ERRCODE = 'P0046';
  END IF;
  IF v_settle.sku_id IS NOT NULL THEN
    RAISE EXCEPTION
      'expense_report_invalid: settlement account is SKU-bearing'
      USING ERRCODE = 'P0046';
  END IF;
  IF v_settle.currency IS DISTINCT FROM p_currency THEN
    RAISE EXCEPTION
      'expense_report_invalid: settlement account currency=% but report currency=%',
      v_settle.currency, p_currency USING ERRCODE = 'P0046';
  END IF;

  -- Validate every line up front. We don't INSERT or call
  -- post_posting_lines until the whole batch is well-formed.
  FOR v_idx IN 1..v_n LOOP
    v_line   := p_lines -> (v_idx - 1);
    v_exp_id := (v_line->>'expense_account_id')::BIGINT;
    v_amount := (v_line->>'amount')::BIGINT;

    IF v_exp_id IS NULL OR v_amount IS NULL THEN
      RAISE EXCEPTION
        'expense_report_invalid: line %: expense_account_id and amount are required',
        v_idx USING ERRCODE = 'P0046';
    END IF;
    IF v_amount <= 0 THEN
      RAISE EXCEPTION 'expense_report_invalid: line %: amount must be > 0 (got %)',
        v_idx, v_amount USING ERRCODE = 'P0046';
    END IF;

    SELECT * INTO v_exp_acct FROM accounts WHERE id = v_exp_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'expense_report_invalid: line %: expense_account_id % not found',
        v_idx, v_exp_id USING ERRCODE = 'P0046';
    END IF;
    IF v_exp_acct.is_closed THEN
      RAISE EXCEPTION 'expense_report_invalid: line %: expense account % is closed',
        v_idx, v_exp_id USING ERRCODE = 'P0046';
    END IF;
    IF v_exp_acct.ledger_kind <> 'value' THEN
      RAISE EXCEPTION
        'expense_report_invalid: line %: expense account must be value-ledger (got %)',
        v_idx, v_exp_acct.ledger_kind USING ERRCODE = 'P0046';
    END IF;
    IF v_exp_acct.sku_id IS NOT NULL THEN
      RAISE EXCEPTION
        'expense_report_invalid: line %: expense account is SKU-bearing; use inventory wrappers',
        v_idx USING ERRCODE = 'P0046';
    END IF;
    IF v_exp_acct.currency IS DISTINCT FROM p_currency THEN
      RAISE EXCEPTION
        'expense_report_invalid: line %: expense account currency=% but report currency=%',
        v_idx, v_exp_acct.currency, p_currency USING ERRCODE = 'P0046';
    END IF;
    IF v_exp_id = p_settlement_account_id THEN
      RAISE EXCEPTION
        'expense_report_invalid: line %: expense account equals settlement account',
        v_idx USING ERRCODE = 'P0046';
    END IF;
  END LOOP;

  -- INSERT header. ON CONFLICT handles the idempotent-replay race
  -- where two callers try to claim the same idempotency_key
  -- between our SELECT above and this INSERT.
  INSERT INTO expense_reports (
    employee_id, currency, business_date, posted_by,
    idempotency_key, settlement_account_id, report_number, memo
  ) VALUES (
    p_employee_id, p_currency, p_business_date, p_posted_by,
    p_idempotency_key, p_settlement_account_id, p_report_number, p_memo
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM expense_reports WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  -- INSERT lines + build the post_posting_lines batch.
  FOR v_idx IN 1..v_n LOOP
    v_line   := p_lines -> (v_idx - 1);
    v_exp_id := (v_line->>'expense_account_id')::BIGINT;
    v_amount := (v_line->>'amount')::BIGINT;
    v_desc   := v_line->>'description';

    INSERT INTO expense_report_lines (
      expense_report_id, line_no, expense_account_id, amount, description
    ) VALUES (
      v_doc_id, v_idx, v_exp_id, v_amount, v_desc
    ) RETURNING id INTO v_line_id;

    -- expense DR / settlement CR for amount
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'expense_report',
      'document_kind',     'expense_report',
      'document_id',       v_doc_id,
      'document_line_id',  v_line_id,
      'debit_account_id',  v_exp_id,
      'credit_account_id', p_settlement_account_id,
      'amount',            v_amount,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'counterparty_id',   p_employee_id,
      'posted_by',         p_posted_by
    ));
  END LOOP;

  PERFORM post_posting_lines(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;
