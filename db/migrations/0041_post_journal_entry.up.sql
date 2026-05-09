-- ============================================================
-- acct-wb75.4.1 — Phase F1: post_journal_entry generic wrapper.
--
-- Per research/posting-lines-convergence-plan.md §4.F.F1.
--
-- Generic dr/cr wrapper. Caller supplies an array of
-- {debit_account_id, credit_account_id, amount, description?}
-- entries plus header fields (business_date, posted_by,
-- idempotency_key, optional memo). Used for accruals, reclasses,
-- manual adjustments that don't fit the inventory- or document-
-- specific wrappers.
--
-- Constraints (enforced before any INSERT):
--   * non-empty array of lines
--   * each leg amount > 0
--   * debit_account_id <> credit_account_id per line
--   * both legs ledger_kind='value' (no qty journal entries —
--     use inventory wrappers for qty-side movement)
--   * neither leg is SKU-bearing (sku_id IS NULL on both) —
--     SKU-bearing accounts dispatch via cost-method-aware
--     wrappers, not raw JE
--   * legs share currency (cross-currency JE not in scope; FX
--     handled by acct-3xcg / acct-3dz2 follow-ups)
--
-- Non-goals (deliberate):
--   * no posting_line_inventory write (no qty leg, no SKU leg)
--   * no inventory_movements write (the 'journal_entry' reason
--     maps to NULL in _inventory_movement_event_type, so the
--     D-block in apply_event skips per-row)
--   * no posting_lines_provisional flagging (reason isn't in
--     the cost-event list)
--
-- Error code: P0042 'journal_entry_invalid'.
-- ============================================================

ALTER TYPE posting_line_reason ADD VALUE IF NOT EXISTS 'journal_entry';

CREATE TABLE journal_entries (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  business_date   DATE NOT NULL,
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID NOT NULL UNIQUE,
  memo            TEXT
);

CREATE INDEX journal_entries_posted_at ON journal_entries (posted_at);
CREATE INDEX journal_entries_business_date ON journal_entries (business_date);

CREATE TABLE journal_entry_lines (
  id                UUID   NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  journal_entry_id  UUID   NOT NULL REFERENCES journal_entries(id),
  line_no           INT    NOT NULL CHECK (line_no >= 1),
  debit_account_id  BIGINT NOT NULL REFERENCES accounts(id),
  credit_account_id BIGINT NOT NULL REFERENCES accounts(id),
  amount            BIGINT NOT NULL CHECK (amount > 0),
  description       TEXT,
  CHECK (debit_account_id <> credit_account_id),
  UNIQUE (journal_entry_id, line_no)
);

CREATE INDEX je_lines_jeid ON journal_entry_lines (journal_entry_id);

CREATE OR REPLACE FUNCTION post_journal_entry(
  p_lines           JSONB,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_memo            TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id UUID;
  v_doc_id      UUID;
  v_n           INT;
  v_idx         INT;
  v_line        JSONB;
  v_d_id        BIGINT;
  v_c_id        BIGINT;
  v_amount      BIGINT;
  v_desc        TEXT;
  v_d_acct      accounts%ROWTYPE;
  v_c_acct      accounts%ROWTYPE;
  v_batch       JSONB := '[]'::JSONB;
BEGIN
  -- Idempotent replay.
  SELECT id INTO v_existing_id FROM journal_entries WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN
    RETURN v_existing_id;
  END IF;

  IF p_lines IS NULL OR jsonb_typeof(p_lines) <> 'array' THEN
    RAISE EXCEPTION 'post_journal_entry: p_lines must be a JSONB array'
      USING ERRCODE = 'P0042';
  END IF;
  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'post_journal_entry: p_lines must be non-empty'
      USING ERRCODE = 'P0042';
  END IF;

  -- Validate every line up front. We don't INSERT or call
  -- post_posting_lines until the whole batch is well-formed.
  FOR v_idx IN 1..v_n LOOP
    v_line   := p_lines -> (v_idx - 1);
    v_d_id   := (v_line->>'debit_account_id')::BIGINT;
    v_c_id   := (v_line->>'credit_account_id')::BIGINT;
    v_amount := (v_line->>'amount')::BIGINT;

    IF v_d_id IS NULL OR v_c_id IS NULL OR v_amount IS NULL THEN
      RAISE EXCEPTION
        'post_journal_entry: line %: debit_account_id, credit_account_id, and amount are required',
        v_idx USING ERRCODE = 'P0042';
    END IF;
    IF v_amount <= 0 THEN
      RAISE EXCEPTION 'post_journal_entry: line %: amount must be > 0 (got %)',
        v_idx, v_amount USING ERRCODE = 'P0042';
    END IF;
    IF v_d_id = v_c_id THEN
      RAISE EXCEPTION
        'post_journal_entry: line %: debit and credit accounts must differ',
        v_idx USING ERRCODE = 'P0042';
    END IF;

    SELECT * INTO v_d_acct FROM accounts WHERE id = v_d_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'post_journal_entry: line %: debit_account_id % not found',
        v_idx, v_d_id USING ERRCODE = 'P0042';
    END IF;
    IF v_d_acct.is_closed THEN
      RAISE EXCEPTION 'post_journal_entry: line %: debit account % is closed',
        v_idx, v_d_id USING ERRCODE = 'P0042';
    END IF;

    SELECT * INTO v_c_acct FROM accounts WHERE id = v_c_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'post_journal_entry: line %: credit_account_id % not found',
        v_idx, v_c_id USING ERRCODE = 'P0042';
    END IF;
    IF v_c_acct.is_closed THEN
      RAISE EXCEPTION 'post_journal_entry: line %: credit account % is closed',
        v_idx, v_c_id USING ERRCODE = 'P0042';
    END IF;

    IF v_d_acct.ledger_kind <> 'value' OR v_c_acct.ledger_kind <> 'value' THEN
      RAISE EXCEPTION
        'post_journal_entry: line %: both legs must be value ledger (d=%, c=%); use inventory wrappers for qty postings',
        v_idx, v_d_acct.ledger_kind, v_c_acct.ledger_kind USING ERRCODE = 'P0042';
    END IF;
    IF v_d_acct.sku_id IS NOT NULL OR v_c_acct.sku_id IS NOT NULL THEN
      RAISE EXCEPTION
        'post_journal_entry: line %: SKU-bearing accounts are out of scope; use post_inventory_adjustment / post_cost_adjustment',
        v_idx USING ERRCODE = 'P0042';
    END IF;
    IF v_d_acct.currency IS DISTINCT FROM v_c_acct.currency THEN
      RAISE EXCEPTION
        'post_journal_entry: line %: cross-currency leg not supported (d=%, c=%); FX handled by acct-3xcg / acct-3dz2',
        v_idx, v_d_acct.currency, v_c_acct.currency USING ERRCODE = 'P0042';
    END IF;
  END LOOP;

  -- INSERT header. ON CONFLICT handles the idempotent-replay race
  -- where two callers try to claim the same idempotency_key
  -- between our SELECT above and this INSERT.
  INSERT INTO journal_entries (business_date, posted_by, idempotency_key, memo)
  VALUES (p_business_date, p_posted_by, p_idempotency_key, p_memo)
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM journal_entries WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  -- INSERT lines + build the post_posting_lines batch.
  FOR v_idx IN 1..v_n LOOP
    v_line   := p_lines -> (v_idx - 1);
    v_d_id   := (v_line->>'debit_account_id')::BIGINT;
    v_c_id   := (v_line->>'credit_account_id')::BIGINT;
    v_amount := (v_line->>'amount')::BIGINT;
    v_desc   := v_line->>'description';

    INSERT INTO journal_entry_lines (
      journal_entry_id, line_no, debit_account_id, credit_account_id, amount, description
    ) VALUES (
      v_doc_id, v_idx, v_d_id, v_c_id, v_amount, v_desc
    );

    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'journal_entry',
      'document_kind',     'journal_entry',
      'document_id',       v_doc_id,
      'debit_account_id',  v_d_id,
      'credit_account_id', v_c_id,
      'amount',            v_amount,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'posted_by',         p_posted_by
    ));
  END LOOP;

  PERFORM post_posting_lines(v_batch, FALSE);

  RETURN v_doc_id;
END;
$$;
