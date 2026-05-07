-- Cost-method strategy registry.
--
-- Consolidates archive mig 0094 (acct-w0lo). Registry table +
-- per-strategy compute functions for the 4 wired methods (standard,
-- wac_perpetual, wac_periodic, wac_retroactive); FIFO + lot raise
-- P0006 by registry absence.
--
-- The registry-LOOKUP dispatcher (`_post_posting_lines_compute_amount`)
-- lives in 0014 with the rest of the `_post_posting_lines_*` helpers.
--
-- Naming unifications baked in:
--   - body references: transfers → posting_lines
--   - resolve_standard_cost_at → _resolve_standard_cost_at

-- ============================================================
-- Registry table.
-- ============================================================

CREATE TABLE cost_method_strategies (
  cost_method      cost_method  NOT NULL,
  event_kind       TEXT         NOT NULL,
  compute_fn_name  TEXT         NOT NULL,
  flag_provisional BOOLEAN      NOT NULL DEFAULT FALSE,
  registered_at    TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (cost_method, event_kind),
  CHECK (event_kind IN ('outbound', 'inbound', 'internal_chain'))
);

COMMENT ON TABLE cost_method_strategies IS
  'Per (cost_method, event_kind) one row points at a plpgsql function '
  'that computes the value-leg amount. Phase 1 wires only '
  'event_kind=outbound. Adding a cost method is INSERT + new fn, not '
  'a dispatcher edit.';

-- ============================================================
-- Per-strategy compute functions.
--
-- Each takes (p_event, p_d_acct, p_c_acct, p_idx) and returns BIGINT.
-- Caller (_post_posting_lines_compute_amount) handles qty-NULL gate +
-- credit-first SKU resolution; strategies assume those invariants hold.
-- ============================================================

-- Standard cost: qty × _resolve_standard_cost_at.

CREATE OR REPLACE FUNCTION _compute_amount_standard_outbound(
  p_event   JSONB,
  p_d_acct  accounts,
  p_c_acct  accounts,
  p_idx     INT
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_qty           BIGINT;
  v_sku           UUID;
  v_unit          BIGINT;
  v_business_date DATE;
BEGIN
  v_qty := (p_event->>'qty')::BIGINT;
  v_sku := COALESCE(p_c_acct.sku_id, p_d_acct.sku_id);
  v_business_date := (p_event->>'business_date')::DATE;
  v_unit := _resolve_standard_cost_at(v_sku, v_business_date);
  RETURN v_qty * v_unit;
END;
$$;

-- WAC perpetual: amount = qty × (value_balance / qty_balance) on the
-- credit-side value pool. Per-class qty divisor via signed SUM on
-- posting_lines.qty (R1).

CREATE OR REPLACE FUNCTION _compute_amount_wac_perpetual_outbound(
  p_event   JSONB,
  p_d_acct  accounts,
  p_c_acct  accounts,
  p_idx     INT
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_qty           BIGINT;
  v_qty_balance   BIGINT;
  v_value_balance BIGINT;
  v_unit          BIGINT;
BEGIN
  v_qty := (p_event->>'qty')::BIGINT;

  IF p_c_acct.ledger_kind <> 'value' THEN
    RAISE EXCEPTION 'wac_perpetual requires credit-side value account, got % at event index %',
                    p_c_acct.kind, p_idx
      USING ERRCODE = 'P0006';
  END IF;

  SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = p_c_acct.id THEN  t.qty
                           WHEN t.credit_account_id = p_c_acct.id THEN -t.qty END), 0)
    INTO v_qty_balance
    FROM posting_lines t
   WHERE p_c_acct.id IN (t.debit_account_id, t.credit_account_id)
     AND t.qty IS NOT NULL;

  IF v_qty_balance <= 0 THEN
    RAISE EXCEPTION 'wac_perpetual qty balance is %, cannot divide for unit cost at event index %',
                    v_qty_balance, p_idx
      USING ERRCODE = 'P0006';
  END IF;

  v_value_balance := p_c_acct.debits_total - p_c_acct.credits_total;
  IF v_value_balance < 0 THEN v_value_balance := 0; END IF;

  v_unit := v_value_balance / v_qty_balance;
  RETURN v_qty * v_unit;
END;
$$;

-- WAC periodic: same mid-period math as perpetual; close hook
-- recomputes period-end avg and posts variance via
-- variance_wac_periodic. flag_provisional=TRUE causes
-- _post_posting_lines_apply_event to insert a posting_lines_provisional
-- row.

CREATE OR REPLACE FUNCTION _compute_amount_wac_periodic_outbound(
  p_event   JSONB,
  p_d_acct  accounts,
  p_c_acct  accounts,
  p_idx     INT
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_qty           BIGINT;
  v_qty_balance   BIGINT;
  v_value_balance BIGINT;
  v_unit          BIGINT;
BEGIN
  v_qty := (p_event->>'qty')::BIGINT;

  IF p_c_acct.kind = 'inv_value_wip' THEN
    RAISE EXCEPTION
      'wac_periodic depletions from inv_value_wip not supported in Phase 1 '
      '(see acct-p7v Phase 2 Epic J); event index %',
      p_idx USING ERRCODE = 'P0006';
  END IF;
  IF p_c_acct.ledger_kind <> 'value' THEN
    RAISE EXCEPTION 'wac_periodic requires credit-side value account, got % at event index %',
                    p_c_acct.kind, p_idx
      USING ERRCODE = 'P0006';
  END IF;

  SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = p_c_acct.id THEN  t.qty
                           WHEN t.credit_account_id = p_c_acct.id THEN -t.qty END), 0)
    INTO v_qty_balance
    FROM posting_lines t
   WHERE p_c_acct.id IN (t.debit_account_id, t.credit_account_id)
     AND t.qty IS NOT NULL;

  IF v_qty_balance <= 0 THEN
    RAISE EXCEPTION 'wac_periodic qty balance is %, cannot divide for unit cost at event index %',
                    v_qty_balance, p_idx
      USING ERRCODE = 'P0006';
  END IF;

  v_value_balance := p_c_acct.debits_total - p_c_acct.credits_total;
  IF v_value_balance < 0 THEN v_value_balance := 0; END IF;

  v_unit := v_value_balance / v_qty_balance;
  RETURN v_qty * v_unit;
END;
$$;

-- WAC retroactive: mid-period at running avg; close hook does
-- chronological replay and posts variance via variance_wac_retroactive.
-- Same mid-period math as wac_periodic.

CREATE OR REPLACE FUNCTION _compute_amount_wac_retroactive_outbound(
  p_event   JSONB,
  p_d_acct  accounts,
  p_c_acct  accounts,
  p_idx     INT
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_qty           BIGINT;
  v_qty_balance   BIGINT;
  v_value_balance BIGINT;
  v_unit          BIGINT;
BEGIN
  v_qty := (p_event->>'qty')::BIGINT;

  IF p_c_acct.kind = 'inv_value_wip' THEN
    RAISE EXCEPTION
      'wac_retroactive depletions from inv_value_wip not supported in Phase 1 '
      '(see acct-p7v Phase 2 Epic J); event index %',
      p_idx USING ERRCODE = 'P0006';
  END IF;
  IF p_c_acct.ledger_kind <> 'value' THEN
    RAISE EXCEPTION 'wac_retroactive requires credit-side value account, got % at event index %',
                    p_c_acct.kind, p_idx
      USING ERRCODE = 'P0006';
  END IF;

  SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = p_c_acct.id THEN  t.qty
                           WHEN t.credit_account_id = p_c_acct.id THEN -t.qty END), 0)
    INTO v_qty_balance
    FROM posting_lines t
   WHERE p_c_acct.id IN (t.debit_account_id, t.credit_account_id)
     AND t.qty IS NOT NULL;

  IF v_qty_balance <= 0 THEN
    RAISE EXCEPTION 'wac_retroactive qty balance is %, cannot divide for unit cost at event index %',
                    v_qty_balance, p_idx
      USING ERRCODE = 'P0006';
  END IF;

  v_value_balance := p_c_acct.debits_total - p_c_acct.credits_total;
  IF v_value_balance < 0 THEN v_value_balance := 0; END IF;

  v_unit := v_value_balance / v_qty_balance;
  RETURN v_qty * v_unit;
END;
$$;

-- Strategy registry seeds (4 standard + WAC variants at outbound).
-- FIFO + lot deliberately absent → P0006 at dispatch.

INSERT INTO cost_method_strategies
  (cost_method,       event_kind, compute_fn_name,                            flag_provisional)
VALUES
  ('standard',        'outbound', '_compute_amount_standard_outbound',        FALSE),
  ('wac_perpetual',   'outbound', '_compute_amount_wac_perpetual_outbound',   FALSE),
  ('wac_periodic',    'outbound', '_compute_amount_wac_periodic_outbound',    TRUE),
  ('wac_retroactive', 'outbound', '_compute_amount_wac_retroactive_outbound', TRUE);
