-- acct-w0lo — refactor _post_transfers_compute_amount from a monolithic
-- CASE statement on cost_method to a strategy-registration pattern.
--
-- Why:
-- ----
-- Today's dispatcher is a 4-way CASE (standard / wac_perpetual /
-- wac_periodic / wac_retroactive) with FIFO + lot raising P0006. Adding
-- FIFO and lot (acct-8gg) means doubling the existing function;
-- cross-currency BOMs (acct-tt1) and provisional cost sources (acct-cms)
-- compound it further. Without refactor, the dispatcher becomes the
-- bottleneck where every future cost-method change concentrates.
--
-- Shape:
-- ------
-- New table `cost_method_strategies(cost_method, event_kind,
-- compute_fn_name, flag_provisional, registered_at)`. Per
-- (cost_method, event_kind) row points at a per-strategy plpgsql
-- function. `_post_transfers_compute_amount` becomes a registry lookup
-- + EXECUTE-format dispatch.
--
-- For Phase 1 only event_kind='outbound' is registered (depletion paths:
-- op_move / scrap / wo_complete / so_ship). Future work can register
-- 'inbound' / 'internal_chain' rows when FIFO needs them — adding a
-- cost method is INSERT + new fn, not a dispatcher edit.
--
-- Migration steps:
--   1. CREATE TABLE cost_method_strategies (registry).
--   2. CREATE per-strategy functions (one per existing cost method).
--   3. INSERT registry rows.
--   4. REPLACE _post_transfers_compute_amount with the registry-lookup
--      shell.
--
-- Behavior preservation:
-- ----------------------
-- The 4 per-strategy functions are line-for-line copies of the current
-- mig 0075 CASE branches. Same gates (WIP class, ledger_kind=value),
-- same error codes (P0006), same math. The dispatcher's pre-strategy
-- work (qty NULL check, credit-first SKU resolution per acct-du2.4) is
-- preserved at the call site so per-strategy functions can assume those
-- invariants hold.
--
-- The refactor introduces one observable change: cost methods not in
-- the registry (FIFO, lot) now raise a slightly different message
-- ("no strategy registered for cost_method=X event_kind=outbound"
-- with errcode P0006). Same SQLSTATE; behaviorally equivalent for
-- callers that catch P0006.
--
-- Class-confusion checklist (R1-R7) per-strategy:
-- - R1: per-class qty divisor → strategy fns read t.qty signed SUM on
--   the credit-side value pool (unchanged).
-- - R2: credit-first SKU resolution → done in dispatcher pre-strategy.
-- - R4: pre-existing FOR UPDATE in callers (post_transfers /
--   _post_transfers_apply_event) — strategy fns are pure read.
-- - R5/R6: not applicable to dispatcher.
-- - R7: dispatcher does not persist any document audit fields (callers do).

-- ============================================================
-- 1. Registry table.
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
  'acct-w0lo registry. Per (cost_method, event_kind) one row points at
   a plpgsql function that computes the value-leg amount for that
   strategy. Phase 1 only event_kind=outbound is wired through
   _post_transfers_compute_amount. flag_provisional indicates whether
   _post_transfers_apply_event should INSERT a transfers_provisional
   row for the value-leg event (used today by wac_periodic +
   wac_retroactive — the dispatcher-recompute hook reads this column
   if it ever migrates from the hard-coded list in apply_event).';

-- ============================================================
-- 2. Per-strategy functions (outbound).
--
-- Each takes the same signature as the old CASE branch's environment:
--   p_event   — the JSONB event row (for business_date / qty / etc.)
--   p_d_acct  — the debit account (already resolved + locked by caller)
--   p_c_acct  — the credit account (depletion source)
--   p_idx     — event index for error messages
-- and returns BIGINT (the computed amount).
--
-- Caller (_post_transfers_compute_amount) handles:
--   - qty NULL gate
--   - credit-first SKU resolution (v_sku from p_c_acct.sku_id then p_d_acct.sku_id)
-- so each strategy fn can assume v_qty is non-NULL and SKU is resolved.
-- ============================================================

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
  v_unit := resolve_standard_cost_at(v_sku, v_business_date);
  RETURN v_qty * v_unit;
END;
$$;

COMMENT ON FUNCTION _compute_amount_standard_outbound(JSONB, accounts, accounts, INT) IS
  'acct-w0lo. standard cost outbound = qty × resolve_standard_cost_at.
   No FOR UPDATE — resolve_standard_cost_at reads standard_costs which
   is append-only.';

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

  -- R1: per-class qty divisor from signed SUM of transfers.qty on the
  -- credit-side value pool (NOT from stock_available, which is cross-
  -- class for raw + fg same-location SKUs).
  SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = p_c_acct.id THEN  t.qty
                           WHEN t.credit_account_id = p_c_acct.id THEN -t.qty END), 0)
    INTO v_qty_balance
    FROM transfers t
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
    FROM transfers t
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
    FROM transfers t
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

-- ============================================================
-- 3. Register existing strategies.
--
-- flag_provisional matches the existing _post_transfers_apply_event
-- flagging logic: wac_periodic + wac_retroactive depletions get a
-- transfers_provisional row inserted for close-hook recompute.
-- (The apply_event function still reads from a hard-coded list today
-- — migrating it to the registry is a follow-up; the column is
-- captured here for forward compatibility.)
-- ============================================================

INSERT INTO cost_method_strategies
  (cost_method,      event_kind, compute_fn_name,                        flag_provisional)
VALUES
  ('standard',       'outbound', '_compute_amount_standard_outbound',        FALSE),
  ('wac_perpetual',  'outbound', '_compute_amount_wac_perpetual_outbound',   FALSE),
  ('wac_periodic',   'outbound', '_compute_amount_wac_periodic_outbound',    TRUE),
  ('wac_retroactive','outbound', '_compute_amount_wac_retroactive_outbound', TRUE);

-- ============================================================
-- 4. Refactored dispatcher: registry lookup + dynamic dispatch.
--
-- Caller-visible behavior is preserved. The qty-NULL gate and credit-
-- first SKU resolution stay in the dispatcher so per-strategy functions
-- can assume those invariants. P0006 fires identically when qty is NULL
-- or SKU is unresolvable, AND when no strategy is registered for the
-- (cost_method, event_kind) pair (FIFO / lot fall through here).
-- ============================================================

CREATE OR REPLACE FUNCTION _post_transfers_compute_amount(
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

  -- R2 (credit-first SKU resolution; acct-du2.4 + acct-7py). Validated
  -- here so per-strategy functions can assume v_sku is non-NULL.
  v_sku := COALESCE(p_c_acct.sku_id, p_d_acct.sku_id);
  IF v_sku IS NULL THEN
    RAISE EXCEPTION 'cost_method_not_implemented: sku not resolvable in compute_amount at event index %',
                    p_idx
      USING ERRCODE = 'P0006';
  END IF;

  -- Registry lookup. event_kind=outbound is the only currently-wired
  -- call site (op_move / scrap / wo_complete / so_ship depletion).
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

  -- Dynamic dispatch. The fn_name is sourced from the registry table
  -- (operator-controlled); format(%I) quotes for safety.
  EXECUTE format(
    'SELECT %I($1, $2, $3, $4)', v_fn_name
  )
  INTO v_amount
  USING p_event, p_d_acct, p_c_acct, p_idx;

  RETURN v_amount;
END;
$$;

COMMENT ON FUNCTION _post_transfers_compute_amount(JSONB, accounts, accounts, cost_method, INT) IS
  'acct-w0lo registry-dispatched cost amount for the outbound depletion
   path. Looks up cost_method_strategies(cost_method, ''outbound'') and
   EXECUTEs the registered per-strategy function. P0006 if no strategy
   is registered (FIFO + lot fall through here pending acct-8gg).';
