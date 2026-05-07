-- acct-w0lo down: restore mig 0075 inlined-CASE dispatcher and drop
-- the registry table + per-strategy functions.

DROP FUNCTION IF EXISTS _compute_amount_wac_retroactive_outbound(JSONB, accounts, accounts, INT);
DROP FUNCTION IF EXISTS _compute_amount_wac_periodic_outbound (JSONB, accounts, accounts, INT);
DROP FUNCTION IF EXISTS _compute_amount_wac_perpetual_outbound(JSONB, accounts, accounts, INT);
DROP FUNCTION IF EXISTS _compute_amount_standard_outbound     (JSONB, accounts, accounts, INT);

DROP TABLE IF EXISTS cost_method_strategies;

-- Restore the inlined-CASE dispatcher verbatim from mig 0075 (the
-- previous CREATE OR REPLACE site).
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
  v_qty            BIGINT;
  v_sku            UUID;
  v_unit           BIGINT;
  v_qty_balance    BIGINT;
  v_value_balance  BIGINT;
  v_business_date  DATE;
BEGIN
  v_qty := (p_event->>'qty')::BIGINT;
  IF v_qty IS NULL THEN
    RAISE EXCEPTION 'cost_method_not_implemented: cost-relevant value event missing qty at event index %',
                    p_idx
      USING ERRCODE = 'P0006';
  END IF;

  v_sku := COALESCE(p_c_acct.sku_id, p_d_acct.sku_id);
  IF v_sku IS NULL THEN
    RAISE EXCEPTION 'cost_method_not_implemented: sku not resolvable in compute_amount at event index %',
                    p_idx
      USING ERRCODE = 'P0006';
  END IF;

  CASE p_cost_method
    WHEN 'standard' THEN
      v_business_date := (p_event->>'business_date')::DATE;
      v_unit := resolve_standard_cost_at(v_sku, v_business_date);
      RETURN v_qty * v_unit;

    WHEN 'wac_perpetual' THEN
      IF p_c_acct.ledger_kind <> 'value' THEN
        RAISE EXCEPTION 'wac_perpetual requires credit-side value account, got % at event index %',
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
        RAISE EXCEPTION 'wac_perpetual qty balance is %, cannot divide for unit cost at event index %',
                        v_qty_balance, p_idx
          USING ERRCODE = 'P0006';
      END IF;

      v_value_balance := p_c_acct.debits_total - p_c_acct.credits_total;
      IF v_value_balance < 0 THEN v_value_balance := 0; END IF;

      v_unit := v_value_balance / v_qty_balance;
      RETURN v_qty * v_unit;

    WHEN 'wac_periodic' THEN
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

    WHEN 'wac_retroactive' THEN
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

    WHEN 'lot' THEN
      RAISE EXCEPTION 'cost_method_not_implemented: lot (tracked as acct-8gg + lot infrastructure) at event index %',
                      p_idx
        USING ERRCODE = 'P0006';

    WHEN 'fifo' THEN
      RAISE EXCEPTION 'cost_method_not_implemented: fifo (tracked as acct-8gg + lot infrastructure) at event index %',
                      p_idx
        USING ERRCODE = 'P0006';
  END CASE;

  RAISE EXCEPTION 'cost_method_not_implemented: unhandled cost_method % at event index %',
                  p_cost_method, p_idx
    USING ERRCODE = 'P0006';
END;
$$;
