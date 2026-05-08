-- ============================================================
-- Phase D D6 — append-only correction movements for close hook
-- variance posts (acct-wb75.3.6).
--
-- The wac_periodic / wac_retroactive / cost_adjust_retroactive
-- close hooks post variance posting_lines with qty IS NULL — pure
-- value adjustments to the inventory pool. The apply_event D-block
-- (gated on qty IS NOT NULL) deliberately skips them, so the
-- subledger has no movement row for the variance even though the
-- GL inv_value_* state changes.
--
-- D6 ships an AFTER INSERT trigger on posting_lines that detects
-- variance posts (reason='cost_restate' AND qty IS NULL touching
-- inv_value_* on at least one side) and emits an append-only
-- correction movement row per qualifying inv_value_* leg with:
--
--   - event_type     = 16 (cost_adjustment)
--   - quantity       = 0  (value-only adjustment; no material flow)
--   - actual_unit_cost = 0 (placeholder; required NOT NULL but
--                         doesn't affect SUM(qty × actual))
--   - ppv_amount     = signed variance amount on this leg
--                      (+amount on DR side; -amount on CR side)
--   - posting_line_id pointed at the variance posting_line for trace
--
-- Per the user's append-only directive (plan-phase-d-inventory-
-- movements memory): variance correction = INSERT new movement
-- row, NOT in-place UPDATE on actual_unit_cost. Original movement
-- rows from D2/D3 dispatcher writes stay untouched. Subledger
-- traceability for "the post-close cost adjustment was applied at
-- this pool on this date" is captured in the new row.
--
-- Recon impact: zero. Check #7 (D5) restricts the GL view to
-- pl.qty IS NOT NULL — variance posts are excluded from GL. The
-- subledger view groups by (product, location, period) and sums
-- quantity × actual_unit_cost — correction rows contribute 0 since
-- quantity=0. So both views see the same (= zero from variance)
-- and no false-positive alerts. The ppv_amount captures the
-- variance trail for analytics; recon doesn't depend on it.
--
-- The trigger approach (vs. editing the close hook bodies directly)
-- keeps the change localized and avoids re-writing the topological
-- sort + chronological replay logic in 0015. The trigger is also
-- self-applying for any future close-hook variance source that
-- uses cost_restate + inv_value_*.
--
-- The append-only trigger on posting_lines (block_posting_line_modifications)
-- only blocks UPDATE/DELETE, not INSERT — no conflict with the new
-- AFTER INSERT trigger.
-- ============================================================

CREATE OR REPLACE FUNCTION _emit_correction_movement_for_variance_post()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  v_d accounts;
  v_c accounts;
BEGIN
  -- Fast path: only fire for cost_restate posts with no qty.
  IF NEW.qty IS NOT NULL THEN RETURN NEW; END IF;
  IF NEW.reason <> 'cost_restate'::posting_line_reason THEN RETURN NEW; END IF;

  SELECT * INTO v_d FROM accounts WHERE id = NEW.debit_account_id;
  SELECT * INTO v_c FROM accounts WHERE id = NEW.credit_account_id;

  -- DR side correction. Variance increased the inv_value_* pool —
  -- ppv_amount = +NEW.amount.
  IF v_d.kind::TEXT LIKE 'inv_value_%'
     AND v_d.sku_id IS NOT NULL
     AND v_d.location_id IS NOT NULL THEN
    INSERT INTO inventory_movements (
      product_id, legal_entity_id, location_id,
      event_type, movement_date, quantity,
      standard_unit_cost, actual_unit_cost,
      cost_currency, ppv_amount, posting_line_id
    ) VALUES (
      v_d.sku_id, v_d.legal_entity_id, v_d.location_id,
      16,                                 -- cost_adjustment
      NEW.business_date,
      0,                                   -- value-only
      NULL,
      0,                                   -- placeholder; recon math = 0
      v_d.currency,
      NEW.amount,                          -- favorable: pool gained value
      NEW.id
    );
  END IF;

  -- CR side correction. Variance reduced the inv_value_* pool —
  -- ppv_amount = -NEW.amount.
  IF v_c.kind::TEXT LIKE 'inv_value_%'
     AND v_c.sku_id IS NOT NULL
     AND v_c.location_id IS NOT NULL THEN
    INSERT INTO inventory_movements (
      product_id, legal_entity_id, location_id,
      event_type, movement_date, quantity,
      standard_unit_cost, actual_unit_cost,
      cost_currency, ppv_amount, posting_line_id
    ) VALUES (
      v_c.sku_id, v_c.legal_entity_id, v_c.location_id,
      16,
      NEW.business_date,
      0,
      NULL,
      0,
      v_c.currency,
      -NEW.amount,                         -- pool lost value
      NEW.id
    );
  END IF;

  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_emit_correction_movement_for_variance
  AFTER INSERT ON posting_lines
  FOR EACH ROW
  EXECUTE FUNCTION _emit_correction_movement_for_variance_post();
