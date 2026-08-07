-- Negative standard costs are unrepresentable (acct-qm7o.10). Aligns
-- standard_cost with trx_line's unit_cost >= 0 CHECK and the hot path's
-- cost-clamping convention: FL_DEPLETE records GREATEST(cost, 0) on the line
-- and uses the same clamped cost for the value_sum decrement, so a stored
-- negative standard would be silently observed as 0 — reject it at the
-- source instead.
ALTER TABLE standard_cost
    ADD CONSTRAINT standard_cost_unit_cost_nonneg CHECK (unit_cost >= 0);
