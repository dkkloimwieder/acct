-- acct-mvq4.22 (Pass-3 Q1): drop the aggregate value_sum non-negativity CHECK.
--
-- 0007 defines value_sum as the NET POSTED book value: receipts add qty*cost,
-- depletions subtract the posted amount (qty*applied_unit_cost). Under a
-- provisional standard-basis depletion (design-v3.1 §3.5) the applied cost is
-- the SKU's standard_cost, decoupled from the pool's book average — when
-- standard_cost exceeds the running average, a deep-but-partial depletion
-- legitimately posts more value out than the pool carries, and the
-- GL-reconcilable value_sum goes negative until the deferred recalc tier (§7)
-- trues it up. Banker-rounded running-average depletions can do the same at
-- ±1 micro-unit scale. The CHECK contradicted the column's own stated
-- semantics; the qty invariant (0006) is unaffected and stays.
ALTER TABLE pool_state
    DROP CONSTRAINT pool_state_aggregate_value_sum_nonneg;
