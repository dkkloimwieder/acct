-- design-v3.1 §3.0/§3.1 — store the cumulative book value (value_sum) on
-- pool_state so the running-average unit_cost is DERIVED exactly from
-- value_sum/qty, not re-rounded incrementally off the prior (already-rounded)
-- average. This makes a receipts-only pool's final unit_cost ==
-- banker_div(Σ qty*cost, Σ qty): exact and order-independent (acct-0qps).
--
-- value_sum is the net posted inventory book value: receipts add qty*cost;
-- depletions subtract the posted depletion amount (qty*applied_unit_cost), so
-- value_sum stays reconcilable to Σ posting_line amounts (the GL). For layer
-- rows (layer_id > 0, specific pools) value_sum = qty*unit_cost, the layer's
-- own book value. (Mixed receipt+depletion sequences remain order-sensitive in
-- unit_cost — depletion-at-rounded-average is lossy — see §11.1/§14.2.)
ALTER TABLE pool_state
    ADD COLUMN value_sum BIGINT NOT NULL DEFAULT 0;

-- Backfill any pre-existing rows: book value = qty * unit_cost.
UPDATE pool_state SET value_sum = qty * unit_cost;

-- Drop the default so every write must supply value_sum explicitly: a write
-- path that forgets it fails loud (NOT NULL) rather than silently storing 0.
ALTER TABLE pool_state ALTER COLUMN value_sum DROP DEFAULT;

-- Defensive sibling to pool_state_aggregate_qty_nonneg (0006): the aggregate
-- book value cannot go negative (non-negative qty × non-negative costs).
ALTER TABLE pool_state
    ADD CONSTRAINT pool_state_aggregate_value_sum_nonneg
    CHECK (layer_id <> 0 OR value_sum >= 0);
