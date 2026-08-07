-- design-v3.1 §3.6 — no negative inventory. Promote the application-layer
-- InsufficientInventory guard (ledger-core aggregate_deplete) to a schema
-- invariant on the on-hand aggregate row, so a future entry point that mutates
-- pool_state without going through the dispatcher cannot drive it negative.
--
-- Scoped to layer_id = 0 (the aggregate). Path C never materializes layer rows
-- (layer_id > 0) on the hot path, and strict-method layer semantics are out of
-- scope (§13), so layer rows are left unconstrained by this check.
ALTER TABLE pool_state
    ADD CONSTRAINT pool_state_aggregate_qty_nonneg
    CHECK (layer_id <> 0 OR qty >= 0);
