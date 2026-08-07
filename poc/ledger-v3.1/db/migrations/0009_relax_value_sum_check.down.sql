-- Best-effort: re-adding the CHECK fails if negative aggregate book values
-- exist (legitimate under provisional standard-basis depletion — see up).
ALTER TABLE pool_state
    ADD CONSTRAINT pool_state_aggregate_value_sum_nonneg
    CHECK (layer_id <> 0 OR value_sum >= 0);
