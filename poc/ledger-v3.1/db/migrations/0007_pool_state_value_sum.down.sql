ALTER TABLE pool_state DROP CONSTRAINT IF EXISTS pool_state_aggregate_value_sum_nonneg;
ALTER TABLE pool_state DROP COLUMN IF EXISTS value_sum;
