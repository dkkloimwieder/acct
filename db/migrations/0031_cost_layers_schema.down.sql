-- Best-effort down (project convention).

DO $$
BEGIN
  PERFORM cron.unschedule('cost_layers_partition_rollover');
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'pg_cron unavailable / job missing (%): %', SQLSTATE, SQLERRM;
END $$;

DROP FUNCTION IF EXISTS _cost_layer_remaining_qty(BIGINT, DATE);
DROP FUNCTION IF EXISTS _create_cost_layer_depletions_partition(DATE);
DROP FUNCTION IF EXISTS _create_cost_layers_partition(DATE);

DROP TRIGGER IF EXISTS trg_cost_layer_depletions_append_only ON cost_layer_depletions;
DROP TRIGGER IF EXISTS trg_cost_layers_append_only ON cost_layers;
DROP FUNCTION IF EXISTS block_cost_layer_modifications();

DROP TABLE IF EXISTS cost_layer_depletions;
DROP TABLE IF EXISTS cost_layers;
