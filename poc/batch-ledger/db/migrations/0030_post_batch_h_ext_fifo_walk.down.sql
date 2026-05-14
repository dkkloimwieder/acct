DROP FUNCTION IF EXISTS fifo_overconsume_check_h_ext();
DROP FUNCTION IF EXISTS post_batch_h_ext_fifo_walk(JSONB);
DROP TABLE IF EXISTS cost_layer_depletions_h_ext;
ALTER TABLE cost_layers_h_ext DROP CONSTRAINT IF EXISTS cost_layers_h_ext_qty_remaining_nonneg;
ALTER TABLE cost_layers_h_ext DROP COLUMN IF EXISTS qty_remaining;
