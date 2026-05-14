DROP FUNCTION IF EXISTS drain_deferred_fifo(INT);
DROP FUNCTION IF EXISTS post_batch_h_ext_deferred(JSONB);
DROP INDEX IF EXISTS cost_consumptions_h_ext_pending_idx;
ALTER TABLE cost_consumptions_h_ext DROP COLUMN IF EXISTS fifo_processed_at;
