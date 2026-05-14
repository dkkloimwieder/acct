-- No-op: revert to mig 0030's version of post_batch_h_ext_fifo_walk.
-- mig 0030 down already drops the function.
DROP FUNCTION IF EXISTS post_batch_h_ext_fifo_walk(JSONB);
