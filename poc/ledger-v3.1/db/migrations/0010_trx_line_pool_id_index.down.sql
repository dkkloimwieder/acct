DROP INDEX IF EXISTS trx_line_pool_id;
CREATE INDEX trx_line_pool ON trx_line (pool_id);
