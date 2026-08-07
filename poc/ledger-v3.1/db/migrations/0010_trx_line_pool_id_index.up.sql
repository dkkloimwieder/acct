-- acct-0at4.3 (FEEDBACK #17) — composite (pool_id, id) index for per-pool,
-- id-ordered trx_line replay. §14.1 recalc/close walk each pool's trx_line
-- stream in id order; the single-column trx_line_pool(pool_id) forces a per-pool
-- sort on id. The composite's leading (pool_id) prefix subsumes every lookup the
-- single-column index served, so it is replaced rather than kept alongside.
DROP INDEX trx_line_pool;
CREATE INDEX trx_line_pool_id ON trx_line (pool_id, id);
