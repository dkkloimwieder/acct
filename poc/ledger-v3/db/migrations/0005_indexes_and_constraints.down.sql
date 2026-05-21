-- Reverse of 0005_indexes_and_constraints.up.sql. Drop in reverse order.

DROP INDEX IF EXISTS posting_line_dimension_lookup;
DROP INDEX IF EXISTS posting_line_posted_at;
DROP INDEX IF EXISTS posting_line_trx_line;
DROP INDEX IF EXISTS trx_line_source;
DROP INDEX IF EXISTS trx_line_pool;
DROP INDEX IF EXISTS trx_line_trx;
