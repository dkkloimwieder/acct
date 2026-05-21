-- Reverse of 0003_ledger_tables.up.sql. Drop in reverse order so trx_line's
-- self-FK and references to trx + pool unwind cleanly.

DROP TABLE IF EXISTS trx_line;
DROP TABLE IF EXISTS trx;
DROP TABLE IF EXISTS pool_lock;
DROP TABLE IF EXISTS pool_state;
DROP TABLE IF EXISTS pool;
