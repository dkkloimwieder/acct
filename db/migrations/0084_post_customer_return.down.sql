-- Down: drop function, tables, and the return_disposition enum.

DROP FUNCTION IF EXISTS post_customer_return(UUID, JSONB, DATE, UUID, UUID, TEXT);

DROP TABLE IF EXISTS customer_return_lines;
DROP TABLE IF EXISTS customer_returns;

DROP TYPE IF EXISTS return_disposition;
