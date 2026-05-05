-- Down: drop function and tables.

DROP FUNCTION IF EXISTS post_po_return(UUID, JSONB, DATE, UUID, UUID, TEXT);

DROP TABLE IF EXISTS po_return_lines;
DROP TABLE IF EXISTS po_returns;
