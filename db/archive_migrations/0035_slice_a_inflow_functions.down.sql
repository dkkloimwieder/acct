-- Down: drop the slice A inflow functions.

DROP FUNCTION IF EXISTS post_ap_bill(UUID, CHAR, JSONB, DATE, UUID, UUID, TEXT);
DROP FUNCTION IF EXISTS post_po_receipt(UUID, JSONB, DATE, UUID, UUID, TEXT);
