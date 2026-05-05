-- Down: drop the function and table.

DROP FUNCTION IF EXISTS post_ap_payment(UUID, CHAR, BIGINT, DATE, UUID, UUID, TEXT);
DROP TABLE IF EXISTS ap_payments;
