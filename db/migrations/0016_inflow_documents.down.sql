DROP FUNCTION IF EXISTS post_po_return(UUID, JSONB, DATE, UUID, UUID, TEXT, BOOLEAN);
DROP FUNCTION IF EXISTS post_ap_payment(UUID, CHAR, BIGINT, DATE, UUID, UUID, TEXT, CHAR, BIGINT);
DROP FUNCTION IF EXISTS post_ap_bill(UUID, CHAR, JSONB, DATE, UUID, UUID, TEXT);
DROP FUNCTION IF EXISTS post_po_receipt(UUID, JSONB, DATE, UUID, UUID, TEXT);
DROP FUNCTION IF EXISTS post_standard_cost_roll(UUID, BIGINT, DATE, DATE, UUID, UUID, TEXT, BIGINT, BOOLEAN);
DROP FUNCTION IF EXISTS post_cost_adjustment_retroactive(BIGINT, UUID, UUID, TEXT, TEXT, BIGINT, DATE, UUID, UUID, TEXT);
DROP FUNCTION IF EXISTS post_cost_adjustment(UUID, UUID, TEXT, TEXT, BIGINT, DATE, UUID, UUID, TEXT);
DROP FUNCTION IF EXISTS post_inventory_adjustment(UUID, UUID, BIGINT, BIGINT, TEXT, TEXT, DATE, UUID, UUID, TEXT);

DROP TABLE IF EXISTS inventory_standard_cost_rolls;
DROP TABLE IF EXISTS inventory_cost_adjustments;
DROP TABLE IF EXISTS inventory_adjustments;
DROP TABLE IF EXISTS po_return_lines;
DROP TABLE IF EXISTS po_returns;
DROP TABLE IF EXISTS ap_payments;
DROP TABLE IF EXISTS vendor_bill_lines;
DROP TABLE IF EXISTS vendor_bills;
DROP TABLE IF EXISTS po_receipt_lines;
DROP TABLE IF EXISTS po_receipts;
DROP TABLE IF EXISTS purchase_order_lines;

DROP INDEX IF EXISTS accounts_customer_pool_uk;
DROP INDEX IF EXISTS accounts_ar_partitioned_uk;
DROP INDEX IF EXISTS accounts_vendor_pool_uk;
DROP INDEX IF EXISTS accounts_ap_partitioned_uk;
