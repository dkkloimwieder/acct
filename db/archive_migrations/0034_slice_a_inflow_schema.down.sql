-- Down: drop in reverse dependency order.

DROP INDEX IF EXISTS accounts_supplier_pool_uk;
DROP INDEX IF EXISTS accounts_ap_partitioned_uk;

DROP TABLE IF EXISTS vendor_bill_lines;
DROP TABLE IF EXISTS vendor_bills;

DROP TABLE IF EXISTS po_receipt_lines;
DROP TABLE IF EXISTS po_receipts;

DROP TABLE IF EXISTS purchase_order_lines;

ALTER TABLE purchase_orders DROP CONSTRAINT IF EXISTS po_supplier_fk;

DROP TABLE IF EXISTS suppliers;
