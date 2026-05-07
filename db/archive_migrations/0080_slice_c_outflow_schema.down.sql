-- Down: drop in reverse dependency order.

DROP INDEX IF EXISTS accounts_customer_pool_uk;
DROP INDEX IF EXISTS accounts_ar_partitioned_uk;

DROP TABLE IF EXISTS ar_payments;

DROP TABLE IF EXISTS customer_invoice_lines;
DROP TABLE IF EXISTS customer_invoices;

DROP TABLE IF EXISTS so_shipment_lines;
DROP TABLE IF EXISTS so_shipments;

DROP TABLE IF EXISTS sales_order_lines;

ALTER TABLE sales_orders DROP CONSTRAINT IF EXISTS so_customer_fk;

DROP TABLE IF EXISTS customers;
