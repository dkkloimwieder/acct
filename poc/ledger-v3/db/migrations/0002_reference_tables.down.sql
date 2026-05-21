-- Reverse of 0002_reference_tables.up.sql. Drop in reverse creation order
-- (account self-FK and accounting_period have no children at this layer).

DROP TABLE IF EXISTS accounting_period;
DROP TABLE IF EXISTS account;
DROP TABLE IF EXISTS location;
DROP TABLE IF EXISTS sku;
