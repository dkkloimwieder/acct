-- Reverse of 0001_enums.up.sql. CASCADE for safety in case later migrations
-- haven't been reverted yet (sqlx reverts one migration at a time in reverse
-- order, so strictly speaking CASCADE shouldn't be needed — but defending
-- against ad-hoc operator-driven down sequences).

DROP TYPE IF EXISTS dimension_type CASCADE;
DROP TYPE IF EXISTS account_type CASCADE;
DROP TYPE IF EXISTS posting_event_type CASCADE;
DROP TYPE IF EXISTS line_type CASCADE;
DROP TYPE IF EXISTS trx_type CASCADE;
DROP TYPE IF EXISTS pool_method CASCADE;
