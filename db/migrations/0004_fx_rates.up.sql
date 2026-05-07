-- FX rates: per-direction conversion rates with effective_at validity.
-- Looked up by (from_currency, to_currency, effective_at DESC).

CREATE TABLE fx_rates (
  id            BIGSERIAL PRIMARY KEY,
  from_currency CHAR(3) NOT NULL,
  to_currency   CHAR(3) NOT NULL,
  rate          NUMERIC(20, 10) NOT NULL,
  effective_at  TIMESTAMPTZ NOT NULL,
  source        TEXT NOT NULL
);

CREATE INDEX fx_rates_lookup
  ON fx_rates(from_currency, to_currency, effective_at DESC);
