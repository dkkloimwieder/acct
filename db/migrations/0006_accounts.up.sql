CREATE TABLE accounts (
  id              BIGSERIAL PRIMARY KEY,
  kind            account_kind NOT NULL,
  ledger_kind     TEXT NOT NULL CHECK (ledger_kind IN ('qty', 'value')),
  currency        CHAR(3),
  sku_id          UUID REFERENCES skus(id),
  location_id     UUID REFERENCES locations(id),
  routing_op      INT,
  counterparty_id UUID,
  normal_side     balance_direction NOT NULL,
  debits_total    BIGINT NOT NULL DEFAULT 0 CHECK (debits_total  >= 0),
  credits_total   BIGINT NOT NULL DEFAULT 0 CHECK (credits_total >= 0),
  is_closed       BOOLEAN NOT NULL DEFAULT FALSE,
  closed_at       TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CHECK (
    CASE normal_side
      WHEN 'debit'  THEN credits_total <= debits_total
      WHEN 'credit' THEN debits_total  <= credits_total
      ELSE TRUE
    END
  ),
  CHECK (
    (ledger_kind = 'value' AND currency IS NOT NULL) OR
    (ledger_kind = 'qty'   AND currency IS NULL)
  )
);

CREATE UNIQUE INDEX accounts_stock_avail_uk
  ON accounts (sku_id, location_id)
  WHERE kind = 'stock_available' AND NOT is_closed;

CREATE UNIQUE INDEX accounts_wip_uk
  ON accounts (sku_id, routing_op)
  WHERE kind = 'stock_wip' AND NOT is_closed;

CREATE UNIQUE INDEX accounts_value_uk
  ON accounts (kind, sku_id, currency)
  WHERE ledger_kind = 'value' AND sku_id IS NOT NULL AND NOT is_closed;

CREATE INDEX accounts_kind
  ON accounts (kind)
  WHERE NOT is_closed;

CREATE INDEX accounts_counterparty
  ON accounts (counterparty_id)
  WHERE counterparty_id IS NOT NULL;
