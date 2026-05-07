-- Accounts: the core ledger account table.
--
-- Consolidates archive migs 0006 (base table) + 0020 (per-class partition
-- UKs + CHECK reinforcements) + 0103 (legal_entity_id column).
--
-- Naming unifications baked in:
--   - ledger_kind: TEXT-with-CHECK → ENUM (per acct-NEW1).
--   - legal_entity_id: real FK to legal_entities (was DEFAULT 1 column).

CREATE TABLE accounts (
  id              BIGSERIAL PRIMARY KEY,
  kind            account_kind NOT NULL,
  ledger_kind     ledger_kind  NOT NULL,
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
  legal_entity_id SMALLINT NOT NULL DEFAULT 1
                    REFERENCES legal_entities(id),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

  -- Normal-side discipline: debit-normal accounts can't go credit-balance,
  -- credit-normal accounts can't go debit-balance, unrestricted is fine.
  CHECK (
    CASE normal_side
      WHEN 'debit'  THEN credits_total <= debits_total
      WHEN 'credit' THEN debits_total  <= credits_total
      ELSE TRUE
    END
  ),

  -- ledger_kind / currency consistency: value accounts MUST have currency,
  -- qty accounts MUST NOT.
  CHECK (
    (ledger_kind = 'value' AND currency IS NOT NULL) OR
    (ledger_kind = 'qty'   AND currency IS NULL)
  ),

  -- inv_value_raw / inv_value_fg with a sku_id MUST have a location_id.
  CONSTRAINT accounts_inv_value_loc_required CHECK (
    NOT (kind IN ('inv_value_raw', 'inv_value_fg') AND sku_id IS NOT NULL)
    OR location_id IS NOT NULL
  ),

  -- inv_value_wip MUST have a routing_op.
  CONSTRAINT accounts_inv_value_wip_op_required CHECK (
    kind <> 'inv_value_wip' OR routing_op IS NOT NULL
  )
);

-- Per-class partition unique indexes.

-- stock_available: (sku, location).
CREATE UNIQUE INDEX accounts_stock_avail_uk
  ON accounts (sku_id, location_id)
  WHERE kind = 'stock_available' AND NOT is_closed;

-- stock_wip: (sku, routing_op).
CREATE UNIQUE INDEX accounts_wip_uk
  ON accounts (sku_id, routing_op)
  WHERE kind = 'stock_wip' AND NOT is_closed;

-- inv_value_raw / inv_value_fg: (kind, sku, location, currency).
CREATE UNIQUE INDEX accounts_value_loc_uk
  ON accounts (kind, sku_id, location_id, currency)
  WHERE ledger_kind = 'value'
    AND kind IN ('inv_value_raw', 'inv_value_fg')
    AND sku_id IS NOT NULL
    AND NOT is_closed;

-- inv_value_wip: (kind, sku, routing_op, currency).
CREATE UNIQUE INDEX accounts_value_wip_uk
  ON accounts (kind, sku_id, routing_op, currency)
  WHERE ledger_kind = 'value'
    AND kind = 'inv_value_wip'
    AND sku_id IS NOT NULL
    AND NOT is_closed;

-- General lookups.
CREATE INDEX accounts_kind
  ON accounts (kind)
  WHERE NOT is_closed;

CREATE INDEX accounts_counterparty
  ON accounts (counterparty_id)
  WHERE counterparty_id IS NOT NULL;
