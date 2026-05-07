-- Posting lines: the row-per-pair core of the ledger (acct-1584).
--
-- Renamed from `transfers` per acct-NEW1 schema-consolidation pass.
-- The proposal's `posting_lines` naming + our row-per-pair shape
-- (debit_account_id + credit_account_id + amount + qty all on one
-- row) per acct-1584's deliberate divergence from the proposal's
-- row-per-leg shape.
--
-- Consolidates archive migs 0007 (base) + 0030 (qty column) +
-- 0102 (posting_layer + partial index) + 0103 (legal_entity_id).
-- Append-only enforcement is in the next migration (0010).

CREATE TABLE posting_lines (
  id                BIGSERIAL PRIMARY KEY,
  reason            posting_line_reason NOT NULL,
  document_kind     TEXT NOT NULL,
  document_id       UUID NOT NULL,
  document_line_id  UUID,
  debit_account_id  BIGINT NOT NULL REFERENCES accounts(id),
  credit_account_id BIGINT NOT NULL REFERENCES accounts(id),
  amount            BIGINT NOT NULL CHECK (amount > 0),

  -- Per-class qty (acct-1vr / acct-75z.1; was added in mig 0030 to
  -- fix per-class divisor confusion in WAC math). NULL for
  -- non-inventory events (cash / AR / AP / FX).
  qty               BIGINT CHECK (qty IS NULL OR qty >= 0),

  routing_op        INT,
  counterparty_id   UUID,
  period_id         BIGINT NOT NULL REFERENCES periods(id),
  business_date     DATE NOT NULL,
  idempotency_key   UUID NOT NULL UNIQUE,
  posted_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  posted_by         UUID NOT NULL,

  -- Posting-layer bitmask for IFRS / local-GAAP / tax-basis tagging
  -- (acct-chzx, was mig 0102). Default 1 = Current/operational layer.
  -- bit 0 (=1) Current; bit 1 (=2) Operations; bit 2 (=4) Tax;
  -- bits 3-9 Custom.
  posting_layer     SMALLINT NOT NULL DEFAULT 1
                      CHECK (posting_layer > 0),

  -- Legal-entity FK (acct-ewhs, was mig 0103). DEFAULT 1 preserves
  -- single-entity behavior; multi-entity work (acct-3gzh) extends.
  legal_entity_id   SMALLINT NOT NULL DEFAULT 1
                      REFERENCES legal_entities(id),

  CHECK (debit_account_id <> credit_account_id)
);

-- Indexes (renamed posting_lines_* per acct-NEW1).

CREATE INDEX posting_lines_document
  ON posting_lines (document_kind, document_id, posted_at);

CREATE INDEX posting_lines_debit_ts
  ON posting_lines (debit_account_id, posted_at DESC);

CREATE INDEX posting_lines_credit_ts
  ON posting_lines (credit_account_id, posted_at DESC);

CREATE INDEX posting_lines_reason_ts
  ON posting_lines (reason, posted_at);

CREATE INDEX posting_lines_counterparty
  ON posting_lines (counterparty_id)
  WHERE counterparty_id IS NOT NULL;

CREATE INDEX posting_lines_routing_op
  ON posting_lines (routing_op)
  WHERE routing_op IS NOT NULL;

-- Partial index on non-default posting_layer (acct-chzx). Default
-- value 1 dominates; full-table index would be useless.
CREATE INDEX posting_lines_non_default_layer
  ON posting_lines (business_date, posting_layer)
  WHERE posting_layer != 1;

COMMENT ON COLUMN posting_lines.qty IS
  'Per-event signed qty (acct-1vr). Populated for inventory-touching '
  'events (resolved at INSERT time from event JSONB qty field, or '
  'from amount when both sides are ledger_kind=qty). NULL for cash / '
  'AR / AP / FX. WAC math reads SUM(qty signed) as per-class divisor; '
  'never goes through stock_available which pools raw + fg.';

COMMENT ON COLUMN posting_lines.posting_layer IS
  'Bitmask for IFRS / local-GAAP / tax-basis tagging (acct-chzx). '
  'Reporting filters via WHERE posting_layer & N != 0. Default 1.';
