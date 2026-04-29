CREATE TABLE transfers (
  id                BIGSERIAL PRIMARY KEY,
  reason            transfer_reason NOT NULL,
  document_kind     TEXT NOT NULL,
  document_id       UUID NOT NULL,
  document_line_id  UUID,
  debit_account_id  BIGINT NOT NULL REFERENCES accounts(id),
  credit_account_id BIGINT NOT NULL REFERENCES accounts(id),
  amount            BIGINT NOT NULL CHECK (amount > 0),
  routing_op        INT,
  counterparty_id   UUID,
  period_id         BIGINT NOT NULL REFERENCES periods(id),
  business_date     DATE NOT NULL,
  idempotency_key   UUID NOT NULL UNIQUE,
  posted_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  posted_by         UUID NOT NULL,
  CHECK (debit_account_id <> credit_account_id)
);

CREATE INDEX transfers_document
  ON transfers (document_kind, document_id, posted_at);

CREATE INDEX transfers_debit_ts
  ON transfers (debit_account_id, posted_at DESC);

CREATE INDEX transfers_credit_ts
  ON transfers (credit_account_id, posted_at DESC);

CREATE INDEX transfers_reason_ts
  ON transfers (reason, posted_at);

CREATE INDEX transfers_counterparty
  ON transfers (counterparty_id)
  WHERE counterparty_id IS NOT NULL;

CREATE INDEX transfers_routing_op
  ON transfers (routing_op)
  WHERE routing_op IS NOT NULL;
