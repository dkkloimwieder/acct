CREATE TABLE ledger_outbox (
  id                     BIGSERIAL PRIMARY KEY,
  enqueued_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  events                 JSONB NOT NULL,
  override_closed_period BOOLEAN NOT NULL DEFAULT FALSE,
  status                 TEXT NOT NULL DEFAULT 'pending'
                           CHECK (status IN ('pending','committed','failed')),
  committed_at           TIMESTAMPTZ,
  error_sqlstate         TEXT,
  error_text             TEXT,
  attempt_count          INT NOT NULL DEFAULT 0
);

CREATE INDEX ledger_outbox_pending
  ON ledger_outbox (id)
  WHERE status = 'pending';
