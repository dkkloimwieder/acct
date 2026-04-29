CREATE TABLE period_snapshots (
  period_id     BIGINT REFERENCES periods(id),
  account_id    BIGINT REFERENCES accounts(id),
  debits_total  BIGINT NOT NULL,
  credits_total BIGINT NOT NULL,
  PRIMARY KEY (period_id, account_id)
);
