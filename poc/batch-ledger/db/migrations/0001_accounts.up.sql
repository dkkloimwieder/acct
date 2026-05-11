-- acct-c0ko (P1 of acct-qdp5 PoC).
--
-- Minimal accounts shape — matches pgledger's posture (no partitioning, no MVCC
-- tricks). We're calibrating against pgledger's reported 10K TPS in P2, so
-- divergence from their schema would muddy the comparison.

CREATE TYPE account_kind AS ENUM ('debit_normal', 'credit_normal');

CREATE TABLE accounts (
    id          BIGSERIAL PRIMARY KEY,
    code        TEXT       NOT NULL UNIQUE,
    balance     BIGINT     NOT NULL DEFAULT 0,
    currency    CHAR(3)    NOT NULL,
    kind        account_kind NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX accounts_currency_kind_idx ON accounts (currency, kind);
