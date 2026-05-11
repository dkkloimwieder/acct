-- acct-c0ko (P1 of acct-qdp5 PoC).
--
-- One row per double-entry transfer (TigerBeetle-shape, also pgledger's
-- effective shape). The PoC deliberately omits:
--
--   - posting_lines append-only trigger (acct has one; pgledger does not). Adding
--     it would slow per-row INSERTs and confound the P2 calibration against
--     pgledger. HC11 in P7 re-introduces it and quantifies the overhead.
--
--   - per-leg `qty` / `cost_method_at_event` / `lot_id` / `unit_ids` and the
--     posting_line_inventory / _currencies / _dimensions extension tables. P4
--     adds `qty` when WAC enters the picture; later phases add inventory bits
--     only as required by the workload under test.

CREATE TABLE posting_lines (
    id                  BIGSERIAL PRIMARY KEY,
    debit_account_id    BIGINT     NOT NULL REFERENCES accounts(id),
    credit_account_id   BIGINT     NOT NULL REFERENCES accounts(id),
    amount              BIGINT     NOT NULL,
    currency            CHAR(3)    NOT NULL,
    idempotency_key     UUID       NOT NULL UNIQUE,
    business_date       DATE       NOT NULL,
    posted_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    document_id         UUID       NULL,
    CHECK (amount > 0),
    CHECK (debit_account_id <> credit_account_id)
);

CREATE INDEX posting_lines_debit_idx    ON posting_lines (debit_account_id,  posted_at DESC);
CREATE INDEX posting_lines_credit_idx   ON posting_lines (credit_account_id, posted_at DESC);
CREATE INDEX posting_lines_document_idx ON posting_lines (document_id) WHERE document_id IS NOT NULL;
