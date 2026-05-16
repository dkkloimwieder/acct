-- M1.1 (acct-63dk) — posting tables per spec §1.2.
--
-- Minimal analogs of acct's posting machinery, included so the v2.1
-- committer pipeline exercises multi-target bulk UNNEST INSERT writes
-- inside a single committer sub-tx (P5 calibration).
--
-- posting_lines.amount is signed (debit-credit asymmetry surfaces in
-- the trait dispatch at M1.3+); debit_account / credit_account are
-- nullable because some event types (e.g., inv_adjust at acct's
-- variance-only flavor) carry only one side. doc_chrono is the per-
-- document chronological key carried verbatim from acct's WAC-replay
-- pattern.
--
-- posting_line_inventory holds qty-side detail keyed by (posting_line,
-- sku, location); layer_id is nullable because STD/AVG consumptions
-- aren't layer-attributed (only FIFO is).

CREATE TABLE poc_v21_posting_lines (
    posting_line_id BIGSERIAL PRIMARY KEY,
    business_date   DATE        NOT NULL,
    doc_chrono      BIGINT      NOT NULL,
    document_id     BIGINT      NOT NULL,
    sub_priority    INT         NOT NULL DEFAULT 0,
    event_type      TEXT        NOT NULL,
    amount          BIGINT      NOT NULL,
    debit_account   BIGINT,
    credit_account  BIGINT,
    correlation_id  UUID        NOT NULL,
    user_tx_xid     xid8        NOT NULL,
    committer_tx_id BIGINT      NOT NULL,
    superbatch_id   BIGINT      NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX poc_v21_posting_lines_chrono
    ON poc_v21_posting_lines (business_date, doc_chrono);
CREATE INDEX poc_v21_posting_lines_correlation
    ON poc_v21_posting_lines (correlation_id);

CREATE TABLE poc_v21_posting_line_inventory (
    posting_line_id BIGINT NOT NULL REFERENCES poc_v21_posting_lines(posting_line_id),
    sku_id          BIGINT NOT NULL,
    location_id     BIGINT NOT NULL,
    qty             BIGINT NOT NULL,
    layer_id        BIGINT,
    PRIMARY KEY (posting_line_id, sku_id, location_id)
);
