-- design-v3.1 §2.3 — journal-side tables (indexes live in 0005).
-- posting_line writers: the strict-method hot paths (WAC/STD/specific — final
-- cost directly, design-v3.2 §1), the recalc engine (cost_adjustment deltas,
-- recalc-d §5), and close-time finalize (recalc-e). FIFO/LIFO hot-path appends
-- post no cost leg (alt C, design-v3.1 §16). Dimensions optional.

CREATE TABLE posting_line (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trx_line_id     BIGINT NOT NULL REFERENCES trx_line(id),
    event_type      posting_event_type NOT NULL,
    amount          BIGINT NOT NULL,
    debit_account   BIGINT NOT NULL REFERENCES account(id),
    credit_account  BIGINT NOT NULL REFERENCES account(id),
    posted_at       TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE posting_line_dimension (
    posting_line_id  BIGINT NOT NULL REFERENCES posting_line(id),
    dimension_type   dimension_type NOT NULL,
    dimension_id     BIGINT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (posting_line_id, dimension_type)
);
