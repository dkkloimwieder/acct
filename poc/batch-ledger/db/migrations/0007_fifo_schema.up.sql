-- acct-1hps (P5 of acct-qdp5 PoC).
--
-- FIFO cost layers. Each receipt creates one cost_layers row. Each issue
-- walks layers in (pool_account_id, receipt_date ASC, id ASC) order,
-- creating cost_layer_depletions rows for the consumed slices. The pool's
-- account.balance + account.qty are still maintained (so the WAC-style
-- account state stays valid alongside the FIFO ledger).

CREATE TABLE cost_layers (
    id                  BIGSERIAL PRIMARY KEY,
    pool_account_id     BIGINT NOT NULL REFERENCES accounts(id),
    qty_remaining       BIGINT NOT NULL,
    unit_cost           BIGINT NOT NULL,
    receipt_date        DATE   NOT NULL,
    received_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    receipt_posting_line_id BIGINT NULL REFERENCES posting_lines(id),
    CHECK (qty_remaining >= 0),
    CHECK (unit_cost > 0)
);

CREATE INDEX cost_layers_pool_active_idx
    ON cost_layers (pool_account_id, receipt_date, id)
    WHERE qty_remaining > 0;

CREATE TABLE cost_layer_depletions (
    id                  BIGSERIAL PRIMARY KEY,
    layer_id            BIGINT NOT NULL REFERENCES cost_layers(id),
    issue_posting_line_id BIGINT NOT NULL REFERENCES posting_lines(id),
    qty_consumed        BIGINT NOT NULL,
    cost_amount         BIGINT NOT NULL,
    issued_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (qty_consumed > 0)
);

CREATE INDEX cost_layer_depletions_layer_idx ON cost_layer_depletions (layer_id);
CREATE INDEX cost_layer_depletions_issue_idx ON cost_layer_depletions (issue_posting_line_id);
