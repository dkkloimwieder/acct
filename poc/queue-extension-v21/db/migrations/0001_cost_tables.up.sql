-- M1.1 (acct-63dk) — cost-method-facing tables per spec §1.2.
--
-- poc_v21_cost_layers: FIFO layer state. Receipts (inflow) emit a layer
-- with positive qty; FIFO depletions reference layers via cost_depletions.
-- AVG receipts also write a layer (so the audit history exists) but AVG
-- consumption uses cost_consumptions + avg_pool_state instead.
--
-- poc_v21_cost_depletions: FIFO-only. Each row attributes a portion of
-- a single consumption to a single layer. One consumption against
-- multiple layers produces multiple depletion rows.
--
-- poc_v21_cost_consumptions: AVG + STD only. 1:1 with the issue event;
-- applied_unit_cost is method-specific (AVG running avg, STD standard
-- lookup at consumed_at).
--
-- born_seq / consumed_seq are committer-assigned monotonic counters
-- per pool / per layer. M1.3+ committer fills these in; at M1.1 the
-- column is plain BIGINT NOT NULL with no DB-side sequence.

CREATE TABLE poc_v21_cost_layers (
    layer_id        BIGSERIAL PRIMARY KEY,
    sku_id          BIGINT      NOT NULL,
    location_id     BIGINT      NOT NULL,
    qty             BIGINT      NOT NULL,
    unit_cost       BIGINT      NOT NULL,
    born_at         TIMESTAMPTZ NOT NULL,
    born_seq        BIGINT      NOT NULL,
    source_kind     TEXT        NOT NULL,
    source_ref      BIGINT,
    correlation_id  UUID        NOT NULL,
    user_tx_xid     xid8        NOT NULL,
    committer_tx_id BIGINT      NOT NULL,
    superbatch_id   BIGINT      NOT NULL
);
CREATE INDEX poc_v21_cost_layers_pool
    ON poc_v21_cost_layers (sku_id, location_id, born_at, born_seq);
CREATE INDEX poc_v21_cost_layers_correlation
    ON poc_v21_cost_layers (correlation_id);

CREATE TABLE poc_v21_cost_depletions (
    depletion_id    BIGSERIAL PRIMARY KEY,
    layer_id        BIGINT      NOT NULL REFERENCES poc_v21_cost_layers(layer_id),
    qty             BIGINT      NOT NULL CHECK (qty > 0),
    unit_cost       BIGINT      NOT NULL,
    consumed_at     TIMESTAMPTZ NOT NULL,
    consumed_seq    BIGINT      NOT NULL,
    issue_id        BIGINT      NOT NULL,
    method_used     TEXT        NOT NULL,
    correlation_id  UUID        NOT NULL,
    user_tx_xid     xid8        NOT NULL,
    committer_tx_id BIGINT      NOT NULL,
    superbatch_id   BIGINT      NOT NULL,
    UNIQUE (issue_id, method_used, layer_id)
);
CREATE INDEX poc_v21_cost_depletions_layer
    ON poc_v21_cost_depletions (layer_id);
CREATE INDEX poc_v21_cost_depletions_issue
    ON poc_v21_cost_depletions (issue_id);

CREATE TABLE poc_v21_cost_consumptions (
    consumption_id    BIGSERIAL PRIMARY KEY,
    sku_id            BIGINT      NOT NULL,
    location_id       BIGINT      NOT NULL,
    qty               BIGINT      NOT NULL CHECK (qty > 0),
    applied_unit_cost BIGINT      NOT NULL,
    consumed_at       TIMESTAMPTZ NOT NULL,
    consumed_seq      BIGINT      NOT NULL,
    issue_id          BIGINT      NOT NULL,
    method_used       TEXT        NOT NULL,
    correlation_id    UUID        NOT NULL,
    user_tx_xid       xid8        NOT NULL,
    committer_tx_id   BIGINT      NOT NULL,
    superbatch_id     BIGINT      NOT NULL,
    UNIQUE (issue_id, method_used)
);
CREATE INDEX poc_v21_cost_consumptions_pool
    ON poc_v21_cost_consumptions (sku_id, location_id, consumed_at);
