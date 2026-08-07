-- design-v3.1 §2.2 — cost-ledger tables (indexes live in 0005).

CREATE TABLE pool (
    id                  BIGINT PRIMARY KEY,
    sku_id              BIGINT NOT NULL,
    location_id         BIGINT NOT NULL,
    identity_key        BIGINT NOT NULL DEFAULT 0,
    method              pool_method NOT NULL,
    provisional_basis   pool_provisional_basis NOT NULL DEFAULT 'running_avg',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (sku_id, location_id, identity_key),
    CHECK (method != 'specific' OR identity_key != 0)
);

CREATE TABLE standard_cost (
    sku_id       BIGINT NOT NULL,
    location_id  BIGINT NOT NULL,
    unit_cost    BIGINT NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (sku_id, location_id)
);

-- layer_id = 0 is the aggregate row (every pool has exactly one). layer_id > 0 is a
-- materialized layer row, used only by specific pools under Path C; FIFO/LIFO under Path C
-- never materialize layers on the hot path. For layer rows, layer_id = trx_line.id of the
-- receipt that created the layer.
CREATE TABLE pool_state (
    pool_id     BIGINT NOT NULL REFERENCES pool(id),
    layer_id    BIGINT NOT NULL,
    qty         BIGINT NOT NULL,
    unit_cost   BIGINT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pool_id, layer_id)
);

CREATE TABLE pool_lock (
    pool_id     BIGINT PRIMARY KEY REFERENCES pool(id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- (trx_type, source_id) is the ledger idempotency key: re-submitting the same pair raises
-- a constraint violation rather than duplicating. This is what lets routed recovery skip
-- already-recorded work.
CREATE TABLE trx (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trx_type     trx_type NOT NULL,
    source_id    BIGINT NOT NULL,
    posted_at    TIMESTAMPTZ NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (trx_type, source_id)
);

-- source_trx_line_id is a self-reference populated only for depletions that consume a
-- specific receipt layer (FIFO/LIFO strict under Paths A/B, and specific-id under all paths).
-- Path C's hot path leaves it NULL for FIFO/LIFO depletions (provisional mode commits to no
-- source layer); recalc/close (deferred) would populate it.
CREATE TABLE trx_line (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trx_id              BIGINT NOT NULL REFERENCES trx(id),
    pool_id             BIGINT NOT NULL REFERENCES pool(id),
    line_type           line_type NOT NULL,
    source_id           BIGINT,
    qty                 BIGINT NOT NULL,
    unit_cost           BIGINT NOT NULL,
    source_trx_line_id  BIGINT REFERENCES trx_line(id),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
