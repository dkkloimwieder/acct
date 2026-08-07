-- design-v3.1 §2.2 — cost-ledger tables, under the alt-C posture (design-v3.1
-- §16 / design-v3.2 §2). Indexes live in 0005/0007.
--
-- No pool_lock table: hot-path writers lock pool_state rows directly (PG row
-- locks on the SPIKE-B single-statement / FOR UPDATE shapes, design-v3.2 §3).

CREATE TABLE pool (
    id                  BIGINT PRIMARY KEY,
    sku_id              BIGINT NOT NULL,
    location_id         BIGINT NOT NULL,
    identity_key        BIGINT NOT NULL DEFAULT 0,
    method              pool_method NOT NULL,
    -- Observed-cost basis a FIFO/LIFO depletion append records on its
    -- trx_line.unit_cost (running average vs standard). Informational under
    -- alt C: recalc assigns the authoritative cost regardless.
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

-- layer_id = 0 is the aggregate row (every stocked pool has exactly one).
-- layer_id > 0 is a materialized layer row: written by specific pools on the
-- hot path (layer_id = the receipt trx_line.id) and by the recalc engine's
-- checkpoint materialization for FIFO/LIFO (recalc-b; design-v3.2 §2).
--
-- qty and value_sum carry NO non-negativity constraints: quantity is a flagged
-- running signal, not a gate (design-v3.1 §16) — a depletion beyond on-hand
-- drives the FIFO/LIFO aggregate negative and is flagged, not rejected; the
-- conservation-invariant sweep guards the flagged negatives. Strict methods
-- (WAC/STD/specific) gate qty synchronously in their own write statements.
-- value_sum is the net posted book value: receipts add qty*cost; depletions
-- subtract the posted amount, so it stays reconcilable to Σ posting_line
-- amounts and the running-average unit_cost is DERIVED as
-- banker_div(value_sum, qty), never re-rounded incrementally (acct-0qps).
-- Banker-rounding residue can dip it negative at ±1 scale even on strict pools.
CREATE TABLE pool_state (
    pool_id     BIGINT NOT NULL REFERENCES pool(id),
    layer_id    BIGINT NOT NULL,
    qty         BIGINT NOT NULL,
    unit_cost   BIGINT NOT NULL,
    value_sum   BIGINT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pool_id, layer_id)
);

-- (trx_type, source_id) is the ledger idempotency key: re-submitting the same
-- pair raises a constraint violation rather than duplicating. Recalc adjustment
-- trxs satisfy it via cost_adjustment_id_seq (0008; recalc-d §2).
CREATE TABLE trx (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trx_type     trx_type NOT NULL,
    source_id    BIGINT NOT NULL,
    posted_at    TIMESTAMPTZ NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (trx_type, source_id)
);

-- source_trx_line_id is a self-reference populated only by specific-id
-- depletions (the consumed layer's receipt trx_line.id). FIFO/LIFO depletions
-- leave it NULL: their per-consumed-layer linkage is the recalc engine's
-- cost_layer_consumption table (0009; recalc-d §4), which supports
-- multi-layer consumption and generation-keyed re-cost history.
--
-- qty <> 0: dispatch is on the SIGN of qty, so a zero-qty line has no defined
-- direction. unit_cost >= 0: an asserted receipt cost is non-negative; zero is
-- permitted (depletion lines whose applied cost is derived from the pool, and
-- zero-cost sample/consigned receipts).
CREATE TABLE trx_line (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trx_id              BIGINT NOT NULL REFERENCES trx(id),
    pool_id             BIGINT NOT NULL REFERENCES pool(id),
    line_type           line_type NOT NULL,
    source_id           BIGINT,
    qty                 BIGINT NOT NULL CONSTRAINT trx_line_qty_nonzero CHECK (qty <> 0),
    unit_cost           BIGINT NOT NULL CONSTRAINT trx_line_unit_cost_nonneg CHECK (unit_cost >= 0),
    source_trx_line_id  BIGINT REFERENCES trx_line(id),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
