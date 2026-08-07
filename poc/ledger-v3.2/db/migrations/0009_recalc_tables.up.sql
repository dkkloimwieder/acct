-- recalc-d §4/§5 — recalc state / bookkeeping tables (D3 resolved + the
-- generation-delta idempotency model, Model 1).
--
-- These are pool-side authoritative-state bookkeeping. The durable STREAM
-- cursor is the logical-decoding slot's confirmed_flush_lsn (design-v3.1 §17):
-- the slot says "what has been delivered in commit order"; these tables say
-- "what has been authoritatively costed, and to which recalc generation". The
-- two are complementary, never merged. Written only by the recalc engine.

-- D3: per-consumed-layer linkage. A depletion can consume multiple layers (the
-- layer-walk's `while remaining > 0`), which the single
-- trx_line.source_trx_line_id cannot represent. The linkage IS the cost basis:
-- a depletion's authoritative unit_cost is banker_div(Σ qty·unit_cost, Σ qty)
-- over its generation-N rows. Generation-keying makes the marking history
-- append-only: an R-2 re-cost writes a fresh generation without deleting the
-- prior one. The GL adjustment stays ONE cost_adjustment_line per depletion
-- (the net delta); the per-layer detail lives here.
CREATE TABLE cost_layer_consumption (
    depletion_trx_line_id  BIGINT NOT NULL REFERENCES trx_line(id),
    -- The receipt whose layer was consumed.
    layer_trx_line_id      BIGINT NOT NULL REFERENCES trx_line(id),
    -- Which recalc pass produced this row.
    recalc_generation      BIGINT NOT NULL,
    -- Qty taken from this layer: always a positive draw.
    qty                    BIGINT NOT NULL CHECK (qty > 0),
    -- The layer's cost (from a receipt, so non-negative like trx_line.unit_cost).
    unit_cost              BIGINT NOT NULL CHECK (unit_cost >= 0),
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (depletion_trx_line_id, layer_trx_line_id, recalc_generation)
);

-- Per-pool settlement state. recalc_generation is the monotonic counter every
-- authoritative write is stamped with; a pass's writes commit in ONE
-- transaction that bumps it as its last act (the generation-delta re-run
-- contract, recalc-d §5). recost_floor_* implements R-2: a backdated receipt
-- delivered behind settled_through_* lowers the floor; the next pass replays
-- the pool from <= floor, then clears it and advances settled_through_*.
CREATE TABLE pool_settlement (
    pool_id                   BIGINT PRIMARY KEY REFERENCES pool(id),
    recalc_generation         BIGINT NOT NULL DEFAULT 0,
    -- Business-time frontier costed authoritatively (+ within-date id tiebreak).
    settled_through_posted_at TIMESTAMPTZ,
    settled_through_id        BIGINT,
    -- Earliest position needing re-cost (R-2); NULL = none.
    recost_floor_posted_at    TIMESTAMPTZ,
    recost_floor_id           BIGINT,
    last_recalc_at            TIMESTAMPTZ,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Per-depletion-per-generation authoritative record (append-only): the "current"
-- authoritative cost is the max-generation row. prior_unit_cost is the gen-N-1
-- authoritative (or the observed trx_line.unit_cost for N = 1); the GL
-- adjustment posted is the delta (authoritative_N - prior) x qty — zero delta
-- posts nothing (adjustment_posting_line_id NULL), which is what makes repeated
-- and no-op passes idempotent (recalc-d §5).
CREATE TABLE cost_settlement (
    depletion_trx_line_id      BIGINT NOT NULL REFERENCES trx_line(id),
    recalc_generation          BIGINT NOT NULL,
    -- banker_div over this generation's cost_layer_consumption rows.
    authoritative_unit_cost    BIGINT NOT NULL,
    prior_unit_cost            BIGINT NOT NULL,
    adjustment_posting_line_id BIGINT REFERENCES posting_line(id),
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (depletion_trx_line_id, recalc_generation)
);
