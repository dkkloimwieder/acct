-- acct-zm69 / zm69.h6-followup — Candidate H + extension (shmem-mediated
-- invariant enforcement).
--
-- Tables `cost_layers_h_ext` + `cost_consumptions_h_ext` mirror the mig
-- 0026 H schema but carry NO deferred constraint trigger — invariant
-- enforcement is delegated to the `ledger_extension`'s h_arena module
-- (per-group `effective_qty` shmem rollup; see
-- poc/ledger-extension/src/h_arena.rs).
--
-- The wrapper `post_batch_h_ext` performs set-based INSERTs into the
-- durable tables AND aggregates per-group signed deltas, calling
-- `h_apply_delta(group_id, net_delta)` once per touched group. The
-- extension's XactCallback applies under shmem CAS at PRE_COMMIT,
-- raising on invariant violation.
--
-- ## Why this might solve the batched-H scaling problem
--
-- Pure-SQL H (mig 0026 + 0027) failed at batch=1000 because SSI's
-- predicate-read tracking on the deferred SUM trigger amplified
-- conflicts to 70-74% abort rate. See
-- `bench/results-h-batched-2026-05-14.md`.
--
-- This design moves the invariant check OUT of MVCC/SSI. Durable
-- inserts to *_ext tables carry no predicate reads (just inserts of
-- disjoint rows). The invariant check runs in shmem at PRE_COMMIT via
-- atomic CAS — concurrent backends serialize through CAS on the same
-- group's cell, not through SSI's predicate-dependency graph.

CREATE TABLE cost_layers_h_ext (
    layer_id BIGSERIAL PRIMARY KEY,
    layer_group_id BIGINT NOT NULL,
    qty BIGINT NOT NULL,
    unit_cost BIGINT NOT NULL,
    born_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    source_kind TEXT NOT NULL
);
CREATE INDEX cost_layers_h_ext_group_idx ON cost_layers_h_ext (layer_group_id);

CREATE TABLE cost_consumptions_h_ext (
    consumption_id BIGSERIAL PRIMARY KEY,
    layer_group_id BIGINT NOT NULL,
    qty BIGINT NOT NULL CHECK (qty > 0),
    unit_cost BIGINT NOT NULL,
    consumed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX cost_consumptions_h_ext_group_idx ON cost_consumptions_h_ext (layer_group_id);

-- No trigger; extension handles invariant.

CREATE OR REPLACE FUNCTION post_batch_h_ext(p_envelopes JSONB)
RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
    v_group RECORD;
BEGIN
    -- Durable inserts. Pure inserts; no predicate reads.
    INSERT INTO cost_layers_h_ext (layer_group_id, qty, unit_cost, source_kind)
    SELECT
        (e->>'layer_group_id')::BIGINT,
        (e->>'qty')::BIGINT,
        COALESCE((e->>'unit_cost')::BIGINT, 100),
        'receipt'
    FROM jsonb_array_elements(p_envelopes) e
    WHERE e->>'kind' = 'receipt';

    INSERT INTO cost_consumptions_h_ext (layer_group_id, qty, unit_cost)
    SELECT
        (e->>'layer_group_id')::BIGINT,
        (e->>'qty')::BIGINT,
        COALESCE((e->>'unit_cost')::BIGINT, 100)
    FROM jsonb_array_elements(p_envelopes) e
    WHERE e->>'kind' = 'issue';

    -- Per-group signed net delta. Call extension once per touched
    -- group to stage in per-backend PENDING; PRE_COMMIT applies +
    -- checks under shmem CAS.
    FOR v_group IN
        SELECT
            layer_group_id::BIGINT AS gid,
            SUM(signed_qty)::BIGINT AS net_delta
        FROM (
            SELECT
                (e->>'layer_group_id')::BIGINT AS layer_group_id,
                CASE WHEN e->>'kind' = 'receipt' THEN  (e->>'qty')::BIGINT
                     WHEN e->>'kind' = 'issue'   THEN -(e->>'qty')::BIGINT
                     ELSE 0 END AS signed_qty
              FROM jsonb_array_elements(p_envelopes) e
        ) s
        GROUP BY layer_group_id
    LOOP
        PERFORM h_apply_delta(v_group.gid, v_group.net_delta);
    END LOOP;
END$$;
