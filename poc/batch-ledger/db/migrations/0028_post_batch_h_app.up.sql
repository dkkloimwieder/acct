-- acct-zm69 / zm69.h6 — Batched-H alternative: explicit batch-check
-- (statement-equivalent) wrapper `post_batch_h_app`.
--
-- ## Why this exists
--
-- The mig 0026 schema relies on `DEFERRABLE INITIALLY DEFERRED`
-- constraint trigger fired FOR EACH ROW. PG forbids DEFERRABLE
-- constraint triggers from being FOR EACH STATEMENT, so we can't
-- directly compare "N row fires" vs "1 statement fire" via just a
-- trigger swap.
--
-- The architectural equivalent: an UNTRIGGERED table pair plus a
-- wrapper that performs an explicit set-based batch check at the
-- end of the plpgsql function — `O(distinct touched groups)` SUM
-- pairs instead of `O(rows inserted)` SUM pairs. Sound under
-- SERIALIZABLE because the check is part of the same txn; SSI
-- still catches concurrent write-skew.
--
-- This is NOT a production H replacement — it leaks invariant
-- protection if a caller bypasses post_batch_h_app and INSERTs
-- directly. The deferred trigger on mig 0026 is structurally
-- safer. This is a PERFORMANCE-CEILING PROBE to characterize how
-- much of post_batch_h's commit-time cost is per-row trigger fires.

CREATE TABLE cost_layers_h_app (
    layer_id BIGSERIAL PRIMARY KEY,
    layer_group_id BIGINT NOT NULL,
    qty BIGINT NOT NULL,
    unit_cost BIGINT NOT NULL,
    born_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    source_kind TEXT NOT NULL
);
CREATE INDEX cost_layers_h_app_group_idx ON cost_layers_h_app (layer_group_id);

CREATE TABLE cost_consumptions_h_app (
    consumption_id BIGSERIAL PRIMARY KEY,
    layer_group_id BIGINT NOT NULL,
    qty BIGINT NOT NULL CHECK (qty > 0),
    unit_cost BIGINT NOT NULL,
    consumed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX cost_consumptions_h_app_group_idx ON cost_consumptions_h_app (layer_group_id);

-- No trigger; wrapper handles validation.

CREATE OR REPLACE FUNCTION post_batch_h_app(p_envelopes JSONB)
RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
    v_bad_count BIGINT;
BEGIN
    INSERT INTO cost_layers_h_app (layer_group_id, qty, unit_cost, source_kind)
    SELECT
        (e->>'layer_group_id')::BIGINT,
        (e->>'qty')::BIGINT,
        COALESCE((e->>'unit_cost')::BIGINT, 100),
        'receipt'
    FROM jsonb_array_elements(p_envelopes) e
    WHERE e->>'kind' = 'receipt';

    INSERT INTO cost_consumptions_h_app (layer_group_id, qty, unit_cost)
    SELECT
        (e->>'layer_group_id')::BIGINT,
        (e->>'qty')::BIGINT,
        COALESCE((e->>'unit_cost')::BIGINT, 100)
    FROM jsonb_array_elements(p_envelopes) e
    WHERE e->>'kind' = 'issue';

    -- Set-based aggregate check: scan only the groups touched in this
    -- batch (extracted from the envelope JSONB). One SUM pair per
    -- distinct group, not per row inserted.
    WITH touched AS (
        SELECT DISTINCT (e->>'layer_group_id')::BIGINT AS layer_group_id
          FROM jsonb_array_elements(p_envelopes) e
         WHERE e->>'kind' = 'issue'
    ),
    bad AS (
        SELECT t.layer_group_id
          FROM touched t
         WHERE
              COALESCE((SELECT SUM(qty) FROM cost_layers_h_app       WHERE layer_group_id = t.layer_group_id), 0)
            < COALESCE((SELECT SUM(qty) FROM cost_consumptions_h_app WHERE layer_group_id = t.layer_group_id), 0)
    )
    SELECT COUNT(*) INTO v_bad_count FROM bad;

    IF v_bad_count > 0 THEN
        RAISE EXCEPTION 'over-consumption in batch: % group(s) violated', v_bad_count
            USING ERRCODE = '40001';
    END IF;
END$$;
