-- acct-oh6l / zm69.h8 — Path 1: FOR UPDATE layer walk for FIFO cost
-- attribution under H+ext.
--
-- ## What this adds
--
-- 1. `qty_remaining` column on `cost_layers_h_ext` — mutable (decremented
--    as depletions land). Backfill = qty (no depletions yet at mig time).
-- 2. `cost_layer_depletions_h_ext` table — per-layer attribution audit
--    trail. (layer_id, consumption_id, qty_consumed, cost_amount).
-- 3. `post_batch_h_ext_fifo_walk` wrapper: receipt-side bulk insert
--    (same as post_batch_h_ext); issue-side per-envelope FIFO walk under
--    `SELECT ... FOR UPDATE` on cost_layers_h_ext rows.
-- 4. `fifo_overconsume_check_h_ext()` — recon detector analogous to
--    A2's `fifo_overconsume_check` from acct-a3rj Phase B.
--
-- ## Why this might solve the layer attribution race
--
-- Two concurrent backends both want to walk FIFO on the same group. The
-- FOR UPDATE on cost_layers_h_ext rows serializes them at the row level:
-- backend A acquires lock on L1, walks down, decrements qty_remaining,
-- writes depletion row. Backend B sees L1 with the updated
-- qty_remaining (committed-via-FOR-UPDATE), continues from there. No
-- double-attribution.
--
-- Cost: A2-style row-level contention re-enters via FOR UPDATE on layer
-- rows. The shmem CAS qty invariant still runs as a redundant safety net
-- (belt + braces). Throughput expected to be in the same band as
-- post_batch_fifo / post_batch_fifo_maximal (mig 0020/0021), which were
-- in the ~400-1000 transfers/s range under contention.

ALTER TABLE cost_layers_h_ext ADD COLUMN qty_remaining BIGINT;
UPDATE cost_layers_h_ext SET qty_remaining = qty;
ALTER TABLE cost_layers_h_ext ALTER COLUMN qty_remaining SET NOT NULL;
ALTER TABLE cost_layers_h_ext ADD CONSTRAINT cost_layers_h_ext_qty_remaining_nonneg
    CHECK (qty_remaining >= 0);

CREATE TABLE cost_layer_depletions_h_ext (
    depletion_id BIGSERIAL PRIMARY KEY,
    layer_id BIGINT NOT NULL,        -- soft pointer to cost_layers_h_ext.layer_id
    consumption_id BIGINT NOT NULL,  -- soft pointer to cost_consumptions_h_ext.consumption_id
    qty_consumed BIGINT NOT NULL CHECK (qty_consumed > 0),
    cost_amount BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX cost_layer_depletions_h_ext_layer_idx
    ON cost_layer_depletions_h_ext (layer_id);
CREATE INDEX cost_layer_depletions_h_ext_consumption_idx
    ON cost_layer_depletions_h_ext (consumption_id);

CREATE OR REPLACE FUNCTION post_batch_h_ext_fifo_walk(p_envelopes JSONB)
RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
    v_env RECORD;
    v_layer RECORD;
    v_group_id BIGINT;
    v_qty BIGINT;
    v_remaining BIGINT;
    v_take BIGINT;
    v_total_cost BIGINT;
    v_consumption_id BIGINT;
BEGIN
    -- 1. Bulk receipt-side inserts (no FIFO walk needed for receipts).
    INSERT INTO cost_layers_h_ext (layer_group_id, qty, qty_remaining, unit_cost, source_kind)
    SELECT
        (e->>'layer_group_id')::BIGINT,
        (e->>'qty')::BIGINT,
        (e->>'qty')::BIGINT,
        COALESCE((e->>'unit_cost')::BIGINT, 100),
        'receipt'
    FROM jsonb_array_elements(p_envelopes) e
    WHERE e->>'kind' = 'receipt';

    -- 2. Per-issue FIFO walk under FOR UPDATE.
    FOR v_env IN
        SELECT
            (e->>'layer_group_id')::BIGINT AS layer_group_id,
            (e->>'qty')::BIGINT AS qty
          FROM jsonb_array_elements(p_envelopes) e
         WHERE e->>'kind' = 'issue'
    LOOP
        v_group_id := v_env.layer_group_id;
        v_qty := v_env.qty;

        -- Insert consumption row first (so depletions can reference it).
        -- unit_cost placeholder; computed below from layer walk.
        INSERT INTO cost_consumptions_h_ext (layer_group_id, qty, unit_cost)
        VALUES (v_group_id, v_qty, 0)
        RETURNING consumption_id INTO v_consumption_id;

        v_remaining := v_qty;
        v_total_cost := 0;

        FOR v_layer IN
            SELECT layer_id, unit_cost, qty_remaining
              FROM cost_layers_h_ext
             WHERE layer_group_id = v_group_id
               AND qty_remaining > 0
             ORDER BY born_at, layer_id
               FOR UPDATE
        LOOP
            EXIT WHEN v_remaining = 0;
            v_take := LEAST(v_remaining, v_layer.qty_remaining);

            INSERT INTO cost_layer_depletions_h_ext
                (layer_id, consumption_id, qty_consumed, cost_amount)
            VALUES
                (v_layer.layer_id, v_consumption_id, v_take, v_take * v_layer.unit_cost);

            UPDATE cost_layers_h_ext
               SET qty_remaining = qty_remaining - v_take
             WHERE layer_id = v_layer.layer_id;

            v_remaining := v_remaining - v_take;
            v_total_cost := v_total_cost + (v_take * v_layer.unit_cost);
        END LOOP;

        IF v_remaining > 0 THEN
            RAISE EXCEPTION 'fifo_walk_overconsume: group=% short by % qty', v_group_id, v_remaining
                USING ERRCODE = '40001';
        END IF;

        -- Stamp the weighted unit_cost on the consumption row.
        UPDATE cost_consumptions_h_ext
           SET unit_cost = v_total_cost / NULLIF(v_qty, 0)
         WHERE consumption_id = v_consumption_id;
    END LOOP;

    -- 3. Belt + braces: stage per-group net delta into h_arena. Catches
    -- bugs in the FOR UPDATE walk where decrement + depletion go out of
    -- sync. PRE_COMMIT compares against shmem; raises if invariant
    -- violated.
    FOR v_env IN
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
        PERFORM h_apply_delta(v_env.gid, v_env.net_delta);
    END LOOP;
END$$;

-- ─── Per-layer recon detector ─────────────────────────────────────────
--
-- Analog of acct-a3rj Phase B's `fifo_overconsume_check`. Returns one
-- row per layer where `SUM(depletions.qty_consumed) > layer.qty`.
-- After a clean concurrent FIFO bench this MUST return 0 rows.

CREATE OR REPLACE FUNCTION fifo_overconsume_check_h_ext()
RETURNS TABLE (
    layer_id BIGINT,
    layer_group_id BIGINT,
    qty_received BIGINT,
    total_consumed BIGINT,
    overshoot BIGINT
)
LANGUAGE sql STABLE AS $$
    SELECT
        l.layer_id,
        l.layer_group_id,
        l.qty,
        COALESCE(SUM(d.qty_consumed), 0)::BIGINT,
        (COALESCE(SUM(d.qty_consumed), 0) - l.qty)::BIGINT
      FROM cost_layers_h_ext l
      LEFT JOIN cost_layer_depletions_h_ext d ON d.layer_id = l.layer_id
     GROUP BY l.layer_id, l.layer_group_id, l.qty
    HAVING COALESCE(SUM(d.qty_consumed), 0) > l.qty
     ORDER BY l.layer_id;
$$;
