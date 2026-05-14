-- acct-oh6l / zm69.h8 — Path 1 fix: process issues sorted by
-- layer_group_id to eliminate cross-group FOR UPDATE deadlocks.
--
-- ## What broke in mig 0030
--
-- The original wrapper iterated `jsonb_array_elements(p_envelopes)` in
-- array order — random per worker. Each issue acquires FOR UPDATE
-- locks on its group's layers. With multi-group batches and random
-- issue orderings, workers grab group-level lock prefixes in
-- incompatible orders → deadlock cycles. Correctness probe with 16
-- workers × 5 batches × 5 issues across 4 groups produced 90%
-- deadlock loss (8/80 batches committed).
--
-- ## Fix
--
-- Sort the issue iteration by `layer_group_id`. All workers acquire
-- group prefixes in the same order → no cross-group deadlock cycles.
-- Within a group, FOR UPDATE walks layers in `(born_at, layer_id)`
-- order, which is also globally consistent. Net: no deadlocks under
-- any concurrent shape that doesn't also hit per-row FOR UPDATE
-- contention from multiple workers on the same hottest layer (which
-- serializes via wait, not deadlock).

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
    -- 1. Bulk receipt-side inserts.
    INSERT INTO cost_layers_h_ext (layer_group_id, qty, qty_remaining, unit_cost, source_kind)
    SELECT
        (e->>'layer_group_id')::BIGINT,
        (e->>'qty')::BIGINT,
        (e->>'qty')::BIGINT,
        COALESCE((e->>'unit_cost')::BIGINT, 100),
        'receipt'
    FROM jsonb_array_elements(p_envelopes) e
    WHERE e->>'kind' = 'receipt';

    -- 2. Per-issue FIFO walk under FOR UPDATE — sorted by group_id
    -- to give all concurrent backends a globally-consistent lock
    -- acquisition order.
    FOR v_env IN
        SELECT
            (e->>'layer_group_id')::BIGINT AS layer_group_id,
            (e->>'qty')::BIGINT AS qty
          FROM jsonb_array_elements(p_envelopes) e
         WHERE e->>'kind' = 'issue'
         ORDER BY (e->>'layer_group_id')::BIGINT
    LOOP
        v_group_id := v_env.layer_group_id;
        v_qty := v_env.qty;

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

        UPDATE cost_consumptions_h_ext
           SET unit_cost = v_total_cost / NULLIF(v_qty, 0)
         WHERE consumption_id = v_consumption_id;
    END LOOP;

    -- 3. Belt + braces: per-group net delta into h_arena.
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
        ORDER BY layer_group_id
    LOOP
        PERFORM h_apply_delta(v_env.gid, v_env.net_delta);
    END LOOP;
END$$;
