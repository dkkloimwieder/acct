-- acct-1grr / zm69.h9 — Path 2 fix: drop the durable
-- `UPDATE cost_layers_h_ext SET qty_remaining = qty_remaining - v_take`
-- inside `post_batch_h_ext_layer_shmem`.
--
-- ## What broke in mig 0032
--
-- The wrapper called `h_layer_decrement` (atomic CAS — fast, lock-free)
-- AND then issued `UPDATE cost_layers_h_ext SET qty_remaining = ...`
-- on the same layer row. That UPDATE acquires Postgres' row-level write
-- lock — the SAME serialization that kills path 1. So path 2 paid both
-- costs: the shmem CAS PLUS the row-lock UPDATE. Bench numbers were
-- nearly identical to path 1 (22.8 vs 24.1 commits/s at b=100 g=50).
--
-- ## Fix
--
-- Drop the qty_remaining UPDATE entirely. The shmem h_layer_arena cell
-- becomes the source of truth for per-layer residual. Durable
-- qty_remaining stays at its seeded value (=qty) — it's stale, but no
-- code path in the bench reads it as truth. The `fifo_overconsume_check_h_ext`
-- function (mig 0030) checks `SUM(depletions) <= qty_received` which
-- doesn't depend on qty_remaining.
--
-- Trade-off: durable qty_remaining is no longer accurate. For
-- production we'd need either (a) bgworker drain of shmem residual to
-- durable, or (b) lazy compute `qty - SUM(depletions)` on read. Mirrors
-- sw4i's shmem-vs-durable pattern.

CREATE OR REPLACE FUNCTION post_batch_h_ext_layer_shmem(p_envelopes JSONB)
RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
    v_env RECORD;
    v_group_id BIGINT;
    v_qty BIGINT;
    v_remaining BIGINT;
    v_actually_taken BIGINT;
    v_total_cost BIGINT;
    v_consumption_id BIGINT;
    v_layer_rec RECORD;
BEGIN
    -- 1. Bulk receipt inserts. Capture layer_ids; seed shmem cells.
    FOR v_env IN
        WITH inserted AS (
            INSERT INTO cost_layers_h_ext (layer_group_id, qty, qty_remaining, unit_cost, source_kind)
            SELECT
                (e->>'layer_group_id')::BIGINT,
                (e->>'qty')::BIGINT,
                (e->>'qty')::BIGINT,
                COALESCE((e->>'unit_cost')::BIGINT, 100),
                'receipt'
            FROM jsonb_array_elements(p_envelopes) e
            WHERE e->>'kind' = 'receipt'
            RETURNING layer_id, qty
        )
        SELECT layer_id, qty FROM inserted
    LOOP
        PERFORM h_layer_create(v_env.layer_id, v_env.qty);
    END LOOP;

    -- 2. Per-issue FIFO walk via shmem CAS. NO durable qty_remaining
    --    UPDATE — shmem cell is the truth for per-layer residual.
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

        FOR v_layer_rec IN
            SELECT layer_id, unit_cost
              FROM cost_layers_h_ext
             WHERE layer_group_id = v_group_id
             ORDER BY born_at, layer_id
        LOOP
            EXIT WHEN v_remaining = 0;
            v_actually_taken := h_layer_decrement(v_layer_rec.layer_id, v_remaining);
            IF v_actually_taken = 0 THEN
                CONTINUE;
            END IF;

            INSERT INTO cost_layer_depletions_h_ext
                (layer_id, consumption_id, qty_consumed, cost_amount)
            VALUES
                (v_layer_rec.layer_id, v_consumption_id,
                 v_actually_taken, v_actually_taken * v_layer_rec.unit_cost);

            v_remaining := v_remaining - v_actually_taken;
            v_total_cost := v_total_cost + (v_actually_taken * v_layer_rec.unit_cost);
        END LOOP;

        IF v_remaining > 0 THEN
            RAISE EXCEPTION 'layer_shmem_overconsume: group=% short by % qty', v_group_id, v_remaining
                USING ERRCODE = '40001';
        END IF;

        UPDATE cost_consumptions_h_ext
           SET unit_cost = v_total_cost / NULLIF(v_qty, 0)
         WHERE consumption_id = v_consumption_id;
    END LOOP;

    -- 3. Belt + braces: group-level invariant in h_arena.
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
