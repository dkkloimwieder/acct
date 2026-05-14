-- acct-1grr / zm69.h9 — Path 2: per-layer shmem residual via h_layer_arena.
--
-- Wrapper writes durable INSERTs (same audit-trail as path 1) but
-- skips FOR UPDATE: per-layer residual lives in shmem, CAS-decremented
-- per layer by the extension's `h_layer_decrement`. Lock-free; CAS
-- contention resolves at memory-bus speed.
--
-- Layer enumeration ORDER still comes from the durable
-- `cost_layers_h_ext` (ORDER BY born_at, layer_id). The wrapper
-- pre-fetches an array of (layer_id, unit_cost) per touched group
-- before the issue loop to avoid one SQL query per issue.

CREATE OR REPLACE FUNCTION post_batch_h_ext_layer_shmem(p_envelopes JSONB)
RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
    v_env RECORD;
    v_group_id BIGINT;
    v_qty BIGINT;
    v_remaining BIGINT;
    v_take BIGINT;
    v_actually_taken BIGINT;
    v_total_cost BIGINT;
    v_consumption_id BIGINT;
    v_new_layer_id BIGINT;
    v_layer_rec RECORD;
BEGIN
    -- 1. Bulk receipt inserts. Capture layer_ids; seed shmem cells
    --    via h_layer_create so this-txn issues can see them.
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

    -- 2. Per-issue FIFO walk via shmem CAS. Issues processed sorted by
    --    layer_group_id (consistent ordering across backends; matches
    --    path 1's deadlock-avoidance pattern even though there's no
    --    FOR UPDATE here — keeps shapes parallel).
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

        -- Enumerate layers of this group in FIFO order. Filter to
        -- shmem-non-empty via h_layer_lookup so we skip drained layers
        -- without a CAS roundtrip.
        FOR v_layer_rec IN
            SELECT layer_id, unit_cost
              FROM cost_layers_h_ext
             WHERE layer_group_id = v_group_id
             ORDER BY born_at, layer_id
        LOOP
            EXIT WHEN v_remaining = 0;
            -- Atomic CAS-decrement. Returns actual qty taken (0..=v_remaining).
            v_actually_taken := h_layer_decrement(v_layer_rec.layer_id, v_remaining);
            IF v_actually_taken = 0 THEN
                CONTINUE;
            END IF;

            INSERT INTO cost_layer_depletions_h_ext
                (layer_id, consumption_id, qty_consumed, cost_amount)
            VALUES
                (v_layer_rec.layer_id, v_consumption_id,
                 v_actually_taken, v_actually_taken * v_layer_rec.unit_cost);

            -- Mirror to durable qty_remaining for compat with the
            -- recon function fifo_overconsume_check_h_ext.
            UPDATE cost_layers_h_ext
               SET qty_remaining = qty_remaining - v_actually_taken
             WHERE layer_id = v_layer_rec.layer_id;

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
