-- acct-xida / zm69.h10 — Path 3: deferred per-layer FIFO attribution.
--
-- Hot path writes flat consumption rows. Per-layer depletion attribution
-- happens later via `drain_deferred_fifo()`. Hot path doesn't touch
-- cost_layer_depletions_h_ext or cost_layers_h_ext.qty_remaining; it
-- only knows the group-level invariant (via h_arena) and the
-- consumption qty.
--
-- ## Schema addition
--
-- `cost_consumptions_h_ext.fifo_processed_at TIMESTAMPTZ NULL` — drain
-- watermark. NULL = pending attribution; non-NULL = drained.
--
-- ## Hot-path wrapper `post_batch_h_ext_deferred`
--
-- 1. Bulk receipt INSERTs into cost_layers_h_ext.
-- 2. Bulk consumption INSERTs into cost_consumptions_h_ext with NULL
--    fifo_processed_at and unit_cost=0 placeholder.
-- 3. Group-level invariant via h_arena (h_apply_delta).
-- No FIFO walk; no depletion rows; no qty_remaining mutation in hot path.
--
-- ## Drain `drain_deferred_fifo()`
--
-- Single-writer-safe function (use advisory lock to serialize concurrent
-- callers). For each pending consumption row in (consumed_at, consumption_id)
-- order:
--   - Walk layers of its group in FIFO (born_at, layer_id) order.
--   - For each layer with qty_remaining > 0, take min(remaining, layer.qty_remaining).
--   - INSERT cost_layer_depletions_h_ext + UPDATE cost_layers_h_ext.qty_remaining.
--   - Stamp consumption.unit_cost = total/qty + fifo_processed_at = now.
-- Raises if a consumption can't be fully attributed (invariant violation
-- that the hot path's h_arena check should have prevented).

ALTER TABLE cost_consumptions_h_ext
    ADD COLUMN fifo_processed_at TIMESTAMPTZ NULL;
CREATE INDEX cost_consumptions_h_ext_pending_idx
    ON cost_consumptions_h_ext (consumed_at, consumption_id)
    WHERE fifo_processed_at IS NULL;

CREATE OR REPLACE FUNCTION post_batch_h_ext_deferred(p_envelopes JSONB)
RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
    v_env RECORD;
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

    -- 2. Bulk consumption-side inserts. fifo_processed_at NULL = pending.
    -- unit_cost placeholder = 0; drain stamps the real value.
    INSERT INTO cost_consumptions_h_ext (layer_group_id, qty, unit_cost)
    SELECT
        (e->>'layer_group_id')::BIGINT,
        (e->>'qty')::BIGINT,
        0
    FROM jsonb_array_elements(p_envelopes) e
    WHERE e->>'kind' = 'issue';

    -- 3. Per-group net delta into h_arena.
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

-- ── Drainer ───────────────────────────────────────────────────────
--
-- Returns: count of consumption rows attributed.
-- Uses pg_advisory_xact_lock(817261001) to serialize concurrent callers
-- — single-drainer pattern (multi-drainer is acct-xida-followup).

CREATE OR REPLACE FUNCTION drain_deferred_fifo(p_batch_size INT DEFAULT 1000)
RETURNS BIGINT
LANGUAGE plpgsql AS $$
DECLARE
    v_consumption RECORD;
    v_layer RECORD;
    v_remaining BIGINT;
    v_take BIGINT;
    v_total_cost BIGINT;
    v_attributed BIGINT := 0;
BEGIN
    PERFORM pg_advisory_xact_lock(817261001);

    FOR v_consumption IN
        SELECT consumption_id, layer_group_id, qty
          FROM cost_consumptions_h_ext
         WHERE fifo_processed_at IS NULL
         ORDER BY consumed_at, consumption_id
         LIMIT p_batch_size
    LOOP
        v_remaining := v_consumption.qty;
        v_total_cost := 0;

        FOR v_layer IN
            SELECT layer_id, unit_cost, qty_remaining
              FROM cost_layers_h_ext
             WHERE layer_group_id = v_consumption.layer_group_id
               AND qty_remaining > 0
             ORDER BY born_at, layer_id
               FOR UPDATE
        LOOP
            EXIT WHEN v_remaining = 0;
            v_take := LEAST(v_remaining, v_layer.qty_remaining);

            INSERT INTO cost_layer_depletions_h_ext
                (layer_id, consumption_id, qty_consumed, cost_amount)
            VALUES
                (v_layer.layer_id, v_consumption.consumption_id,
                 v_take, v_take * v_layer.unit_cost);

            UPDATE cost_layers_h_ext
               SET qty_remaining = qty_remaining - v_take
             WHERE layer_id = v_layer.layer_id;

            v_remaining := v_remaining - v_take;
            v_total_cost := v_total_cost + (v_take * v_layer.unit_cost);
        END LOOP;

        IF v_remaining > 0 THEN
            RAISE EXCEPTION 'drain_deferred_fifo: consumption=% short by % (h_arena invariant breach?)',
                v_consumption.consumption_id, v_remaining;
        END IF;

        UPDATE cost_consumptions_h_ext
           SET unit_cost = v_total_cost / NULLIF(v_consumption.qty, 0),
               fifo_processed_at = clock_timestamp()
         WHERE consumption_id = v_consumption.consumption_id;

        v_attributed := v_attributed + 1;
    END LOOP;

    RETURN v_attributed;
END$$;
