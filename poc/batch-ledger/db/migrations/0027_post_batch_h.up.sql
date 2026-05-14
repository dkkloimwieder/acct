-- acct-zm69 / zm69.h6 — Batched-H wrapper `post_batch_h`.
--
-- Set-based INSERT wrapper for the H schema (cost_layers_h +
-- cost_consumptions_h, mig 0026). Mirrors A2's per-batch shape:
-- one plpgsql call per txn, N envelopes per call. NO per-envelope
-- FOR LOOP, NO jsonb_set state mutation — two set-based INSERTs
-- via jsonb_array_elements + WHERE filter.
--
-- The deferred FOR-EACH-ROW trigger on cost_consumptions_h fires at
-- COMMIT — N issue rows = N trigger fires = 2N SUM(qty) calls.
-- This is the load-bearing perf question characterized by
-- bench_h_batched: does the per-row deferred trigger dominate cost
-- at batch=1000?
--
-- See poc/ledger-extension/docs/fifo-arena-correctness-audit-2026-05-14.md
-- §Candidate H — the design under evaluation.

CREATE OR REPLACE FUNCTION post_batch_h(p_envelopes JSONB)
RETURNS VOID
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO cost_layers_h (layer_group_id, qty, unit_cost, source_kind)
    SELECT
        (e->>'layer_group_id')::BIGINT,
        (e->>'qty')::BIGINT,
        COALESCE((e->>'unit_cost')::BIGINT, 100),
        'receipt'
    FROM jsonb_array_elements(p_envelopes) e
    WHERE e->>'kind' = 'receipt';

    INSERT INTO cost_consumptions_h (layer_group_id, qty, unit_cost)
    SELECT
        (e->>'layer_group_id')::BIGINT,
        (e->>'qty')::BIGINT,
        COALESCE((e->>'unit_cost')::BIGINT, 100)
    FROM jsonb_array_elements(p_envelopes) e
    WHERE e->>'kind' = 'issue';
END$$;
