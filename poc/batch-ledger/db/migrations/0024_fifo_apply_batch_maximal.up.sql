-- acct-uy4p — FIFO maximal F (sub 2): expose fifo_apply_batch_maximal
-- under the post_batch_fifo_maximal_F name so the bench harness +
-- correctness tests can route to it via POC_BENCH_FUNCTION.
--
-- The pgrx extension's `fifo_apply_batch_maximal` does:
--
--   parse -> replay-detect SPI (no locks) -> probe cells under
--   FIFO_ARENA.share() -> EXCLUSIVE insert pass for missing keys ->
--   pre-allocate cost_layers ids via nextval (no cell locks) ->
--   lazy-seed SPI per-cell (no cell locks) -> sort cell indices
--   ascending -> acquire cells in sorted order -> FIFO walk + mutate
--   ring under cell lock -> release -> bulk SPI INSERTs
--   (posting_lines, cost_layers, cost_layer_depletions) outside locks
--   -> UPDATE cost_layers.qty_remaining for drained layers ->
--   stage_apply balance/qty deltas (existing shmem path).
--
-- Layer state lives in shmem (FIFO arena). Durable cost_layers +
-- cost_layer_depletions are written inline as the audit trail; sub 4
-- (acct-0450) moves the UPDATE cost_layers.qty_remaining step into
-- the bgworker.

CREATE OR REPLACE FUNCTION post_batch_fifo_maximal_F(p_envelopes JSONB)
RETURNS TABLE (
    envelope_idx     INT,
    status           TEXT,
    posting_line_id  BIGINT,
    error_code       TEXT,
    error_message    TEXT
)
LANGUAGE sql AS $$
    SELECT envelope_idx, status, posting_line_id, error_code, error_message
    FROM fifo_apply_batch_maximal(p_envelopes);
$$;
