-- acct-vh5y — FIFO pre-F probe: inline-no-shmem fifo_apply_batch.
--
-- Exposes the pgrx-defined `fifo_apply_batch_inline` under a wrapper name
-- `post_batch_fifo_maximal_inline` matching the post_batch_fifo* family so
-- the existing bench harness (POC_BENCH_FUNCTION=post_batch_fifo_maximal_inline)
-- + correctness test routing can swap to it via a single name.
--
-- This is the SQL entry point. No plpgsql wrapper, no CTE chain. The Rust
-- pg_extern does everything: parse → FOR UPDATE accounts → fetch cost_layers
-- → FIFO walk → multi-row SPI INSERTs/UPDATEs → return per-envelope status.
--
-- Hypothesis being tested: removing the dispatcher+CTE wrapper alone recovers
-- the L-accounts regression. See acct-vh5y description for decision branches
-- on the bench outcome.

CREATE OR REPLACE FUNCTION post_batch_fifo_maximal_inline(p_envelopes JSONB)
RETURNS TABLE (
    envelope_idx     INT,
    status           TEXT,
    posting_line_id  BIGINT,
    error_code       TEXT,
    error_message    TEXT
)
LANGUAGE sql AS $$
    SELECT envelope_idx, status, posting_line_id, error_code, error_message
    FROM fifo_apply_batch_inline(p_envelopes);
$$;
