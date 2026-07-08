-- SPIKE-A (acct-0at4.11.1): the ledger_inbox staging table.
--
-- The durable, transactional, ordered queue that FEEDBACK-ARCH.md alt A argues
-- replaces the shmem routed stack. A caller INSERTs one row per submission INSIDE
-- its own transaction (atomicity with the business write is free); the committer
-- claims rows with `FOR UPDATE SKIP LOCKED ORDER BY id` via ledger_staging_drain_c.
--
-- Not a numbered migration — this is a throwaway spike artifact applied directly
-- to poc_v3_1. `posted_at` is stored as RFC3339 text (exactly as callers pass it
-- to ledger_enqueue_trx_c / ledger_submit_trx_c), so the drain decodes it the
-- same way the direct/routed paths do.

CREATE TABLE IF NOT EXISTS ledger_inbox (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trx_type    text        NOT NULL,
    source_id   bigint      NOT NULL,
    posted_at   text        NOT NULL,      -- RFC3339, as callers pass it
    lines       jsonb       NOT NULL,
    done        boolean     NOT NULL DEFAULT false,
    enqueued_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- The claim reads only pending rows in id order; a partial index keeps the
-- SKIP LOCKED scan off the growing tail of done rows.
CREATE INDEX IF NOT EXISTS ledger_inbox_pending
    ON ledger_inbox (id) WHERE NOT done;
