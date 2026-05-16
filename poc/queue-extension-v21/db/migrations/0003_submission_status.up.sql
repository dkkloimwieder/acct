-- M1.1 (acct-63dk) — submission status table per spec §1.2.
--
-- correlation_id is the user-facing handle: callers receive it from
-- poc_v21_enqueue (M1.2), poll this table to observe state, and use
-- it for idempotency.
--
-- The state machine (spec §1.8):
--   queued     — staging.valid=1 (pending); INSERTed by enqueue under
--                caller_intx/caller_subtx, or by committer-lazy at
--                committer claim time
--   processing — staging.valid=2 (processing); committer has taken
--                ownership but hasn't yet committed
--   committed  — Step 5 sub-tx committed; cost rows visible
--   failed    — Step 4 envelope-level error (per spec §3.7), Step 5
--                sub-tx abort (§3.1), eject-exhausted (§3.11), or
--                postmaster-restart loss (§3.6)
--   replayed  — dedup-lookup hit at Step 2.5; committer recognized
--                committed prior work and re-emitted status without
--                re-writing cost rows
--
-- The state-IN-list partial index serves the M5d.x slot leak audit
-- and M5a.x orphan recovery scans; both look only at unresolved rows.

CREATE TABLE poc_v21_submission_status (
    correlation_id  UUID PRIMARY KEY,
    state           TEXT        NOT NULL CHECK (state IN ('queued', 'processing', 'committed', 'failed', 'replayed')),
    enqueued_at     TIMESTAMPTZ NOT NULL,
    processed_at    TIMESTAMPTZ,
    committed_at    TIMESTAMPTZ,
    error_code      TEXT,
    error_detail    JSONB,
    committer_tx_id BIGINT,
    superbatch_id   BIGINT
);
CREATE INDEX poc_v21_submission_status_state
    ON poc_v21_submission_status (state)
    WHERE state IN ('queued', 'processing');
CREATE INDEX poc_v21_submission_status_enqueued
    ON poc_v21_submission_status (enqueued_at);
