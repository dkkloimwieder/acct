-- M5e.1 (acct-m8pg): WAL-backed durable staging.
--
-- Spec §1.9. Callers pass durable_queue=true to poc_v21_enqueue when
-- this GUC is on (poc_v21.persistent_staging). The INSERT rides inside
-- the caller's user-tx so it commits atomically with the rest of the
-- caller's work — no extra fsync; group-commit absorbs the WAL.
--
-- State machine (M5e.2 wires the transitions):
--   'staged'    — INSERTed by enqueue; caller user-tx not yet committed
--                 (or just committed, committer hasn't picked up).
--   'in_shmem'  — committer pulled the shmem StagingEntry; pg_xact for
--                 user_tx_xid shows committed; about to dispatch.
--   'completed' — committer Step 5 sub-tx committed; cost rows persisted.
--                 GC eligible after persistent_staging_gc_retention_hours.
--
-- Partial index on (state, enqueued_at) WHERE state IN ('staged','in_shmem')
-- — the hot lookup is "what's still in flight that recovery needs to
-- consider." 'completed' rows are GC fodder; full scan is fine.
CREATE TABLE poc_v21_persistent_staging (
    request_seq      BIGSERIAL PRIMARY KEY,
    correlation_id   UUID NOT NULL UNIQUE,
    enqueued_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    user_tx_xid      xid8 NOT NULL,
    event_type       TEXT NOT NULL,
    payload          JSONB NOT NULL,
    sku_pool_keys    JSONB NOT NULL,
    wip_pool_keys    JSONB,
    business_date    DATE NOT NULL,
    state            TEXT NOT NULL
                       CHECK (state IN ('staged','in_shmem','completed'))
                       DEFAULT 'staged'
);

CREATE INDEX poc_v21_persistent_staging_state_idx
    ON poc_v21_persistent_staging (state, enqueued_at)
    WHERE state IN ('staged', 'in_shmem');
