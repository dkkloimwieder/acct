-- ledger_inbox — the staging-table outbox (SPIKE-A shape, design-v3.2 §3).
--
-- Where batching/coalescing is wanted, the caller's enqueue is an in-tx heap
-- INSERT into this table (atomicity with the caller's business write is free);
-- committers drain via ledger_staging_drain(), which claims pending rows with
-- SELECT ... FOR UPDATE SKIP LOCKED ORDER BY id and applies them through the
-- same per-method dispatch as ledger_submit_trx. The row IS the status: durable
-- (WAL-logged), observable, re-claimed cleanly after a committer crash.
--
-- A failed submission (e.g. a strict-method qty gate, or a duplicate
-- (trx_type, source_id)) is marked 'failed' with the error text and is not
-- retried; its ledger writes rolled back with its subtransaction.
--
-- Cross-committer same-pool apply order is NOT guaranteed (per-committer FIFO
-- only). Under the alt-C posture that is benign: costing chronology is the
-- recalc engine's R-1 (pool_id, posted_at, id) re-sort, and the qty aggregate
-- delta is commutative.
--
-- Retention: done/failed rows accumulate; the partial index keeps the claim
-- scan off them. Pruning/partitioning of the drained tail is an operational
-- follow-up, out of scope here.

CREATE TABLE ledger_inbox (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trx_type     trx_type NOT NULL,
    source_id    BIGINT NOT NULL,
    posted_at    TIMESTAMPTZ NOT NULL,
    lines        JSONB NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'done', 'failed')),
    error        TEXT,
    enqueued_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    drained_at   TIMESTAMPTZ
);

CREATE INDEX ledger_inbox_pending ON ledger_inbox (id) WHERE status = 'pending';
