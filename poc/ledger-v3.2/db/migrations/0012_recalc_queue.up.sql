-- recalc_queue — the durable dirty-set (recalc-b D9 / recalc-c D8 option B).
--
-- One row per pool with un-recalced activity. The feed consumer marks a pool
-- dirty for every trx_line insert the slot delivers (method-agnostic: recalc
-- decides per-pool what a pass means, and no-op passes are free — recalc-d D6);
-- recalc workers drain it with SELECT ... FOR UPDATE SKIP LOCKED (the SPIKE-A
-- claim pattern) and delete the row when the pool is settled.
--
-- This table is the crash-recovery boundary for the feed (recalc-c §6): the
-- slot cursor advances once a batch's marks are durably committed here, so a
-- crash re-drains the dirty-set, not the slot. Marking is idempotent (upsert,
-- keep the earliest enqueued_at) — at-least-once delivery is sufficient.
--
-- Deliberately thin: the recost floor lives on pool_settlement (recalc-d §5),
-- never here — one authoritative home per fact. enqueued_at feeds the G2c
-- oldest-dirty gauge.
CREATE TABLE recalc_queue (
    pool_id     BIGINT PRIMARY KEY REFERENCES pool(id),
    enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
