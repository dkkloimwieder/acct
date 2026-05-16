-- M1.1 (acct-63dk) — two-domain lock tables per spec §1.2 + §1.5.
--
-- pool_locks: SKU pool domain. Acquired by the router (envelopes
-- declare sku_pool_keys; router packs disjoint sets) AND by the
-- committer (Step 2 lex-lock acquisition). The router-side disjointness
-- guarantee makes pool_locks contention bounded by lex order in
-- practice; the lock is a defensive correctness gate, not the hot path.
--
-- wip_pool_locks: WIP pool domain. Acquired ONLY by committer (Step 2)
-- because §1.5 proves the WIP-pool domain is uncontended by
-- construction (each wo_complete envelope owns a unique
-- (work_order_id, operation_id) tuple). Still locked defensively to
-- preserve the lex-locking invariant under future workload changes.
--
-- lock_version is an audit column the lex-lock takeover sequence may
-- increment on contention (M4.1+); INSERT ON CONFLICT pattern uses
-- the row's existence as the gate, FOR UPDATE on the row as the lock.

CREATE TABLE poc_v21_pool_locks (
    sku_id       BIGINT NOT NULL,
    location_id  BIGINT NOT NULL,
    lock_version BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (sku_id, location_id)
);

CREATE TABLE poc_v21_wip_pool_locks (
    work_order_id BIGINT NOT NULL,
    operation_id  BIGINT NOT NULL,
    lock_version  BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (work_order_id, operation_id)
);
