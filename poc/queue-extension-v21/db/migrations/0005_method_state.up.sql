-- M1.1 (acct-63dk) — per-method configuration + per-pool state tables
-- per spec §1.2 + §1.3.
--
-- sku_method_assignments: per-SKU method dispatch. Read by Step 3
-- snapshot hydration; cached per-committer (M1.3+). One row per
-- production SKU. CHECK constraint mirrors the spec's exhaustive list
-- (FIFO / AVG / STD); FIFO is the M1.3 first-class method, AVG lands
-- at M2.1, STD at M2.2.
--
-- standard_costs: STD method's source-of-truth. Composite PK keeps
-- effective-dated history; Step 3 reads via DISTINCT ON (sku_id,
-- location_id) ... ORDER BY effective_from DESC. Negative pool depth
-- under STD is acceptable by policy — see CLAUDE.md / spec §1.3.
--
-- avg_pool_state: AVG method's running-aggregate state. Read by Step 3
-- snapshot hydration; UPSERTed at Step 5 (6-target UNNEST INSERT).
-- "NEVER reconstruct AVG from cost_layers history" (spec §1.3) — this
-- table is the contract. M2.1 seeds it from initial layers in the
-- bake-off setup helper; subsequent receipts/consumptions mutate it
-- incrementally.

CREATE TABLE poc_v21_sku_method_assignments (
    sku_id    BIGINT NOT NULL PRIMARY KEY,
    method_id TEXT   NOT NULL CHECK (method_id IN ('fifo', 'avg', 'std'))
);

CREATE TABLE poc_v21_standard_costs (
    sku_id         BIGINT      NOT NULL,
    location_id    BIGINT      NOT NULL,
    unit_cost      BIGINT      NOT NULL,
    effective_from TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (sku_id, location_id, effective_from)
);

CREATE TABLE poc_v21_avg_pool_state (
    sku_id               BIGINT      NOT NULL,
    location_id          BIGINT      NOT NULL,
    avg_unit_cost        BIGINT      NOT NULL,
    total_qty            BIGINT      NOT NULL,
    last_updated_at      TIMESTAMPTZ NOT NULL,
    last_committer_tx_id BIGINT      NOT NULL,
    PRIMARY KEY (sku_id, location_id)
);
