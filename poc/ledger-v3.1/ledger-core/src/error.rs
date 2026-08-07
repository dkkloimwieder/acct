//! LedgerError: closed enum of failure modes returned by the plan_apply entry points.

use crate::method::PoolMethod;

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum LedgerError {
    /// Depletion would drive aggregate qty below zero. The PoC does not allow
    /// negative inventory (§3.6); production would gate this behind a flag.
    #[error("insufficient inventory: pool={pool_id} requested={requested} available={available}")]
    InsufficientInventory {
        pool_id: i64,
        requested: i64,
        available: i64,
    },

    /// A method was dispatched to the wrong handler. In PoC scope this surfaces
    /// when something routes a FIFO/LIFO pool to the strict `plan_apply` path —
    /// Path C must use `plan_apply_provisional` for those (§8).
    #[error("method mismatch: pool={pool_id} expected={expected:?} got={got:?}")]
    MethodMismatch {
        pool_id: i64,
        expected: PoolMethod,
        got: PoolMethod,
    },

    /// The submission referenced a pool not present in the snapshot.
    #[error("snapshot missing pool_id={0}")]
    UnknownPool(i64),

    /// An STD pool (or a 'standard'-basis FIFO/LIFO pool) was referenced but no
    /// standard_cost row exists for its (sku_id, location_id). Configuration
    /// error; fail loud (§2.2, §3.3).
    #[error("missing standard cost: sku={sku_id} location={location_id}")]
    MissingStandardCost { sku_id: i64, location_id: i64 },

    /// A line was submitted for a pool whose (sku_id, location_id) has no
    /// `posting_account_map` row. The ledger resolves debit/credit (and the STD
    /// variance account) from that table (§3.7); a missing row is a configuration
    /// error. Every line posts a journal row, so this surfaces for any touched
    /// pool that lacks config — fail loud.
    #[error("missing posting accounts: sku={sku_id} location={location_id}")]
    MissingPostingAccounts { sku_id: i64, location_id: i64 },

    /// An STD receipt's actual cost differs from standard, but the pool's
    /// `posting_account_map.variance_acct` is NULL — no account to absorb the
    /// purchase-price variance (§3.3). Configuration error; fail loud.
    #[error("missing variance account for STD pool={pool_id} (posting_account_map.variance_acct is NULL)")]
    MissingVarianceAccount { pool_id: i64 },

    /// A receipt targeted a specific-method pool that already holds a
    /// materialized unit. The specific method is K=1 — one layer per pool
    /// (§3.4) — so a second receipt while the pool is stocked would create a
    /// co-existing layer that breaks single-layer depletion. Deplete the unit
    /// first, or use a distinct identity_key (a separate pool).
    #[error("specific pool {pool_id} already holds a unit (qty={existing_qty}); K=1 forbids a second receipt")]
    SpecificPoolOccupied { pool_id: i64, existing_qty: i64 },

    #[error("arithmetic overflow: {detail}")]
    Overflow { detail: String },
}
