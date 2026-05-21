//! LedgerError: closed enum of failure modes returned by plan_apply.

use crate::method::PoolMethod;

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum LedgerError {
    #[error("insufficient inventory: pool={pool_id} requested={requested} available={available}")]
    InsufficientInventory {
        pool_id: i64,
        requested: i64,
        available: i64,
    },

    #[error("method mismatch: pool={pool_id} expected={expected:?} got={got:?}")]
    MethodMismatch {
        pool_id: i64,
        expected: PoolMethod,
        got: PoolMethod,
    },

    #[error("snapshot missing pool_id={0}")]
    UnknownPool(i64),

    #[error("missing standard cost: sku={sku_id} location={location_id}")]
    MissingStandardCost { sku_id: i64, location_id: i64 },

    #[error("arithmetic overflow: {detail}")]
    Overflow { detail: String },
}
