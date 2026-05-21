//! Snapshot: pre-apply state for the pools touched by a submission.
//!
//! Built by ledger-direct / ledger-routed under pool_lock FOR UPDATE
//! (design-v3 §4.2 step 4 / §5.4 step 7), then passed to `plan_apply`.

use std::collections::HashMap;

use crate::method::PoolMethod;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolStateRow {
    pub layer_seq: i64,
    pub qty: i64,
    pub unit_cost: i64,
    pub last_trx_line_id: i64,
}

/// Snapshot of the pools touched by a submission.
///
/// Caller responsibilities (design-v3 §4.2 step 4 / §5.4 step 7):
///  - Hold pool_lock FOR UPDATE on every pool in `pools` before reading.
///  - Deliver rows in (pool_id, layer_seq) order — pool_state's PRIMARY KEY
///    makes the SQL `ORDER BY pool_id, layer_seq` an index-ordered traversal.
///  - plan_apply defensively re-sorts each pool's vec by layer_seq internally;
///    the database-side ORDER BY is the contract, the client-side re-sort is
///    the enforcement.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    /// Per-pool layers.
    ///  - WAC: at most one row at layer_seq = 0.
    ///  - FIFO / LIFO / Specific: one row per live receipt layer.
    ///  - STD: empty (no layers; cost lookup goes through `std_cost_of`).
    pub pools: HashMap<i64, Vec<PoolStateRow>>,

    /// Per-pool method, hydrated from `pool.method`.
    pub method_of: HashMap<i64, PoolMethod>,

    /// Per-pool MAX(trx_seq) at hydration time (design-v3 §13.1 option a).
    /// Read once under pool_lock; plan_apply increments and assigns per emitted
    /// TrxLineOutput. Pools absent from the hydration result default to 0 —
    /// the caller seeds with `COALESCE(MAX(trx_seq), 0)`.
    pub max_trx_seq_of: HashMap<i64, i64>,

    /// Standard-cost lookup keyed by (sku_id, location_id) for STD-method pools.
    /// Per locked plan decision Q4, ledger-core trusts the caller's unit_cost
    /// on STD lines in the PoC — this map is reserved for a future internal
    /// `standard_costs` lookup (deferred).
    pub std_cost_of: HashMap<(i64, i64), i64>,
}
