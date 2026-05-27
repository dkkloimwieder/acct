//! Snapshot: pre-apply state for the pools touched by a submission.
//!
//! Built by ledger-direct-c / ledger-routed-c under `pool_lock` FOR UPDATE
//! (design-v3.1 §5.1 step 3 / §6.4 step 8), then passed to a plan_apply entry
//! point. `plan_apply` mutates it in place as it walks lines, so later lines in
//! the same submission see earlier lines' effects.

use std::collections::HashMap;

use crate::method::{PoolMethod, ProvisionalBasis};

/// One row from `pool_state` (design-v3.1 §2.2 / §3.0):
///  - `layer_id = 0` aggregate: `qty` = total on-hand, `value_sum` = cumulative
///    book value, `unit_cost` = the derived running average `value_sum/qty`
///    (banker-rounded). WAC and the Path C provisional basis for FIFO/LIFO.
///  - `layer_id > 0` materialized layer (specific pools only): `qty` = layer qty
///    (1 for specific), `unit_cost` = the layer's immutable per-unit cost,
///    `value_sum` = `qty*unit_cost`. `layer_id` equals the receipt trx_line.id
///    that created the layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolStateRow {
    pub layer_id: i64,
    pub qty: i64,
    pub unit_cost: i64,
    /// Cumulative book value. `unit_cost == banker_div(value_sum, qty)` for the
    /// aggregate; `qty*unit_cost` for layer rows (acct-0qps).
    pub value_sum: i64,
}

/// Snapshot of the pools touched by a submission.
///
/// Caller responsibilities (§5.1 step 2-5):
///  - Hold `pool_lock` FOR UPDATE on every touched pool before reading.
///  - Populate `method_of` from `pool.method` and `provisional_basis_of` from
///    `pool.provisional_basis`.
///  - For STD pools and 'standard'-basis FIFO/LIFO pools, resolve the pool's
///    `standard_cost.unit_cost` into `standard_cost_of`; populate
///    `sku_location_of` so a missing standard surfaces sku/location in the error.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    /// Per-pool rows. WAC/STD/provisional FIFO-LIFO: at most one row at
    /// `layer_id = 0`. Specific: aggregate at 0 plus one layer row.
    pub pools: HashMap<i64, Vec<PoolStateRow>>,

    /// Per-pool method, from `pool.method`.
    pub method_of: HashMap<i64, PoolMethod>,

    /// Per-pool provisional basis, from `pool.provisional_basis`. Only consulted
    /// for FIFO/LIFO depletions; ignored for WAC/STD/specific. Pools absent here
    /// default to `RunningAvg`.
    pub provisional_basis_of: HashMap<i64, ProvisionalBasis>,

    /// Per-pool (sku_id, location_id), used to build the `MissingStandardCost`
    /// error message.
    pub sku_location_of: HashMap<i64, (i64, i64)>,

    /// Per-pool resolved standard cost (`standard_cost.unit_cost`). Populated by
    /// the caller for STD pools and 'standard'-basis FIFO/LIFO pools.
    pub standard_cost_of: HashMap<i64, i64>,
}

impl Snapshot {
    /// The aggregate row (`layer_id = 0`) for a pool, if it exists.
    pub fn aggregate(&self, pool_id: i64) -> Option<&PoolStateRow> {
        self.pools
            .get(&pool_id)
            .and_then(|rows| rows.iter().find(|r| r.layer_id == 0))
    }

    /// `(qty, unit_cost, value_sum, existed)` for the aggregate row;
    /// `(0, 0, 0, false)` when the pool has no aggregate yet (first receipt).
    pub(crate) fn read_aggregate(&self, pool_id: i64) -> (i64, i64, i64, bool) {
        match self.aggregate(pool_id) {
            Some(r) => (r.qty, r.unit_cost, r.value_sum, true),
            None => (0, 0, 0, false),
        }
    }

    /// Insert-or-update the in-memory aggregate row so later lines in the same
    /// submission observe the new state.
    pub(crate) fn put_aggregate(&mut self, pool_id: i64, qty: i64, unit_cost: i64, value_sum: i64) {
        let rows = self.pools.entry(pool_id).or_default();
        if let Some(r) = rows.iter_mut().find(|r| r.layer_id == 0) {
            r.qty = qty;
            r.unit_cost = unit_cost;
            r.value_sum = value_sum;
        } else {
            rows.push(PoolStateRow { layer_id: 0, qty, unit_cost, value_sum });
        }
    }
}
