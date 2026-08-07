//! Snapshot: pre-apply state for the pools touched by a submission.
//!
//! Built by the SPI caller under PG row locks on the touched `pool_state` rows
//! (SPIKE-B shape — no advisory lock table; design-v3.2 §3), then passed to
//! `plan_apply`, which mutates it in place as it walks lines, so later lines in
//! the same submission see earlier lines' effects.

use std::collections::HashMap;

use crate::error::LedgerError;
use crate::method::PoolMethod;
use crate::plan::LineType;

/// The full posting-account set for one `(sku_id, location_id)`, hydrated from
/// `posting_account_map` (design-v3.1 §3.7).
///
/// Each operation's pair is stored in the RECEIPT (inventory-increase) direction
/// `(debit, credit)`: debit = the pool's inventory side, credit = the contra. The
/// cost engine uses the pair as-is for receipts and SWAPS it for depletions.
/// `variance` is the STD purchase-price-variance account (`None` when the pool's
/// row leaves `variance_acct` NULL — fine for pools that are never STD).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostingAccounts {
    pub receipt: (i64, i64),
    pub transfer: (i64, i64),
    pub build: (i64, i64),
    pub scrap: (i64, i64),
    pub adjustment: (i64, i64),
    pub revaluation: (i64, i64),
    pub variance: Option<i64>,
}

impl PostingAccounts {
    /// The `(debit, credit)` pair for `line_type`, in receipt (increase)
    /// direction. Depletion callers swap it (debit ↔ credit).
    pub fn pair_for(&self, line_type: LineType) -> (i64, i64) {
        match line_type {
            LineType::PoReceiptLine => self.receipt,
            LineType::TransferReceiptLine | LineType::TransferShipmentLine => self.transfer,
            LineType::WoOutput | LineType::WoBackflush => self.build,
            LineType::WoScrap => self.scrap,
            LineType::InvAdjustmentLine | LineType::ManualAdjustmentLine => self.adjustment,
            LineType::RevaluationLine => self.revaluation,
        }
    }
}

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
/// Caller responsibilities:
///  - Hold PG row locks (FOR UPDATE) on every touched pool's `pool_state` rows
///    before reading.
///  - Populate `method_of` from `pool.method`.
///  - For STD pools, resolve the pool's `standard_cost.unit_cost` into
///    `standard_cost_of`; populate `sku_location_of` so a missing standard
///    surfaces sku/location in the error.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    /// Per-pool rows. WAC/STD: at most one row at `layer_id = 0`. Specific:
    /// aggregate at 0 plus one layer row.
    pub pools: HashMap<i64, Vec<PoolStateRow>>,

    /// Per-pool method, from `pool.method`.
    pub method_of: HashMap<i64, PoolMethod>,

    /// Per-pool (sku_id, location_id), used to build the `MissingStandardCost`
    /// error message.
    pub sku_location_of: HashMap<i64, (i64, i64)>,

    /// Per-pool resolved standard cost (`standard_cost.unit_cost`). Populated by
    /// the caller for STD pools.
    pub standard_cost_of: HashMap<i64, i64>,

    /// Per-pool posting accounts, from `posting_account_map` joined on the pool's
    /// `(sku_id, location_id)` (§3.7). Populated by the caller for every touched
    /// pool — every line posts a journal row, so a pool absent here fails loud as
    /// `MissingPostingAccounts` the moment its line is processed.
    pub posting_accounts_of: HashMap<i64, PostingAccounts>,
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

    /// The posting accounts for a pool, or `MissingPostingAccounts` (carrying
    /// sku/location for the message). Returns an owned `Copy`, so the immutable
    /// borrow ends immediately and callers can still mutate the snapshot (§3.7).
    pub(crate) fn resolve_posting_accounts(
        &self,
        pool_id: i64,
    ) -> Result<PostingAccounts, LedgerError> {
        self.posting_accounts_of.get(&pool_id).copied().ok_or_else(|| {
            let (sku_id, location_id) =
                self.sku_location_of.get(&pool_id).copied().unwrap_or((0, 0));
            LedgerError::MissingPostingAccounts { sku_id, location_id }
        })
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
