//! PlanResult: output of the plan_apply entry points.
//!
//! Three parallel vectors plus indices: `trx_lines` (rows to INSERT into
//! trx_line), `pool_state_mutations` (applied after trx_line INSERTs land),
//! `posting_lines` (journal rows; `trx_line_idx` resolves to the real trx_line.id
//! from INSERT ... RETURNING). v3.1 has no per-pool trx_seq — ordering uses
//! trx_line.id (PG identity), assigned at INSERT. design-v3.1 §2.2.

use chrono::{DateTime, Utc};

/// Mirror of the SQL `line_type` enum (design-v3.1 §2.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LineType {
    PoReceiptLine,
    WoOutput,
    WoBackflush,
    WoScrap,
    InvAdjustmentLine,
    TransferShipmentLine,
    TransferReceiptLine,
    ManualAdjustmentLine,
    RevaluationLine,
}

impl LineType {
    /// SQL `line_type` text. Stable contract — must match migration 0001.
    pub fn as_sql(self) -> &'static str {
        match self {
            LineType::PoReceiptLine => "po_receipt_line",
            LineType::WoOutput => "wo_output",
            LineType::WoBackflush => "wo_backflush",
            LineType::WoScrap => "wo_scrap",
            LineType::InvAdjustmentLine => "inv_adjustment_line",
            LineType::TransferShipmentLine => "transfer_shipment_line",
            LineType::TransferReceiptLine => "transfer_receipt_line",
            LineType::ManualAdjustmentLine => "manual_adjustment_line",
            LineType::RevaluationLine => "revaluation_line",
        }
    }
}

/// Mirror of the SQL `posting_event_type` enum (design-v3.1 §2.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PostingEventType {
    InventoryReceipt,
    InventoryDepletion,
    WipMovement,
    Variance,
    Scrap,
    Adjustment,
    Revaluation,
}

impl PostingEventType {
    /// SQL `posting_event_type` text. Stable contract — must match migration 0001.
    pub fn as_sql(self) -> &'static str {
        match self {
            PostingEventType::InventoryReceipt => "inventory_receipt",
            PostingEventType::InventoryDepletion => "inventory_depletion",
            PostingEventType::WipMovement => "wip_movement",
            PostingEventType::Variance => "variance",
            PostingEventType::Scrap => "scrap",
            PostingEventType::Adjustment => "adjustment",
            PostingEventType::Revaluation => "revaluation",
        }
    }
}

/// One line from a caller's submission. Input to a plan_apply entry point.
///
/// Posting accounts (debit/credit, and the STD variance account) are NOT on the
/// line — the ledger resolves them from `posting_account_map` keyed on the pool's
/// `(sku_id, location_id)`, hydrated into `Snapshot::posting_accounts_of` (§3.7).
/// `line_type` selects which operation's account pair applies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrxLineRequest {
    pub pool_id: i64,
    pub line_type: LineType,
    pub source_id: Option<i64>,
    /// Signed: positive = receipt, negative = depletion.
    pub qty: i64,
    /// Caller-supplied actual cost. For receipts this is the asserted unit cost.
    /// For WAC/provisional depletions plan_apply overrides the recorded cost with
    /// the running average (or standard); for STD it uses the standard cost.
    pub unit_cost: i64,
}

/// One row to INSERT into trx_line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrxLineOutput {
    pub pool_id: i64,
    /// Signed (positive receipt / negative depletion).
    pub qty: i64,
    /// Recorded cost: actual for receipts; running-average or standard for
    /// WAC/provisional depletions; standard for STD; the layer's cost for specific.
    pub unit_cost: i64,
    /// Set only for depletions against a specific receipt layer (specific-id):
    /// the originating receipt's trx_line.id (= the layer's `layer_id`). NULL for
    /// WAC/STD and for Path C FIFO/LIFO provisional depletions (§2.2, §3.5).
    pub source_trx_line_id: Option<i64>,
    pub line_type: LineType,
    pub source_id: Option<i64>,
}

/// A mutation to apply to pool_state after the trx_line INSERTs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolStateMutation {
    /// Aggregate row (`layer_id = 0`): WAC, STD, and provisional FIFO/LIFO.
    /// `INSERT ... ON CONFLICT (pool_id, layer_id) DO UPDATE SET qty, unit_cost,
    /// value_sum`. `unit_cost` is the derived running average `value_sum/qty`;
    /// `value_sum` is the authoritative cumulative book value (acct-0qps).
    UpsertAggregate {
        pool_id: i64,
        qty: i64,
        unit_cost: i64,
        value_sum: i64,
    },
    /// New materialized layer (specific receipt). The layer's `layer_id` is the
    /// receipt's trx_line.id, unknown until INSERT — `layer_trx_line_idx` indexes
    /// into `PlanResult.trx_lines`; the caller resolves it from RETURNING.
    /// `value_sum` is the layer's book value (`qty*unit_cost`), stored so every
    /// pool_state row carries a non-null value_sum (acct-0qps).
    InsertLayer {
        pool_id: i64,
        layer_trx_line_idx: usize,
        qty: i64,
        unit_cost: i64,
        value_sum: i64,
    },
    /// Materialized layer fully consumed (specific depletion): `DELETE` the row.
    DeleteLayer {
        pool_id: i64,
        layer_id: i64,
    },
}

/// One row to INSERT into posting_line. `trx_line_idx` indexes into
/// `PlanResult.trx_lines`; the caller resolves it to the real trx_line.id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostingLineRequest {
    pub trx_line_idx: usize,
    pub event_type: PostingEventType,
    pub amount: i64,
    pub debit_account: i64,
    pub credit_account: i64,
    pub posted_at: DateTime<Utc>,
}

/// Output of a plan_apply entry point. Caller bulk-writes in FK order:
/// trx → trx_line (RETURNING id) → pool_state → posting_line.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlanResult {
    pub trx_lines: Vec<TrxLineOutput>,
    pub pool_state_mutations: Vec<PoolStateMutation>,
    pub posting_lines: Vec<PostingLineRequest>,
}

impl PlanResult {
    /// Collapse `UpsertAggregate` mutations to at most one per pool, keeping the
    /// LAST. Each `UpsertAggregate` carries the full post-line aggregate state
    /// (`wac::aggregate_receipt` / `aggregate_deplete` push the new total qty and
    /// running-average cost, not a delta), and lines mutate the snapshot in place,
    /// so the last mutation for a pool already reflects every earlier line on it.
    /// `InsertLayer` / `DeleteLayer` (specific, `layer_id != 0`) are kept in order.
    ///
    /// Without this, a single submission with two lines on the same pool emits
    /// duplicate `(pool_id, layer_id = 0)` rows, and the direct bulk
    /// `INSERT ... ON CONFLICT (pool_id, layer_id) DO UPDATE` rejects the batch
    /// ("ON CONFLICT DO UPDATE command cannot affect row a second time"). The
    /// routed committer is unaffected (it reconstructs one aggregate per touched
    /// pool from the post-pass snapshot); coalescing here makes both paths emit
    /// one aggregate mutation per pool.
    pub fn coalesce_aggregates(&mut self) {
        use std::collections::HashMap;
        let mut last_idx: HashMap<i64, usize> = HashMap::new();
        for (i, m) in self.pool_state_mutations.iter().enumerate() {
            if let PoolStateMutation::UpsertAggregate { pool_id, .. } = m {
                last_idx.insert(*pool_id, i);
            }
        }
        if last_idx.len() == self.pool_state_mutations.len()
            && self
                .pool_state_mutations
                .iter()
                .all(|m| matches!(m, PoolStateMutation::UpsertAggregate { .. }))
        {
            return; // already one aggregate per pool, no layer ops — nothing to do
        }
        let mut kept = Vec::with_capacity(self.pool_state_mutations.len());
        for (i, m) in std::mem::take(&mut self.pool_state_mutations)
            .into_iter()
            .enumerate()
        {
            match &m {
                PoolStateMutation::UpsertAggregate { pool_id, .. } => {
                    if last_idx.get(pool_id) == Some(&i) {
                        kept.push(m);
                    }
                }
                _ => kept.push(m),
            }
        }
        self.pool_state_mutations = kept;
    }
}
