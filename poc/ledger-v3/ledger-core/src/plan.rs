//! PlanResult: output of plan_apply.
//!
//! Three parallel vectors: trx_lines (per-line outputs with assigned trx_seq),
//! pool_state_mutations (the Insert/Upsert/Update/Delete to apply to pool_state
//! after the trx_line INSERTs land), posting_lines (the journal-side rows).
//! The caller's bulk-write step turns these into UNNEST INSERTs in FK order
//! per design-v3 §4.2 step 7 / §5.4 step 9.

use chrono::{DateTime, Utc};

/// Mirror of the SQL `line_type` enum from design-v3 §2.1. The discriminant
/// values are not load-bearing for the Rust side; mapping to/from the SQL enum
/// happens at the SPI boundary (ledger-direct / ledger-routed).
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
    /// SQL `line_type` enum text. Stable contract — must match migration 0001.
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

/// Mirror of the SQL `posting_event_type` enum from design-v3 §2.1.
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
    /// SQL `posting_event_type` enum text. Stable contract — must match
    /// migration 0001.
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

/// One line from a caller's submission. Input to plan_apply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrxLineRequest {
    pub pool_id: i64,
    pub line_type: LineType,
    pub source_id: Option<i64>,
    /// Signed: positive = receipt, negative = depletion.
    pub qty: i64,
    /// Caller-supplied. For STD, taken at face value per locked plan decision Q4.
    /// For WAC depletions, plan_apply overrides with the running pool average.
    /// For FIFO/LIFO depletions, plan_apply overrides with the consumed layer's
    /// unit_cost per emitted TrxLineOutput.
    pub unit_cost: i64,
    pub debit_account: i64,
    pub credit_account: i64,
}

/// One row to INSERT into trx_line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrxLineOutput {
    pub pool_id: i64,
    pub trx_seq: i64,
    /// Signed; may differ from request.qty when a depletion spans multiple
    /// FIFO/LIFO layers (one TrxLineOutput per layer touched).
    pub qty: i64,
    pub unit_cost: i64,
    /// Set when this row is a depletion against a specific receipt layer
    /// (FIFO/LIFO/Specific): the originating receipt's trx_line.id.
    /// Looked up via PoolStateRow.last_trx_line_id during plan_apply.
    pub source_trx_line_id: Option<i64>,
    pub line_type: LineType,
    pub source_id: Option<i64>,
}

/// A mutation to apply to pool_state after the trx_line INSERTs.
///
/// `last_trx_line_idx` is an INDEX into PlanResult.trx_lines (not a DB id);
/// the caller resolves it to the real trx_line.id from the INSERT ... RETURNING
/// clause in design-v3 §4.2 step 7.2 / §5.4 step 9.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolStateMutation {
    /// New receipt layer (FIFO / LIFO / Specific).
    Insert {
        pool_id: i64,
        layer_seq: i64,
        qty: i64,
        unit_cost: i64,
        last_trx_line_idx: usize,
    },
    /// WAC: ON CONFLICT (pool_id, layer_seq) DO UPDATE SET qty, unit_cost, last_trx_line_id.
    Upsert {
        pool_id: i64,
        layer_seq: i64,
        qty: i64,
        unit_cost: i64,
        last_trx_line_idx: usize,
    },
    /// Partial depletion: layer qty decremented; row remains.
    Update {
        pool_id: i64,
        layer_seq: i64,
        qty: i64,
    },
    /// Layer fully consumed: row deleted.
    Delete {
        pool_id: i64,
        layer_seq: i64,
    },
}

/// One row to INSERT into posting_line. `trx_line_idx` is an INDEX into
/// PlanResult.trx_lines; caller resolves to the real trx_line.id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostingLineRequest {
    pub trx_line_idx: usize,
    pub event_type: PostingEventType,
    pub amount: i64,
    pub debit_account: i64,
    pub credit_account: i64,
    pub posted_at: DateTime<Utc>,
}

/// Output of plan_apply.
///
/// Caller bulk-writes in FK order per design-v3 §4.2 step 7:
///   trx → trx_line (RETURNING id) → pool_state (I/U/U/D) → posting_line.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlanResult {
    pub trx_lines: Vec<TrxLineOutput>,
    pub pool_state_mutations: Vec<PoolStateMutation>,
    pub posting_lines: Vec<PostingLineRequest>,
}

impl PlanResult {
    /// Combine another PlanResult into self. Used by ledger-routed's committer
    /// to concatenate per-submission results within a commit_group (design-v3
    /// §5.4 step 8 → step 9 single bulk write).
    ///
    /// Translates `last_trx_line_idx` (in PoolStateMutation::Insert/Upsert)
    /// and `trx_line_idx` (in PostingLineRequest) by the current
    /// `self.trx_lines.len()` offset so the merged indices remain valid
    /// against the combined `trx_lines` vec.
    pub fn merge(&mut self, other: PlanResult) {
        let offset = self.trx_lines.len();
        self.trx_lines.extend(other.trx_lines);
        for mutation in other.pool_state_mutations {
            self.pool_state_mutations.push(match mutation {
                PoolStateMutation::Insert {
                    pool_id,
                    layer_seq,
                    qty,
                    unit_cost,
                    last_trx_line_idx,
                } => PoolStateMutation::Insert {
                    pool_id,
                    layer_seq,
                    qty,
                    unit_cost,
                    last_trx_line_idx: last_trx_line_idx + offset,
                },
                PoolStateMutation::Upsert {
                    pool_id,
                    layer_seq,
                    qty,
                    unit_cost,
                    last_trx_line_idx,
                } => PoolStateMutation::Upsert {
                    pool_id,
                    layer_seq,
                    qty,
                    unit_cost,
                    last_trx_line_idx: last_trx_line_idx + offset,
                },
                m @ (PoolStateMutation::Update { .. } | PoolStateMutation::Delete { .. }) => m,
            });
        }
        for mut posting in other.posting_lines {
            posting.trx_line_idx += offset;
            self.posting_lines.push(posting);
        }
    }
}
