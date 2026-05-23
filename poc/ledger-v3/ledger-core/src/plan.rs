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

/// Flag emitted by wac_periodic depletions. `posting_line_idx` is an INDEX
/// into PlanResult.posting_lines; caller resolves to the real posting_line.id
/// from the INSERT...RETURNING in bulk_write step 7.7, then inserts a
/// posting_lines_provisional row carrying the provisional amount so the
/// close hook can recompute variance per row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvisionalPostingRequest {
    pub posting_line_idx: usize,
    pub pool_id: i64,
    /// Depletion qty, positive (abs of the trx_line's signed qty).
    pub qty: i64,
    /// Amount that was posted at the mid-period running pool average.
    pub provisional_amount: i64,
}

/// Output of plan_apply.
///
/// Caller bulk-writes in FK order per design-v3 §4.2 step 7:
///   trx → trx_line (RETURNING id) → pool_state (I/U/U/D) → posting_line
///   (RETURNING id) → posting_lines_provisional.
///
/// `provisional_postings` carries the wac_periodic depletion flags (acct-s6fa).
/// Empty on submissions that touch no wac_periodic pools. Each entry's
/// `posting_line_idx` indexes into `posting_lines` and the caller resolves it
/// to the real posting_line.id from the INSERT...RETURNING in bulk_write.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlanResult {
    pub trx_lines: Vec<TrxLineOutput>,
    pub pool_state_mutations: Vec<PoolStateMutation>,
    pub posting_lines: Vec<PostingLineRequest>,
    pub provisional_postings: Vec<ProvisionalPostingRequest>,
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
    /// Collapse multiple same-`(pool_id, layer_seq)` mutations of the same
    /// kind into one. Each per-method `apply_*` appends a mutation per line;
    /// submissions with two lines into the same WAC pool emit two Upserts at
    /// layer_seq=0, which the bulk-write UPSERT batch rejects with
    /// `ON CONFLICT DO UPDATE command cannot affect row a second time`
    /// (SQLSTATE 21000).
    ///
    /// Bulk-write executes four separate statements in order (Insert →
    /// Upsert → Update → Delete), so cross-kind same-key sequences are
    /// already correct in PG: e.g. Insert(qty=10) followed by Update(qty=7)
    /// runs Insert first (row created at 10), then Update (row set to 7),
    /// ending at qty=7. Only within-kind same-key duplicates collide
    /// (Upsert batch's ON CONFLICT; Update batch's non-deterministic
    /// per-row pick; Delete batch's redundant no-ops). This fold keeps the
    /// LAST same-kind same-key entry — the latest one carries the
    /// cumulative state because the snapshot is updated in-place as
    /// plan_apply walks lines.
    pub fn dedupe_pool_state_mutations(&mut self) {
        use std::collections::HashMap;

        if self.pool_state_mutations.len() < 2 {
            return;
        }

        // Pass 1: for each (kind_tag, pool_id, layer_seq), find the index
        // of the LAST occurrence. Earlier ones become redundant.
        let mut last_seen: HashMap<(u8, i64, i64), usize> = HashMap::new();
        for (i, m) in self.pool_state_mutations.iter().enumerate() {
            let key = match m {
                PoolStateMutation::Insert { pool_id, layer_seq, .. } => (0, *pool_id, *layer_seq),
                PoolStateMutation::Upsert { pool_id, layer_seq, .. } => (1, *pool_id, *layer_seq),
                PoolStateMutation::Update { pool_id, layer_seq, .. } => (2, *pool_id, *layer_seq),
                PoolStateMutation::Delete { pool_id, layer_seq } => (3, *pool_id, *layer_seq),
            };
            last_seen.insert(key, i);
        }

        // Pass 2: keep an entry only when it is the LAST same-kind same-key
        // entry. Order is preserved.
        let mut keep_idx = 0usize;
        self.pool_state_mutations.retain(|m| {
            let key = match m {
                PoolStateMutation::Insert { pool_id, layer_seq, .. } => (0, *pool_id, *layer_seq),
                PoolStateMutation::Upsert { pool_id, layer_seq, .. } => (1, *pool_id, *layer_seq),
                PoolStateMutation::Update { pool_id, layer_seq, .. } => (2, *pool_id, *layer_seq),
                PoolStateMutation::Delete { pool_id, layer_seq } => (3, *pool_id, *layer_seq),
            };
            let is_last = last_seen.get(&key) == Some(&keep_idx);
            keep_idx += 1;
            is_last
        });
    }

    pub fn merge(&mut self, other: PlanResult) {
        let trx_offset = self.trx_lines.len();
        let posting_offset = self.posting_lines.len();
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
                    last_trx_line_idx: last_trx_line_idx + trx_offset,
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
                    last_trx_line_idx: last_trx_line_idx + trx_offset,
                },
                m @ (PoolStateMutation::Update { .. } | PoolStateMutation::Delete { .. }) => m,
            });
        }
        for mut posting in other.posting_lines {
            posting.trx_line_idx += trx_offset;
            self.posting_lines.push(posting);
        }
        for mut provisional in other.provisional_postings {
            provisional.posting_line_idx += posting_offset;
            self.provisional_postings.push(provisional);
        }
    }
}

#[cfg(test)]
mod dedupe_tests {
    use super::*;

    fn ins(pool: i64, seq: i64, qty: i64, cost: i64, idx: usize) -> PoolStateMutation {
        PoolStateMutation::Insert {
            pool_id: pool,
            layer_seq: seq,
            qty,
            unit_cost: cost,
            last_trx_line_idx: idx,
        }
    }
    fn ups(pool: i64, seq: i64, qty: i64, cost: i64, idx: usize) -> PoolStateMutation {
        PoolStateMutation::Upsert {
            pool_id: pool,
            layer_seq: seq,
            qty,
            unit_cost: cost,
            last_trx_line_idx: idx,
        }
    }
    fn upd(pool: i64, seq: i64, qty: i64) -> PoolStateMutation {
        PoolStateMutation::Update {
            pool_id: pool,
            layer_seq: seq,
            qty,
        }
    }
    fn del(pool: i64, seq: i64) -> PoolStateMutation {
        PoolStateMutation::Delete {
            pool_id: pool,
            layer_seq: seq,
        }
    }
    fn run(ms: Vec<PoolStateMutation>) -> Vec<PoolStateMutation> {
        let mut p = PlanResult {
            pool_state_mutations: ms,
            ..Default::default()
        };
        p.dedupe_pool_state_mutations();
        p.pool_state_mutations
    }

    #[test]
    fn three_same_key_upserts_collapse_to_last_one() {
        let out = run(vec![
            ups(7, 0, 100, 10, 0),
            ups(7, 0, 200, 11, 1),
            ups(7, 0, 250, 12, 2),
        ]);
        assert_eq!(out, vec![ups(7, 0, 250, 12, 2)]);
    }

    #[test]
    fn two_same_key_updates_collapse_to_last_one() {
        let out = run(vec![upd(7, 0, 50), upd(7, 0, 30)]);
        assert_eq!(out, vec![upd(7, 0, 30)]);
    }

    #[test]
    fn cross_kind_same_key_insert_then_update_preserved() {
        // Insert batch runs before Update batch in bulk_write; this sequence
        // is correct as-is and must NOT be folded.
        let out = run(vec![ins(7, 0, 100, 10, 0), upd(7, 0, 80)]);
        assert_eq!(out, vec![ins(7, 0, 100, 10, 0), upd(7, 0, 80)]);
    }

    #[test]
    fn cross_kind_same_key_insert_then_delete_preserved() {
        // Inefficient but correct: Insert batch creates row, Delete removes it.
        let out = run(vec![ins(7, 5, 100, 10, 0), del(7, 5)]);
        assert_eq!(out, vec![ins(7, 5, 100, 10, 0), del(7, 5)]);
    }

    #[test]
    fn pre_existing_layer_update_then_delete_preserved() {
        // Update batch runs before Delete batch; both run, Delete wins as end state.
        let out = run(vec![upd(7, 5, 50), del(7, 5)]);
        assert_eq!(out, vec![upd(7, 5, 50), del(7, 5)]);
    }

    #[test]
    fn multi_pool_keys_dont_cross_fold() {
        let out = run(vec![
            ups(7, 0, 100, 10, 0),
            ups(8, 0, 200, 20, 1),
            ups(7, 0, 150, 11, 2),
        ]);
        // (7,0)'s latest Upsert at index 2 replaces the one at index 0;
        // (8,0) is left untouched at its original position.
        assert_eq!(out, vec![ups(8, 0, 200, 20, 1), ups(7, 0, 150, 11, 2)]);
    }

    #[test]
    fn no_duplicates_pass_through_unchanged() {
        let input = vec![
            ins(7, 0, 100, 10, 0),
            ins(8, 1, 50, 20, 1),
            upd(9, 2, 30),
            ups(10, 0, 100, 10, 3),
            del(11, 4),
        ];
        assert_eq!(run(input.clone()), input);
    }

    #[test]
    fn order_preserved_for_last_occurrences_across_kinds() {
        let out = run(vec![
            ups(7, 0, 100, 10, 0),
            ins(8, 0, 50, 5, 1),
            ups(7, 0, 150, 11, 2),
            upd(9, 0, 20),
        ]);
        // (7,0) Upsert collapses to index-2 entry; ins(8,0,1) and upd(9,0,3)
        // stay; final order matches the surviving entries' original positions.
        assert_eq!(
            out,
            vec![
                ins(8, 0, 50, 5, 1),
                ups(7, 0, 150, 11, 2),
                upd(9, 0, 20),
            ]
        );
    }

    #[test]
    fn duplicate_deletes_collapse_to_one() {
        let out = run(vec![del(7, 1), del(7, 1)]);
        assert_eq!(out, vec![del(7, 1)]);
    }
}
