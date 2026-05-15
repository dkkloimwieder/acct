//! acct-4d4n.5 (M2.2): FifoMethod — walks `PocSnapshot.layers` oldest-first.
//!
//! Per spec §1.3, `plan_apply` MUST be pure (no SPI, no shmem mutation).
//! All committed state lives in `PocSnapshot.layers` (built by the
//! committer via SPI under FOR UPDATE before dispatch). The method
//! walks the layers with a mutable LOCAL cursor so events later in the
//! batch see depletions from earlier events in the same call — without
//! that in-batch chain, a batch of `consume 30 + consume 30` against a
//! single 50-qty layer would both satisfy from the 50 (wrong) instead
//! of the second one erroring (correct).
//!
//! ## Cardinality
//!
//! - Receipts: 0 outputs from this method (receipts bypass the queue;
//!   `poc_ledger_receive` INSERTs `poc_cost_layers` directly).
//! - Issues: 1 `PocDepletionRow` per layer touched. A consume against
//!   a single layer produces 1 row; spanning 3 layers produces 3 rows.
//!
//! ## Empty pool / partial fill
//!
//! Per-event `InsufficientInventory` when the local cursor's total
//! available qty is less than the requested qty. Other events in the
//! same batch are unaffected (spec §3.10 partial-success contract).
//!
//! ## Rounding contract
//!
//! `applied_total_cost = sum(qty_per_layer × unit_cost_per_layer)` —
//! exact, no rounding. `applied_unit_cost = total / qty` (integer
//! division, truncating). Callers needing the precise per-layer mix
//! read `poc_cost_depletions` rows.

use crate::cost_method::{
    PocApplyBatch, PocApplyResult, PocCostMethod, PocDepletionRow, PocError,
    PocEventResult, PocSnapshot,
};

pub struct FifoMethod;

impl PocCostMethod for FifoMethod {
    fn method_id(&self) -> &'static str {
        "fifo"
    }

    fn plan_apply(
        &self,
        batch: &PocApplyBatch,
        snapshot: &PocSnapshot,
    ) -> PocApplyResult {
        // Mutable local cursor: (layer_id, unit_cost, remaining_qty).
        // Snapshot layers are pre-sorted by (born_at, born_seq) by the
        // SPI helper; we walk them as-is and decrement in place.
        let mut cursor: Vec<(i64, i64, i64)> = snapshot
            .layers
            .iter()
            .map(|l| (l.layer_id, l.unit_cost, l.effective_qty))
            .collect();

        let mut per_event: Vec<PocEventResult> =
            Vec::with_capacity(batch.events.len());
        let mut depletion_inserts: Vec<PocDepletionRow> = Vec::new();

        for ev in &batch.events {
            // FIFO is consumption-only at M2.2 — receipts don't flow
            // through plan_apply. Treat non-positive qty as an error to
            // surface the misuse rather than silently no-op.
            if ev.qty <= 0 {
                per_event.push(PocEventResult {
                    event_seq: ev.event_seq,
                    applied_unit_cost: 0,
                    applied_total_cost: 0,
                    error: Some(PocError::InsufficientInventory),
                });
                continue;
            }

            // Total available across local cursor (post in-batch decrement).
            let available: i64 = cursor.iter().map(|(_, _, q)| *q).sum();
            if available < ev.qty {
                per_event.push(PocEventResult {
                    event_seq: ev.event_seq,
                    applied_unit_cost: 0,
                    applied_total_cost: 0,
                    error: Some(PocError::InsufficientInventory),
                });
                continue;
            }

            // Walk oldest-first, deplete layers until ev.qty satisfied.
            let mut remaining = ev.qty;
            let mut total_cost: i128 = 0;
            // Stash the depletion rows for THIS event first; if we run
            // out mid-walk it's a snapshot-skew bug (available was the
            // sum that was reported above). Bail loudly.
            let event_start = depletion_inserts.len();
            for (layer_id, unit_cost, layer_qty) in cursor.iter_mut() {
                if remaining == 0 {
                    break;
                }
                if *layer_qty == 0 {
                    continue;
                }
                let take = (*layer_qty).min(remaining);
                *layer_qty -= take;
                remaining -= take;
                total_cost = total_cost
                    .saturating_add((take as i128) * (*unit_cost as i128));
                depletion_inserts.push(PocDepletionRow {
                    layer_id: *layer_id,
                    qty: take,
                    unit_cost: *unit_cost,
                    issue_id: ev.issue_id,
                    event_seq: ev.event_seq,
                    user_tx_xid: ev.user_tx_xid,
                });
            }

            if remaining != 0 {
                // Shouldn't happen — the sum check above gated us. If
                // we're here, snapshot.layers had a layer with
                // effective_qty different from what we walked through.
                // Roll back this event's rows and emit error.
                depletion_inserts.truncate(event_start);
                per_event.push(PocEventResult {
                    event_seq: ev.event_seq,
                    applied_unit_cost: 0,
                    applied_total_cost: 0,
                    error: Some(PocError::InsufficientInventory),
                });
                continue;
            }

            let applied_total_cost = total_cost as i64;
            let applied_unit_cost = if ev.qty != 0 {
                applied_total_cost / ev.qty
            } else {
                0
            };
            per_event.push(PocEventResult {
                event_seq: ev.event_seq,
                applied_unit_cost,
                applied_total_cost,
                error: None,
            });
        }

        PocApplyResult {
            per_event,
            depletion_inserts,
            consumption_inserts: vec![],
            layer_inserts: vec![],
        }
    }
}

pub static FIFO: FifoMethod = FifoMethod;
