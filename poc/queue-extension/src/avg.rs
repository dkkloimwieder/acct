//! acct-4d4n.6 (M2.3): AvgMethod — running-average cost dispatch.
//!
//! Reads `snapshot.avg_unit_cost` + `snapshot.total_available_qty`
//! (populated by `build_snapshot_avg` from the committer; sources from
//! `poc_cost_avg` under FOR UPDATE). Every consumption in the batch
//! applies at the SAME running average — this is the WAC invariant:
//!
//!     avg = V / Q  ⇒  new_avg = (V - q·avg) / (Q - q) = avg
//!
//! Outflows don't shift the running average; only inflows do. So one
//! snapshot read suffices for an arbitrary number of consumption events
//! in the same batch.
//!
//! ## Cardinality
//!
//! One `PocConsumptionRow` per successful event (UNIQUE constraint on
//! `(issue_id, method_used)` enforces no-duplicate at the table grain).
//!
//! ## Empty pool / no aggregate
//!
//! If `avg_unit_cost` is None (no `poc_cost_avg` row for the pool) OR
//! `total_available_qty < ev.qty`, per-event `InsufficientInventory`.
//! Other events in the same batch are unaffected.

use crate::cost_method::{
    PocApplyBatch, PocApplyResult, PocConsumptionRow, PocCostMethod, PocError,
    PocEventResult, PocSnapshot,
};

pub struct AvgMethod;

impl PocCostMethod for AvgMethod {
    fn method_id(&self) -> &'static str {
        "avg"
    }

    fn plan_apply(
        &self,
        batch: &PocApplyBatch,
        snapshot: &PocSnapshot,
    ) -> PocApplyResult {
        let avg = snapshot.avg_unit_cost;
        // Track remaining qty in a local cursor so multiple consumption
        // events in the same batch can't double-consume past the pool.
        let mut remaining_qty = snapshot.total_available_qty;

        let mut per_event: Vec<PocEventResult> =
            Vec::with_capacity(batch.events.len());
        let mut consumption_inserts: Vec<PocConsumptionRow> = Vec::new();

        for ev in &batch.events {
            if ev.qty <= 0 || ev.qty > remaining_qty || avg.is_none() {
                per_event.push(PocEventResult {
                    event_seq: ev.event_seq,
                    applied_unit_cost: 0,
                    applied_total_cost: 0,
                    error: Some(PocError::InsufficientInventory),
                });
                continue;
            }
            let unit = avg.unwrap();
            let total = unit.saturating_mul(ev.qty);
            remaining_qty -= ev.qty;
            per_event.push(PocEventResult {
                event_seq: ev.event_seq,
                applied_unit_cost: unit,
                applied_total_cost: total,
                error: None,
            });
            consumption_inserts.push(PocConsumptionRow {
                sku_id: batch.pool.sku_id,
                location_id: batch.pool.location_id,
                qty: ev.qty,
                applied_unit_cost: unit,
                issue_id: ev.issue_id,
                event_seq: ev.event_seq,
                user_tx_xid: ev.user_tx_xid,
            });
        }

        PocApplyResult {
            per_event,
            depletion_inserts: vec![],
            consumption_inserts,
            layer_inserts: vec![],
        }
    }
}

pub static AVG: AvgMethod = AvgMethod;
