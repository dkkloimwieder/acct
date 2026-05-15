//! acct-4d4n.6 (M2.3): StdMethod — standard-cost lookup dispatch.
//!
//! Reads `snapshot.standard_cost` (populated by `build_snapshot_std`
//! from the committer; sources from `poc_standard_costs` by sku_id).
//! Every consumption applies at the same `standard_cost` regardless of
//! inflow/outflow history — there is no running aggregate for STD.
//!
//! ## Cardinality
//!
//! One `PocConsumptionRow` per event.
//!
//! ## No standard cost set
//!
//! If `snapshot.standard_cost` is None, every event surfaces
//! `InsufficientInventory` (the variant overloads to mean "method's
//! prerequisite — a configured standard — is missing"; a dedicated
//! StandardCostNotConfigured error variant would be cleaner but adds
//! surface beyond the M2.1 PocError enum's PoC scope).
//!
//! ## No qty check
//!
//! STD has no inventory dimension at PoC scope — a "consumption" is
//! purely a cost lookup. Real ERPs combine STD with an inventory leg
//! and PPV; that's out of M2.3 scope (the SoT is in the consolidated
//! `acct` codebase, not the queue PoC).

use crate::cost_method::{
    PocApplyBatch, PocApplyResult, PocConsumptionRow, PocCostMethod, PocError,
    PocEventResult, PocSnapshot,
};

pub struct StdMethod;

impl PocCostMethod for StdMethod {
    fn method_id(&self) -> &'static str {
        "std"
    }

    fn plan_apply(
        &self,
        batch: &PocApplyBatch,
        snapshot: &PocSnapshot,
    ) -> PocApplyResult {
        let std_cost = snapshot.standard_cost;

        let mut per_event: Vec<PocEventResult> =
            Vec::with_capacity(batch.events.len());
        let mut consumption_inserts: Vec<PocConsumptionRow> = Vec::new();

        for ev in &batch.events {
            if ev.qty <= 0 || std_cost.is_none() {
                per_event.push(PocEventResult {
                    event_seq: ev.event_seq,
                    applied_unit_cost: 0,
                    applied_total_cost: 0,
                    error: Some(PocError::InsufficientInventory),
                });
                continue;
            }
            let unit = std_cost.unwrap();
            let total = unit.saturating_mul(ev.qty);
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

pub static STD: StdMethod = StdMethod;
