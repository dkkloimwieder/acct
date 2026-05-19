//! Standard-cost method per spec §1.3.
//!
//! M2.2 (acct-lgll). Standard cost lives in `poc_v21_standard_costs`
//! (effective-dated history); the committer hydrates the latest cost
//! per pool via `DISTINCT ON (sku_id, location_id) ORDER BY
//! effective_from DESC`. STD events post at standard cost regardless
//! of caller-supplied `unit_cost` — purchase / sales variance is acct's
//! concern, not the extension's. Per spec, STD does NOT track pool depth
//! (no `avg_total_qty`-equivalent state). Negative pool quantities are
//! acceptable by accounting policy.
//!
//! Receipt under STD: no `cost_layers` row is emitted (acct-xwu3) —
//! STD layers are never depleted (cost_consumptions is the live path),
//! so the row is dead-weight on the bulk-insert hot path. Posting line:
//! debit inventory, credit ap_unsettled at qty * std_cost. WoComplete
//! output under STD is the one exception: its cost_layer is the audit
//! trail of WO output cost (component-derived, not std), read by
//! downstream audit/recon queries — that path passes emit_layer=true.
//!
//! Consumption under STD: emit a cost_consumptions row at standard
//! cost; no pool-depth check; posting line: debit COGS, credit
//! inventory at qty * std_cost. Dedup contract is shared with AVG:
//! `UNIQUE (issue_id, method_used)` on cost_consumptions.
//!
//! If no standard cost is configured for the (sku, location), the
//! event reaches `state='failed'` with `error_code=standard_cost_missing`.

use crate::cost_method::{
    LayerView, PocV21ApplyResult, PocV21ConsumptionRow, PocV21CostMethod, PocV21Event,
    PocV21EventResult, PocV21EventType, PocV21LayerRow, PocV21PostingLineInventoryRow,
    PocV21PostingLineRow, PocV21Snapshot, SkuPoolState,
};

pub struct StandardMethod;

pub static STANDARD_METHOD: StandardMethod = StandardMethod;

impl PocV21CostMethod for StandardMethod {
    fn method_id(&self) -> &'static str {
        "std"
    }

    fn apply_one(
        &self,
        event: &PocV21Event,
        snapshot: &mut PocV21Snapshot,
        result: &mut PocV21ApplyResult,
    ) -> PocV21EventResult {
        // M6.1 (acct-o1yv): WoComplete output (qty>0) emits at
        // event.unit_cost (= total_component_cost / output_qty,
        // dispatcher-computed). Variance vs the output's own standard
        // cost is acct's concern, not the extension's. Standard cost
        // lookup is skipped for this path.
        if matches!(event.event_type, PocV21EventType::WoComplete) && event.qty > 0 {
            self.receive(event, event.unit_cost, snapshot, result, true);
            return PocV21EventResult {
                correlation_id: event.correlation_id,
                error_code: None,
            };
        }

        // Look up the standard cost. If absent, fail the event with a
        // clear code; acct must seed standard_costs before STD events
        // are accepted for that (sku, location).
        let std_cost = match snapshot
            .standard_costs
            .get(&(event.sku_id, event.location_id))
        {
            Some(&c) => c,
            None => {
                return PocV21EventResult {
                    correlation_id: event.correlation_id,
                    error_code: Some(format!(
                        "standard_cost_missing: sku={} location={}",
                        event.sku_id, event.location_id
                    )),
                };
            }
        };

        let is_receipt = matches!(event.event_type, PocV21EventType::PoReceipt)
            || (matches!(event.event_type, PocV21EventType::InvAdjust) && event.qty > 0);
        let is_consumption = matches!(
            event.event_type,
            PocV21EventType::InvIssue | PocV21EventType::SoShipment
        ) || (matches!(
            event.event_type,
            PocV21EventType::InvAdjust | PocV21EventType::WoComplete
        ) && event.qty < 0);

        if is_receipt {
            self.receive(event, std_cost, snapshot, result, false);
            PocV21EventResult { correlation_id: event.correlation_id, error_code: None }
        } else if is_consumption {
            self.consume(event, std_cost, snapshot, result);
            PocV21EventResult { correlation_id: event.correlation_id, error_code: None }
        } else {
            // qty==0 InvAdjust — audit-only no-op.
            result.posting_line_inserts.push(PocV21PostingLineRow {
                business_date_jdate: event.business_date_jdate,
                doc_chrono: event.doc_chrono,
                document_id: event.document_id,
                sub_priority: event.sub_priority,
                event_type: event_type_name(event.event_type),
                amount: 0,
                debit_account: None,
                credit_account: None,
                correlation_id: event.correlation_id,
                user_tx_xid: event.user_tx_xid,
            });
            PocV21EventResult { correlation_id: event.correlation_id, error_code: None }
        }
    }
}

impl StandardMethod {
    fn receive(
        &self,
        event: &PocV21Event,
        std_cost: i64,
        snapshot: &mut PocV21Snapshot,
        result: &mut PocV21ApplyResult,
        emit_layer: bool,
    ) {
        let key = (event.sku_id, event.location_id);
        let pool = snapshot.sku_pools.entry(key).or_insert_with(SkuPoolState::default);
        let qty = event.qty.abs();
        let amount = qty * std_cost;

        if emit_layer {
            pool.max_born_seq += 1;
            let new_born_seq = pool.max_born_seq;
            result.layer_inserts.push(PocV21LayerRow {
                sku_id: event.sku_id,
                location_id: event.location_id,
                qty,
                unit_cost: std_cost,
                born_at_micros: event.at_micros,
                born_seq: new_born_seq,
                source_kind: source_kind_name(event.event_type),
                source_ref: None,
                correlation_id: event.correlation_id,
                user_tx_xid: event.user_tx_xid,
            });
            pool.layers.push(LayerView {
                layer_id: 0,
                layer_insert_index: Some(result.layer_inserts.len() - 1),
                unit_cost: std_cost,
                effective_qty: qty,
                born_at_micros: event.at_micros,
                born_seq: new_born_seq,
                correlation_id: event.correlation_id,
            });
        }

        result.posting_line_inserts.push(PocV21PostingLineRow {
            business_date_jdate: event.business_date_jdate,
            doc_chrono: event.doc_chrono,
            document_id: event.document_id,
            sub_priority: event.sub_priority,
            event_type: event_type_name(event.event_type),
            amount,
            debit_account: Some(account_inventory(event.sku_id, event.location_id)),
            credit_account: Some(offset_receipt(event)),
            correlation_id: event.correlation_id,
            user_tx_xid: event.user_tx_xid,
        });
        result.posting_line_inventory_inserts.push(PocV21PostingLineInventoryRow {
            posting_line_ordinal: result.posting_line_inserts.len() - 1,
            sku_id: event.sku_id,
            location_id: event.location_id,
            qty,
            layer_id: None,
        });
    }

    fn consume(
        &self,
        event: &PocV21Event,
        std_cost: i64,
        _snapshot: &mut PocV21Snapshot,
        result: &mut PocV21ApplyResult,
    ) {
        let qty = event.qty.abs();
        let amount = qty * std_cost;

        result.consumption_inserts.push(PocV21ConsumptionRow {
            sku_id: event.sku_id,
            location_id: event.location_id,
            qty,
            unit_cost: std_cost,
            consumed_at_micros: event.at_micros,
            consumed_seq: 1,
            issue_id: event.issue_id,
            method_used: "std",
            correlation_id: event.correlation_id,
            user_tx_xid: event.user_tx_xid,
        });

        result.posting_line_inserts.push(PocV21PostingLineRow {
            business_date_jdate: event.business_date_jdate,
            doc_chrono: event.doc_chrono,
            document_id: event.document_id,
            sub_priority: event.sub_priority,
            event_type: event_type_name(event.event_type),
            amount,
            debit_account: Some(offset_consumption(event)),
            credit_account: Some(account_inventory(event.sku_id, event.location_id)),
            correlation_id: event.correlation_id,
            user_tx_xid: event.user_tx_xid,
        });
        result.posting_line_inventory_inserts.push(PocV21PostingLineInventoryRow {
            posting_line_ordinal: result.posting_line_inserts.len() - 1,
            sku_id: event.sku_id,
            location_id: event.location_id,
            qty: -qty,
            layer_id: None,
        });
    }
}

fn event_type_name(t: PocV21EventType) -> &'static str {
    match t {
        PocV21EventType::InvAdjust => "inv_adjust",
        PocV21EventType::InvIssue => "inv_issue",
        PocV21EventType::PoReceipt => "po_receipt",
        PocV21EventType::SoShipment => "so_shipment",
        PocV21EventType::WoComplete => "wo_complete",
    }
}

fn source_kind_name(t: PocV21EventType) -> &'static str {
    match t {
        PocV21EventType::PoReceipt => "receipt",
        PocV21EventType::InvAdjust => "adjustment",
        PocV21EventType::WoComplete => "wo_complete",
        _ => "other",
    }
}

fn account_inventory(sku_id: i64, location_id: i64) -> i64 {
    1_000_000 + sku_id * 1000 + location_id
}

fn account_wip(wo_id: i64, op_id: i64) -> i64 {
    7_000_000 + wo_id * 1000 + op_id
}

fn offset_receipt(event: &PocV21Event) -> i64 {
    match event.event_type {
        PocV21EventType::PoReceipt => 2_001,
        PocV21EventType::InvAdjust => 3_001,
        PocV21EventType::WoComplete => account_wip(event.wo_id, event.op_id),
        _ => 9_999,
    }
}

fn offset_consumption(event: &PocV21Event) -> i64 {
    match event.event_type {
        PocV21EventType::SoShipment => 4_001,
        PocV21EventType::InvIssue => 5_001,
        PocV21EventType::InvAdjust => 3_001,
        PocV21EventType::WoComplete => account_wip(event.wo_id, event.op_id),
        _ => 9_999,
    }
}
