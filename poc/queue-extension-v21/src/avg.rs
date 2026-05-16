//! AVG (running weighted average) method per spec §1.3.
//!
//! M2.1 (acct-hmuf). The pool's running state lives in
//! `SkuPoolState.avg_unit_cost` + `avg_total_qty`, hydrated by the
//! committer at Step 3 from `poc_v21_avg_pool_state` (NEVER
//! reconstructed from cost_layers history — spec §1.3 contract).
//!
//! Receipt: weighted-average roll-in.
//!   new_total_value = avg_unit_cost * avg_total_qty + qty * unit_cost
//!   new_total_qty   = avg_total_qty + qty
//!   new_avg_unit_cost = new_total_value / new_total_qty
//! Rounding: BIGINT integer division (truncation toward zero). Emits a
//! layer row for traceability (AVG doesn't deplete layers individually,
//! but cost_layers is the audit trail of cost-pool inflows; bake-off's
//! S8 mixed-method shape audits this).
//!
//! Consumption: emit at current avg_unit_cost; decrement avg_total_qty;
//! avg_unit_cost UNCHANGED (only receipts shift it). Insufficient
//! inventory (qty > avg_total_qty) → error, same shape as FIFO.
//!
//! Multi-event same-batch AVG: each event mutates snapshot in place,
//! so the second event sees the first's roll-in. Step 5 UPSERT
//! persists the post-batch state.

use crate::cost_method::{
    LayerView, PocV21ApplyResult, PocV21ConsumptionRow, PocV21CostMethod, PocV21Event,
    PocV21EventResult, PocV21EventType, PocV21LayerRow, PocV21PostingLineInventoryRow,
    PocV21PostingLineRow, PocV21Snapshot, SkuPoolState,
};

pub struct AvgMethod;

pub static AVG_METHOD: AvgMethod = AvgMethod;

impl PocV21CostMethod for AvgMethod {
    fn method_id(&self) -> &'static str {
        "avg"
    }

    fn apply_one(
        &self,
        event: &PocV21Event,
        snapshot: &mut PocV21Snapshot,
        result: &mut PocV21ApplyResult,
    ) -> PocV21EventResult {
        let is_receipt = matches!(event.event_type, PocV21EventType::PoReceipt)
            || (matches!(event.event_type, PocV21EventType::InvAdjust) && event.qty > 0);
        let is_consumption = matches!(
            event.event_type,
            PocV21EventType::InvIssue | PocV21EventType::SoShipment
        ) || (matches!(event.event_type, PocV21EventType::InvAdjust) && event.qty < 0);

        if is_receipt {
            self.roll_in(event, snapshot, result);
            PocV21EventResult { correlation_id: event.correlation_id, error_code: None }
        } else if is_consumption {
            self.consume(event, snapshot, result)
        } else {
            // qty==0 InvAdjust: audit-only, no movement.
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

impl AvgMethod {
    fn roll_in(
        &self,
        event: &PocV21Event,
        snapshot: &mut PocV21Snapshot,
        result: &mut PocV21ApplyResult,
    ) {
        let key = (event.sku_id, event.location_id);
        let pool = snapshot.sku_pools.entry(key).or_insert_with(SkuPoolState::default);
        let qty = event.qty.abs();
        let unit_cost = event.unit_cost;
        let amount = qty * unit_cost;

        // Weighted-average roll-in. i128 intermediate for headroom.
        let old_value: i128 = (pool.avg_unit_cost as i128) * (pool.avg_total_qty as i128);
        let new_value: i128 = old_value + (qty as i128) * (unit_cost as i128);
        let new_qty: i128 = (pool.avg_total_qty as i128) + (qty as i128);
        pool.avg_unit_cost = if new_qty == 0 {
            0
        } else {
            (new_value / new_qty) as i64
        };
        pool.avg_total_qty = new_qty as i64;
        pool.avg_dirty = true;
        pool.max_born_seq += 1;
        let new_born_seq = pool.max_born_seq;

        // Layer row for traceability. AVG doesn't deplete layers
        // individually, but the cost_layers audit trail records every
        // pool inflow with its source. M2.2's STD method does NOT emit
        // layer rows (no layer concept under STD); AVG sits between
        // FIFO (layers consumed) and STD (no layers).
        result.layer_inserts.push(PocV21LayerRow {
            sku_id: event.sku_id,
            location_id: event.location_id,
            qty,
            unit_cost,
            born_at_micros: event.at_micros,
            born_seq: new_born_seq,
            source_kind: source_kind_name(event.event_type),
            source_ref: None,
            correlation_id: event.correlation_id,
            user_tx_xid: event.user_tx_xid,
        });
        // Track the layer in the snapshot too — bake-off P5 needs it
        // for the dump-and-replay invariant. AVG doesn't *read* layers
        // for cost computation, but it does write them, so the
        // snapshot stays consistent with what Step 5 will INSERT.
        pool.layers.push(LayerView {
            layer_id: 0,
            unit_cost,
            effective_qty: qty,
            born_at_micros: event.at_micros,
            born_seq: new_born_seq,
            correlation_id: event.correlation_id,
        });

        result.posting_line_inserts.push(PocV21PostingLineRow {
            business_date_jdate: event.business_date_jdate,
            doc_chrono: event.doc_chrono,
            document_id: event.document_id,
            sub_priority: event.sub_priority,
            event_type: event_type_name(event.event_type),
            amount,
            debit_account: Some(account_inventory(event.sku_id, event.location_id)),
            credit_account: Some(account_offset_receipt(event.event_type)),
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
        snapshot: &mut PocV21Snapshot,
        result: &mut PocV21ApplyResult,
    ) -> PocV21EventResult {
        let key = (event.sku_id, event.location_id);
        let qty = event.qty.abs();
        let pool = match snapshot.sku_pools.get_mut(&key) {
            Some(p) if p.avg_total_qty > 0 => p,
            _ => {
                return PocV21EventResult {
                    correlation_id: event.correlation_id,
                    error_code: Some(format!(
                        "insufficient_inventory: sku={} location={} requested={} avg_pool_qty=0",
                        event.sku_id, event.location_id, qty
                    )),
                };
            }
        };
        if qty > pool.avg_total_qty {
            return PocV21EventResult {
                correlation_id: event.correlation_id,
                error_code: Some(format!(
                    "insufficient_inventory: sku={} location={} requested={} avg_pool_qty={}",
                    event.sku_id, event.location_id, qty, pool.avg_total_qty
                )),
            };
        }
        let unit_cost = pool.avg_unit_cost;
        let amount = qty * unit_cost;
        pool.avg_total_qty -= qty;
        pool.avg_dirty = true;

        // consumption row (cost_consumptions, not cost_depletions).
        // No layer_id; AVG bills against the pool average.
        // consumed_seq is a per-pool monotone counter — for AVG, since
        // there's no per-layer tracking, we use a synthetic key
        // (issue_id-scoped is enough; the UNIQUE (issue_id, method_used)
        // constraint on cost_consumptions is the dedup contract).
        result.consumption_inserts.push(PocV21ConsumptionRow {
            sku_id: event.sku_id,
            location_id: event.location_id,
            qty,
            unit_cost,
            consumed_at_micros: event.at_micros,
            consumed_seq: 1,
            issue_id: event.issue_id,
            method_used: "avg",
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
            debit_account: Some(account_offset_consumption(event.event_type)),
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

        PocV21EventResult { correlation_id: event.correlation_id, error_code: None }
    }
}

fn event_type_name(t: PocV21EventType) -> &'static str {
    match t {
        PocV21EventType::InvAdjust => "inv_adjust",
        PocV21EventType::InvIssue => "inv_issue",
        PocV21EventType::PoReceipt => "po_receipt",
        PocV21EventType::SoShipment => "so_shipment",
    }
}

fn source_kind_name(t: PocV21EventType) -> &'static str {
    match t {
        PocV21EventType::PoReceipt => "receipt",
        PocV21EventType::InvAdjust => "adjustment",
        _ => "other",
    }
}

fn account_inventory(sku_id: i64, location_id: i64) -> i64 {
    1_000_000 + sku_id * 1000 + location_id
}

fn account_offset_receipt(t: PocV21EventType) -> i64 {
    match t {
        PocV21EventType::PoReceipt => 2_001,
        PocV21EventType::InvAdjust => 3_001,
        _ => 9_999,
    }
}

fn account_offset_consumption(t: PocV21EventType) -> i64 {
    match t {
        PocV21EventType::SoShipment => 4_001,
        PocV21EventType::InvIssue => 5_001,
        PocV21EventType::InvAdjust => 3_001,
        _ => 9_999,
    }
}
