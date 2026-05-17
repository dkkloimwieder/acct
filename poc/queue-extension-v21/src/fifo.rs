//! FIFO method impl per spec §1.3.
//!
//! M1.3 (acct-wrwf). Walks `SkuPoolState.layers` in (born_at, born_seq)
//! order; depletes effective_qty layer-by-layer until the consumed
//! qty is exhausted. For positive-qty events (receipts) emits a new
//! layer.
//!
//! M1.3 scope: InvAdjust (signed qty), PoReceipt (positive), InvIssue
//! (negative), SoShipment (negative). WoComplete deferred to M6.1.

use crate::cost_method::{
    LayerView, PocV21ApplyResult, PocV21CostMethod, PocV21DepletionRow, PocV21Event,
    PocV21EventResult, PocV21EventType, PocV21LayerRow, PocV21PostingLineInventoryRow,
    PocV21PostingLineRow, PocV21Snapshot,
};

pub struct FifoMethod;

pub static FIFO_METHOD: FifoMethod = FifoMethod;

impl PocV21CostMethod for FifoMethod {
    fn method_id(&self) -> &'static str {
        "fifo"
    }

    fn apply_one(
        &self,
        event: &PocV21Event,
        snapshot: &mut PocV21Snapshot,
        result: &mut PocV21ApplyResult,
    ) -> PocV21EventResult {
        // Classify direction by qty sign + event_type. M6.1: WoComplete
        // dispatches on qty sign — positive = output receipt, negative =
        // component consumption.
        let is_receipt = matches!(event.event_type, PocV21EventType::PoReceipt)
            || (matches!(
                event.event_type,
                PocV21EventType::InvAdjust | PocV21EventType::WoComplete
            ) && event.qty > 0);

        let is_consumption = matches!(
            event.event_type,
            PocV21EventType::InvIssue | PocV21EventType::SoShipment
        ) || (matches!(
            event.event_type,
            PocV21EventType::InvAdjust | PocV21EventType::WoComplete
        ) && event.qty < 0);

        if is_receipt {
            self.emit_layer(event, snapshot, result);
            PocV21EventResult { correlation_id: event.correlation_id, error_code: None }
        } else if is_consumption {
            self.deplete(event, snapshot, result)
        } else {
            // qty==0 InvAdjust is a no-op; emit a posting line at amount=0
            // so the audit trail shows the call but no cost movement.
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

impl FifoMethod {
    fn emit_layer(
        &self,
        event: &PocV21Event,
        snapshot: &mut PocV21Snapshot,
        result: &mut PocV21ApplyResult,
    ) {
        let key = (event.sku_id, event.location_id);
        let pool = snapshot.sku_pools.entry(key).or_default();
        pool.max_born_seq += 1;
        let new_born_seq = pool.max_born_seq;
        let qty = event.qty.abs();
        let unit_cost = event.unit_cost;

        let layer_view = LayerView {
            layer_id: 0, // Filled in at Step 5 via RETURNING (size-1 batch trick: post-INSERT scan).
            unit_cost,
            effective_qty: qty,
            born_at_micros: event.at_micros,
            born_seq: new_born_seq,
            correlation_id: event.correlation_id,
        };
        pool.layers.push(layer_view);

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

        // Posting line: debit inventory, credit cash/ap_unsettled stub.
        let amount = qty * unit_cost;
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
        // posting_line_inventory: positive qty on receipt.
        result.posting_line_inventory_inserts.push(PocV21PostingLineInventoryRow {
            posting_line_ordinal: result.posting_line_inserts.len() - 1,
            sku_id: event.sku_id,
            location_id: event.location_id,
            qty,
            layer_id: None, // Resolved post-INSERT in Step 5.
        });
    }

    fn deplete(
        &self,
        event: &PocV21Event,
        snapshot: &mut PocV21Snapshot,
        result: &mut PocV21ApplyResult,
    ) -> PocV21EventResult {
        let key = (event.sku_id, event.location_id);
        let mut remaining = event.qty.abs();
        let mut total_cost: i64 = 0;
        let depletions_before = result.depletion_inserts.len();

        let pool = match snapshot.sku_pools.get_mut(&key) {
            Some(p) => p,
            None => {
                // Insufficient inventory — PoC policy: emit error result;
                // the committer marks the envelope failed (status='failed').
                return PocV21EventResult {
                    correlation_id: event.correlation_id,
                    error_code: Some(format!(
                        "insufficient_inventory: sku={} location={} requested={} available=0",
                        event.sku_id, event.location_id, remaining
                    )),
                };
            }
        };

        for layer in pool.layers.iter_mut() {
            if remaining == 0 {
                break;
            }
            if layer.effective_qty <= 0 {
                continue;
            }
            let take = remaining.min(layer.effective_qty);
            layer.effective_qty -= take;

            let consumed_seq = snapshot
                .max_consumed_seq_per_layer
                .entry(layer.layer_id)
                .and_modify(|s| *s += 1)
                .or_insert(1);

            result.depletion_inserts.push(PocV21DepletionRow {
                layer_id: layer.layer_id,
                qty: take,
                unit_cost: layer.unit_cost,
                consumed_at_micros: event.at_micros,
                consumed_seq: *consumed_seq,
                issue_id: event.issue_id,
                method_used: "fifo",
                correlation_id: event.correlation_id,
                user_tx_xid: event.user_tx_xid,
            });
            total_cost += take * layer.unit_cost;
            remaining -= take;
        }

        if remaining > 0 {
            // Roll back the in-memory mutations for this event.
            result.depletion_inserts.truncate(depletions_before);
            // Restore layer effective_qty (we know we drained from top).
            // For M1.3 simplicity, signal error; the committer drops this
            // envelope. Snapshot mutations within the failed event are
            // partial but the committer aborts the whole envelope so
            // those mutations never get persisted. Subsequent events in
            // the same batch (none at M1.3 size-1) would see partial
            // state — at M3+ the trait will need a more robust rollback
            // hook.
            return PocV21EventResult {
                correlation_id: event.correlation_id,
                error_code: Some(format!(
                    "insufficient_inventory: sku={} location={} short_by={}",
                    event.sku_id, event.location_id, remaining
                )),
            };
        }

        // Posting line: debit cogs/ar_unsettled stub, credit inventory.
        result.posting_line_inserts.push(PocV21PostingLineRow {
            business_date_jdate: event.business_date_jdate,
            doc_chrono: event.doc_chrono,
            document_id: event.document_id,
            sub_priority: event.sub_priority,
            event_type: event_type_name(event.event_type),
            amount: total_cost,
            debit_account: Some(offset_consumption(event)),
            credit_account: Some(account_inventory(event.sku_id, event.location_id)),
            correlation_id: event.correlation_id,
            user_tx_xid: event.user_tx_xid,
        });
        result.posting_line_inventory_inserts.push(PocV21PostingLineInventoryRow {
            posting_line_ordinal: result.posting_line_inserts.len() - 1,
            sku_id: event.sku_id,
            location_id: event.location_id,
            qty: -event.qty.abs(),
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

// Stub account-mapping fns; M1.3 PoC doesn't model a proper chart of
// accounts. These return deterministic surrogate account IDs.
fn account_inventory(sku_id: i64, location_id: i64) -> i64 {
    1_000_000 + sku_id * 1000 + location_id
}

fn account_wip(wo_id: i64, op_id: i64) -> i64 {
    7_000_000 + wo_id * 1000 + op_id
}

fn offset_receipt(event: &PocV21Event) -> i64 {
    match event.event_type {
        PocV21EventType::PoReceipt => 2_001, // ap_unsettled stub
        PocV21EventType::InvAdjust => 3_001, // inv_adjust_offset stub
        PocV21EventType::WoComplete => account_wip(event.wo_id, event.op_id),
        _ => 9_999,
    }
}

fn offset_consumption(event: &PocV21Event) -> i64 {
    match event.event_type {
        PocV21EventType::SoShipment => 4_001, // ar_unsettled stub
        PocV21EventType::InvIssue => 5_001,   // cogs stub
        PocV21EventType::InvAdjust => 3_001,  // inv_adjust_offset stub
        PocV21EventType::WoComplete => account_wip(event.wo_id, event.op_id),
        _ => 9_999,
    }
}
