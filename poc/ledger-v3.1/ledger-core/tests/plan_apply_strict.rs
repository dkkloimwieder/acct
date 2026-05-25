//! §9.1 unit tests for plan_apply strict mode (WAC / STD / specific), plus the
//! FIFO/LIFO MethodMismatch stubs.

use chrono::{DateTime, TimeZone, Utc};
use ledger_core::{
    plan_apply, LedgerError, LineType, PoolMethod, PoolStateMutation, PoolStateRow,
    PostingEventType, Snapshot, TrxLineRequest,
};

fn ts() -> DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000, 0).unwrap()
}

fn pool(s: &mut Snapshot, id: i64, m: PoolMethod) {
    s.method_of.insert(id, m);
    s.sku_location_of.insert(id, (id * 10, id * 10 + 1));
}

fn seed_aggregate(s: &mut Snapshot, id: i64, qty: i64, unit_cost: i64) {
    s.pools
        .entry(id)
        .or_default()
        .push(PoolStateRow { layer_id: 0, qty, unit_cost });
}

fn receipt(pool_id: i64, qty: i64, cost: i64) -> TrxLineRequest {
    TrxLineRequest {
        pool_id,
        line_type: LineType::PoReceiptLine,
        source_id: Some(1),
        qty,
        unit_cost: cost,
        debit_account: 100,
        credit_account: 200,
        variance_account: Some(300),
    }
}

fn deplete(pool_id: i64, qty: i64) -> TrxLineRequest {
    TrxLineRequest {
        pool_id,
        line_type: LineType::InvAdjustmentLine,
        source_id: Some(2),
        qty: -qty,
        unit_cost: 0,
        debit_account: 400,
        credit_account: 100,
        variance_account: None,
    }
}

fn only_aggregate_mutations(r: &ledger_core::PlanResult) -> bool {
    r.pool_state_mutations
        .iter()
        .all(|m| matches!(m, PoolStateMutation::UpsertAggregate { .. }))
}

// ---- WAC ----

#[test]
fn wac_single_receipt_sets_aggregate() {
    let mut s = Snapshot::default();
    pool(&mut s, 1, PoolMethod::Wac);
    let r = plan_apply(&mut s, &[receipt(1, 10, 100)], ts()).unwrap();
    assert_eq!(r.trx_lines.len(), 1);
    assert_eq!(r.trx_lines[0].unit_cost, 100);
    assert_eq!(r.trx_lines[0].source_trx_line_id, None);
    assert_eq!(
        r.pool_state_mutations,
        vec![PoolStateMutation::UpsertAggregate { pool_id: 1, qty: 10, unit_cost: 100 }]
    );
    assert_eq!(r.posting_lines[0].event_type, PostingEventType::InventoryReceipt);
    assert_eq!(r.posting_lines[0].amount, 1000);
}

#[test]
fn wac_mixed_cost_receipts_average_with_banker_rounding() {
    let mut s = Snapshot::default();
    pool(&mut s, 1, PoolMethod::Wac);
    // 3 @ 100 then 1 @ 150 -> (300 + 150) / 4 = 112.5 -> 112 (round-half-to-even).
    let r = plan_apply(&mut s, &[receipt(1, 3, 100), receipt(1, 1, 150)], ts()).unwrap();
    let last = r.pool_state_mutations.last().unwrap();
    assert_eq!(*last, PoolStateMutation::UpsertAggregate { pool_id: 1, qty: 4, unit_cost: 112 });
}

#[test]
fn wac_depletion_uses_running_average_unchanged_cost() {
    let mut s = Snapshot::default();
    pool(&mut s, 1, PoolMethod::Wac);
    seed_aggregate(&mut s, 1, 20, 150);
    let r = plan_apply(&mut s, &[deplete(1, 5)], ts()).unwrap();
    assert_eq!(r.trx_lines[0].qty, -5);
    assert_eq!(r.trx_lines[0].unit_cost, 150);
    assert_eq!(r.trx_lines[0].source_trx_line_id, None);
    assert_eq!(
        r.pool_state_mutations,
        vec![PoolStateMutation::UpsertAggregate { pool_id: 1, qty: 15, unit_cost: 150 }]
    );
    assert_eq!(r.posting_lines[0].amount, 750);
    assert_eq!(r.posting_lines[0].event_type, PostingEventType::InventoryDepletion);
}

#[test]
fn wac_zero_qty_receipt_is_noop_no_panic() {
    // Defensive guard (§3.1): qty=0 against an empty pool must not divide by zero.
    let mut s = Snapshot::default();
    pool(&mut s, 1, PoolMethod::Wac);
    let r = plan_apply(&mut s, &[receipt(1, 0, 100)], ts()).unwrap();
    assert!(r.trx_lines.is_empty());
    assert!(r.pool_state_mutations.is_empty());
}

#[test]
fn wac_depletion_exceeding_qty_raises_insufficient() {
    let mut s = Snapshot::default();
    pool(&mut s, 1, PoolMethod::Wac);
    seed_aggregate(&mut s, 1, 3, 100);
    let err = plan_apply(&mut s, &[deplete(1, 5)], ts()).unwrap_err();
    assert_eq!(
        err,
        LedgerError::InsufficientInventory { pool_id: 1, requested: 5, available: 3 }
    );
}

// ---- STD ----

#[test]
fn std_receipt_emits_variance_posting() {
    let mut s = Snapshot::default();
    pool(&mut s, 1, PoolMethod::Std);
    s.standard_cost_of.insert(1, 100); // C_std = 100
    // receipt 10 @ actual 120 -> trx_line at std 100; variance = 10*(120-100)=200.
    let r = plan_apply(&mut s, &[receipt(1, 10, 120)], ts()).unwrap();
    assert_eq!(r.trx_lines[0].unit_cost, 100);
    assert_eq!(r.posting_lines.len(), 2);
    assert_eq!(r.posting_lines[0].event_type, PostingEventType::InventoryReceipt);
    assert_eq!(r.posting_lines[0].amount, 1000); // 10 * 100 std
    let var = &r.posting_lines[1];
    assert_eq!(var.event_type, PostingEventType::Variance);
    assert_eq!(var.amount, 200);
    assert_eq!(var.debit_account, 300); // variance acct debited (unfavorable)
    assert_eq!(var.credit_account, 200); // source
    assert_eq!(
        r.pool_state_mutations,
        vec![PoolStateMutation::UpsertAggregate { pool_id: 1, qty: 10, unit_cost: 100 }]
    );
}

#[test]
fn std_favorable_variance_flips_direction() {
    let mut s = Snapshot::default();
    pool(&mut s, 1, PoolMethod::Std);
    s.standard_cost_of.insert(1, 100);
    // actual 90 < std 100 -> favorable; variance 10*(90-100) = -100.
    let r = plan_apply(&mut s, &[receipt(1, 10, 90)], ts()).unwrap();
    let var = &r.posting_lines[1];
    assert_eq!(var.amount, 100);
    assert_eq!(var.debit_account, 200); // source debited
    assert_eq!(var.credit_account, 300); // variance credited
}

#[test]
fn std_equal_cost_emits_no_variance_leg() {
    let mut s = Snapshot::default();
    pool(&mut s, 1, PoolMethod::Std);
    s.standard_cost_of.insert(1, 100);
    let r = plan_apply(&mut s, &[receipt(1, 10, 100)], ts()).unwrap();
    assert_eq!(r.posting_lines.len(), 1);
}

#[test]
fn std_depletion_at_standard_cost() {
    let mut s = Snapshot::default();
    pool(&mut s, 1, PoolMethod::Std);
    s.standard_cost_of.insert(1, 100);
    seed_aggregate(&mut s, 1, 10, 100);
    let r = plan_apply(&mut s, &[deplete(1, 4)], ts()).unwrap();
    assert_eq!(r.trx_lines[0].unit_cost, 100);
    assert_eq!(r.posting_lines.len(), 1);
    assert_eq!(r.posting_lines[0].amount, 400);
    assert_eq!(
        r.pool_state_mutations,
        vec![PoolStateMutation::UpsertAggregate { pool_id: 1, qty: 6, unit_cost: 100 }]
    );
}

#[test]
fn std_missing_standard_cost_raises() {
    let mut s = Snapshot::default();
    pool(&mut s, 7, PoolMethod::Std); // sku_location = (70, 71)
    let err = plan_apply(&mut s, &[receipt(7, 5, 100)], ts()).unwrap_err();
    assert_eq!(err, LedgerError::MissingStandardCost { sku_id: 70, location_id: 71 });
}

// ---- Specific ----

#[test]
fn specific_receipt_materializes_layer() {
    let mut s = Snapshot::default();
    pool(&mut s, 1, PoolMethod::Specific);
    let r = plan_apply(&mut s, &[receipt(1, 1, 500)], ts()).unwrap();
    assert_eq!(r.trx_lines[0].qty, 1);
    assert!(matches!(
        r.pool_state_mutations[0],
        PoolStateMutation::InsertLayer { pool_id: 1, layer_trx_line_idx: 0, qty: 1, unit_cost: 500 }
    ));
    assert!(matches!(
        r.pool_state_mutations[1],
        PoolStateMutation::UpsertAggregate { pool_id: 1, qty: 1, .. }
    ));
}

#[test]
fn specific_depletion_consumes_layer_and_links_source() {
    let mut s = Snapshot::default();
    pool(&mut s, 1, PoolMethod::Specific);
    // Simulate a committed receipt: layer_id = 42 (the receipt's trx_line.id).
    seed_aggregate(&mut s, 1, 1, 500);
    s.pools.get_mut(&1).unwrap().push(PoolStateRow { layer_id: 42, qty: 1, unit_cost: 500 });

    let r = plan_apply(&mut s, &[deplete(1, 1)], ts()).unwrap();
    assert_eq!(r.trx_lines[0].unit_cost, 500);
    assert_eq!(r.trx_lines[0].source_trx_line_id, Some(42));
    assert!(r
        .pool_state_mutations
        .iter()
        .any(|m| matches!(m, PoolStateMutation::DeleteLayer { pool_id: 1, layer_id: 42 })));
}

#[test]
fn specific_second_depletion_raises_insufficient() {
    let mut s = Snapshot::default();
    pool(&mut s, 1, PoolMethod::Specific);
    seed_aggregate(&mut s, 1, 1, 500);
    s.pools.get_mut(&1).unwrap().push(PoolStateRow { layer_id: 42, qty: 1, unit_cost: 500 });
    // Two depletions in one submission: first consumes the layer, second finds none.
    let err = plan_apply(&mut s, &[deplete(1, 1), deplete(1, 1)], ts()).unwrap_err();
    assert_eq!(
        err,
        LedgerError::InsufficientInventory { pool_id: 1, requested: 1, available: 0 }
    );
}

// ---- FIFO/LIFO strict stubs ----

#[test]
fn strict_fifo_returns_method_mismatch() {
    let mut s = Snapshot::default();
    pool(&mut s, 1, PoolMethod::Fifo);
    let err = plan_apply(&mut s, &[receipt(1, 10, 100)], ts()).unwrap_err();
    assert!(matches!(err, LedgerError::MethodMismatch { pool_id: 1, .. }));
}

#[test]
fn strict_lifo_returns_method_mismatch() {
    let mut s = Snapshot::default();
    pool(&mut s, 1, PoolMethod::Lifo);
    let err = plan_apply(&mut s, &[receipt(1, 10, 100)], ts()).unwrap_err();
    assert!(matches!(err, LedgerError::MethodMismatch { pool_id: 1, .. }));
}

#[test]
fn unknown_pool_raises() {
    let mut s = Snapshot::default();
    let err = plan_apply(&mut s, &[receipt(99, 1, 1)], ts()).unwrap_err();
    assert_eq!(err, LedgerError::UnknownPool(99));
}

#[test]
fn aggregate_only_for_wac_and_std() {
    let mut s = Snapshot::default();
    pool(&mut s, 1, PoolMethod::Wac);
    pool(&mut s, 2, PoolMethod::Std);
    s.standard_cost_of.insert(2, 50);
    let r = plan_apply(&mut s, &[receipt(1, 5, 10), receipt(2, 5, 50)], ts()).unwrap();
    assert!(only_aggregate_mutations(&r));
}
