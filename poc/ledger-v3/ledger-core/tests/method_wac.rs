//! Tests for WAC method (design-v3 §3.1) including the LOAD-BEARING
//! §3.1 divide-by-zero guard.

use std::collections::HashMap;

use chrono::Utc;
use ledger_core::{
    plan_apply, LedgerError, LineType, PoolMethod, PoolStateMutation, PoolStateRow, Snapshot,
    TrxLineRequest,
};

fn wac_snapshot(pool_id: i64, layer0: Option<(i64, i64)>, max_seq: i64) -> Snapshot {
    let mut method_of = HashMap::new();
    method_of.insert(pool_id, PoolMethod::Wac);
    let mut max_trx_seq_of = HashMap::new();
    max_trx_seq_of.insert(pool_id, max_seq);
    let mut pools = HashMap::new();
    if let Some((qty, unit_cost)) = layer0 {
        pools.insert(
            pool_id,
            vec![PoolStateRow {
                layer_seq: 0,
                qty,
                unit_cost,
                last_trx_line_id: 0,
            }],
        );
    }
    Snapshot {
        pools,
        method_of,
        max_trx_seq_of,
        std_cost_of: HashMap::new(),
    }
}

fn line(pool_id: i64, qty: i64, unit_cost: i64) -> TrxLineRequest {
    TrxLineRequest {
        pool_id,
        line_type: LineType::PoReceiptLine,
        source_id: Some(1),
        qty,
        unit_cost,
        debit_account: 100,
        credit_account: 200,
    }
}

#[test]
fn first_receipt_inserts_layer_at_caller_unit_cost() {
    let mut snap = wac_snapshot(1, None, 0);
    let r = plan_apply(&mut snap, &[line(1, 10, 50)], Utc::now()).unwrap();

    assert_eq!(r.trx_lines.len(), 1);
    assert_eq!(r.trx_lines[0].qty, 10);
    assert_eq!(r.trx_lines[0].unit_cost, 50);

    assert_eq!(r.pool_state_mutations.len(), 1);
    assert_eq!(
        r.pool_state_mutations[0],
        PoolStateMutation::Insert {
            pool_id: 1,
            layer_seq: 0,
            qty: 10,
            unit_cost: 50,
            last_trx_line_idx: 0,
        }
    );

    assert_eq!(snap.pools[&1][0].qty, 10);
    assert_eq!(snap.pools[&1][0].unit_cost, 50);
}

#[test]
fn subsequent_receipt_upserts_running_average() {
    // Start: qty=10, cost=50 → 500 value. Add 10 at 100 → 1000 value.
    // new_qty=20, new_avg = (500+1000)/20 = 75.
    let mut snap = wac_snapshot(1, Some((10, 50)), 5);
    let r = plan_apply(&mut snap, &[line(1, 10, 100)], Utc::now()).unwrap();

    assert_eq!(r.pool_state_mutations.len(), 1);
    assert_eq!(
        r.pool_state_mutations[0],
        PoolStateMutation::Upsert {
            pool_id: 1,
            layer_seq: 0,
            qty: 20,
            unit_cost: 75,
            last_trx_line_idx: 0,
        }
    );
    assert_eq!(snap.pools[&1][0].unit_cost, 75);
}

#[test]
fn depletion_uses_running_average_and_updates_qty() {
    // Pool: qty=10, cost=50. Deplete 3 → cost stays 50, qty=7.
    let mut snap = wac_snapshot(1, Some((10, 50)), 0);
    let r = plan_apply(&mut snap, &[line(1, -3, 999)], Utc::now()).unwrap();

    assert_eq!(r.trx_lines.len(), 1);
    assert_eq!(r.trx_lines[0].qty, -3);
    assert_eq!(r.trx_lines[0].unit_cost, 50); // running avg, NOT caller's 999

    assert_eq!(r.pool_state_mutations.len(), 1);
    assert_eq!(
        r.pool_state_mutations[0],
        PoolStateMutation::Update {
            pool_id: 1,
            layer_seq: 0,
            qty: 7,
        }
    );

    assert_eq!(r.posting_lines[0].amount, 150); // 3 * 50

    // Snapshot: qty updated, cost unchanged.
    assert_eq!(snap.pools[&1][0].qty, 7);
    assert_eq!(snap.pools[&1][0].unit_cost, 50);
}

#[test]
fn depletion_exceeding_qty_returns_insufficient_inventory() {
    let mut snap = wac_snapshot(1, Some((5, 100)), 0);
    let err = plan_apply(&mut snap, &[line(1, -10, 0)], Utc::now()).unwrap_err();
    assert!(matches!(
        err,
        LedgerError::InsufficientInventory {
            requested: 10,
            available: 5,
            ..
        }
    ));
    // Snapshot untouched
    assert_eq!(snap.pools[&1][0].qty, 5);
}

// === LOAD-BEARING §3.1 DIVIDE-BY-ZERO GUARD TESTS ===

#[test]
fn guard_deplete_to_exactly_zero_then_receipt_preserves_basis() {
    // Start: qty=10, cost=50. Deplete 10 → qty=0, cost still 50 (preserved).
    // Then receive 5 at 999 → new_qty=5, guard fired earlier (no division
    // through zero); now new_avg = (0*50 + 5*999) / 5 = 999.
    let mut snap = wac_snapshot(1, Some((10, 50)), 0);
    let r = plan_apply(&mut snap, &[line(1, -10, 0), line(1, 5, 999)], Utc::now()).unwrap();

    // After step 1 (depletion to zero), in-memory layer has qty=0 cost=50
    // (preserved). After step 2 (receipt), the running avg is 999 because
    // the prior basis at qty=0 doesn't weight in (0*50=0).
    assert_eq!(r.trx_lines.len(), 2);
    assert_eq!(r.trx_lines[0].unit_cost, 50); // depletion at original avg
    assert_eq!(r.trx_lines[1].unit_cost, 999); // receipt at caller cost

    // Final snapshot
    assert_eq!(snap.pools[&1][0].qty, 5);
    assert_eq!(snap.pools[&1][0].unit_cost, 999);
}

#[test]
fn guard_receipt_into_oversold_position_preserves_old_cost() {
    // Construct an oversold pool directly (qty=-5, cost=100). This wouldn't
    // happen via the dispatcher (deplete past zero is blocked by
    // InsufficientInventory) but the guard exists for defensive reasons —
    // future code paths or replays may construct such states.
    //
    // Receive 3 at 50 → new_qty = -5 + 3 = -2 ≤ 0 → guard fires, cost stays 100.
    let mut snap = wac_snapshot(1, Some((-5, 100)), 0);
    let r = plan_apply(&mut snap, &[line(1, 3, 50)], Utc::now()).unwrap();

    assert_eq!(r.pool_state_mutations.len(), 1);
    // new_qty=-2, but cost preserved
    assert_eq!(
        r.pool_state_mutations[0],
        PoolStateMutation::Upsert {
            pool_id: 1,
            layer_seq: 0,
            qty: -2,
            unit_cost: 100, // preserved through new_qty ≤ 0
            last_trx_line_idx: 0,
        }
    );
    assert_eq!(snap.pools[&1][0].unit_cost, 100);
}

#[test]
fn guard_receipt_into_oversold_then_back_positive_resumes_weighted_average() {
    // Start: qty=-5, cost=100. Receipt 3 at 50 → qty=-2, cost preserved=100.
    // Receipt 5 at 80 → new_qty=3 (>0!), so weighted avg = (-2*100 + 5*80) / 3
    //   = (-200 + 400) / 3 = 200/3 = 66 (integer division).
    // The basis-preservation in the prior step propagated correctly.
    let mut snap = wac_snapshot(1, Some((-5, 100)), 0);
    let r = plan_apply(&mut snap, &[line(1, 3, 50), line(1, 5, 80)], Utc::now()).unwrap();

    // Two lines into the same WAC pool emit two Upserts at layer_seq=0;
    // plan_apply's dedupe collapses them to one Upsert carrying the
    // cumulative state (snapshot is updated in-place between lines, so
    // the second Upsert already has the final qty / unit_cost). Folding
    // is required because the bulk-write UPSERT batch can't accept two
    // input rows targeting the same conflict-target row (SQLSTATE 21000).
    assert_eq!(r.pool_state_mutations.len(), 1);
    let only = &r.pool_state_mutations[0];
    match only {
        PoolStateMutation::Upsert {
            qty: 3,
            unit_cost: 66,
            ..
        } => (),
        other => panic!("expected Upsert qty=3 unit_cost=66, got {:?}", other),
    }
    assert_eq!(snap.pools[&1][0].qty, 3);
    assert_eq!(snap.pools[&1][0].unit_cost, 66);
}

#[test]
fn arithmetic_overflow_in_weighted_average_is_caught() {
    // Construct one that overflows i64 in term1 = old_qty * old_unit_cost.
    let mut snap = wac_snapshot(1, Some((i64::MAX, 2)), 0);
    let err = plan_apply(&mut snap, &[line(1, 1, 1)], Utc::now()).unwrap_err();
    assert!(matches!(err, LedgerError::Overflow { .. }), "got {err:?}");
}

#[test]
fn unknown_pool_errors_out_for_wac_too() {
    let mut snap = wac_snapshot(1, None, 0);
    let err = plan_apply(&mut snap, &[line(999, 10, 100)], Utc::now()).unwrap_err();
    assert!(matches!(err, LedgerError::UnknownPool(999)));
}
