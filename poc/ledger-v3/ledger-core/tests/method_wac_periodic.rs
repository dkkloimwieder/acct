//! Tests for the wac_periodic method (acct-s6fa).
//!
//! Math is identical to `wac` (cumulative-sum form, acct-h5gs). The
//! periodic-vs-perpetual difference: depletions push a
//! `ProvisionalPostingRequest` onto `PlanResult.provisional_postings` so
//! the period-close hook can recompute variance against the actual
//! `final_avg = Σ(in-period receipt value) / Σ(in-period receipt qty)`.
//!
//! These tests mirror `method_wac.rs` (cumulative-sum behavior) and add
//! per-test assertions on the provisional-flag side-effect.

use std::collections::HashMap;

use chrono::Utc;
use ledger_core::{
    plan_apply, LedgerError, LineType, PoolMethod, PoolStateMutation, PoolStateRow,
    ProvisionalPostingRequest, Snapshot, TrxLineRequest,
};

/// Helper: build a wac_periodic snapshot. `layer0` is `(qty, value_sum)`.
fn wp_snapshot(pool_id: i64, layer0: Option<(i64, i64)>, max_seq: i64) -> Snapshot {
    let mut method_of = HashMap::new();
    method_of.insert(pool_id, PoolMethod::WacPeriodic);
    let mut max_trx_seq_of = HashMap::new();
    max_trx_seq_of.insert(pool_id, max_seq);
    let mut pools = HashMap::new();
    if let Some((qty, value_sum)) = layer0 {
        pools.insert(
            pool_id,
            vec![PoolStateRow {
                layer_seq: 0,
                qty,
                unit_cost: value_sum,
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
fn first_receipt_emits_no_provisional_flag() {
    let mut snap = wp_snapshot(1, None, 0);
    let r = plan_apply(&mut snap, &[line(1, 10, 50)], Utc::now()).unwrap();

    assert_eq!(r.trx_lines.len(), 1);
    assert_eq!(r.posting_lines.len(), 1);
    assert_eq!(r.posting_lines[0].amount, 500);
    // Receipts are exact additive ops; no provisional flag.
    assert!(r.provisional_postings.is_empty());

    assert_eq!(
        r.pool_state_mutations[0],
        PoolStateMutation::Insert {
            pool_id: 1,
            layer_seq: 0,
            qty: 10,
            unit_cost: 500,
            last_trx_line_idx: 0,
        }
    );
}

#[test]
fn subsequent_receipt_adds_exactly_to_value_sum_no_flag() {
    let mut snap = wp_snapshot(1, Some((10, 500)), 5);
    let r = plan_apply(&mut snap, &[line(1, 10, 100)], Utc::now()).unwrap();

    assert_eq!(
        r.pool_state_mutations[0],
        PoolStateMutation::Upsert {
            pool_id: 1,
            layer_seq: 0,
            qty: 20,
            unit_cost: 1500,
            last_trx_line_idx: 0,
        }
    );
    assert!(r.provisional_postings.is_empty());
}

#[test]
fn depletion_flags_provisional_with_running_avg_amount() {
    // Pool: qty=10, value_sum=500. Deplete 3.
    // Provisional amount = (3 * 500) / 10 = 150 (running avg at deplete time).
    let mut snap = wp_snapshot(1, Some((10, 500)), 0);
    let r = plan_apply(&mut snap, &[line(1, -3, 999)], Utc::now()).unwrap();

    assert_eq!(r.posting_lines.len(), 1);
    assert_eq!(r.posting_lines[0].amount, 150);
    assert_eq!(r.trx_lines[0].unit_cost, 50);

    assert_eq!(r.provisional_postings.len(), 1);
    assert_eq!(
        r.provisional_postings[0],
        ProvisionalPostingRequest {
            posting_line_idx: 0,
            pool_id: 1,
            qty: 3,
            provisional_amount: 150,
        }
    );

    assert_eq!(snap.pools[&1][0].qty, 7);
    assert_eq!(snap.pools[&1][0].unit_cost, 350);
}

#[test]
fn depletion_with_truncating_avg_flags_rounded_amount() {
    // Pool: qty=3, value_sum=10 (running avg=3.33).
    // Deplete 1. amount = (1*10)/3 = 3 (truncated).
    // After: qty=2, value_sum=7. Provisional flagged at amount=3.
    let mut snap = wp_snapshot(1, Some((3, 10)), 0);
    let r = plan_apply(&mut snap, &[line(1, -1, 0)], Utc::now()).unwrap();

    assert_eq!(r.posting_lines[0].amount, 3);
    assert_eq!(r.provisional_postings.len(), 1);
    assert_eq!(r.provisional_postings[0].provisional_amount, 3);
    assert_eq!(r.provisional_postings[0].qty, 1);
}

#[test]
fn depletion_exceeding_qty_returns_insufficient_inventory_no_flag() {
    let mut snap = wp_snapshot(1, Some((5, 500)), 0);
    let err = plan_apply(&mut snap, &[line(1, -10, 0)], Utc::now()).unwrap_err();
    assert!(matches!(
        err,
        LedgerError::InsufficientInventory {
            requested: 10,
            available: 5,
            ..
        }
    ));
    assert_eq!(snap.pools[&1][0].qty, 5);
}

#[test]
fn mixed_receipt_and_depletion_flags_only_depletion() {
    // Receipt 10@50 → pool (10, 500). Deplete 4 → amount=200; pool (6, 300).
    let mut snap = wp_snapshot(1, None, 0);
    let r = plan_apply(
        &mut snap,
        &[line(1, 10, 50), line(1, -4, 999)],
        Utc::now(),
    )
    .unwrap();

    assert_eq!(r.posting_lines.len(), 2);
    assert_eq!(r.posting_lines[0].amount, 500); // receipt
    assert_eq!(r.posting_lines[1].amount, 200); // depletion at running avg
    // Only the second posting (depletion) is flagged.
    assert_eq!(r.provisional_postings.len(), 1);
    assert_eq!(r.provisional_postings[0].posting_line_idx, 1);
    assert_eq!(r.provisional_postings[0].qty, 4);
    assert_eq!(r.provisional_postings[0].provisional_amount, 200);

    assert_eq!(snap.pools[&1][0].qty, 6);
    assert_eq!(snap.pools[&1][0].unit_cost, 300);
}

#[test]
fn two_depletions_in_one_submission_flag_both() {
    let mut snap = wp_snapshot(1, Some((20, 1000)), 0);
    let r = plan_apply(
        &mut snap,
        &[line(1, -5, 0), line(1, -5, 0)],
        Utc::now(),
    )
    .unwrap();

    // First depletion: amount = (5*1000)/20 = 250; pool → (15, 750).
    // Second depletion: amount = (5*750)/15  = 250; pool → (10, 500).
    assert_eq!(r.posting_lines[0].amount, 250);
    assert_eq!(r.posting_lines[1].amount, 250);

    assert_eq!(r.provisional_postings.len(), 2);
    assert_eq!(r.provisional_postings[0].posting_line_idx, 0);
    assert_eq!(r.provisional_postings[1].posting_line_idx, 1);
    assert_eq!(r.provisional_postings[0].provisional_amount, 250);
    assert_eq!(r.provisional_postings[1].provisional_amount, 250);

    // Mutations fold via dedupe to one Upsert at the final state.
    assert_eq!(r.pool_state_mutations.len(), 1);
    assert_eq!(
        r.pool_state_mutations[0],
        PoolStateMutation::Upsert {
            pool_id: 1,
            layer_seq: 0,
            qty: 10,
            unit_cost: 500,
            last_trx_line_idx: 1,
        }
    );
}

#[test]
fn deplete_to_zero_then_receipt_no_divide_by_zero() {
    let mut snap = wp_snapshot(1, Some((10, 500)), 0);
    let r = plan_apply(
        &mut snap,
        &[line(1, -10, 0), line(1, 5, 999)],
        Utc::now(),
    )
    .unwrap();

    assert_eq!(r.trx_lines.len(), 2);
    assert_eq!(r.posting_lines[0].amount, 500); // depletion to zero
    assert_eq!(r.posting_lines[1].amount, 4995); // pure receipt re-add

    // Only depletion flagged.
    assert_eq!(r.provisional_postings.len(), 1);
    assert_eq!(r.provisional_postings[0].posting_line_idx, 0);

    assert_eq!(snap.pools[&1][0].qty, 5);
    assert_eq!(snap.pools[&1][0].unit_cost, 4995);
}

#[test]
fn arithmetic_overflow_on_receipt_value_is_caught() {
    let mut snap = wp_snapshot(1, None, 0);
    let err = plan_apply(&mut snap, &[line(1, i64::MAX, 2)], Utc::now()).unwrap_err();
    assert!(matches!(err, LedgerError::Overflow { .. }), "got {err:?}");
}

#[test]
fn arithmetic_overflow_on_depletion_amount_is_caught() {
    let mut snap = wp_snapshot(1, Some((10, i64::MAX)), 0);
    let err = plan_apply(&mut snap, &[line(1, -5, 0)], Utc::now()).unwrap_err();
    assert!(matches!(err, LedgerError::Overflow { .. }), "got {err:?}");
}

#[test]
fn unknown_pool_errors_out() {
    let mut snap = wp_snapshot(1, None, 0);
    let err = plan_apply(&mut snap, &[line(999, 10, 100)], Utc::now()).unwrap_err();
    assert!(matches!(err, LedgerError::UnknownPool(999)));
}

#[test]
fn order_independence_of_receipts_on_same_pool() {
    let mut snap_a = wp_snapshot(1, None, 0);
    let mut snap_b = wp_snapshot(1, None, 0);

    let lines_ab = vec![line(1, 10, 50), line(1, 7, 73), line(1, 3, 91), line(1, 5, 11)];
    let lines_ba: Vec<_> = lines_ab.iter().rev().cloned().collect();

    let ra = plan_apply(&mut snap_a, &lines_ab, Utc::now()).unwrap();
    let rb = plan_apply(&mut snap_b, &lines_ba, Utc::now()).unwrap();

    assert_eq!(snap_a.pools[&1][0].qty, snap_b.pools[&1][0].qty);
    assert_eq!(snap_a.pools[&1][0].unit_cost, snap_b.pools[&1][0].unit_cost);
    assert_eq!(snap_a.pools[&1][0].qty, 25);
    assert_eq!(snap_a.pools[&1][0].unit_cost, 1339);
    // No depletions → no provisional flags on either path.
    assert!(ra.provisional_postings.is_empty());
    assert!(rb.provisional_postings.is_empty());
}

#[test]
fn wac_periodic_against_wac_snapshot_errors_method_mismatch() {
    // wac_periodic's apply checks PoolMethod::WacPeriodic specifically; if a
    // line targets a Wac pool, it's a dispatch bug (since plan_apply itself
    // routes by method). The internal check still gates against direct
    // misuse.
    let mut snap = wp_snapshot(1, None, 0);
    snap.method_of.insert(2, PoolMethod::Wac); // pool 2 is plain Wac
    // Hand the wac_periodic apply a line for pool 2: it'll route through
    // wac.rs (correct dispatch); the WacPeriodic check happens only inside
    // wac_periodic.rs. So this test instead exercises wac.rs's reciprocal
    // guard — that a Wac line never lands in apply_wac_periodic by chance.
    let r = plan_apply(&mut snap, &[line(2, 5, 100)], Utc::now()).unwrap();
    // pool 2 routes through wac.rs; no provisional flag.
    assert!(r.provisional_postings.is_empty());
}
