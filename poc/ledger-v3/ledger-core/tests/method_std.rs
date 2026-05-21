//! Tests for STD method (design-v3 §3.4) driven through the public
//! `plan_apply` API. No DB; pure Rust.

use std::collections::HashMap;

use chrono::{TimeZone, Utc};
use ledger_core::{
    plan_apply, LedgerError, LineType, PoolMethod, PostingEventType, Snapshot, TrxLineRequest,
};

fn std_pool_snapshot(pool_id: i64, max_seq: i64) -> Snapshot {
    let mut method_of = HashMap::new();
    method_of.insert(pool_id, PoolMethod::Std);
    let mut max_trx_seq_of = HashMap::new();
    max_trx_seq_of.insert(pool_id, max_seq);
    Snapshot {
        pools: HashMap::new(),
        method_of,
        max_trx_seq_of,
        std_cost_of: HashMap::new(),
    }
}

fn line(pool_id: i64, qty: i64, unit_cost: i64) -> TrxLineRequest {
    TrxLineRequest {
        pool_id,
        line_type: LineType::PoReceiptLine,
        source_id: Some(42),
        qty,
        unit_cost,
        debit_account: 100,
        credit_account: 200,
    }
}

#[test]
fn single_receipt_emits_trx_line_and_posting_no_state_mutation() {
    let mut snap = std_pool_snapshot(1, 0);
    let posted = Utc.with_ymd_and_hms(2026, 5, 21, 12, 0, 0).unwrap();
    let result = plan_apply(&mut snap, &[line(1, 10, 100)], posted).unwrap();

    assert_eq!(result.trx_lines.len(), 1);
    let trx = &result.trx_lines[0];
    assert_eq!(trx.pool_id, 1);
    assert_eq!(trx.qty, 10);
    assert_eq!(trx.unit_cost, 100);
    assert_eq!(trx.trx_seq, 1);
    assert!(trx.source_trx_line_id.is_none());

    assert_eq!(result.posting_lines.len(), 1);
    let posting = &result.posting_lines[0];
    assert_eq!(posting.amount, 1000);
    assert_eq!(posting.event_type, PostingEventType::InventoryReceipt);
    assert_eq!(posting.trx_line_idx, 0);
    assert_eq!(posting.posted_at, posted);

    assert!(
        result.pool_state_mutations.is_empty(),
        "STD pools must not produce pool_state mutations"
    );
}

#[test]
fn single_depletion_flips_event_type_and_keeps_amount_unsigned() {
    let mut snap = std_pool_snapshot(1, 0);
    let result = plan_apply(&mut snap, &[line(1, -5, 100)], Utc::now()).unwrap();

    assert_eq!(result.trx_lines[0].qty, -5);
    assert_eq!(result.posting_lines[0].amount, 500);
    assert_eq!(
        result.posting_lines[0].event_type,
        PostingEventType::InventoryDepletion
    );
    assert!(result.pool_state_mutations.is_empty());
}

#[test]
fn two_lines_increment_trx_seq_continuously_from_max() {
    let mut snap = std_pool_snapshot(1, 5);
    let result = plan_apply(&mut snap, &[line(1, 10, 100), line(1, -3, 100)], Utc::now()).unwrap();

    assert_eq!(result.trx_lines.len(), 2);
    assert_eq!(result.trx_lines[0].trx_seq, 6);
    assert_eq!(result.trx_lines[1].trx_seq, 7);
    assert_eq!(snap.max_trx_seq_of[&1], 7);
}

#[test]
fn multiple_pools_have_independent_trx_seq_counters() {
    let mut snap = std_pool_snapshot(1, 0);
    snap.method_of.insert(2, PoolMethod::Std);
    snap.max_trx_seq_of.insert(2, 100);

    let result = plan_apply(
        &mut snap,
        &[line(1, 10, 50), line(2, 5, 50), line(1, -3, 50)],
        Utc::now(),
    )
    .unwrap();

    assert_eq!(result.trx_lines.len(), 3);
    assert_eq!(result.trx_lines[0].trx_seq, 1);
    assert_eq!(result.trx_lines[1].trx_seq, 101);
    assert_eq!(result.trx_lines[2].trx_seq, 2);
    assert_eq!(snap.max_trx_seq_of[&1], 2);
    assert_eq!(snap.max_trx_seq_of[&2], 101);
}

#[test]
fn posting_indices_point_at_their_trx_line() {
    let mut snap = std_pool_snapshot(1, 0);
    let result =
        plan_apply(&mut snap, &[line(1, 10, 100), line(1, 5, 200)], Utc::now()).unwrap();

    assert_eq!(result.posting_lines[0].trx_line_idx, 0);
    assert_eq!(result.posting_lines[1].trx_line_idx, 1);
    assert_eq!(result.posting_lines[0].amount, 1000);
    assert_eq!(result.posting_lines[1].amount, 1000);
}

#[test]
fn unknown_pool_errors_out() {
    let mut snap = std_pool_snapshot(1, 0);
    let err = plan_apply(&mut snap, &[line(999, 10, 100)], Utc::now()).unwrap_err();
    assert!(matches!(err, LedgerError::UnknownPool(999)), "got {err:?}");
}

#[test]
fn amount_overflow_is_caught() {
    let mut snap = std_pool_snapshot(1, 0);
    let err = plan_apply(&mut snap, &[line(1, i64::MAX, 2)], Utc::now()).unwrap_err();
    assert!(
        matches!(err, LedgerError::Overflow { .. }),
        "expected Overflow, got {err:?}"
    );
}

#[test]
fn qty_i64_min_abs_overflow_is_caught() {
    let mut snap = std_pool_snapshot(1, 0);
    let err = plan_apply(&mut snap, &[line(1, i64::MIN, 1)], Utc::now()).unwrap_err();
    assert!(
        matches!(err, LedgerError::Overflow { .. }),
        "expected Overflow, got {err:?}"
    );
}
