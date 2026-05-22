//! Tests for WAC method under the cumulative-sum form (acct-h5gs).
//!
//! PoolStateRow.unit_cost field semantics for WAC rows: value_sum
//! (cumulative total value), NOT per-unit cost. See
//! `ledger_core::snapshot::PoolStateRow` and `ledger_core::wac` for the
//! per-method storage contract.

use std::collections::HashMap;

use chrono::Utc;
use ledger_core::{
    plan_apply, LedgerError, LineType, PoolMethod, PoolStateMutation, PoolStateRow, Snapshot,
    TrxLineRequest,
};

/// Helper: build a WAC snapshot. `layer0` is `(qty, value_sum)` — the
/// field name is `unit_cost` in PoolStateRow but on WAC rows it stores
/// the cumulative value_sum.
fn wac_snapshot(pool_id: i64, layer0: Option<(i64, i64)>, max_seq: i64) -> Snapshot {
    let mut method_of = HashMap::new();
    method_of.insert(pool_id, PoolMethod::Wac);
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
fn first_receipt_inserts_layer_with_exact_value_sum() {
    // Receipt of 10 at unit_cost=50 → qty=10, value_sum = 10*50 = 500.
    let mut snap = wac_snapshot(1, None, 0);
    let r = plan_apply(&mut snap, &[line(1, 10, 50)], Utc::now()).unwrap();

    assert_eq!(r.trx_lines.len(), 1);
    assert_eq!(r.trx_lines[0].qty, 10);
    // trx_line.unit_cost on receipts is the caller's input cost, unchanged.
    assert_eq!(r.trx_lines[0].unit_cost, 50);

    assert_eq!(r.pool_state_mutations.len(), 1);
    assert_eq!(
        r.pool_state_mutations[0],
        PoolStateMutation::Insert {
            pool_id: 1,
            layer_seq: 0,
            qty: 10,
            unit_cost: 500, // value_sum, not per-unit cost
            last_trx_line_idx: 0,
        }
    );
    assert_eq!(snap.pools[&1][0].qty, 10);
    assert_eq!(snap.pools[&1][0].unit_cost, 500);
    assert_eq!(r.posting_lines[0].amount, 500);
}

#[test]
fn subsequent_receipt_adds_exactly_to_value_sum() {
    // Start: qty=10, value_sum=500 (=10*50). Add 10 at 100 → 10*100 = 1000.
    // After: qty=20, value_sum = 500 + 1000 = 1500. No rounding, exact.
    // Running per-unit cost = 1500 / 20 = 75 (computed on demand only,
    // not stored).
    let mut snap = wac_snapshot(1, Some((10, 500)), 5);
    let r = plan_apply(&mut snap, &[line(1, 10, 100)], Utc::now()).unwrap();

    assert_eq!(r.pool_state_mutations.len(), 1);
    assert_eq!(
        r.pool_state_mutations[0],
        PoolStateMutation::Upsert {
            pool_id: 1,
            layer_seq: 0,
            qty: 20,
            unit_cost: 1500, // value_sum (exact additive)
            last_trx_line_idx: 0,
        }
    );
    assert_eq!(snap.pools[&1][0].qty, 20);
    assert_eq!(snap.pools[&1][0].unit_cost, 1500);
}

#[test]
fn depletion_rounds_amount_once_and_updates_value_sum_exactly() {
    // Pool: qty=10, value_sum=500 (running avg=50). Deplete 3.
    // amount = (3 * 500) / 10 = 150 (exact integer this time).
    // After: qty=7, value_sum = 500 - 150 = 350. (350/7 = 50, avg preserved.)
    let mut snap = wac_snapshot(1, Some((10, 500)), 0);
    let r = plan_apply(&mut snap, &[line(1, -3, 999)], Utc::now()).unwrap();

    assert_eq!(r.trx_lines.len(), 1);
    assert_eq!(r.trx_lines[0].qty, -3);
    // trx_line.unit_cost on depletions = amount / qty_depleted (derived,
    // possibly lossy). 150 / 3 = 50.
    assert_eq!(r.trx_lines[0].unit_cost, 50);

    // Depletion uses Upsert (carrying new value_sum), not Update — WAC
    // rows need to mutate both qty and the value_sum-bearing unit_cost
    // column atomically.
    assert_eq!(r.pool_state_mutations.len(), 1);
    assert_eq!(
        r.pool_state_mutations[0],
        PoolStateMutation::Upsert {
            pool_id: 1,
            layer_seq: 0,
            qty: 7,
            unit_cost: 350,
            last_trx_line_idx: 0,
        }
    );
    assert_eq!(r.posting_lines[0].amount, 150);
    assert_eq!(snap.pools[&1][0].qty, 7);
    assert_eq!(snap.pools[&1][0].unit_cost, 350);
}

#[test]
fn depletion_with_truncating_avg_rounds_amount_once() {
    // Pool: qty=3, value_sum=10 (running avg=3.33 → would store 3).
    // Deplete 1. amount = (1 * 10) / 3 = 3 (truncated from 3.33).
    // After: qty=2, value_sum = 10 - 3 = 7. (7/2 = 3.5; next read shows
    // an avg of 3 from int div, but value_sum is exact.)
    let mut snap = wac_snapshot(1, Some((3, 10)), 0);
    let r = plan_apply(&mut snap, &[line(1, -1, 0)], Utc::now()).unwrap();

    assert_eq!(r.posting_lines[0].amount, 3);
    assert_eq!(r.trx_lines[0].unit_cost, 3); // 3 / 1
    assert_eq!(snap.pools[&1][0].qty, 2);
    assert_eq!(snap.pools[&1][0].unit_cost, 7); // value_sum = 10 - 3

    // Sanity check: cumulative-sum vs running-average. Pre-h5gs would have
    // stored unit_cost=3 (running avg of 10/3 truncated), then on next read
    // re-derived as 3, losing the 1/3 unit of value per receipt. Here we
    // preserve the 7 cumulative value exactly.
}

#[test]
fn depletion_exceeding_qty_returns_insufficient_inventory() {
    let mut snap = wac_snapshot(1, Some((5, 500)), 0);
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
    assert_eq!(snap.pools[&1][0].unit_cost, 500);
}

#[test]
fn deplete_to_zero_then_receipt_no_divide_by_zero() {
    // Pre-h5gs: receipt at qty=0 hit the §3.1 divide-by-zero guard
    // (preserving old unit_cost as basis through the zero crossing).
    // Cumulative-sum form: receipts never divide, so no guard is needed.
    // Pool: qty=10, value_sum=500 (avg=50). Deplete 10:
    //   amount = (10*500)/10 = 500; qty=0, value_sum=0.
    // Then receive 5 at 999:
    //   qty=5, value_sum = 0 + 5*999 = 4995. (avg = 999.)
    let mut snap = wac_snapshot(1, Some((10, 500)), 0);
    let r = plan_apply(&mut snap, &[line(1, -10, 0), line(1, 5, 999)], Utc::now()).unwrap();

    assert_eq!(r.trx_lines.len(), 2);
    assert_eq!(r.trx_lines[0].unit_cost, 50); // depletion display avg = 500/10
    assert_eq!(r.trx_lines[1].unit_cost, 999); // receipt at caller cost

    // Two lines targeting the same WAC layer_seq=0 fold to a single Upsert
    // carrying the cumulative state (dedupe in plan_apply prevents the
    // bulk-write UPSERT batch from collapsing on its ON CONFLICT clause).
    assert_eq!(r.pool_state_mutations.len(), 1);
    assert_eq!(
        r.pool_state_mutations[0],
        PoolStateMutation::Upsert {
            pool_id: 1,
            layer_seq: 0,
            qty: 5,
            unit_cost: 4995, // value_sum after both ops
            last_trx_line_idx: 1,
        }
    );

    assert_eq!(snap.pools[&1][0].qty, 5);
    assert_eq!(snap.pools[&1][0].unit_cost, 4995);
}

#[test]
fn receipt_into_oversold_position_just_adds() {
    // Pre-h5gs: the §3.1 guard preserved old per-unit cost when new_qty ≤ 0
    // because dividing by a non-positive denominator was undefined.
    // Cumulative-sum: receipts are pure adds, no denominator. The
    // (qty=-5, value_sum=-500) state simply absorbs the new receipt
    // additively. (Such states aren't produced by the dispatcher — deplete
    // past zero raises InsufficientInventory — but the math is still
    // well-defined for any caller-injected snapshot.)
    let mut snap = wac_snapshot(1, Some((-5, -500)), 0);
    let r = plan_apply(&mut snap, &[line(1, 3, 50)], Utc::now()).unwrap();

    assert_eq!(r.pool_state_mutations.len(), 1);
    assert_eq!(
        r.pool_state_mutations[0],
        PoolStateMutation::Upsert {
            pool_id: 1,
            layer_seq: 0,
            qty: -2,                        // -5 + 3
            unit_cost: -500 + 3 * 50,       // -350; pure additive
            last_trx_line_idx: 0,
        }
    );
    assert_eq!(snap.pools[&1][0].qty, -2);
    assert_eq!(snap.pools[&1][0].unit_cost, -350);
}

#[test]
fn arithmetic_overflow_on_receipt_value_is_caught() {
    // Add a receipt whose Q*C overflows i64.
    let mut snap = wac_snapshot(1, None, 0);
    let err = plan_apply(&mut snap, &[line(1, i64::MAX, 2)], Utc::now()).unwrap_err();
    assert!(matches!(err, LedgerError::Overflow { .. }), "got {err:?}");
}

#[test]
fn arithmetic_overflow_on_depletion_amount_is_caught() {
    // Q * value_sum overflows i64 in the depletion numerator before
    // the /qty divisor brings it back down.
    let mut snap = wac_snapshot(1, Some((10, i64::MAX)), 0);
    let err = plan_apply(&mut snap, &[line(1, -5, 0)], Utc::now()).unwrap_err();
    assert!(matches!(err, LedgerError::Overflow { .. }), "got {err:?}");
}

#[test]
fn unknown_pool_errors_out_for_wac_too() {
    let mut snap = wac_snapshot(1, None, 0);
    let err = plan_apply(&mut snap, &[line(999, 10, 100)], Utc::now()).unwrap_err();
    assert!(matches!(err, LedgerError::UnknownPool(999)));
}

#[test]
fn order_independence_of_receipts_on_same_pool() {
    // Cumulative-sum property: receipts on a shared pool produce the same
    // (qty, value_sum) regardless of order. This is the structural fix
    // for acct-mcey (running-average compounded per-receipt rounding;
    // cumulative form has zero per-receipt rounding).
    let mut snap_a = wac_snapshot(1, None, 0);
    let mut snap_b = wac_snapshot(1, None, 0);

    let lines_ab = vec![line(1, 10, 50), line(1, 7, 73), line(1, 3, 91), line(1, 5, 11)];
    let lines_ba: Vec<_> = lines_ab.iter().rev().cloned().collect();

    plan_apply(&mut snap_a, &lines_ab, Utc::now()).unwrap();
    plan_apply(&mut snap_b, &lines_ba, Utc::now()).unwrap();

    assert_eq!(snap_a.pools[&1][0].qty, snap_b.pools[&1][0].qty);
    assert_eq!(snap_a.pools[&1][0].unit_cost, snap_b.pools[&1][0].unit_cost);
    // Sanity: qty = 10+7+3+5 = 25; value_sum = 500+511+273+55 = 1339.
    assert_eq!(snap_a.pools[&1][0].qty, 25);
    assert_eq!(snap_a.pools[&1][0].unit_cost, 1339);
}
