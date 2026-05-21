//! Tests for LIFO method (design-v3 §3.3). Same shape as FIFO but
//! depletion walks layers DESC by layer_seq.

use std::collections::HashMap;

use chrono::Utc;
use ledger_core::{
    plan_apply, LedgerError, LineType, PoolMethod, PoolStateMutation, PoolStateRow, Snapshot,
    TrxLineRequest,
};

fn lifo_snapshot(pool_id: i64, layers: Vec<PoolStateRow>, max_seq: i64) -> Snapshot {
    let mut method_of = HashMap::new();
    method_of.insert(pool_id, PoolMethod::Lifo);
    let mut max_trx_seq_of = HashMap::new();
    max_trx_seq_of.insert(pool_id, max_seq);
    let mut pools = HashMap::new();
    if !layers.is_empty() {
        pools.insert(pool_id, layers);
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
        line_type: LineType::InvAdjustmentLine,
        source_id: None,
        qty,
        unit_cost,
        debit_account: 100,
        credit_account: 200,
    }
}

fn layer(seq: i64, qty: i64, unit_cost: i64, src_id: i64) -> PoolStateRow {
    PoolStateRow {
        layer_seq: seq,
        qty,
        unit_cost,
        last_trx_line_id: src_id,
    }
}

#[test]
fn multi_layer_depletion_walks_newest_first() {
    // L1(seq=3, qty=5, cost=10), L2(seq=5, qty=5, cost=20)
    let mut snap = lifo_snapshot(
        1,
        vec![layer(3, 5, 10, 111), layer(5, 5, 20, 222)],
        5,
    );
    // Deplete 8 → consumes L2 fully (5 @ cost 20) + L1 partially (3 @ cost 10)
    let r = plan_apply(&mut snap, &[line(1, -8, 0)], Utc::now()).unwrap();

    // First emitted is L2 (newest), then L1
    assert_eq!(r.trx_lines.len(), 2);
    assert_eq!(r.trx_lines[0].unit_cost, 20);
    assert_eq!(r.trx_lines[0].source_trx_line_id, Some(222));
    assert_eq!(r.trx_lines[1].unit_cost, 10);
    assert_eq!(r.trx_lines[1].source_trx_line_id, Some(111));

    // L2 deleted, L1 updated to qty=2
    assert!(matches!(
        r.pool_state_mutations[0],
        PoolStateMutation::Delete { layer_seq: 5, .. }
    ));
    assert!(matches!(
        r.pool_state_mutations[1],
        PoolStateMutation::Update {
            layer_seq: 3,
            qty: 2,
            ..
        }
    ));

    assert_eq!(r.posting_lines[0].amount, 100); // 5*20 from L2 first
    assert_eq!(r.posting_lines[1].amount, 30); //  3*10 from L1

    assert_eq!(snap.pools[&1].len(), 1);
    assert_eq!(snap.pools[&1][0].layer_seq, 3);
    assert_eq!(snap.pools[&1][0].qty, 2);
}

#[test]
fn single_layer_partial_depletion_matches_fifo_shape() {
    let mut snap = lifo_snapshot(1, vec![layer(7, 10, 50, 999)], 7);
    let r = plan_apply(&mut snap, &[line(1, -3, 0)], Utc::now()).unwrap();
    assert_eq!(r.trx_lines.len(), 1);
    assert_eq!(r.trx_lines[0].qty, -3);
    assert_eq!(r.trx_lines[0].source_trx_line_id, Some(999));
}

#[test]
fn receipt_creates_layer_at_trx_seq() {
    let mut snap = lifo_snapshot(1, vec![], 0);
    let r = plan_apply(&mut snap, &[line(1, 10, 50)], Utc::now()).unwrap();
    assert_eq!(r.trx_lines[0].trx_seq, 1);
    assert_eq!(snap.pools[&1].len(), 1);
}

#[test]
fn oversold_lifo() {
    let mut snap = lifo_snapshot(1, vec![layer(1, 3, 10, 1), layer(2, 2, 20, 2)], 2);
    let err = plan_apply(&mut snap, &[line(1, -10, 0)], Utc::now()).unwrap_err();
    assert!(matches!(
        err,
        LedgerError::InsufficientInventory {
            requested: 10,
            available: 5,
            ..
        }
    ));
}
