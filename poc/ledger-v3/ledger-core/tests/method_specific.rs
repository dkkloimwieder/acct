//! Tests for specific-id method (design-v3 §3.5). K=1 invariant is a usage
//! convention, not enforced — apply_specific is FIFO with a different method
//! check.

use std::collections::HashMap;

use chrono::Utc;
use ledger_core::{
    plan_apply, LedgerError, LineType, PoolMethod, PoolStateMutation, PoolStateRow, Snapshot,
    TrxLineRequest,
};

fn specific_snapshot(pool_id: i64, layers: Vec<PoolStateRow>, max_seq: i64) -> Snapshot {
    let mut method_of = HashMap::new();
    method_of.insert(pool_id, PoolMethod::Specific);
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
        line_type: LineType::PoReceiptLine,
        source_id: Some(1),
        qty,
        unit_cost,
        debit_account: 100,
        credit_account: 200,
    }
}

#[test]
fn receipt_of_single_unit_creates_one_qty1_layer() {
    let mut snap = specific_snapshot(1, vec![], 0);
    let r = plan_apply(&mut snap, &[line(1, 1, 500)], Utc::now()).unwrap();

    assert_eq!(r.trx_lines.len(), 1);
    assert_eq!(r.trx_lines[0].qty, 1);
    assert_eq!(r.trx_lines[0].unit_cost, 500);

    assert!(matches!(
        r.pool_state_mutations[0],
        PoolStateMutation::Insert {
            qty: 1,
            unit_cost: 500,
            ..
        }
    ));

    assert_eq!(snap.pools[&1].len(), 1);
}

#[test]
fn depletion_of_the_unit_deletes_the_layer() {
    let mut snap = specific_snapshot(
        1,
        vec![PoolStateRow {
            layer_seq: 3,
            qty: 1,
            unit_cost: 500,
            last_trx_line_id: 777,
        }],
        3,
    );
    let r = plan_apply(&mut snap, &[line(1, -1, 0)], Utc::now()).unwrap();

    assert_eq!(r.trx_lines.len(), 1);
    assert_eq!(r.trx_lines[0].qty, -1);
    assert_eq!(r.trx_lines[0].unit_cost, 500);
    assert_eq!(r.trx_lines[0].source_trx_line_id, Some(777));

    assert!(matches!(
        r.pool_state_mutations[0],
        PoolStateMutation::Delete { layer_seq: 3, .. }
    ));

    assert!(snap.pools[&1].is_empty());
}

#[test]
fn double_depletion_of_a_single_unit_is_insufficient() {
    let mut snap = specific_snapshot(
        1,
        vec![PoolStateRow {
            layer_seq: 3,
            qty: 1,
            unit_cost: 500,
            last_trx_line_id: 777,
        }],
        3,
    );
    let err = plan_apply(&mut snap, &[line(1, -2, 0)], Utc::now()).unwrap_err();
    assert!(matches!(
        err,
        LedgerError::InsufficientInventory {
            requested: 2,
            available: 1,
            ..
        }
    ));
}
