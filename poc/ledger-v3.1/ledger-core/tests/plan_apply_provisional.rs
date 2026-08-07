//! §9.1 unit tests for plan_apply_provisional (Path C FIFO/LIFO hot path).
//!
//! Verifies: receipts update the aggregate per the WAC formula; depletions use
//! the running-average or standard basis; source_trx_line_id is NULL; no layer
//! rows are mutated; missing standard cost on a 'standard'-basis pool raises.

use chrono::{DateTime, TimeZone, Utc};
use ledger_core::{
    plan_apply_provisional, LedgerError, LineType, PlanResult, PoolMethod, PoolStateMutation,
    PoolStateRow, PostingAccounts, ProvisionalBasis, Snapshot, TrxLineRequest,
};

fn ts() -> DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000, 0).unwrap()
}

/// Posting accounts hydrated for every test pool (§3.7). Pairs are stored in
/// receipt direction; depletions swap. PoReceiptLine -> receipt (100,200);
/// TransferShipmentLine -> transfer (100,400) swapped to (400,100) on depletion.
fn posting_accounts() -> PostingAccounts {
    PostingAccounts {
        receipt: (100, 200),
        transfer: (100, 400),
        build: (100, 500),
        scrap: (100, 700),
        adjustment: (100, 800),
        revaluation: (100, 900),
        variance: Some(300),
    }
}

fn fifo_pool(s: &mut Snapshot, id: i64, basis: ProvisionalBasis) {
    s.method_of.insert(id, PoolMethod::Fifo);
    s.provisional_basis_of.insert(id, basis);
    s.sku_location_of.insert(id, (id * 10, id * 10 + 1));
    s.posting_accounts_of.insert(id, posting_accounts());
}

fn seed_aggregate(s: &mut Snapshot, id: i64, qty: i64, unit_cost: i64) {
    s.pools
        .entry(id)
        .or_default()
        .push(PoolStateRow { layer_id: 0, qty, unit_cost, value_sum: qty * unit_cost });
}

fn receipt(pool_id: i64, qty: i64, cost: i64) -> TrxLineRequest {
    TrxLineRequest {
        pool_id,
        line_type: LineType::PoReceiptLine,
        source_id: Some(1),
        qty,
        unit_cost: cost,
    }
}

fn deplete(pool_id: i64, qty: i64) -> TrxLineRequest {
    TrxLineRequest {
        pool_id,
        line_type: LineType::TransferShipmentLine,
        source_id: Some(2),
        qty: -qty,
        unit_cost: 0,
    }
}

/// Path C must never materialize layer rows on the hot path (§3.5): every
/// mutation is an aggregate upsert.
fn no_layer_mutations(r: &PlanResult) -> bool {
    r.pool_state_mutations
        .iter()
        .all(|m| matches!(m, PoolStateMutation::UpsertAggregate { .. }))
}

#[test]
fn fifo_receipts_maintain_running_average() {
    let mut s = Snapshot::default();
    fifo_pool(&mut s, 1, ProvisionalBasis::RunningAvg);
    // 10 @ 100 then 10 @ 200 -> running avg 150.
    let r = plan_apply_provisional(&mut s, &[receipt(1, 10, 100), receipt(1, 10, 200)], ts())
        .unwrap();
    assert!(no_layer_mutations(&r));
    assert_eq!(
        *r.pool_state_mutations.last().unwrap(),
        // value_sum = 10*100 + 10*200 = 3000; unit_cost = 3000/20 = 150.
        PoolStateMutation::UpsertAggregate { pool_id: 1, qty: 20, unit_cost: 150, value_sum: 3000 }
    );
    // Receipt trx_lines record the actual asserted cost, not the average.
    assert_eq!(r.trx_lines[0].unit_cost, 100);
    assert_eq!(r.trx_lines[1].unit_cost, 200);
}

#[test]
fn fifo_running_avg_depletion_uses_aggregate_cost() {
    let mut s = Snapshot::default();
    fifo_pool(&mut s, 1, ProvisionalBasis::RunningAvg);
    seed_aggregate(&mut s, 1, 20, 150);
    let r = plan_apply_provisional(&mut s, &[deplete(1, 5)], ts()).unwrap();
    assert_eq!(r.trx_lines[0].unit_cost, 150); // provisional = running avg
    assert_eq!(r.trx_lines[0].source_trx_line_id, None); // §3.5: NULL
    assert!(no_layer_mutations(&r));
    assert_eq!(
        r.pool_state_mutations,
        // seed value_sum 20*150=3000; deplete 5 at avg 150 removes 750 -> 2250; 2250/15 = 150.
        vec![PoolStateMutation::UpsertAggregate { pool_id: 1, qty: 15, unit_cost: 150, value_sum: 2250 }]
    );
    assert_eq!(r.posting_lines[0].amount, 750);
}

#[test]
fn fifo_standard_basis_depletion_uses_standard_not_average() {
    let mut s = Snapshot::default();
    fifo_pool(&mut s, 1, ProvisionalBasis::Standard);
    s.standard_cost_of.insert(1, 90);
    // Receipt drives the running avg to 100, but standard basis ignores it.
    let r = plan_apply_provisional(&mut s, &[receipt(1, 10, 100), deplete(1, 5)], ts()).unwrap();
    let dep = &r.trx_lines[1];
    assert_eq!(dep.unit_cost, 90); // standard, not the 100 running avg
    assert_eq!(dep.source_trx_line_id, None);
    // amount booked at the standard provisional cost.
    let dep_posting = r.posting_lines.last().unwrap();
    assert_eq!(dep_posting.amount, 450);
}

#[test]
fn standard_basis_missing_standard_cost_raises_on_depletion() {
    let mut s = Snapshot::default();
    fifo_pool(&mut s, 3, ProvisionalBasis::Standard); // sku_location (30,31), no std cost
    seed_aggregate(&mut s, 3, 10, 100);
    let err = plan_apply_provisional(&mut s, &[deplete(3, 1)], ts()).unwrap_err();
    assert_eq!(err, LedgerError::MissingStandardCost { sku_id: 30, location_id: 31 });
}

#[test]
fn standard_basis_over_book_depletion_drives_value_sum_negative() {
    // §3.5 + migration 0009 (acct-mvq4.22): standard-basis depletions price at
    // standard_cost, decoupled from the book average. Depleting deep while
    // standard_cost > avg posts more value out than the pool carries; the
    // GL-reconcilable value_sum (and its derived average) go negative rather
    // than erroring or clamping — the deferred recalc tier (§7) trues it up.
    let mut s = Snapshot::default();
    fifo_pool(&mut s, 1, ProvisionalBasis::Standard);
    s.standard_cost_of.insert(1, 150);
    seed_aggregate(&mut s, 1, 10, 100); // book value 1000
    let r = plan_apply_provisional(&mut s, &[deplete(1, 7)], ts()).unwrap();
    assert!(no_layer_mutations(&r));
    assert_eq!(r.trx_lines[0].unit_cost, 150);
    assert_eq!(r.posting_lines[0].amount, 1050, "7 × 150 posted to the GL");
    assert_eq!(
        r.pool_state_mutations,
        // 1000 − 1050 = −50 at qty 3; derived avg banker_div(−50, 3) = −17.
        vec![PoolStateMutation::UpsertAggregate { pool_id: 1, qty: 3, unit_cost: -17, value_sum: -50 }]
    );
}

#[test]
fn running_avg_rounding_edge_briefly_negative_then_clears_on_empty() {
    // Twin edge on the running-avg basis: banker-rounding the derived average
    // UP (value 3 / qty 5 → avg 1) times a large partial deplete posts 4 out
    // of a book value of 3 → value_sum −1 at qty 1. Emptying the pool clears
    // the residual to 0 (acct-0qps).
    let mut s = Snapshot::default();
    fifo_pool(&mut s, 1, ProvisionalBasis::RunningAvg);
    // 3 @ 1 + 2 @ 0 → qty 5, value_sum 3, avg = banker_div(3, 5) = 1.
    let r = plan_apply_provisional(
        &mut s,
        &[receipt(1, 3, 1), receipt(1, 2, 0), deplete(1, 4)],
        ts(),
    )
    .unwrap();
    assert_eq!(
        r.pool_state_mutations,
        // deplete 4 @ avg 1 = 4 > book 3 → value_sum −1; avg follows to −1.
        vec![PoolStateMutation::UpsertAggregate { pool_id: 1, qty: 1, unit_cost: -1, value_sum: -1 }]
    );
    // Draining the last unit empties the pool: residual forced to 0.
    let r2 = plan_apply_provisional(&mut s, &[deplete(1, 1)], ts()).unwrap();
    assert_eq!(
        r2.pool_state_mutations,
        vec![PoolStateMutation::UpsertAggregate { pool_id: 1, qty: 0, unit_cost: -1, value_sum: 0 }]
    );
}

#[test]
fn lifo_behaves_identically_under_provisional() {
    let mut s = Snapshot::default();
    s.method_of.insert(1, PoolMethod::Lifo);
    s.provisional_basis_of.insert(1, ProvisionalBasis::RunningAvg);
    s.sku_location_of.insert(1, (10, 11));
    s.posting_accounts_of.insert(1, posting_accounts());
    let r = plan_apply_provisional(&mut s, &[receipt(1, 10, 100), deplete(1, 4)], ts()).unwrap();
    assert!(no_layer_mutations(&r));
    assert_eq!(r.trx_lines[1].unit_cost, 100);
    assert_eq!(r.trx_lines[1].source_trx_line_id, None);
}

#[test]
fn provisional_depletion_exceeding_qty_raises_insufficient() {
    let mut s = Snapshot::default();
    fifo_pool(&mut s, 1, ProvisionalBasis::RunningAvg);
    seed_aggregate(&mut s, 1, 3, 100);
    let err = plan_apply_provisional(&mut s, &[deplete(1, 5)], ts()).unwrap_err();
    assert_eq!(
        err,
        LedgerError::InsufficientInventory { pool_id: 1, requested: 5, available: 3 }
    );
}

#[test]
fn all_provisional_depletions_have_null_source_link() {
    let mut s = Snapshot::default();
    fifo_pool(&mut s, 1, ProvisionalBasis::RunningAvg);
    seed_aggregate(&mut s, 1, 100, 50);
    let r = plan_apply_provisional(
        &mut s,
        &[deplete(1, 10), deplete(1, 20), deplete(1, 5)],
        ts(),
    )
    .unwrap();
    assert_eq!(r.trx_lines.len(), 3);
    assert!(r.trx_lines.iter().all(|l| l.source_trx_line_id.is_none()));
    assert!(no_layer_mutations(&r));
}

#[test]
fn two_lines_same_pool_coalesce_to_one_aggregate_mutation() {
    // acct-036x: a single submission with two lines on the same pool must emit
    // exactly ONE aggregate mutation (keep-last, cumulative), so the direct bulk
    // ON CONFLICT (pool_id, layer_id=0) UPSERT does not see a row twice.
    let mut s = Snapshot::default();
    fifo_pool(&mut s, 1, ProvisionalBasis::RunningAvg);
    // 10 @ 100 then 10 @ 200 -> running avg 150, total qty 20.
    let r = plan_apply_provisional(&mut s, &[receipt(1, 10, 100), receipt(1, 10, 200)], ts())
        .unwrap();
    // Both lines still produce their own trx_line (audit), but the aggregate
    // mutations collapse to one carrying the cumulative state.
    assert_eq!(r.trx_lines.len(), 2);
    assert_eq!(
        r.pool_state_mutations,
        // value_sum = 10*100 + 10*200 = 3000; unit_cost = 3000/20 = 150.
        vec![PoolStateMutation::UpsertAggregate { pool_id: 1, qty: 20, unit_cost: 150, value_sum: 3000 }]
    );
}

#[test]
fn coalesce_keeps_one_aggregate_per_pool_across_mixed_pools() {
    // Distinct pools keep their own aggregate; only same-pool duplicates collapse.
    let mut s = Snapshot::default();
    fifo_pool(&mut s, 1, ProvisionalBasis::RunningAvg);
    fifo_pool(&mut s, 2, ProvisionalBasis::RunningAvg);
    seed_aggregate(&mut s, 1, 100, 50);
    seed_aggregate(&mut s, 2, 100, 70);
    // pool 1 touched twice (deplete, deplete), pool 2 once.
    let r = plan_apply_provisional(
        &mut s,
        &[deplete(1, 10), deplete(2, 5), deplete(1, 20)],
        ts(),
    )
    .unwrap();
    assert_eq!(r.trx_lines.len(), 3);
    // One aggregate per touched pool: pool 1 at 100-10-20=70, pool 2 at 95.
    let mut aggs: Vec<_> = r
        .pool_state_mutations
        .iter()
        .map(|m| match m {
            PoolStateMutation::UpsertAggregate { pool_id, qty, .. } => (*pool_id, *qty),
            _ => panic!("unexpected layer mutation"),
        })
        .collect();
    aggs.sort();
    assert_eq!(aggs, vec![(1, 70), (2, 95)]);
}

#[test]
fn provisional_dispatches_wac_std_specific_to_strict() {
    // A WAC pool routed through the Path C entry point still runs strict WAC.
    let mut s = Snapshot::default();
    s.method_of.insert(5, PoolMethod::Wac);
    s.sku_location_of.insert(5, (50, 51));
    s.posting_accounts_of.insert(5, posting_accounts());
    seed_aggregate(&mut s, 5, 10, 100);
    let r = plan_apply_provisional(&mut s, &[deplete(5, 4)], ts()).unwrap();
    assert_eq!(r.trx_lines[0].unit_cost, 100);
    assert!(no_layer_mutations(&r));
}
