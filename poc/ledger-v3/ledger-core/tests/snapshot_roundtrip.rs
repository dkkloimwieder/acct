//! Idempotent-replay property test for plan_apply (acct-nmlc).
//!
//! Per design-v3 §7, pristine-snapshot replay is the contract that lets
//! ledger-routed's committer retry a commit_group after excluding a failing
//! submission. The replay rests on plan_apply being deterministic: given the
//! same (snapshot, lines, posted_at), running it twice on independent clones
//! of the snapshot must produce identical `PlanResult`s AND identical
//! snapshot mutations — Ok or Err alike.
//!
//! This binary probes that invariant across all five methods (FIFO / LIFO /
//! WAC / STD / Specific) with random snapshots and random line mixes
//! (receipts, depletions, oversold). PROPTEST_CASES defaults to 100; raise via
//! env var when investigating regressions.

use std::collections::HashMap;

use chrono::{DateTime, TimeZone, Utc};
use proptest::collection::vec as prop_vec;
use proptest::prelude::*;

use ledger_core::{
    plan_apply, LineType, PoolMethod, PoolStateRow, Snapshot, TrxLineRequest,
};

const MAX_POOLS: usize = 5;
const MAX_LAYERS_PER_POOL: usize = 3;
const MAX_LINES_PER_SUBMISSION: usize = 6;

#[derive(Debug, Clone)]
struct Case {
    snapshot: Snapshot,
    lines: Vec<TrxLineRequest>,
    posted_at: DateTime<Utc>,
}

fn method_strategy() -> impl Strategy<Value = PoolMethod> {
    prop_oneof![
        Just(PoolMethod::Fifo),
        Just(PoolMethod::Lifo),
        Just(PoolMethod::Wac),
        Just(PoolMethod::Std),
        Just(PoolMethod::Specific),
    ]
}

fn line_type_strategy() -> impl Strategy<Value = LineType> {
    prop_oneof![
        Just(LineType::PoReceiptLine),
        Just(LineType::InvAdjustmentLine),
        Just(LineType::TransferShipmentLine),
        Just(LineType::TransferReceiptLine),
        Just(LineType::ManualAdjustmentLine),
    ]
}

prop_compose! {
    fn layer_strategy(seq: i64)
        (qty in 1i64..=1_000,
         unit_cost in 1i64..=1_000,
         src in 1i64..=10_000)
        -> PoolStateRow {
        PoolStateRow {
            layer_seq: seq,
            qty,
            unit_cost,
            last_trx_line_id: src,
        }
    }
}

fn layers_for_method_strategy(
    method: PoolMethod,
) -> BoxedStrategy<Vec<PoolStateRow>> {
    match method {
        PoolMethod::Std => Just(Vec::new()).boxed(),
        PoolMethod::Wac => (0i64..=1)
            .prop_flat_map(|n| {
                if n == 0 {
                    Just(Vec::new()).boxed()
                } else {
                    layer_strategy(0).prop_map(|l| vec![l]).boxed()
                }
            })
            .boxed(),
        // FIFO / LIFO / Specific: 0..=MAX_LAYERS_PER_POOL layers with sequential
        // layer_seq starting at 1. Per design-v3 §2.2 the (pool_id, layer_seq)
        // PK + ORDER BY layer_seq contract guarantees monotonic seq.
        _ => (0usize..=MAX_LAYERS_PER_POOL)
            .prop_flat_map(|n| {
                let layer_strats: Vec<BoxedStrategy<PoolStateRow>> =
                    (1..=n as i64).map(|s| layer_strategy(s).boxed()).collect();
                layer_strats
            })
            .boxed(),
    }
}

fn pool_universe_strategy() -> impl Strategy<Value = HashMap<i64, (PoolMethod, Vec<PoolStateRow>)>> {
    (1usize..=MAX_POOLS).prop_flat_map(|n_pools| {
        let methods = prop_vec(method_strategy(), n_pools);
        methods.prop_flat_map(move |ms| {
            let layer_strats: Vec<BoxedStrategy<Vec<PoolStateRow>>> = ms
                .iter()
                .map(|m| layers_for_method_strategy(*m))
                .collect();
            let owned_methods = ms.clone();
            layer_strats.prop_map(move |all_layers| {
                let mut universe = HashMap::new();
                for (i, layers) in all_layers.into_iter().enumerate() {
                    let pool_id = (i as i64) + 1;
                    universe.insert(pool_id, (owned_methods[i], layers));
                }
                universe
            })
        })
    })
}

fn build_snapshot(universe: &HashMap<i64, (PoolMethod, Vec<PoolStateRow>)>) -> Snapshot {
    let mut pools = HashMap::new();
    let mut method_of = HashMap::new();
    let mut max_trx_seq_of = HashMap::new();
    for (pid, (method, layers)) in universe {
        method_of.insert(*pid, *method);
        let max_seq = layers.iter().map(|l| l.layer_seq).max().unwrap_or(0);
        max_trx_seq_of.insert(*pid, max_seq);
        if !layers.is_empty() {
            pools.insert(*pid, layers.clone());
        }
    }
    Snapshot {
        pools,
        method_of,
        max_trx_seq_of,
        std_cost_of: HashMap::new(),
    }
}

prop_compose! {
    fn line_against_universe(pool_ids: Vec<i64>)
        (pool_idx in 0usize..pool_ids.len(),
         qty in -2_000i64..=2_000,
         unit_cost in 1i64..=1_000,
         src in 1i64..=10_000,
         lt in line_type_strategy())
        -> TrxLineRequest {
        let qty = if qty == 0 { 1 } else { qty };
        TrxLineRequest {
            pool_id: pool_ids[pool_idx],
            line_type: lt,
            source_id: Some(src),
            qty,
            unit_cost,
            debit_account: 100,
            credit_account: 200,
        }
    }
}

fn case_strategy() -> impl Strategy<Value = Case> {
    pool_universe_strategy().prop_flat_map(|universe| {
        let pool_ids: Vec<i64> = {
            let mut v: Vec<i64> = universe.keys().copied().collect();
            v.sort();
            v
        };
        let lines = prop_vec(
            line_against_universe(pool_ids.clone()),
            0..=MAX_LINES_PER_SUBMISSION,
        );
        let micros = 1_700_000_000_000_000i64..=1_800_000_000_000_000i64;
        (lines, micros).prop_map(move |(lines, micros)| {
            let snapshot = build_snapshot(&universe);
            let posted_at = Utc.timestamp_micros(micros).single().unwrap();
            Case {
                snapshot,
                lines,
                posted_at,
            }
        })
    })
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100),
        .. ProptestConfig::default()
    })]

    /// design-v3 §7 contract: plan_apply on two independent clones of the
    /// pristine snapshot must produce identical results AND identical
    /// snapshot mutations. Holds for Ok and Err.
    #[test]
    fn idempotent_replay_across_methods(case in case_strategy()) {
        let mut snap_a = case.snapshot.clone();
        let mut snap_b = case.snapshot.clone();
        let result_a = plan_apply(&mut snap_a, &case.lines, case.posted_at);
        let result_b = plan_apply(&mut snap_b, &case.lines, case.posted_at);
        prop_assert_eq!(&result_a, &result_b,
            "plan_apply returned different results on identical input");
        prop_assert_eq!(&snap_a.pools, &snap_b.pools,
            "snapshot.pools diverged after identical plan_apply");
        prop_assert_eq!(&snap_a.max_trx_seq_of, &snap_b.max_trx_seq_of,
            "snapshot.max_trx_seq_of diverged after identical plan_apply");
    }

    /// Sanity check that the dispatcher reaches every method branch — no
    /// `unimplemented!()` lurking. Belt-and-suspenders against accidental
    /// regression in `method::plan_apply`.
    #[test]
    fn every_method_reachable(method in method_strategy()) {
        let mut snap = single_pool_snapshot(1, method);
        let line = TrxLineRequest {
            pool_id: 1,
            line_type: LineType::PoReceiptLine,
            source_id: Some(1),
            qty: 10,
            unit_cost: 50,
            debit_account: 100,
            credit_account: 200,
        };
        // A receipt is the universally-valid first line for every method;
        // none of the five branches should panic.
        let r = plan_apply(&mut snap, &[line], Utc::now());
        prop_assert!(r.is_ok(),
            "plan_apply panicked or errored on a single receipt for method {:?}: {:?}", method, r);
    }
}

fn single_pool_snapshot(pool_id: i64, method: PoolMethod) -> Snapshot {
    let mut method_of = HashMap::new();
    method_of.insert(pool_id, method);
    let mut max_trx_seq_of = HashMap::new();
    max_trx_seq_of.insert(pool_id, 0);
    Snapshot {
        pools: HashMap::new(),
        method_of,
        max_trx_seq_of,
        std_cost_of: HashMap::new(),
    }
}
