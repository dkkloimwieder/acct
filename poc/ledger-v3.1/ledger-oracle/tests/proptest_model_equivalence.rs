//! Property-based differential at the ledger-core boundary (acct-0at4.6, the
//! re-scoped concurrency-verification issue — the surviving in-process layer).
//!
//! # What this is
//!
//! The `.4` exact differential (`ledger-direct-c/tests/acceptance_reference_oracle_exact.rs`)
//! already proves `ModelLedger` ≡ `ledger_submit_trx_c` (which calls
//! `plan_apply_provisional`) byte-for-byte, but it goes through the DB, so it can
//! only afford a bounded, hand-tuned op stream. This binary drives the SAME two
//! implementations — `ledger_core::plan_apply_provisional` and the independent
//! `ledger_oracle::ModelLedger` — entirely IN PROCESS, so proptest can explore
//! thousands of random interleavings with *adversarial* value ranges the DB test
//! never reaches (qty=1, qty near i64 bounds, unit_cost=0, running averages on the
//! banker-rounding cliff, value_sum near the i64 accumulation ceiling).
//!
//! Because both flavors (direct, routed) call `ledger-core`, a bug `ledger-core`
//! carries is invisible to the direct-vs-routed diff (§9.4). The model re-derives
//! §3 with independent half-to-even rounding (BigRational, not `banker_div`), so a
//! `ledger-core` costing/rounding/ordering bug surfaces here as a model-vs-core
//! disagreement.
//!
//! # The four properties (from the gate re-scope)
//!
//!  1. **Model equivalence** — arbitrary receipt/depletion interleavings across
//!     mixed methods (WAC / provisional FIFO-LIFO / standard-basis FIFO-LIFO /
//!     STD) with adversarial values: aggregate `(qty, unit_cost, value_sum)`,
//!     every emitted `trx_line`, every emitted `posting_line`, and error-for-error
//!     all agree.
//!  2. **Monotone value_sum** for running_avg receipts-only: `value_sum` is
//!     non-decreasing and equals `Σ qty*cost` exactly.
//!  3. **Receipts-only permutation invariance** — the aggregate a running_avg pool
//!     lands on is independent of receipt order.
//!  4. **Negative-value_sum reachability for the standard basis** — a standard-
//!     basis over-book drives `value_sum` legitimately negative while `qty > 0`
//!     and does NOT error (post-migration-0009 the `≥ 0` CHECK is gone; this test
//!     pins that behavior so a re-added guard fails loud).
//!
//! Pure Rust: run with `cargo test -p ledger-oracle --test proptest_model_equivalence`.
//! Case count honors the `PROPTEST_CASES` env var (proptest default 256).

use chrono::{DateTime, Utc};
use ledger_core::{
    plan_apply_provisional, LineType, PoolMethod, ProvisionalBasis, Snapshot, TrxLineRequest,
};
use ledger_oracle::{uniform_accounts, ExpectedSource, ModelLedger};
use proptest::prelude::*;
use rand::seq::SliceRandom;
use rand::SeedableRng;

/// The mixed-method pool universe both sides are seeded with. Aggregate methods
/// only — `specific`'s layer/source linking needs DB-assigned `trx_line.id`s to
/// compare and is covered exactly by the `.4` differential; the four properties
/// here all target aggregate `value_sum` evolution.
const N_POOLS: usize = 6;

/// `posted_at` only flows into `PostingLineRequest.posted_at`, which the model has
/// no counterpart for and this diff never inspects — a fixed epoch keeps runs
/// deterministic.
fn ts() -> DateTime<Utc> {
    DateTime::from_timestamp(0, 0).unwrap()
}

#[derive(Clone, Copy)]
struct PoolCfg {
    pool_id: i64,
    sku: i64,
    loc: i64,
    method: PoolMethod,
    basis: ProvisionalBasis,
    std_cost: Option<i64>,
    /// (inventory, contra, variance) accounts — distinct per pool so posting-line
    /// account comparisons are pool-discriminating.
    accts: (i64, i64, i64),
}

fn pools() -> Vec<PoolCfg> {
    let p = vec![
        PoolCfg { pool_id: 1, sku: 1, loc: 1, method: PoolMethod::Wac, basis: ProvisionalBasis::RunningAvg, std_cost: None, accts: (1001, 2001, 3001) },
        PoolCfg { pool_id: 2, sku: 2, loc: 1, method: PoolMethod::Fifo, basis: ProvisionalBasis::RunningAvg, std_cost: None, accts: (1002, 2002, 3002) },
        PoolCfg { pool_id: 3, sku: 3, loc: 1, method: PoolMethod::Lifo, basis: ProvisionalBasis::RunningAvg, std_cost: None, accts: (1003, 2003, 3003) },
        PoolCfg { pool_id: 4, sku: 4, loc: 1, method: PoolMethod::Fifo, basis: ProvisionalBasis::Standard, std_cost: Some(250), accts: (1004, 2004, 3004) },
        PoolCfg { pool_id: 5, sku: 5, loc: 1, method: PoolMethod::Lifo, basis: ProvisionalBasis::Standard, std_cost: Some(150), accts: (1005, 2005, 3005) },
        PoolCfg { pool_id: 6, sku: 6, loc: 1, method: PoolMethod::Std, basis: ProvisionalBasis::RunningAvg, std_cost: Some(100), accts: (1006, 2006, 3006) },
    ];
    debug_assert_eq!(p.len(), N_POOLS);
    p
}

/// Build the `ledger-core` snapshot for the given pools, mirroring what
/// `seed_pool` writes to `pool_state` / `posting_account_map` / `standard_cost`.
fn fresh_snapshot(cfgs: &[PoolCfg]) -> Snapshot {
    let mut s = Snapshot::default();
    for c in cfgs {
        s.method_of.insert(c.pool_id, c.method);
        s.provisional_basis_of.insert(c.pool_id, c.basis);
        s.sku_location_of.insert(c.pool_id, (c.sku, c.loc));
        if let Some(sc) = c.std_cost {
            s.standard_cost_of.insert(c.pool_id, sc);
        }
        let (inv, ap, var) = c.accts;
        s.posting_accounts_of.insert(c.pool_id, uniform_accounts(inv, ap, var));
    }
    s
}

/// Build the independent model with the identical config.
fn fresh_model(cfgs: &[PoolCfg]) -> ModelLedger {
    let mut m = ModelLedger::new();
    for c in cfgs {
        let (inv, ap, var) = c.accts;
        m.add_pool(c.pool_id, c.sku, c.loc, c.method, c.basis, Some(uniform_accounts(inv, ap, var)), c.std_cost);
    }
    m
}

/// Drive one submission through BOTH implementations and assert full agreement.
///
/// `plan_apply_provisional` mutates the snapshot line-by-line and, on the first
/// failing line, leaves it PARTIALLY mutated (it returns `Err` without rolling
/// back — the SQL caller's aborting transaction is what discards the changes). The
/// model's `apply` is transactional. To keep the two states congruent across a
/// sequence we snapshot-and-restore on the core side too, reproducing the tx abort.
fn diff_submit(
    snap: &mut Snapshot,
    model: &mut ModelLedger,
    lines: &[TrxLineRequest],
) -> Result<(), TestCaseError> {
    let backup = snap.clone();
    let core_res = plan_apply_provisional(snap, lines, ts());
    let model_res = model.apply(lines);

    match (&core_res, &model_res) {
        (Err(ce), Err(me)) => {
            let cs = ce.to_string();
            prop_assert!(
                cs.contains(&me.message()),
                "error-for-error mismatch: core said '{cs}', model expected substring '{}'",
                me.message()
            );
            *snap = backup; // mirror the SQL tx abort the model already performed
        }
        (Ok(_), Err(me)) => prop_assert!(false, "core succeeded but model predicted Err {me:?}"),
        (Err(ce), Ok(_)) => prop_assert!(false, "core erred {ce:?} but model predicted Ok"),
        (Ok(plan), Ok(applied)) => {
            // trx_line stream: qty + recorded unit_cost, in emission order. Sources
            // are all NULL (aggregate methods; specific is excluded).
            prop_assert_eq!(plan.trx_lines.len(), applied.lines.len(), "trx_line count");
            for (i, (tl, el)) in plan.trx_lines.iter().zip(applied.lines.iter()).enumerate() {
                prop_assert_eq!(tl.qty, el.qty, "line {} qty", i);
                prop_assert_eq!(tl.unit_cost, el.unit_cost, "line {} unit_cost", i);
                prop_assert_eq!(tl.source_trx_line_id, None, "line {} core source", i);
                prop_assert_eq!(el.source, ExpectedSource::Null, "line {} model source", i);
            }
            // posting_line stream: event_type + amount + debit/credit direction.
            prop_assert_eq!(plan.posting_lines.len(), applied.postings.len(), "posting count");
            for (i, (pl, ep)) in plan.posting_lines.iter().zip(applied.postings.iter()).enumerate() {
                prop_assert_eq!(pl.event_type.as_sql(), ep.event_type, "posting {} event_type", i);
                prop_assert_eq!(pl.amount, ep.amount, "posting {} amount", i);
                prop_assert_eq!(pl.debit_account, ep.debit_account, "posting {} debit", i);
                prop_assert_eq!(pl.credit_account, ep.credit_account, "posting {} credit", i);
            }
        }
    }

    // Aggregate `(qty, unit_cost, value_sum)` agrees for every touched pool — on
    // both the success and the rolled-back error path.
    //
    // Normalize `None ↔ (0,0,0)`: ledger-core's snapshot (like the DB, which only
    // ever UPSERTs the layer_id=0 row, never deletes it) retains a drained pool as
    // a physical `(0, 0, 0)` row, whereas the model's `aggregate()` collapses an
    // all-zero pool to `None` (it treats "never written" and "drained to all-zero"
    // alike). Both denote the same ledger state — empty pool, zero book value,
    // zero derived cost — so the meaningful comparison is of the value, not of row
    // presence. (A pool drained while carrying a *nonzero* unit_cost stays `Some`
    // on both sides and is compared exactly.)
    let norm = |o: Option<(i64, i64, i64)>| o.unwrap_or((0, 0, 0));
    let touched: std::collections::BTreeSet<i64> = lines.iter().map(|l| l.pool_id).collect();
    for p in touched {
        let core_agg = norm(snap.aggregate(p).map(|r| (r.qty, r.unit_cost, r.value_sum)));
        let model_agg = norm(model.aggregate(p).map(|a| (a.qty, a.unit_cost, a.value_sum)));
        prop_assert_eq!(core_agg, model_agg, "aggregate mismatch on pool {}", p);
    }
    Ok(())
}

// ── adversarial value strategies ────────────────────────────────────────────

/// Depletion/receipt qty magnitude (always positive; sign is applied by op kind).
/// Weighted toward small (tie-prone, and small depletions actually succeed) with a
/// tail into the i64 overflow regime.
fn qty_mag_strat() -> impl Strategy<Value = i64> {
    prop_oneof![
        4 => 1i64..=8,
        3 => 1i64..=500,
        1 => Just(1i64),
        1 => (i64::MAX / 2)..=i64::MAX,
    ]
}

/// Receipt asserted unit_cost. Includes 0 and the overflow regime; the small band
/// keeps running averages landing on the banker-rounding half-cliff.
fn unit_cost_strat() -> impl Strategy<Value = i64> {
    prop_oneof![
        3 => 0i64..=20,
        3 => 1i64..=1000,
        1 => Just(0i64),
        1 => (i64::MAX / 2)..=i64::MAX,
    ]
}

#[derive(Clone, Copy, Debug)]
struct LineGen {
    pool_idx: usize,
    receipt: bool,
    qty_mag: i64,
    unit_cost: i64,
}

fn line_gen_strat() -> impl Strategy<Value = LineGen> {
    (
        0..N_POOLS,
        prop_oneof![3 => Just(true), 2 => Just(false)], // receipts dominate → pools get stocked
        qty_mag_strat(),
        unit_cost_strat(),
    )
        .prop_map(|(pool_idx, receipt, qty_mag, unit_cost)| LineGen { pool_idx, receipt, qty_mag, unit_cost })
}

fn to_req(g: &LineGen, cfgs: &[PoolCfg]) -> TrxLineRequest {
    let c = cfgs[g.pool_idx];
    if g.receipt {
        TrxLineRequest {
            pool_id: c.pool_id,
            line_type: LineType::PoReceiptLine,
            source_id: None,
            qty: g.qty_mag,
            unit_cost: g.unit_cost,
        }
    } else {
        TrxLineRequest {
            pool_id: c.pool_id,
            line_type: LineType::TransferShipmentLine,
            source_id: None,
            qty: -g.qty_mag,
            unit_cost: 0,
        }
    }
}

/// Apply a receipts-only stream to a fresh core snapshot; return the final
/// aggregate `(qty, unit_cost, value_sum)`. Used by the permutation-invariance
/// property (which is about `ledger-core`'s order-independence directly).
fn apply_receipts_to_fresh(cfgs: &[PoolCfg], receipts: &[(i64, i64)]) -> (i64, i64, i64) {
    let mut snap = fresh_snapshot(cfgs);
    let pid = cfgs[0].pool_id;
    for (q, c) in receipts {
        let req = TrxLineRequest {
            pool_id: pid,
            line_type: LineType::PoReceiptLine,
            source_id: None,
            qty: *q,
            unit_cost: *c,
        };
        plan_apply_provisional(&mut snap, std::slice::from_ref(&req), ts()).expect("receipt ok");
    }
    let r = snap.aggregate(pid).expect("aggregate exists after receipts");
    (r.qty, r.unit_cost, r.value_sum)
}

/// A WAC (running_avg) single-pool config for the focused properties.
fn wac_only() -> Vec<PoolCfg> {
    vec![PoolCfg { pool_id: 1, sku: 1, loc: 1, method: PoolMethod::Wac, basis: ProvisionalBasis::RunningAvg, std_cost: None, accts: (1001, 2001, 3001) }]
}

/// (qty_r, qty_d, cost_r, extra) for the negative-reachability property, with
/// `qty_d < qty_r` guaranteed (so the depleted pool stays non-empty and value_sum
/// is not reset to 0). Bounds keep every product inside i64.
fn negative_reach_strat() -> impl Strategy<Value = (i64, i64, i64, i64)> {
    (2i64..=100_000, 1i64..=1000, 0i64..=1000)
        .prop_flat_map(|(qty_r, cost_r, extra)| (Just(qty_r), 1i64..qty_r, Just(cost_r), Just(extra)))
}

// ── the properties ──────────────────────────────────────────────────────────

proptest! {
    /// (1) Model equivalence over arbitrary mixed-method interleavings.
    #[test]
    fn model_equivalence_over_interleavings(
        seq in prop::collection::vec(
            prop::collection::vec(line_gen_strat(), 1..=3),
            1..=40,
        )
    ) {
        let cfgs = pools();
        let mut snap = fresh_snapshot(&cfgs);
        let mut model = fresh_model(&cfgs);
        for submission in &seq {
            let reqs: Vec<TrxLineRequest> = submission.iter().map(|g| to_req(g, &cfgs)).collect();
            diff_submit(&mut snap, &mut model, &reqs)?;
        }
    }

    /// (2) Running_avg receipts drive value_sum monotonically upward, exactly
    /// tracking Σ qty*cost (bounded so the stream never overflows).
    #[test]
    fn value_sum_monotone_for_running_avg_receipts(
        receipts in prop::collection::vec((1i64..=1_000_000, 0i64..=1_000_000), 1..=50)
    ) {
        let cfgs = wac_only();
        let mut snap = fresh_snapshot(&cfgs);
        let mut model = fresh_model(&cfgs);
        let mut prev_value = 0i64;
        let mut running_sum = 0i128;
        for (i, (q, c)) in receipts.iter().enumerate() {
            let req = TrxLineRequest {
                pool_id: 1,
                line_type: LineType::PoReceiptLine,
                source_id: None,
                qty: *q,
                unit_cost: *c,
            };
            diff_submit(&mut snap, &mut model, std::slice::from_ref(&req))?;
            let r = snap.aggregate(1).expect("aggregate after receipt");
            running_sum += (*q as i128) * (*c as i128);
            prop_assert_eq!(r.value_sum as i128, running_sum, "value_sum == Σ qty*cost after receipt {}", i);
            prop_assert!(r.value_sum >= prev_value, "value_sum monotone non-decreasing (receipt {}): {} < {}", i, r.value_sum, prev_value);
            prop_assert!(r.qty > 0, "qty positive after receipt {}", i);
            prev_value = r.value_sum;
        }
    }

    /// (3) A receipts-only running_avg pool lands on the same aggregate regardless
    /// of the order the receipts arrive in.
    #[test]
    fn receipts_only_permutation_invariant(
        receipts in prop::collection::vec((1i64..=1_000_000, 0i64..=1_000_000), 1..=40),
        seed in any::<u64>(),
    ) {
        let cfgs = wac_only();
        let final_a = apply_receipts_to_fresh(&cfgs, &receipts);

        let mut shuffled = receipts.clone();
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        shuffled.shuffle(&mut rng);
        let final_b = apply_receipts_to_fresh(&cfgs, &shuffled);

        prop_assert_eq!(final_a, final_b, "receipts-only aggregate must be order-independent");
    }

    /// (4) Standard-basis over-book drives value_sum legitimately negative while
    /// qty stays > 0, WITHOUT erroring — pinning the post-0009 behavior.
    #[test]
    fn standard_basis_negative_value_sum_reachable(
        (qty_r, qty_d, cost_r, extra) in negative_reach_strat()
    ) {
        // c_std strictly over-books: qty_d * c_std > qty_r * cost_r (proof in the
        // helper's construction), so value_sum = book − qty_d*c_std < 0 while
        // new_qty = qty_r − qty_d > 0 (no reset-to-zero).
        let book_value = qty_r as i128 * cost_r as i128;
        let c_std = (book_value / qty_d as i128) as i64 + 1 + extra;
        let cfgs = vec![PoolCfg {
            pool_id: 1, sku: 1, loc: 1,
            method: PoolMethod::Fifo, basis: ProvisionalBasis::Standard,
            std_cost: Some(c_std), accts: (1001, 2001, 3001),
        }];
        let mut snap = fresh_snapshot(&cfgs);
        let mut model = fresh_model(&cfgs);

        let recv = TrxLineRequest { pool_id: 1, line_type: LineType::PoReceiptLine, source_id: None, qty: qty_r, unit_cost: cost_r };
        diff_submit(&mut snap, &mut model, std::slice::from_ref(&recv))?;

        // MUST succeed: qty_d < qty_r (sufficient inventory) and no ≥0 CHECK exists.
        let depl = TrxLineRequest { pool_id: 1, line_type: LineType::TransferShipmentLine, source_id: None, qty: -qty_d, unit_cost: 0 };
        diff_submit(&mut snap, &mut model, std::slice::from_ref(&depl))?;

        let r = snap.aggregate(1).expect("aggregate after depletion");
        prop_assert!(r.qty > 0, "qty must stay positive (qty_r={} qty_d={})", qty_r, qty_d);
        prop_assert!(
            r.value_sum < 0,
            "standard-basis over-book must drive value_sum negative: got {} (book={}, depleted={})",
            r.value_sum, book_value, qty_d as i128 * c_std as i128
        );
    }
}
