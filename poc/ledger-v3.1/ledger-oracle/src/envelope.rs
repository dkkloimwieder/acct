//! Envelope mode (design-v3.1 §14.2 / §9.3): the legal-outcome set.
//!
//! For the routed flavor and multi-caller harness runs, exact per-line equality
//! is undefined — batch reordering changes within-batch running-average evolution,
//! and a depletion legal in one serialization is dropped (InsufficientInventory,
//! drop-and-continue) in another. Instead of one answer, the model computes the
//! *set of legal answers* and checks the observed result is explainable by SOME
//! serialization of the submitted multiset:
//!
//!  - **Receipts-only pools are still byte-exact.** `value_sum` accumulates
//!    exactly and the running average is `round_half_even(ΣQ·C, ΣQ)` regardless of
//!    order (§3.1), so even the routed reorder must land on one value — no
//!    envelope, a point.
//!  - **Reachable final qty.** Under drop-and-continue, the final on-hand depends
//!    on order. For small op counts this enumerates every ordering exactly; for
//!    large counts it falls back to sound necessary conditions (never a false
//!    reject): conservation bounds plus a sub-multiset-sum witness for the applied
//!    depletion total.
//!
//! These are the building blocks the routed / harness differential tests call
//! against observed `pool_state`; this module has no DB dependency.

use std::collections::BTreeSet;

use num_bigint::BigInt;

use crate::round_half_even;

/// Above this many ops on one pool, `reachable_final_qtys` enumeration (O(n!))
/// is skipped for the sound-bounds path. 8 ops = 40 320 orderings, well within a
/// test's budget; 9 would be 362 880.
pub const ENUM_OP_CAP: usize = 8;

/// The exact aggregate a *receipts-only* pool must show, independent of order or
/// batching. `receipts` is `(qty, unit_cost)` per receipt; `start` is any
/// pre-seeded `(qty, value_sum)` opening state (`(0, 0)` for a fresh pool).
///
/// Returns `(qty, unit_cost, value_sum)`. `unit_cost` uses the independent
/// [`round_half_even`], so it cross-checks the SQL running average bit-for-bit.
pub fn receipts_only_aggregate(start: (i64, i64), receipts: &[(i64, i64)]) -> (i64, i64, i64) {
    let (mut qty, mut value_sum_i128) = (start.0, start.1 as i128);
    for &(q, c) in receipts {
        qty += q;
        value_sum_i128 += (q as i128) * (c as i128);
    }
    let value_sum: i64 = value_sum_i128.try_into().expect("receipts_only_aggregate: value_sum overflows i64");
    let unit_cost = if qty > 0 { round_half_even(&BigInt::from(value_sum), qty) } else { 0 };
    (qty, unit_cost, value_sum)
}

/// Simulate one ordering of a pool's ops under drop-and-continue, returning the
/// final on-hand qty. `ops` are signed (`> 0` receipt, `< 0` depletion); a
/// depletion whose magnitude exceeds current on-hand is dropped (not applied),
/// mirroring the no-negative invariant with continue-past-failure (§14.2).
pub fn simulate_order(start_qty: i64, ops: &[i64]) -> i64 {
    let mut on_hand = start_qty;
    for &op in ops {
        if op >= 0 {
            on_hand += op;
        } else if on_hand >= -op {
            on_hand += op;
        }
        // else: dropped, continue.
    }
    on_hand
}

/// The exact set of reachable final on-hand qtys over *all* orderings of `ops`
/// (signed), via full permutation enumeration. Only valid for small `ops`
/// (`<= ENUM_OP_CAP`); callers gate on length.
pub fn reachable_final_qtys(start_qty: i64, ops: &[i64]) -> BTreeSet<i64> {
    let mut out = BTreeSet::new();
    let mut idx: Vec<usize> = (0..ops.len()).collect();
    permute(&mut idx, 0, &mut |perm| {
        let ordered: Vec<i64> = perm.iter().map(|&i| ops[i]).collect();
        out.insert(simulate_order(start_qty, &ordered));
    });
    out
}

/// Heap's algorithm: invoke `visit` on every permutation of `arr`.
fn permute<F: FnMut(&[usize])>(arr: &mut [usize], k: usize, visit: &mut F) {
    let n = arr.len();
    if k == n {
        visit(arr);
        return;
    }
    for i in k..n {
        arr.swap(k, i);
        permute(arr, k + 1, visit);
        arr.swap(k, i);
    }
}

/// Every sub-multiset sum of `values` that is `<= cap` (bounded DP). Used as a
/// necessary witness: the total applied depletion (`totalReceipts - finalQty`)
/// must be a sub-multiset sum of the submitted depletions.
pub fn subset_sums(values: &[i64], cap: i64) -> BTreeSet<i64> {
    let mut reachable = BTreeSet::new();
    reachable.insert(0i64);
    for &v in values {
        if v <= 0 {
            continue;
        }
        let mut next = reachable.clone();
        for &s in &reachable {
            let t = s + v;
            if t <= cap {
                next.insert(t);
            }
        }
        reachable = next;
    }
    reachable
}

/// The verdict of an envelope check on one pool's observed final qty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// The op count was small enough to enumerate: `explained` = observed is in
    /// the exact reachable set.
    Exact { explained: bool, reachable: BTreeSet<i64> },
    /// Too many ops to enumerate: sound necessary conditions only. `explained`
    /// requires all of them; a `true` here is "not refuted", not "uniquely
    /// reproduced".
    Bounds { explained: bool, within_bounds: bool, applied_is_subset_sum: bool, min: i64, max: i64 },
}

impl Verdict {
    pub fn explained(&self) -> bool {
        match self {
            Verdict::Exact { explained, .. } => *explained,
            Verdict::Bounds { explained, .. } => *explained,
        }
    }
}

/// Is `observed_final_qty` explainable by some legal serialization of a pool's
/// submitted ops (signed)? `start_qty` is any pre-seeded opening on-hand.
///
/// Small op count → exact enumeration. Large → conservation bounds
/// (`max(start - ΣD, 0) .. start + ΣR` — the reachable window under
/// drop-and-continue) plus a sub-multiset-sum witness that the applied-depletion
/// total is achievable. The bounds path is *sound* (never rejects a truly legal
/// outcome); it can over-accept, which is the correct bias for an envelope.
pub fn explains_final_qty(start_qty: i64, ops: &[i64], observed_final_qty: i64) -> Verdict {
    if ops.len() <= ENUM_OP_CAP {
        let reachable = reachable_final_qtys(start_qty, ops);
        let explained = reachable.contains(&observed_final_qty);
        return Verdict::Exact { explained, reachable };
    }
    let total_receipts: i64 = ops.iter().filter(|&&o| o > 0).sum();
    let depletions: Vec<i64> = ops.iter().filter(|&&o| o < 0).map(|o| -o).collect();
    let total_depletions: i64 = depletions.iter().sum();
    // Reachable window: at most every receipt lands (start + ΣR); at least every
    // schedulable depletion lands, floored at 0 (drop-and-continue can't go
    // negative). Applying every depletion is only possible if enough is on hand,
    // so the true floor is max(start + ΣR - ΣD, 0) when all fit, but a depletion
    // dropped for underflow raises the floor — so 0 is the sound lower bound.
    let max = start_qty + total_receipts;
    let min = (start_qty + total_receipts - total_depletions).max(0);
    let within_bounds = observed_final_qty >= min && observed_final_qty <= max && observed_final_qty >= 0;
    // Applied depletion total must be a sub-multiset sum of the submitted
    // depletions (necessary): applied = (start + ΣR) - observed.
    let applied = start_qty + total_receipts - observed_final_qty;
    let applied_is_subset_sum = applied >= 0
        && applied <= total_depletions
        && subset_sums(&depletions, applied.max(0)).contains(&applied);
    Verdict::Bounds {
        explained: within_bounds && applied_is_subset_sum,
        within_bounds,
        applied_is_subset_sum,
        min,
        max,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receipts_only_is_order_independent_and_banker_rounded() {
        // 3@111 + 7@222 + 5@333 = 333 + 1554 + 1665 = 3552 over 15 → 236.8 → 237.
        let a = receipts_only_aggregate((0, 0), &[(3, 111), (7, 222), (5, 333)]);
        let b = receipts_only_aggregate((0, 0), &[(5, 333), (3, 111), (7, 222)]);
        assert_eq!(a, b);
        assert_eq!(a, (15, 237, 3552));
    }

    #[test]
    fn receipts_only_with_seed() {
        // Seeded 10 @ value 1000, then +5 @ 200 → qty 15, value 2000, avg 133.33 → 133.
        let r = receipts_only_aggregate((10, 1000), &[(5, 200)]);
        assert_eq!(r, (15, 133, 2000));
    }

    #[test]
    fn simulate_drop_and_continue() {
        // +10, -6 (ok→4), -5 (drop, 4<5), +3 (→7), -7 (ok→0).
        assert_eq!(simulate_order(0, &[10, -6, -5, 3, -7]), 0);
        // +10, -5(→5), -6(drop 5<6), +3(→8), -7(→1).
        assert_eq!(simulate_order(0, &[10, -5, -6, 3, -7]), 1);
    }

    #[test]
    fn reachable_set_small() {
        // ops +10, -6, -5: orderings yield finals depending on which depletion drops.
        // receipts-first: 10, then -6→4, -5 drops → 4; or -5→5,-6 drops... wait 10 first.
        // Enumerate: possible finals ∈ {10-6-5=-1 impossible; drops make it {4,5,-? }}.
        let set = reachable_final_qtys(0, &[10, -6, -5]);
        // Both depletions can't both apply (6+5=11 > 10). One applies → 10-6=4 or
        // 10-5=5. Or BOTH lead the receipt (on-hand 0) and drop → 10. So {4,5,10}.
        assert_eq!(set, BTreeSet::from([4, 5, 10]));
    }

    #[test]
    fn reachable_set_all_fit() {
        // +20, -6, -5: both fit in any order after the receipt → but a depletion
        // first drops. Reachable finals: 20-11=9 (both after receipt), or one drops
        // (14 or 15), or both drop (20).
        let set = reachable_final_qtys(0, &[20, -6, -5]);
        assert_eq!(set, BTreeSet::from([9, 14, 15, 20]));
    }

    #[test]
    fn subset_sums_bounded() {
        let s = subset_sums(&[6, 5, 3], 11);
        assert_eq!(s, BTreeSet::from([0, 3, 5, 6, 8, 9, 11]));
    }

    #[test]
    fn explains_small_uses_exact() {
        let v = explains_final_qty(0, &[10, -6, -5], 4);
        assert!(v.explained());
        assert!(matches!(v, Verdict::Exact { .. }));
        let v = explains_final_qty(0, &[10, -6, -5], 3); // 3 not reachable
        assert!(!v.explained());
    }

    #[test]
    fn explains_large_uses_bounds() {
        // 9 receipts of 10 (=90) and one depletion of 30. Observed 60 = 90-30.
        let mut ops = vec![10i64; 9];
        ops.push(-30);
        let v = explains_final_qty(0, &ops, 60);
        assert!(matches!(v, Verdict::Bounds { .. }));
        assert!(v.explained());
        // 55 is within bounds but 35 applied is not a subset sum of {30} → refuted.
        let v = explains_final_qty(0, &ops, 55);
        assert!(!v.explained());
    }
}
