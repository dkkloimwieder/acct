//! Synthetic workload generator (design-v3.1 §10.2/§10.3, P4 acct-2ttr.8).
//!
//! Orthogonal axes:
//!   OverlapMode  — how callers' submissions overlap on pool_ids (§10.2)
//!   Complexity   — lines-per-submission size (§10.3)
//!   deplete_pct  — fraction of lines that are depletions vs receipts
//!
//! A depletion is a negative-qty line (ledger-core dispatches receipt/depletion
//! on the SIGN of qty, not line_type — see provisional.rs / wac.rs). Depletions
//! are the operation strict-mode FIFO/LIFO would walk layers for; under Path C
//! they touch only the aggregate, which is what §11.2 measures. Depletion
//! scenarios require pre-seeded pool aggregates (crate::seed) so they don't hit
//! InsufficientInventory (§3.6 — no negative inventory).

use rand::Rng;
use rand_distr::{Distribution, Zipf};

use crate::pool_universe::PoolUniverse;

/// How callers spread their pool picks across the universe.
#[derive(Debug, Clone, Copy)]
pub enum OverlapMode {
    /// Every caller samples uniformly across all pool_ids.
    Uniform,
    /// Heavy-tailed Zipf with `exponent` (higher = more skewed). A large
    /// exponent (~100) degenerates to a single hot pool (pool_ids[0]).
    Zipf { exponent: f64 },
    /// Disjoint stripes: caller `c` only sees
    /// pool_ids[c*stripe_size .. (c+1)*stripe_size].
    Disjoint { stripe_size: usize },
}

#[derive(Debug, Clone, Copy)]
pub enum Complexity {
    /// 1 line per submission.
    Simple,
    /// Uniform 2-5 lines.
    #[allow(dead_code)]
    Medium,
    /// Uniform 10-20 lines.
    Complex,
}

#[derive(Debug, Clone)]
pub struct LineParam {
    pub pool_id: i64,
    pub line_type: &'static str,
    pub source_id: Option<i64>,
    /// Signed: positive = receipt, negative = depletion.
    pub qty: i64,
    pub unit_cost: i64,
    pub debit_account: i64,
    pub credit_account: i64,
    /// Target for the STD variance leg (actual-vs-standard). Carried on every
    /// line; non-STD methods ignore it. STD receipts at a cost != the seeded
    /// standard need it, else `MissingVarianceAccount`.
    pub variance_account: i64,
}

/// One workload spec. Holds a snapshot of the pool universe + shape axes.
#[derive(Debug, Clone)]
pub struct Workload {
    pub universe: PoolUniverse,
    pub overlap: OverlapMode,
    pub complexity: Complexity,
    /// Percent of generated lines that are depletions (0 = all receipts,
    /// 100 = all depletions). FIFO/LIFO depletion is the depth-sensitive
    /// operation Path C makes constant-time.
    pub deplete_pct: u8,
    /// Informational — the scenario's advertised caller count.
    #[allow(dead_code)]
    pub caller_count: usize,
}

impl Workload {
    /// Generate one submission's worth of lines for `caller_id`.
    ///
    /// Pools are DISTINCT within a submission: the SPI bulk-UPSERTs aggregate
    /// rows keyed by (pool_id, layer_id=0), so listing the same pool twice in
    /// one direct submission would make the UPSERT "affect a row a second time".
    /// This matches §5.1 ("touched pool_ids ... dedup") and §10.3's "multiple
    /// pools" framing. When the overlap mode can't surface enough distinct pools
    /// (single-hot-pool Zipf, stripe_size=1), the submission shrinks to what's
    /// available — Simple scenarios (1 line) are unaffected.
    pub fn next_lines<R: Rng + ?Sized>(&self, rng: &mut R, caller_id: usize) -> Vec<LineParam> {
        let n_lines = match self.complexity {
            Complexity::Simple => 1,
            Complexity::Medium => rng.random_range(2..=5),
            Complexity::Complex => rng.random_range(10..=20),
        };
        let target = n_lines.min(self.universe.pool_ids.len());

        let mut chosen: Vec<i64> = Vec::with_capacity(target);
        let attempt_cap = target.saturating_mul(20).max(target);
        let mut attempts = 0;
        while chosen.len() < target && attempts < attempt_cap {
            let pid = self.pick_pool(rng, caller_id);
            if !chosen.contains(&pid) {
                chosen.push(pid);
            }
            attempts += 1;
        }

        chosen
            .into_iter()
            .map(|pool_id| {
                let deplete = (rng.random_range(0..100) as u8) < self.deplete_pct;
                let magnitude = rng.random_range(1..=100);
                if deplete {
                    LineParam {
                        pool_id,
                        line_type: "transfer_shipment_line",
                        source_id: Some(rng.random_range(1..=1_000_000)),
                        qty: -magnitude,
                        unit_cost: rng.random_range(1..=1000),
                        debit_account: self.universe.ap_account,
                        credit_account: self.universe.inv_account,
                        variance_account: self.universe.variance_account,
                    }
                } else {
                    LineParam {
                        pool_id,
                        line_type: "po_receipt_line",
                        source_id: Some(rng.random_range(1..=1_000_000)),
                        qty: magnitude,
                        unit_cost: rng.random_range(1..=1000),
                        debit_account: self.universe.inv_account,
                        credit_account: self.universe.ap_account,
                        variance_account: self.universe.variance_account,
                    }
                }
            })
            .collect()
    }

    fn pick_pool<R: Rng + ?Sized>(&self, rng: &mut R, caller_id: usize) -> i64 {
        match self.overlap {
            OverlapMode::Uniform => pick_uniform(rng, &self.universe.pool_ids),
            OverlapMode::Zipf { exponent } => pick_zipf(rng, &self.universe.pool_ids, exponent),
            OverlapMode::Disjoint { stripe_size } => {
                pick_disjoint(rng, &self.universe.pool_ids, caller_id, stripe_size)
            }
        }
    }
}

// ── Pickers (free fns so unit tests can exercise them) ───────────────

pub fn pick_uniform<R: Rng + ?Sized>(rng: &mut R, pool_ids: &[i64]) -> i64 {
    let i = rng.random_range(0..pool_ids.len());
    pool_ids[i]
}

/// Zipf-distributed pick (1-indexed rank → 0-indexed array).
pub fn pick_zipf<R: Rng + ?Sized>(rng: &mut R, pool_ids: &[i64], exponent: f64) -> i64 {
    let n = pool_ids.len() as u64;
    let z = Zipf::new(n as f64, exponent).expect("valid Zipf parameters");
    let rank = z.sample(rng) as usize;
    let idx = rank.saturating_sub(1).min(pool_ids.len() - 1);
    pool_ids[idx]
}

/// Disjoint stripes: caller `caller_id` picks within
/// `pool_ids[caller_id * stripe_size .. (caller_id + 1) * stripe_size]`.
pub fn pick_disjoint<R: Rng + ?Sized>(
    rng: &mut R,
    pool_ids: &[i64],
    caller_id: usize,
    stripe_size: usize,
) -> i64 {
    let start = caller_id.saturating_mul(stripe_size);
    let end = start.saturating_add(stripe_size).min(pool_ids.len());
    let start = start.min(pool_ids.len().saturating_sub(1));
    let end = end.max(start + 1);
    let i = rng.random_range(start..end);
    pool_ids[i]
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use std::collections::HashMap;

    fn ids(n: usize) -> Vec<i64> {
        (1..=n as i64).collect()
    }

    fn universe(n: usize) -> PoolUniverse {
        PoolUniverse {
            pool_ids: ids(n),
            inv_account: 1000,
            ap_account: 2000,
            variance_account: 3000,
        }
    }

    #[test]
    fn uniform_spans_whole_universe() {
        let mut rng = StdRng::seed_from_u64(0);
        let pool_ids = ids(100);
        let mut hist: HashMap<i64, usize> = HashMap::new();
        for _ in 0..10_000 {
            *hist.entry(pick_uniform(&mut rng, &pool_ids)).or_default() += 1;
        }
        assert_eq!(hist.len(), 100);
    }

    #[test]
    fn zipf_concentrates_mass_on_low_ranks() {
        let mut rng = StdRng::seed_from_u64(1);
        let pool_ids = ids(100);
        let mut hist: HashMap<i64, usize> = HashMap::new();
        for _ in 0..10_000 {
            *hist.entry(pick_zipf(&mut rng, &pool_ids, 1.5)).or_default() += 1;
        }
        let mut counts: Vec<usize> = hist.values().copied().collect();
        counts.sort_unstable_by(|a, b| b.cmp(a));
        let top10: usize = counts.iter().take(10).sum();
        assert!(top10 > 5_000, "zipf(1.5) top-10 should hold >5000; got {top10}");
    }

    #[test]
    fn high_exponent_zipf_degenerates_to_single_hot_pool() {
        let mut rng = StdRng::seed_from_u64(7);
        let pool_ids = ids(1000);
        let mut on_first = 0usize;
        for _ in 0..10_000 {
            if pick_zipf(&mut rng, &pool_ids, 100.0) == pool_ids[0] {
                on_first += 1;
            }
        }
        assert!(on_first > 9_900, "zipf(100) should pin >99% on pool 0; got {on_first}");
    }

    #[test]
    fn disjoint_stripes_are_non_overlapping() {
        let mut rng = StdRng::seed_from_u64(2);
        let pool_ids = ids(100);
        let stripe = 10;
        for caller_id in 0..10 {
            for _ in 0..500 {
                let id = pick_disjoint(&mut rng, &pool_ids, caller_id, stripe);
                let lo = (caller_id * stripe) as i64 + 1;
                let hi = ((caller_id + 1) * stripe) as i64;
                assert!(id >= lo && id <= hi, "caller {caller_id} saw {id} outside [{lo},{hi}]");
            }
        }
    }

    #[test]
    fn deplete_pct_zero_emits_only_receipts() {
        let w = Workload {
            universe: universe(20),
            overlap: OverlapMode::Uniform,
            complexity: Complexity::Complex,
            deplete_pct: 0,
            caller_count: 4,
        };
        let mut rng = StdRng::seed_from_u64(3);
        for _ in 0..50 {
            for l in w.next_lines(&mut rng, 0) {
                assert!(l.qty > 0, "deplete_pct=0 must yield only positive qty");
                assert_eq!(l.line_type, "po_receipt_line");
            }
        }
    }

    #[test]
    fn deplete_pct_hundred_emits_only_depletions() {
        let w = Workload {
            universe: universe(20),
            overlap: OverlapMode::Uniform,
            complexity: Complexity::Simple,
            deplete_pct: 100,
            caller_count: 4,
        };
        let mut rng = StdRng::seed_from_u64(4);
        for _ in 0..50 {
            for l in w.next_lines(&mut rng, 0) {
                assert!(l.qty < 0, "deplete_pct=100 must yield only negative qty");
                assert_eq!(l.line_type, "transfer_shipment_line");
            }
        }
    }

    #[test]
    fn submission_pools_are_distinct() {
        let w = Workload {
            universe: universe(50),
            overlap: OverlapMode::Zipf { exponent: 1.2 },
            complexity: Complexity::Complex,
            deplete_pct: 50,
            caller_count: 4,
        };
        let mut rng = StdRng::seed_from_u64(9);
        for _ in 0..100 {
            let lines = w.next_lines(&mut rng, 0);
            let mut pools: Vec<i64> = lines.iter().map(|l| l.pool_id).collect();
            let len = pools.len();
            pools.sort_unstable();
            pools.dedup();
            assert_eq!(pools.len(), len, "a submission must not repeat a pool");
        }
    }

    #[test]
    fn complex_returns_10_to_20_lines() {
        let w = Workload {
            universe: universe(20),
            overlap: OverlapMode::Uniform,
            complexity: Complexity::Complex,
            deplete_pct: 50,
            caller_count: 4,
        };
        let mut rng = StdRng::seed_from_u64(5);
        for _ in 0..20 {
            let n = w.next_lines(&mut rng, 0).len();
            assert!((10..=20).contains(&n), "complex lines.len() = {n} outside [10,20]");
        }
    }
}
