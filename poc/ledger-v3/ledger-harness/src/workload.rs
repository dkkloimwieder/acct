//! Synthetic workload generator (acct-y3v1).
//!
//! Three orthogonal axes per plan §F:
//!   OverlapMode  — how callers' submissions overlap on pool_ids
//!   Complexity   — lines-per-submission size
//!   MethodMix    — surfaces in the pool universe (per-pool method);
//!                  the workload here is method-agnostic (caller-supplied
//!                  unit_cost stands for STD per locked plan Q4)
//!
//! Workload::next_lines(rng, caller_id) returns a `Vec<LineParam>` for one
//! submission. Drivers (acct-ykyl direct, acct-qiaz routed) wrap each
//! Vec<LineParam> in the JSONB shape expected by ledger_submit_trx.

use rand::Rng;
use rand_distr::{Distribution, Zipf};

use crate::pool_universe::PoolUniverse;

/// How callers spread their pool picks across the universe.
#[derive(Debug, Clone, Copy)]
pub enum OverlapMode {
    /// Every caller samples uniformly across all pool_ids — high overlap
    /// at large caller counts.
    Uniform,
    /// Heavy-tailed Zipf with `exponent` (1.0 = uniform-ish, 2.0 = highly
    /// skewed). Default for hot-pool scenarios.
    Zipf { exponent: f64 },
    /// Disjoint stripes: caller `c` only sees pool_ids[c*stripe_size ..
    /// (c+1)*stripe_size]. Used to characterize the no-overlap ceiling.
    Disjoint { stripe_size: usize },
}

#[derive(Debug, Clone, Copy)]
pub enum Complexity {
    /// 1 line per submission.
    Simple,
    /// Uniform 2-5 lines.
    #[allow(dead_code)]
    Medium,
    /// Uniform 10-50 lines.
    Complex,
}

#[derive(Debug, Clone)]
pub struct LineParam {
    pub pool_id: i64,
    pub line_type: &'static str,
    pub source_id: Option<i64>,
    pub qty: i64,
    pub unit_cost: i64,
    pub debit_account: i64,
    pub credit_account: i64,
}

/// One workload spec. Holds a snapshot of the pool universe + the three
/// shape axes. `caller_count` is the upper bound on `caller_id` accepted
/// by `next_lines`; Disjoint mode uses it to bound per-caller stripe
/// pickers.
#[derive(Debug, Clone)]
pub struct Workload {
    pub universe: PoolUniverse,
    pub overlap: OverlapMode,
    pub complexity: Complexity,
    /// Informational — the scenario's advertised caller count. Pickers
    /// don't consult it (Disjoint already takes stripe_size); kept so
    /// the workload struct is self-describing for diagnostics.
    #[allow(dead_code)]
    pub caller_count: usize,
}

impl Workload {
    /// Generate one submission's worth of lines for `caller_id`.
    ///
    /// All lines are receipts (positive qty). Depletion sequencing is
    /// the scenario layer's problem (acct-7ywx); for measurement
    /// baseline, all-receipts isolates the SPI + bulk-write surface
    /// from InsufficientInventory error pathways.
    pub fn next_lines<R: Rng + ?Sized>(&self, rng: &mut R, caller_id: usize) -> Vec<LineParam> {
        let n_lines = match self.complexity {
            Complexity::Simple => 1,
            Complexity::Medium => rng.random_range(2..=5),
            Complexity::Complex => rng.random_range(10..=50),
        };

        (0..n_lines)
            .map(|_| {
                let pool_id = self.pick_pool(rng, caller_id);
                LineParam {
                    pool_id,
                    line_type: "po_receipt_line",
                    source_id: Some(rng.random_range(1..=1_000_000)),
                    qty: rng.random_range(1..=100),
                    unit_cost: rng.random_range(1..=1000),
                    debit_account: self.universe.inv_account,
                    credit_account: self.universe.ap_account,
                }
            })
            .collect()
    }

    fn pick_pool<R: Rng + ?Sized>(&self, rng: &mut R, caller_id: usize) -> i64 {
        match self.overlap {
            OverlapMode::Uniform => pick_uniform(rng, &self.universe.pool_ids),
            OverlapMode::Zipf { exponent } => {
                pick_zipf(rng, &self.universe.pool_ids, exponent)
            }
            OverlapMode::Disjoint { stripe_size } => {
                pick_disjoint(rng, &self.universe.pool_ids, caller_id, stripe_size)
            }
        }
    }
}

// ── Pickers (free fns so unit tests can exercise them without building a
// full Workload) ────────────────────────────────────────────────────

pub fn pick_uniform<R: Rng + ?Sized>(rng: &mut R, pool_ids: &[i64]) -> i64 {
    let i = rng.random_range(0..pool_ids.len());
    pool_ids[i]
}

/// Zipf-distributed pick (1-indexed rank → 0-indexed array). `exponent`
/// is the shape parameter (1.0 = log-uniform-ish, 2.0 = highly skewed).
pub fn pick_zipf<R: Rng + ?Sized>(rng: &mut R, pool_ids: &[i64], exponent: f64) -> i64 {
    let n = pool_ids.len() as u64;
    let z = Zipf::new(n as f64, exponent).expect("valid Zipf parameters");
    // rand_distr::Zipf returns 1..=N as f64; clamp + cast to usize index.
    let rank = z.sample(rng) as usize;
    let idx = rank.saturating_sub(1).min(pool_ids.len() - 1);
    pool_ids[idx]
}

/// Disjoint stripes: caller `caller_id` picks within
/// `pool_ids[caller_id * stripe_size .. (caller_id + 1) * stripe_size]`,
/// clamped to the universe end.
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

    #[test]
    fn uniform_spans_whole_universe() {
        let mut rng = StdRng::seed_from_u64(0);
        let pool_ids = ids(100);
        let mut hist: HashMap<i64, usize> = HashMap::new();
        for _ in 0..10_000 {
            let pid = pick_uniform(&mut rng, &pool_ids);
            *hist.entry(pid).or_default() += 1;
        }
        assert_eq!(hist.len(), 100, "uniform should hit every pool in 10k draws");
        let min = *hist.values().min().unwrap();
        let max = *hist.values().max().unwrap();
        // Loose bound — 10k draws across 100 bins, expected mean=100,
        // ±50% range is well inside binomial noise.
        assert!(min > 50, "uniform min frequency too low: {min}");
        assert!(max < 200, "uniform max frequency too high: {max}");
    }

    #[test]
    fn zipf_concentrates_mass_on_low_ranks() {
        let mut rng = StdRng::seed_from_u64(1);
        let pool_ids = ids(100);
        let mut hist: HashMap<i64, usize> = HashMap::new();
        for _ in 0..10_000 {
            let pid = pick_zipf(&mut rng, &pool_ids, 1.5);
            *hist.entry(pid).or_default() += 1;
        }
        // Top-10 ranks should hold >50% of the mass at exponent=1.5.
        let mut counts: Vec<usize> = hist.values().copied().collect();
        counts.sort_unstable_by(|a, b| b.cmp(a));
        let top10: usize = counts.iter().take(10).sum();
        assert!(
            top10 > 5_000,
            "zipf(1.5) top-10 should hold >5000 of 10000; got {top10}"
        );
    }

    #[test]
    fn disjoint_stripes_are_non_overlapping() {
        let mut rng = StdRng::seed_from_u64(2);
        let pool_ids = ids(100);
        let stripe = 10;
        for caller_id in 0..10 {
            let mut seen: Vec<i64> = Vec::new();
            for _ in 0..500 {
                seen.push(pick_disjoint(&mut rng, &pool_ids, caller_id, stripe));
            }
            seen.sort_unstable();
            seen.dedup();
            // Every id seen by this caller must live within its stripe.
            let lo = (caller_id * stripe) as i64 + 1;
            let hi = ((caller_id + 1) * stripe) as i64;
            for id in &seen {
                assert!(
                    *id >= lo && *id <= hi,
                    "caller {caller_id} saw id {id} outside stripe [{lo}, {hi}]"
                );
            }
        }
    }

    fn small_universe() -> PoolUniverse {
        PoolUniverse {
            pool_ids: ids(20),
            inv_account: 1,
            ap_account: 2,
        }
    }

    #[test]
    fn workload_next_lines_simple_returns_one_line() {
        let w = Workload {
            universe: small_universe(),
            overlap: OverlapMode::Uniform,
            complexity: Complexity::Simple,
            caller_count: 4,
        };
        let mut rng = StdRng::seed_from_u64(3);
        let lines = w.next_lines(&mut rng, 0);
        assert_eq!(lines.len(), 1);
        assert!(lines[0].qty >= 1 && lines[0].qty <= 100);
        assert!(lines[0].unit_cost >= 1 && lines[0].unit_cost <= 1000);
        assert_eq!(lines[0].debit_account, 1);
        assert_eq!(lines[0].credit_account, 2);
    }

    #[test]
    fn workload_next_lines_complex_returns_10_to_50() {
        let w = Workload {
            universe: small_universe(),
            overlap: OverlapMode::Uniform,
            complexity: Complexity::Complex,
            caller_count: 4,
        };
        let mut rng = StdRng::seed_from_u64(4);
        for _ in 0..20 {
            let lines = w.next_lines(&mut rng, 0);
            assert!(
                (10..=50).contains(&lines.len()),
                "complex lines.len() = {} outside [10, 50]",
                lines.len()
            );
        }
    }
}
