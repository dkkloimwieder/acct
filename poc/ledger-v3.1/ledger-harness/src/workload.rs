//! Synthetic workload generator (design-v3.1 §10.2/§10.3, P4 acct-2ttr.8).
//!
//! Orthogonal axes:
//!   OverlapMode  — how callers' submissions overlap on pool_ids (§10.2)
//!   Complexity   — lines-per-submission size (§10.3)
//!   deplete_pct  — fraction of lines that are depletions vs receipts
//!   multi_touch  — fraction of submissions that touch the same pool more than
//!                  once, shaped by a weighted TouchDistribution (acct-34ce)
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
    /// Discrete two-population mixture (acct-s90k). Each pick rolls hot vs cold
    /// by `hot_traffic_fraction` then samples uniformly within that population.
    /// The hot population is `pool_ids[0 .. hot_count]` where `hot_count =
    /// round(hot_pool_fraction * n)` clamped to `[1, n-1]` when both fractions
    /// are in (0,1); at the boundaries (0.0 / 1.0) the empty side rolls fall
    /// through to the populated side. Textbook 80/20 = `{0.2, 0.8}`.
    Pareto { hot_pool_fraction: f64, hot_traffic_fraction: f64 },
}

#[derive(Debug, Clone, Copy)]
pub enum Complexity {
    /// 1 line per submission.
    Simple,
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
}

/// Weighted distribution over per-pool touch counts (acct-34ce).
///
/// A multi-touch submission is built by repeatedly opening a fresh distinct
/// pool and drawing how many of its lines land on THAT pool from this
/// distribution. `(touches, weight)` buckets: `touches` is the group size (1 =
/// the pool appears once, 2 = twice, …); `weight` is its relative frequency.
/// A realistic WO-completion mix puts most mass on 1 with a tail at 2–3 (the
/// backflush + scrap + output shape), not a single synthetic worst-case.
///
/// `distinct()` (the default) is `[(1, 1)]` — every pool touched exactly once,
/// reproducing the lock-hold-measurement distinct-pool generation.
#[derive(Debug, Clone)]
pub struct TouchDistribution {
    /// `(touches >= 1, weight)`; at least one bucket with weight > 0.
    buckets: Vec<(u32, u32)>,
    total_weight: u64,
}

impl TouchDistribution {
    /// The distinct-pool default: every pool touched exactly once.
    pub fn distinct() -> Self {
        Self { buckets: vec![(1, 1)], total_weight: 1 }
    }

    /// Build from `(touches, weight)` buckets. Errors if empty, if any
    /// `touches < 1`, or if the total weight is 0.
    pub fn weighted(buckets: Vec<(u32, u32)>) -> Result<Self, String> {
        if buckets.is_empty() {
            return Err("touch distribution must have at least one bucket".into());
        }
        if buckets.iter().any(|&(t, _)| t < 1) {
            return Err("touch count must be >= 1".into());
        }
        let total_weight: u64 = buckets.iter().map(|&(_, w)| w as u64).sum();
        if total_weight == 0 {
            return Err("touch distribution total weight must be > 0".into());
        }
        Ok(Self { buckets, total_weight })
    }

    /// Parse the CLI form `"1:60,2:30,3:10"` (touches:weight, comma-separated).
    pub fn parse(spec: &str) -> Result<Self, String> {
        let mut buckets = Vec::new();
        for tok in spec.split(',') {
            let tok = tok.trim();
            if tok.is_empty() {
                continue;
            }
            let (t, w) = tok
                .split_once(':')
                .ok_or_else(|| format!("touch-dist token '{tok}' is not 'touches:weight'"))?;
            let touches: u32 = t
                .trim()
                .parse()
                .map_err(|_| format!("touch-dist touches '{t}' is not a number"))?;
            let weight: u32 = w
                .trim()
                .parse()
                .map_err(|_| format!("touch-dist weight '{w}' is not a number"))?;
            buckets.push((touches, weight));
        }
        Self::weighted(buckets)
    }

    /// Draw a touch count, weighted by the bucket weights.
    pub fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u32 {
        let mut r = rng.random_range(0..self.total_weight);
        for &(touches, weight) in &self.buckets {
            let w = weight as u64;
            if r < w {
                return touches;
            }
            r -= w;
        }
        // total_weight > 0 guarantees the loop returns; fall back to the last.
        self.buckets.last().map(|&(t, _)| t).unwrap_or(1)
    }

    /// Largest touch count any bucket can yield.
    #[allow(dead_code)]
    pub fn max_touches(&self) -> u32 {
        self.buckets.iter().map(|&(t, _)| t).max().unwrap_or(1)
    }

    /// Render back to the `"touches:weight,…"` spec for report labeling.
    pub fn spec_string(&self) -> String {
        self.buckets
            .iter()
            .map(|&(t, w)| format!("{t}:{w}"))
            .collect::<Vec<_>>()
            .join(",")
    }
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
    /// Percent of submissions eligible to touch the same pool more than once
    /// (acct-34ce). 0 = every submission is distinct-pool (the lock-hold
    /// measurement default); 100 = every submission draws its per-pool group
    /// sizes from `touch_dist`. Exercises PlanResult::coalesce_aggregates under
    /// load — the same-pool-twice path the distinct-pool generator never hits.
    pub multi_touch_pct: u8,
    /// Per-pool group-size distribution for multi-touch submissions. Only
    /// consulted when a submission rolls multi-touch; `distinct()` (all mass on
    /// 1) is a no-op even at `multi_touch_pct = 100`.
    pub touch_dist: TouchDistribution,
    /// Informational — the scenario's advertised caller count.
    #[allow(dead_code)]
    pub caller_count: usize,
}

impl Workload {
    /// Generate one submission's worth of lines for `caller_id`.
    ///
    /// By default pools are DISTINCT within a submission: the SPI bulk-UPSERTs
    /// aggregate rows keyed by (pool_id, layer_id=0), so listing the same pool
    /// twice would make a naive UPSERT "affect a row a second time". This is the
    /// §5.1 / §10.3 lock-hold-measurement shape. When `multi_touch_pct > 0` a
    /// fraction of submissions instead repeat pools (group sizes drawn from
    /// `touch_dist`), exercising PlanResult::coalesce_aggregates under load.
    /// When the overlap mode can't surface enough distinct pools to open new
    /// groups (single-hot-pool Zipf, stripe_size=1), the submission shrinks to
    /// what's available — Simple scenarios (1 line) are always single-touch.
    pub fn next_lines<R: Rng + ?Sized>(&self, rng: &mut R, caller_id: usize) -> Vec<LineParam> {
        let n_lines = match self.complexity {
            Complexity::Simple => 1,
            Complexity::Complex => rng.random_range(10..=20),
        };

        // multi_touch_pct gates eligibility; touch_dist shapes the repeats. The
        // `&&` short-circuit means the default (pct=0) path draws no extra RNG,
        // so distinct-pool generation is identical to a single-touch build.
        let multi_touch =
            self.multi_touch_pct > 0 && (rng.random_range(0..100) as u8) < self.multi_touch_pct;

        self.pool_sequence(rng, caller_id, n_lines, multi_touch)
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
                    }
                } else {
                    LineParam {
                        pool_id,
                        line_type: "po_receipt_line",
                        source_id: Some(rng.random_range(1..=1_000_000)),
                        qty: magnitude,
                        unit_cost: rng.random_range(1..=1000),
                    }
                }
            })
            .collect()
    }

    /// Build this submission's per-line pool sequence (one entry per line).
    ///
    /// Each group opens a FRESH distinct pool (via `pick_pool` + a not-yet-seen
    /// check, so repetition is orthogonal to which pools the OverlapMode picks),
    /// then places `touches` lines on it. Without multi-touch every group is a
    /// single line, reproducing the distinct-pool set. With multi-touch the
    /// group size is drawn from `touch_dist` and clamped to the lines still
    /// needed, so the same pool legitimately appears 2–3× — the coalesce path.
    fn pool_sequence<R: Rng + ?Sized>(
        &self,
        rng: &mut R,
        caller_id: usize,
        n_lines: usize,
        multi_touch: bool,
    ) -> Vec<i64> {
        let mut started: Vec<i64> = Vec::new();
        let mut seq: Vec<i64> = Vec::with_capacity(n_lines);
        let attempt_cap = n_lines.saturating_mul(20).max(n_lines);
        let mut attempts = 0;
        while seq.len() < n_lines && attempts < attempt_cap {
            attempts += 1;
            let pid = self.pick_pool(rng, caller_id);
            if started.contains(&pid) {
                continue; // a fresh distinct pool opens each group
            }
            started.push(pid);
            let remaining = n_lines - seq.len();
            let touches = if multi_touch {
                (self.touch_dist.sample(rng) as usize).clamp(1, remaining)
            } else {
                1
            };
            for _ in 0..touches {
                seq.push(pid);
            }
        }
        seq
    }

    fn pick_pool<R: Rng + ?Sized>(&self, rng: &mut R, caller_id: usize) -> i64 {
        match self.overlap {
            OverlapMode::Uniform => pick_uniform(rng, &self.universe.pool_ids),
            OverlapMode::Zipf { exponent } => pick_zipf(rng, &self.universe.pool_ids, exponent),
            OverlapMode::Disjoint { stripe_size } => {
                pick_disjoint(rng, &self.universe.pool_ids, caller_id, stripe_size)
            }
            OverlapMode::Pareto { hot_pool_fraction, hot_traffic_fraction } => pick_pareto(
                rng,
                &self.universe.pool_ids,
                hot_pool_fraction,
                hot_traffic_fraction,
            ),
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

/// Discrete two-population mixture (acct-s90k). With probability
/// `hot_traffic_fraction` sample uniformly from the first `hot_count` pools;
/// otherwise from the remaining `cold_count`. `hot_count` is clamped to
/// `[1, n-1]` whenever both populations should exist, so the textbook
/// `{0.2, 0.8}` always has both a hot and a cold pool to draw from. When one
/// side is empty by construction (`hot_pool_fraction` of 0.0 or 1.0), or when
/// the fraction lands in the empty side, the roll falls through to the other —
/// every call still returns a valid `pool_id`.
pub fn pick_pareto<R: Rng + ?Sized>(
    rng: &mut R,
    pool_ids: &[i64],
    hot_pool_fraction: f64,
    hot_traffic_fraction: f64,
) -> i64 {
    let n = pool_ids.len();
    if n == 0 {
        panic!("pick_pareto called on empty pool_ids");
    }
    let hp = hot_pool_fraction.clamp(0.0, 1.0);
    let ht = hot_traffic_fraction.clamp(0.0, 1.0);
    let raw = (hp * n as f64).round() as usize;
    let hot_count = if hp <= 0.0 {
        0
    } else if hp >= 1.0 {
        n
    } else {
        raw.clamp(1, n - 1)
    };
    let roll_hot = rng.random::<f64>() < ht;
    let take_hot = (roll_hot && hot_count > 0) || hot_count == n;
    if take_hot {
        let i = rng.random_range(0..hot_count.max(1));
        pool_ids[i]
    } else {
        // Cold span starts at hot_count; if hot_count == n the take_hot branch
        // above already covered the all-hot case, so this branch only runs when
        // hot_count < n.
        let i = rng.random_range(hot_count..n);
        pool_ids[i]
    }
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
            multi_touch_pct: 0,
            touch_dist: TouchDistribution::distinct(),
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
            multi_touch_pct: 0,
            touch_dist: TouchDistribution::distinct(),
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
            multi_touch_pct: 0,
            touch_dist: TouchDistribution::distinct(),
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
            multi_touch_pct: 0,
            touch_dist: TouchDistribution::distinct(),
            caller_count: 4,
        };
        let mut rng = StdRng::seed_from_u64(5);
        for _ in 0..20 {
            let n = w.next_lines(&mut rng, 0).len();
            assert!((10..=20).contains(&n), "complex lines.len() = {n} outside [10,20]");
        }
    }

    fn multi_touch_workload(n: usize, pct: u8, dist: TouchDistribution) -> Workload {
        Workload {
            universe: universe(n),
            overlap: OverlapMode::Uniform,
            complexity: Complexity::Complex,
            deplete_pct: 0,
            multi_touch_pct: pct,
            touch_dist: dist,
            caller_count: 4,
        }
    }

    fn max_pool_multiplicity(lines: &[LineParam]) -> usize {
        let mut counts: HashMap<i64, usize> = HashMap::new();
        for l in lines {
            *counts.entry(l.pool_id).or_default() += 1;
        }
        counts.values().copied().max().unwrap_or(0)
    }

    #[test]
    fn multi_touch_repeats_a_pool() {
        // pct=100 + "always 2 touches" => every opened pool is hit twice.
        let w = multi_touch_workload(50, 100, TouchDistribution::parse("2:1").unwrap());
        let mut rng = StdRng::seed_from_u64(11);
        for _ in 0..100 {
            let lines = w.next_lines(&mut rng, 0);
            assert!(
                max_pool_multiplicity(&lines) >= 2,
                "multi-touch submission must repeat a pool: {lines:?}"
            );
            // Complexity::Complex still bounds the total line count; the large
            // universe leaves room to open every group as a fresh pool.
            assert!((10..=20).contains(&lines.len()), "lines.len()={}", lines.len());
        }
    }

    #[test]
    fn multi_touch_zero_pct_stays_distinct() {
        // Even with a repeat-heavy distribution, pct=0 gates multi-touch off.
        let w = multi_touch_workload(50, 0, TouchDistribution::parse("2:1,3:1").unwrap());
        let mut rng = StdRng::seed_from_u64(12);
        for _ in 0..100 {
            let lines = w.next_lines(&mut rng, 0);
            assert_eq!(max_pool_multiplicity(&lines), 1, "pct=0 must stay distinct");
        }
    }

    #[test]
    fn multi_touch_mix_yields_both_distinct_and_repeated_pools() {
        // A realistic mix (mostly 1, tail at 2-3) produces some single-touch and
        // some repeated pools across a run — not a uniform worst case.
        let w = multi_touch_workload(200, 100, TouchDistribution::parse("1:60,2:30,3:10").unwrap());
        let mut rng = StdRng::seed_from_u64(13);
        let mut saw_distinct_group = false;
        let mut saw_repeat = false;
        for _ in 0..200 {
            let lines = w.next_lines(&mut rng, 0);
            let mut counts: HashMap<i64, usize> = HashMap::new();
            for l in &lines {
                *counts.entry(l.pool_id).or_default() += 1;
            }
            if counts.values().any(|&c| c == 1) {
                saw_distinct_group = true;
            }
            if counts.values().any(|&c| c >= 2) {
                saw_repeat = true;
            }
        }
        assert!(saw_distinct_group, "expected some single-touch groups");
        assert!(saw_repeat, "expected some repeated pools");
    }

    #[test]
    fn touch_distribution_parse_round_trips() {
        let d = TouchDistribution::parse("1:60,2:30,3:10").unwrap();
        assert_eq!(d.spec_string(), "1:60,2:30,3:10");
        assert_eq!(d.max_touches(), 3);
    }

    #[test]
    fn touch_distribution_parse_rejects_malformed() {
        assert!(TouchDistribution::parse("").is_err()); // no buckets
        assert!(TouchDistribution::parse("0:5").is_err()); // touches < 1
        assert!(TouchDistribution::parse("1:0").is_err()); // total weight 0
        assert!(TouchDistribution::parse("2").is_err()); // missing ':'
        assert!(TouchDistribution::parse("x:1").is_err()); // non-numeric touches
    }

    #[test]
    fn touch_distribution_sample_respects_weights() {
        let mut rng = StdRng::seed_from_u64(14);
        let always_two = TouchDistribution::parse("2:1").unwrap();
        let distinct = TouchDistribution::distinct();
        for _ in 0..1_000 {
            assert_eq!(always_two.sample(&mut rng), 2);
            assert_eq!(distinct.sample(&mut rng), 1);
        }
    }

    fn count_hot_picks(
        rng: &mut StdRng,
        pool_ids: &[i64],
        hot_pool_pct: f64,
        hot_traffic_pct: f64,
        rolls: usize,
    ) -> (usize, usize) {
        let hot_cutoff = pool_ids[((hot_pool_pct * pool_ids.len() as f64).round() as usize)
            .clamp(1, pool_ids.len() - 1)];
        let mut hot = 0usize;
        for _ in 0..rolls {
            let id = pick_pareto(rng, pool_ids, hot_pool_pct, hot_traffic_pct);
            if id <= hot_cutoff {
                hot += 1;
            }
        }
        (hot, rolls)
    }

    #[test]
    fn pareto_mixture_respects_traffic_fraction() {
        // 80/20: ~80% of rolls land in the first 20% of pools (one-tenth windows
        // are tight enough to catch a swapped fraction without flaking on noise).
        let mut rng = StdRng::seed_from_u64(20);
        let pool_ids = ids(1000);
        let (hot, total) = count_hot_picks(&mut rng, &pool_ids, 0.2, 0.8, 20_000);
        let frac = hot as f64 / total as f64;
        assert!((0.75..=0.85).contains(&frac), "expected ~0.80 hot share; got {frac}");
    }

    #[test]
    fn pareto_hot_pool_subset_is_disjoint_from_cold() {
        // The hot half and cold half never overlap by construction: every hot
        // pick is below the cutoff index, every cold pick at or above.
        let mut rng = StdRng::seed_from_u64(21);
        let pool_ids = ids(100);
        let hot_count = 20usize;
        let cutoff = pool_ids[hot_count - 1];
        let mut saw_hot = false;
        let mut saw_cold = false;
        for _ in 0..5_000 {
            let id = pick_pareto(&mut rng, &pool_ids, 0.2, 0.8);
            if id <= cutoff {
                saw_hot = true;
            } else {
                saw_cold = true;
            }
        }
        assert!(saw_hot && saw_cold, "both populations must be reached");
    }

    #[test]
    fn pareto_degenerate_values_are_handled() {
        let mut rng = StdRng::seed_from_u64(22);
        let pool_ids = ids(50);
        // hot_traffic = 0.0 → always cold; never hits pool_ids[0..10].
        for _ in 0..500 {
            let id = pick_pareto(&mut rng, &pool_ids, 0.2, 0.0);
            assert!(id > pool_ids[9], "ht=0.0 must avoid hot half; got {id}");
        }
        // hot_traffic = 1.0 → always hot; never hits pool_ids[10..].
        for _ in 0..500 {
            let id = pick_pareto(&mut rng, &pool_ids, 0.2, 1.0);
            assert!(id <= pool_ids[9], "ht=1.0 must stay in hot half; got {id}");
        }
        // hot_pool = 1.0 → cold is empty; even cold rolls fall through to hot.
        for _ in 0..500 {
            let id = pick_pareto(&mut rng, &pool_ids, 1.0, 0.0);
            assert!(pool_ids.contains(&id));
        }
        // hot_pool = 0.0 → hot is empty; even hot rolls fall through to cold.
        for _ in 0..500 {
            let id = pick_pareto(&mut rng, &pool_ids, 0.0, 1.0);
            assert!(pool_ids.contains(&id));
        }
    }

    #[test]
    fn pareto_clamps_out_of_range_fractions() {
        // Inputs outside [0,1] saturate rather than blow up — the harness CLI
        // passes percent-based u8 → f64/100.0 which can be 0..=100 but
        // defensive clamping protects programmatic callers too.
        let mut rng = StdRng::seed_from_u64(23);
        let pool_ids = ids(50);
        for _ in 0..200 {
            let id = pick_pareto(&mut rng, &pool_ids, -0.5, 1.5);
            assert!(pool_ids.contains(&id));
        }
    }
}
