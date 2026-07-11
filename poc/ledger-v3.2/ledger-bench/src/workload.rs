//! Workload generation: the receipt-cost-volatility knob and the
//! backdated-event injector the replay oracle flagged as missing
//! (`poc/ledger-v3.1/bench/replay-oracle-results.md` §2a / Bottom line).
//!
//! A constant-cost seed produces exactly zero costing variance — the workload
//! must vary receipt costs on the same scale as pool value before the
//! provisional-vs-authoritative divergence the recalc engine exists to
//! correct can be measured. Profiles mirror the oracle's synthetic sweep:
//! `low` ±2%, `med` ±40%, `high` ±90% uniform around a base cost, and
//! `trend` = monotone rising over the run (the profile under which drift is
//! *biased*, not mean-zero — the load-bearing finding).
//!
//! The backdate injector stamps a fraction of submissions with a `posted_at`
//! behind the wall clock (within the open period), making business-order ≠
//! commit-order — the exact input that forces recalc's R-2 re-cost floors
//! and, during a close, the gate/fence machinery.

use chrono::{DateTime, Duration, Utc};
use rand::rngs::StdRng;
use rand::Rng;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum CostProfile {
    /// ±2% uniform around base.
    Low,
    /// ±40% uniform around base.
    #[default]
    Med,
    /// ±90% uniform around base.
    High,
    /// Monotone rising: 0.5×base at run start → 1.5×base at run end (±2%
    /// jitter). Drift under this profile is directional (FIFO overstates,
    /// LIFO understates).
    Trend,
}

impl CostProfile {
    /// Sample a receipt unit cost. `progress` ∈ [0, 1] is the run fraction
    /// (drives the trend profile; ignored by the uniform profiles).
    pub fn sample(self, rng: &mut StdRng, base: i64, progress: f64) -> i64 {
        let spread = |rng: &mut StdRng, pct: f64| -> i64 {
            let lo = (base as f64 * (1.0 - pct)) as i64;
            let hi = (base as f64 * (1.0 + pct)) as i64;
            rng.random_range(lo..=hi)
        };
        match self {
            CostProfile::Low => spread(rng, 0.02),
            CostProfile::Med => spread(rng, 0.40),
            CostProfile::High => spread(rng, 0.90),
            CostProfile::Trend => {
                let center = base as f64 * (0.5 + progress.clamp(0.0, 1.0));
                let lo = (center * 0.98) as i64;
                let hi = (center * 1.02) as i64;
                rng.random_range(lo.max(1)..=hi.max(2))
            }
        }
        .max(1)
    }
}

/// One generated submission (single-line; the multi-line dispatch surface is
/// the acceptance nets' subject, not the soak's).
#[derive(Debug, Clone)]
pub struct OpSpec {
    pub pool_id: i64,
    pub trx_type: &'static str,
    pub line_type: &'static str,
    /// Signed: positive receipt, negative depletion.
    pub qty: i64,
    /// Receipt cost from the profile; 0 for depletions (running_avg basis
    /// derives the observed cost from the aggregate).
    pub unit_cost: i64,
    pub posted_at: DateTime<Utc>,
    pub backdated: bool,
}

#[derive(Clone, Debug)]
pub struct Workload {
    pub n_pools: i64,
    /// Pareto-hot pool pick (80% of ops on the first 20% of ids) vs uniform.
    pub pareto: bool,
    /// Percent of ops that are depletions (0–100).
    pub deplete_pct: u8,
    pub profile: CostProfile,
    pub base_cost: i64,
    /// Percent of ops stamped behind the wall clock (0–100).
    pub backdate_pct: u8,
    /// Backdate offset drawn uniformly from (0, window] seconds.
    pub backdate_window_secs: u64,
    pub receipt_qty_max: i64,
    pub deplete_qty_max: i64,
}

impl Workload {
    pub fn next_op(&self, rng: &mut StdRng, progress: f64) -> OpSpec {
        let pool_id = self.pick_pool(rng);
        let backdated =
            self.backdate_pct > 0 && rng.random_range(0u8..100) < self.backdate_pct;
        let posted_at = if backdated {
            let back = rng.random_range(1..=self.backdate_window_secs.max(1)) as i64;
            Utc::now() - Duration::seconds(back)
        } else {
            Utc::now()
        };
        if rng.random_range(0u8..100) < self.deplete_pct {
            OpSpec {
                pool_id,
                trx_type: "inv_adjustment",
                line_type: "inv_adjustment_line",
                qty: -rng.random_range(1..=self.deplete_qty_max),
                unit_cost: 0,
                posted_at,
                backdated,
            }
        } else {
            OpSpec {
                pool_id,
                trx_type: "po_receipt",
                line_type: "po_receipt_line",
                qty: rng.random_range(1..=self.receipt_qty_max),
                unit_cost: self.profile.sample(rng, self.base_cost, progress),
                posted_at,
                backdated,
            }
        }
    }

    /// Pareto 80/20: 80% of picks land on the first 20% of pool ids (the hot
    /// set), uniform within each band.
    fn pick_pool(&self, rng: &mut StdRng) -> i64 {
        if !self.pareto || self.n_pools < 5 {
            return rng.random_range(1..=self.n_pools);
        }
        let hot = (self.n_pools / 5).max(1);
        if rng.random_range(0u8..100) < 80 {
            rng.random_range(1..=hot)
        } else {
            rng.random_range(hot + 1..=self.n_pools)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    fn workload(profile: CostProfile, backdate_pct: u8) -> Workload {
        Workload {
            n_pools: 100,
            pareto: true,
            deplete_pct: 45,
            profile,
            base_cost: 500_000,
            backdate_pct,
            backdate_window_secs: 3600,
            receipt_qty_max: 100,
            deplete_qty_max: 60,
        }
    }

    #[test]
    fn profiles_stay_within_their_bands() {
        let mut rng = StdRng::seed_from_u64(3);
        for _ in 0..10_000 {
            let c = CostProfile::Low.sample(&mut rng, 500_000, 0.0);
            assert!((490_000..=510_000).contains(&c), "low out of band: {c}");
            let c = CostProfile::High.sample(&mut rng, 500_000, 0.0);
            assert!((50_000..=950_000).contains(&c), "high out of band: {c}");
        }
    }

    #[test]
    fn trend_rises_with_progress() {
        let mut rng = StdRng::seed_from_u64(4);
        let early: i64 =
            (0..1000).map(|_| CostProfile::Trend.sample(&mut rng, 500_000, 0.05)).sum();
        let late: i64 =
            (0..1000).map(|_| CostProfile::Trend.sample(&mut rng, 500_000, 0.95)).sum();
        assert!(late > early * 2, "trend must rise: early={early} late={late}");
    }

    #[test]
    fn backdate_rate_is_respected() {
        let w = workload(CostProfile::Med, 10);
        let mut rng = StdRng::seed_from_u64(5);
        let n = 20_000;
        let backdated = (0..n).filter(|_| w.next_op(&mut rng, 0.5).backdated).count();
        let rate = backdated as f64 / n as f64;
        assert!((0.08..=0.12).contains(&rate), "rate={rate}");
        let w = workload(CostProfile::Med, 0);
        assert!((0..n).all(|_| !w.next_op(&mut rng, 0.5).backdated));
    }

    #[test]
    fn pareto_concentrates_on_the_hot_band() {
        let w = workload(CostProfile::Med, 0);
        let mut rng = StdRng::seed_from_u64(6);
        let n = 20_000;
        let hot = (0..n).filter(|_| w.next_op(&mut rng, 0.5).pool_id <= 20).count();
        let share = hot as f64 / n as f64;
        assert!((0.75..=0.85).contains(&share), "hot share={share}");
    }

    #[test]
    fn depletions_carry_zero_cost_receipts_positive() {
        let w = workload(CostProfile::Med, 0);
        let mut rng = StdRng::seed_from_u64(7);
        for _ in 0..1000 {
            let op = w.next_op(&mut rng, 0.5);
            if op.qty < 0 {
                assert_eq!(op.unit_cost, 0);
                assert_eq!(op.trx_type, "inv_adjustment");
            } else {
                assert!(op.unit_cost > 0);
                assert_eq!(op.trx_type, "po_receipt");
            }
        }
    }
}
