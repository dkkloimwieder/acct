//! Scenarios S1-S6 per plan §F (acct-7ywx).
//!
//! Each `sN(&PoolUniverse) -> ScenarioSpec` builder fills in the three
//! workload axes plus the caller count appropriate for that cell. The
//! `run` subcommand resolves --scenario sN by looking up the builder
//! here, then passes the resulting ScenarioSpec to the driver.
//!
//! | id | callers | overlap                 | complexity | method-mix
//! |----|---------|-------------------------|------------|------------
//! | s1 | 10      | Uniform                 | Simple     | AllWac
//! | s2 | 200     | Zipf(1.5)               | Simple     | AllWac
//! | s3 | 10      | Uniform                 | Complex    | Mixed
//! | s4 | 200     | Zipf(1.2)               | Complex    | Mixed
//! | s5 | 1000    | single hot pool         | Simple     | AllWac
//! | s6 | 1000    | Disjoint stripes        | Simple     | AllWac
//!
//! S5 is a Disjoint{stripe_size: 1} pinned to pool_ids[0] (caller_id
//! mod 1 always picks the same pool — degenerate hot-pool case).
//!
//! S6 stripe_size is universe.len() / caller_count, floored at 1, so
//! 1000 callers across 10k pools each pin a 10-pool stripe (disjoint
//! by construction).

use crate::pool_universe::PoolUniverse;
use crate::workload::{Complexity, OverlapMode, Workload};
use crate::cli::MethodMix;

#[derive(Debug, Clone)]
pub struct ScenarioSpec {
    pub id: &'static str,
    pub description: &'static str,
    pub callers: usize,
    pub workload: Workload,
    /// Method mix advertised in the JSON report. The actual per-pool
    /// methods come from the seed-pools run; this is a label that the
    /// scenario builder *expects* the universe to have been seeded
    /// with. Drivers don't enforce it (the workload is method-agnostic).
    pub expected_method_mix: MethodMix,
}

/// Resolve an "sN" id to the matching builder. Returns None on unknown.
pub fn by_id(id: &str, universe: PoolUniverse) -> Option<ScenarioSpec> {
    let f: fn(PoolUniverse) -> ScenarioSpec = match id {
        "s1" => s1,
        "s2" => s2,
        "s3" => s3,
        "s4" => s4,
        "s5" => s5,
        "s6" => s6,
        _ => return None,
    };
    Some(f(universe))
}

pub fn s1(u: PoolUniverse) -> ScenarioSpec {
    let callers = 10;
    ScenarioSpec {
        id: "s1",
        description: "baseline — 10 callers, uniform, simple, all-wac",
        callers,
        workload: Workload {
            universe: u,
            overlap: OverlapMode::Uniform,
            complexity: Complexity::Simple,
            caller_count: callers,
        },
        expected_method_mix: MethodMix::AllWac,
    }
}

pub fn s2(u: PoolUniverse) -> ScenarioSpec {
    let callers = 200;
    ScenarioSpec {
        id: "s2",
        description: "routing lock-amortization — 200 callers, zipf(1.5), simple, all-wac",
        callers,
        workload: Workload {
            universe: u,
            overlap: OverlapMode::Zipf { exponent: 1.5 },
            complexity: Complexity::Simple,
            caller_count: callers,
        },
        expected_method_mix: MethodMix::AllWac,
    }
}

pub fn s3(u: PoolUniverse) -> ScenarioSpec {
    let callers = 10;
    ScenarioSpec {
        id: "s3",
        description: "per-trx work intensity — 10 callers, uniform, complex, mixed",
        callers,
        workload: Workload {
            universe: u,
            overlap: OverlapMode::Uniform,
            complexity: Complexity::Complex,
            caller_count: callers,
        },
        expected_method_mix: MethodMix::Mixed,
    }
}

pub fn s4(u: PoolUniverse) -> ScenarioSpec {
    let callers = 200;
    ScenarioSpec {
        id: "s4",
        description: "production-like stress — 200 callers, zipf(1.2), complex, mixed",
        callers,
        workload: Workload {
            universe: u,
            overlap: OverlapMode::Zipf { exponent: 1.2 },
            complexity: Complexity::Complex,
            caller_count: callers,
        },
        expected_method_mix: MethodMix::Mixed,
    }
}

/// S5 — pathological hot pool for direct path (every caller hammers
/// the same pool_lock).
///
/// `Disjoint{stripe_size: 1}` makes pick_disjoint pin caller 0 to
/// pool_ids[0..1], caller 1 to pool_ids[1..2], etc. — that's
/// MULTI-pool, not single-pool. For true single-hot-pool we use
/// `Zipf{exponent: 100.0}` which concentrates >99% of picks on
/// pool_ids[0] without needing a separate enum variant. Approximation
/// is acceptable for the PoC's directional measurement; a dedicated
/// SinglePool overlap mode can land if a scenario discriminator needs
/// strict singleton behavior.
pub fn s5(u: PoolUniverse) -> ScenarioSpec {
    let callers = 1000;
    ScenarioSpec {
        id: "s5",
        description: "pathological for direct — 1000 callers, single hot pool, simple, all-wac",
        callers,
        workload: Workload {
            universe: u,
            overlap: OverlapMode::Zipf { exponent: 100.0 },
            complexity: Complexity::Simple,
            caller_count: callers,
        },
        expected_method_mix: MethodMix::AllWac,
    }
}

/// S6 — pathological for routed (no router benefit when pool sets are
/// fully disjoint per caller). stripe_size = universe / callers,
/// floored at 1.
pub fn s6(u: PoolUniverse) -> ScenarioSpec {
    let callers = 1000;
    let stripe_size = u.pool_ids.len().checked_div(callers).unwrap_or(1).max(1);
    ScenarioSpec {
        id: "s6",
        description: "pathological for routed — 1000 callers, disjoint stripes, simple, all-wac",
        callers,
        workload: Workload {
            universe: u,
            overlap: OverlapMode::Disjoint { stripe_size },
            complexity: Complexity::Simple,
            caller_count: callers,
        },
        expected_method_mix: MethodMix::AllWac,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small_universe(n: usize) -> PoolUniverse {
        PoolUniverse {
            pool_ids: (1..=n as i64).collect(),
            inv_account: 1,
            ap_account: 2,
        }
    }

    #[test]
    fn by_id_resolves_all_six() {
        for id in ["s1", "s2", "s3", "s4", "s5", "s6"] {
            let s = by_id(id, small_universe(100));
            assert!(s.is_some(), "by_id should resolve {id}");
            let s = s.unwrap();
            assert_eq!(s.id, id);
            assert!(s.callers > 0);
        }
    }

    #[test]
    fn by_id_unknown_returns_none() {
        assert!(by_id("s99", small_universe(10)).is_none());
        assert!(by_id("", small_universe(10)).is_none());
    }

    #[test]
    fn s6_stripe_size_partitions_universe() {
        let u = small_universe(10_000);
        let s = s6(u);
        // 10000 pools / 1000 callers = 10 per stripe.
        if let OverlapMode::Disjoint { stripe_size } = s.workload.overlap {
            assert_eq!(stripe_size, 10);
        } else {
            panic!("s6 should use Disjoint overlap");
        }
    }

    #[test]
    fn s6_stripe_floor_is_one() {
        // Smaller universe than callers — stripe floors at 1.
        let u = small_universe(500);
        let s = s6(u);
        if let OverlapMode::Disjoint { stripe_size } = s.workload.overlap {
            assert_eq!(stripe_size, 1);
        } else {
            panic!("s6 should use Disjoint overlap");
        }
    }

    #[test]
    fn caller_counts_match_plan_table() {
        let u = small_universe(10_000);
        assert_eq!(s1(u.clone()).callers, 10);
        assert_eq!(s2(u.clone()).callers, 200);
        assert_eq!(s3(u.clone()).callers, 10);
        assert_eq!(s4(u.clone()).callers, 200);
        assert_eq!(s5(u.clone()).callers, 1000);
        assert_eq!(s6(u).callers, 1000);
    }
}
