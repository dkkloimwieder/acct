//! Scenarios S1-S21 per design-v3.1 §10.6 (P4 acct-2ttr.8; S9 acct-34ce;
//! S10-S21 Pareto families acct-s90k).
//!
//! Each `sN(PoolUniverse) -> ScenarioSpec` fills the workload axes + caller
//! count + the pool depth the universe is EXPECTED to have been seeded to
//! (`depth_hint`). `run` resolves --scenario sN here, then drives the spec.
//!
//! | id  | callers | overlap          | complexity | method | deplete | depth | multi-touch
//! |-----|---------|------------------|------------|--------|---------|-------|------------
//! | s1  | 10      | Uniform          | Simple     | WAC    | 0       | 0     | —
//! | s2  | 200     | Zipf(1.5)        | Simple     | WAC    | 0       | 0     | —
//! | s3  | 10      | Uniform          | Complex    | Mixed  | 0       | 0     | —
//! | s4  | 200     | Zipf(1.2)        | Complex    | Mixed  | 0       | 0     | —
//! | s5  | 1000    | single hot pool  | Simple     | FIFO   | 100     | 10    | —
//! | s6  | 1000    | Disjoint stripes | Simple     | FIFO   | 100     | 10    | —
//! | s7  | 1000    | Zipf(1.2)        | Simple     | FIFO   | 100     | 1000  | — ← deep
//! | s8  | 1000    | Zipf(1.2)        | Complex    | FIFO   | 50      | 1000  | — ← deep
//! | s9  | 1000    | Zipf(1.2)        | Complex    | FIFO   | 50      | 1000  | 40% / 1:60,2:30,3:10 ← deep
//! |     | RECEIPTS family — deplete 0, distinct-pool                            |
//! | s10 | 50      | Pareto 80/20     | Complex    | FIFO   | 0       | 0     | —  ← mid-typ
//! | s11 | 200     | Pareto 80/20     | Complex    | FIFO   | 0       | 100   | —  ← high-vol
//! | s12 | 50      | Pareto 90/10     | Complex    | FIFO   | 0       | 0     | —  ← long-tail
//! | s13 | 50      | Pareto 50/50     | Complex    | FIFO   | 0       | 0     | —  ← balanced
//! |     | BUILDS family — deplete 50, multi-touch 40% / 1:60,2:30,3:10          |
//! | s14 | 50      | Pareto 80/20     | Complex    | FIFO   | 50      | 10    | 40% mix  ← mid-typ
//! | s15 | 200     | Pareto 80/20     | Complex    | FIFO   | 50      | 100   | 40% mix  ← high-vol
//! | s16 | 50      | Pareto 90/10     | Complex    | FIFO   | 50      | 10    | 40% mix  ← long-tail
//! | s17 | 50      | Pareto 50/50     | Complex    | FIFO   | 50      | 10    | 40% mix  ← balanced
//! |     | MIXED family — deplete 50, distinct-pool                              |
//! | s18 | 50      | Pareto 80/20     | Complex    | FIFO   | 50      | 10    | —  ← mid-typ
//! | s19 | 200     | Pareto 80/20     | Complex    | FIFO   | 50      | 100   | —  ← high-vol
//! | s20 | 50      | Pareto 90/10     | Complex    | FIFO   | 50      | 10    | —  ← long-tail
//! | s21 | 50      | Pareto 50/50     | Complex    | FIFO   | 50      | 10    | —  ← balanced
//!
//! v3.1 adds S7/S8 (deep pools — Path C's home field, §11.2) over ledger-v3's
//! S1-S6. S5-S9 deplete and therefore require pre-seeded aggregates
//! (`seed-pools --depth`); S1-S4 are receipt-only and self-seed. S9 is S8 +
//! multi-touch (acct-34ce): a head-to-head for the coalesce-under-load path.
//!
//! S10-S21 are the Pareto 80/20 family block (acct-s90k): a discrete two-
//! population mixture filling in the mid-market region the extremes-bracketed
//! matrix only approximates. Three workload shapes — RECEIPTS (s10-s13) all
//! receipts, BUILDS (s14-s17) deplete+multi-touch (WO-completion), MIXED
//! (s18-s21) balanced deplete — × four overlap/scale variants (mid-typ,
//! high-vol, long-tail, balanced). Use `--pareto-hot-pool-pct` /
//! `--pareto-hot-traffic-pct` to overlay alternate Pareto knobs on any scenario.
//! Any scenario can also be made multi-touch ad hoc via `run --multi-touch-pct`
//! + `--touch-dist`.

use crate::cli::MethodMix;
use crate::pool_universe::PoolUniverse;
use crate::workload::{Complexity, OverlapMode, TouchDistribution, Workload};

#[derive(Debug, Clone)]
pub struct ScenarioSpec {
    pub id: &'static str,
    pub description: &'static str,
    pub callers: usize,
    pub workload: Workload,
    /// Method mix the universe is expected to have been seeded with (a label;
    /// the workload is method-agnostic — per-pool method comes from seed-pools).
    #[allow(dead_code)]
    pub expected_method_mix: MethodMix,
    /// Pool depth the universe should be seeded to before driving this scenario
    /// (`seed-pools --depth`). 0 = shallow/receipt-only. Recorded in the report.
    pub depth_hint: usize,
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
        "s7" => s7,
        "s8" => s8,
        "s9" => s9,
        "s10" => s10,
        "s11" => s11,
        "s12" => s12,
        "s13" => s13,
        "s14" => s14,
        "s15" => s15,
        "s16" => s16,
        "s17" => s17,
        "s18" => s18,
        "s19" => s19,
        "s20" => s20,
        "s21" => s21,
        _ => return None,
    };
    Some(f(universe))
}

fn spec(
    id: &'static str,
    description: &'static str,
    callers: usize,
    universe: PoolUniverse,
    overlap: OverlapMode,
    complexity: Complexity,
    deplete_pct: u8,
    method: MethodMix,
    depth_hint: usize,
) -> ScenarioSpec {
    ScenarioSpec {
        id,
        description,
        callers,
        workload: Workload {
            universe,
            overlap,
            complexity,
            deplete_pct,
            multi_touch_pct: 0,
            touch_dist: TouchDistribution::distinct(),
            caller_count: callers,
        },
        expected_method_mix: method,
        depth_hint,
    }
}

pub fn s1(u: PoolUniverse) -> ScenarioSpec {
    spec("s1", "baseline — 10 callers, uniform, simple receipts, all-wac, shallow",
        10, u, OverlapMode::Uniform, Complexity::Simple, 0, MethodMix::AllWac, 0)
}

pub fn s2(u: PoolUniverse) -> ScenarioSpec {
    spec("s2", "routing lock-amortization — 200 callers, zipf(1.5), simple receipts, all-wac, shallow",
        200, u, OverlapMode::Zipf { exponent: 1.5 }, Complexity::Simple, 0, MethodMix::AllWac, 0)
}

pub fn s3(u: PoolUniverse) -> ScenarioSpec {
    spec("s3", "per-trx work intensity — 10 callers, uniform, complex receipts, mixed, shallow",
        10, u, OverlapMode::Uniform, Complexity::Complex, 0, MethodMix::Mixed, 0)
}

pub fn s4(u: PoolUniverse) -> ScenarioSpec {
    spec("s4", "production-like stress — 200 callers, zipf(1.2), complex receipts, mixed, shallow",
        200, u, OverlapMode::Zipf { exponent: 1.2 }, Complexity::Complex, 0, MethodMix::Mixed, 0)
}

/// S5 — pathological hot pool for direct (every caller depletes one pool).
/// `Zipf{exponent: 100}` concentrates >99% of picks on pool_ids[0]; depth 10
/// shallow seed gives the hot pool depletion headroom.
pub fn s5(u: PoolUniverse) -> ScenarioSpec {
    spec("s5", "pathological for direct — 1000 callers, single hot pool, simple fifo depletions, shallow",
        1000, u, OverlapMode::Zipf { exponent: 100.0 }, Complexity::Simple, 100, MethodMix::AllFifo, 10)
}

/// S6 — pathological for routed (no router benefit when pool sets are fully
/// disjoint). stripe_size = universe / callers, floored at 1.
pub fn s6(u: PoolUniverse) -> ScenarioSpec {
    let stripe_size = u.pool_ids.len().checked_div(1000).unwrap_or(1).max(1);
    spec("s6", "pathological for routed — 1000 callers, disjoint stripes, simple fifo depletions, shallow",
        1000, u, OverlapMode::Disjoint { stripe_size }, Complexity::Simple, 100, MethodMix::AllFifo, 10)
}

/// S7 — **Path C's home field** (§11.2). 1000 callers, Zipfian overlap, simple
/// FIFO depletions against DEEP pools (1000 layers). Direct Path C holds the
/// lock for aggregate-only work independent of depth; routed collapses the
/// concurrent depletions into one commit_group.
pub fn s7(u: PoolUniverse) -> ScenarioSpec {
    spec("s7", "deep-pool home field — 1000 callers, zipf(1.2), simple fifo depletions, DEEP (1000 layers)",
        1000, u, OverlapMode::Zipf { exponent: 1.2 }, Complexity::Simple, 100, MethodMix::AllFifo, 1000)
}

/// S8 — production-realistic FIFO stress: 1000 callers, Zipfian, complex
/// multi-line trxs mixing receipts and depletions, against deep pools.
pub fn s8(u: PoolUniverse) -> ScenarioSpec {
    spec("s8", "deep-pool complex — 1000 callers, zipf(1.2), complex multi-line fifo, DEEP (1000 layers)",
        1000, u, OverlapMode::Zipf { exponent: 1.2 }, Complexity::Complex, 50, MethodMix::AllFifo, 1000)
}

/// S9 — WO-completion multi-touch mix (acct-34ce). S8's shape (1000 callers,
/// zipf(1.2), complex deep-pool FIFO) with multi-touch ENABLED, so it is a
/// head-to-head against s8: same axes, but ~40% of submissions touch one pool
/// 2–3× (the backflush + scrap + output shape). Exercises
/// PlanResult::coalesce_aggregates under load. A realistic mix, not a worst
/// case — most groups stay single-touch (touch-dist 1:60,2:30,3:10).
pub fn s9(u: PoolUniverse) -> ScenarioSpec {
    let mut s = spec(
        "s9",
        "WO-completion multi-touch — s8 shape + ~40% same-pool 2-3x (coalesce under load), DEEP",
        1000, u, OverlapMode::Zipf { exponent: 1.2 }, Complexity::Complex, 50, MethodMix::AllFifo, 1000,
    );
    s.workload.multi_touch_pct = 40;
    s.workload.touch_dist =
        TouchDistribution::parse("1:60,2:30,3:10").expect("valid s9 touch distribution");
    s
}

// ── Pareto family block (acct-s90k) ─────────────────────────────────
//
// Three workload families × four variants per family. The family applies a
// post-construction overlay: BUILDS sets multi-touch + deplete=50; MIXED sets
// deplete=50 only; RECEIPTS leaves the receipts/distinct-pool defaults.

/// Standard build-distribution preset shared by every BUILDS-family scenario.
/// Matches s9's WO-completion mix (`1:60,2:30,3:10` weighted, 40% multi-touch).
fn builds_touch_dist() -> TouchDistribution {
    TouchDistribution::parse("1:60,2:30,3:10").expect("valid builds touch distribution")
}

fn pareto(hot_pool_pct: u8, hot_traffic_pct: u8) -> OverlapMode {
    OverlapMode::Pareto {
        hot_pool_fraction: hot_pool_pct as f64 / 100.0,
        hot_traffic_fraction: hot_traffic_pct as f64 / 100.0,
    }
}

// RECEIPTS family — deplete=0, distinct-pool. PO receipt / transfer-in shape.

pub fn s10(u: PoolUniverse) -> ScenarioSpec {
    spec("s10", "receipts mid-typ — 50 callers, Pareto 80/20, complex receipts, fifo, shallow",
        50, u, pareto(20, 80), Complexity::Complex, 0, MethodMix::AllFifo, 0)
}

pub fn s11(u: PoolUniverse) -> ScenarioSpec {
    spec("s11", "receipts high-vol — 200 callers, Pareto 80/20, complex receipts, fifo, depth 100",
        200, u, pareto(20, 80), Complexity::Complex, 0, MethodMix::AllFifo, 100)
}

pub fn s12(u: PoolUniverse) -> ScenarioSpec {
    spec("s12", "receipts long-tail — 50 callers, Pareto 90/10, complex receipts, fifo, shallow",
        50, u, pareto(10, 90), Complexity::Complex, 0, MethodMix::AllFifo, 0)
}

pub fn s13(u: PoolUniverse) -> ScenarioSpec {
    spec("s13", "receipts balanced — 50 callers, Pareto 50/50, complex receipts, fifo, shallow",
        50, u, pareto(50, 50), Complexity::Complex, 0, MethodMix::AllFifo, 0)
}

// BUILDS family — deplete=50, multi-touch 40% / 1:60,2:30,3:10. WO-completion
// shape (backflush + scrap + output on one SKU/location).

pub fn s14(u: PoolUniverse) -> ScenarioSpec {
    let mut s = spec("s14", "builds mid-typ — 50 callers, Pareto 80/20, complex mixed (WO shape), fifo, depth 10",
        50, u, pareto(20, 80), Complexity::Complex, 50, MethodMix::AllFifo, 10);
    s.workload.multi_touch_pct = 40;
    s.workload.touch_dist = builds_touch_dist();
    s
}

pub fn s15(u: PoolUniverse) -> ScenarioSpec {
    let mut s = spec("s15", "builds high-vol — 200 callers, Pareto 80/20, complex mixed (WO shape), fifo, depth 100",
        200, u, pareto(20, 80), Complexity::Complex, 50, MethodMix::AllFifo, 100);
    s.workload.multi_touch_pct = 40;
    s.workload.touch_dist = builds_touch_dist();
    s
}

pub fn s16(u: PoolUniverse) -> ScenarioSpec {
    let mut s = spec("s16", "builds long-tail — 50 callers, Pareto 90/10, complex mixed (WO shape), fifo, depth 10",
        50, u, pareto(10, 90), Complexity::Complex, 50, MethodMix::AllFifo, 10);
    s.workload.multi_touch_pct = 40;
    s.workload.touch_dist = builds_touch_dist();
    s
}

pub fn s17(u: PoolUniverse) -> ScenarioSpec {
    let mut s = spec("s17", "builds balanced — 50 callers, Pareto 50/50, complex mixed (WO shape), fifo, depth 10",
        50, u, pareto(50, 50), Complexity::Complex, 50, MethodMix::AllFifo, 10);
    s.workload.multi_touch_pct = 40;
    s.workload.touch_dist = builds_touch_dist();
    s
}

// MIXED family — deplete=50, distinct-pool. Generic transfer / sale baseline.

pub fn s18(u: PoolUniverse) -> ScenarioSpec {
    spec("s18", "mixed mid-typ — 50 callers, Pareto 80/20, complex deplete+receipt, fifo, depth 10",
        50, u, pareto(20, 80), Complexity::Complex, 50, MethodMix::AllFifo, 10)
}

pub fn s19(u: PoolUniverse) -> ScenarioSpec {
    spec("s19", "mixed high-vol — 200 callers, Pareto 80/20, complex deplete+receipt, fifo, depth 100",
        200, u, pareto(20, 80), Complexity::Complex, 50, MethodMix::AllFifo, 100)
}

pub fn s20(u: PoolUniverse) -> ScenarioSpec {
    spec("s20", "mixed long-tail — 50 callers, Pareto 90/10, complex deplete+receipt, fifo, depth 10",
        50, u, pareto(10, 90), Complexity::Complex, 50, MethodMix::AllFifo, 10)
}

pub fn s21(u: PoolUniverse) -> ScenarioSpec {
    spec("s21", "mixed balanced — 50 callers, Pareto 50/50, complex deplete+receipt, fifo, depth 10",
        50, u, pareto(50, 50), Complexity::Complex, 50, MethodMix::AllFifo, 10)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small_universe(n: usize) -> PoolUniverse {
        PoolUniverse {
            pool_ids: (1..=n as i64).collect(),
            inv_account: 1000,
            ap_account: 2000,
            variance_account: 3000,
        }
    }

    #[test]
    fn by_id_resolves_all_canned_scenarios() {
        let ids = [
            "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9",
            "s10", "s11", "s12", "s13",
            "s14", "s15", "s16", "s17",
            "s18", "s19", "s20", "s21",
        ];
        for id in ids {
            let s = by_id(id, small_universe(10_000)).expect("resolves");
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
    fn caller_counts_match_matrix() {
        let u = small_universe(10_000);
        assert_eq!(s1(u.clone()).callers, 10);
        assert_eq!(s2(u.clone()).callers, 200);
        assert_eq!(s3(u.clone()).callers, 10);
        assert_eq!(s4(u.clone()).callers, 200);
        assert_eq!(s5(u.clone()).callers, 1000);
        assert_eq!(s6(u.clone()).callers, 1000);
        assert_eq!(s7(u.clone()).callers, 1000);
        assert_eq!(s8(u.clone()).callers, 1000);
        assert_eq!(s9(u).callers, 1000);
    }

    #[test]
    fn s9_enables_multi_touch_and_s8_does_not() {
        let u = small_universe(10_000);
        // s8 (and every other canned scenario) is distinct-pool by default.
        assert_eq!(s8(u.clone()).workload.multi_touch_pct, 0);
        // s9 turns it on with a realistic mix (tail at 2-3, not worst case).
        let s9 = s9(u);
        assert_eq!(s9.workload.multi_touch_pct, 40);
        assert_eq!(s9.workload.touch_dist.spec_string(), "1:60,2:30,3:10");
        assert_eq!(s9.workload.touch_dist.max_touches(), 3);
        assert_eq!(s9.depth_hint, 1000);
    }

    #[test]
    fn deep_scenarios_advertise_depth_1000() {
        let u = small_universe(10_000);
        assert_eq!(s7(u.clone()).depth_hint, 1000);
        assert_eq!(s8(u).depth_hint, 1000);
    }

    #[test]
    fn depletion_scenarios_set_deplete_pct() {
        let u = small_universe(10_000);
        assert_eq!(s1(u.clone()).workload.deplete_pct, 0);
        assert_eq!(s5(u.clone()).workload.deplete_pct, 100);
        assert_eq!(s8(u).workload.deplete_pct, 50);
    }

    #[test]
    fn s6_stripe_partitions_universe() {
        let s = s6(small_universe(10_000));
        if let OverlapMode::Disjoint { stripe_size } = s.workload.overlap {
            assert_eq!(stripe_size, 10);
        } else {
            panic!("s6 should use Disjoint overlap");
        }
    }

    fn pareto_params(s: &ScenarioSpec) -> (f64, f64) {
        match s.workload.overlap {
            OverlapMode::Pareto { hot_pool_fraction, hot_traffic_fraction } => {
                (hot_pool_fraction, hot_traffic_fraction)
            }
            other => panic!("expected Pareto overlap, got {other:?}"),
        }
    }

    #[test]
    fn pareto_families_have_expected_shapes() {
        let u = small_universe(10_000);

        // RECEIPTS family: deplete=0, multi-touch off.
        for s in [s10(u.clone()), s11(u.clone()), s12(u.clone()), s13(u.clone())] {
            assert_eq!(s.workload.deplete_pct, 0, "{} should be receipts-only", s.id);
            assert_eq!(s.workload.multi_touch_pct, 0, "{} should be distinct-pool", s.id);
        }

        // BUILDS family: deplete=50, multi-touch 40% / 1:60,2:30,3:10.
        for s in [s14(u.clone()), s15(u.clone()), s16(u.clone()), s17(u.clone())] {
            assert_eq!(s.workload.deplete_pct, 50, "{} should be 50% deplete", s.id);
            assert_eq!(s.workload.multi_touch_pct, 40, "{} should be 40% multi-touch", s.id);
            assert_eq!(s.workload.touch_dist.spec_string(), "1:60,2:30,3:10");
        }

        // MIXED family: deplete=50, multi-touch off.
        for s in [s18(u.clone()), s19(u.clone()), s20(u.clone()), s21(u.clone())] {
            assert_eq!(s.workload.deplete_pct, 50, "{} should be 50% deplete", s.id);
            assert_eq!(s.workload.multi_touch_pct, 0, "{} should be distinct-pool", s.id);
        }
    }

    #[test]
    fn pareto_variants_match_overlap_and_caller_axes() {
        let u = small_universe(10_000);

        // Mid-typ: 50 callers, 80/20. Receipts depth 0, deplete families depth 10.
        for (s, expected_depth) in [
            (s10(u.clone()), 0),  // receipts
            (s14(u.clone()), 10), // builds
            (s18(u.clone()), 10), // mixed
        ] {
            assert_eq!(s.callers, 50, "mid-typ ({}) should have 50 callers", s.id);
            let (hp, ht) = pareto_params(&s);
            assert!((hp - 0.20).abs() < 1e-9, "{} hot_pool: {hp}", s.id);
            assert!((ht - 0.80).abs() < 1e-9, "{} hot_traffic: {ht}", s.id);
            assert_eq!(s.depth_hint, expected_depth, "{} depth", s.id);
        }

        // High-vol: 200 callers, 80/20, depth 100.
        for s in [s11(u.clone()), s15(u.clone()), s19(u.clone())] {
            assert_eq!(s.callers, 200, "high-vol ({}) should have 200 callers", s.id);
            assert_eq!(s.depth_hint, 100, "high-vol ({}) should be depth 100", s.id);
            let (hp, ht) = pareto_params(&s);
            assert!((hp - 0.20).abs() < 1e-9);
            assert!((ht - 0.80).abs() < 1e-9);
        }

        // Long-tail: 50 callers, 90/10. Receipts depth 0, deplete families depth 10.
        for (s, expected_depth) in [
            (s12(u.clone()), 0),  // receipts
            (s16(u.clone()), 10), // builds
            (s20(u.clone()), 10), // mixed
        ] {
            assert_eq!(s.callers, 50);
            let (hp, ht) = pareto_params(&s);
            assert!((hp - 0.10).abs() < 1e-9, "{} hot_pool: {hp}", s.id);
            assert!((ht - 0.90).abs() < 1e-9, "{} hot_traffic: {ht}", s.id);
            assert_eq!(s.depth_hint, expected_depth, "{} depth", s.id);
        }

        // Balanced: 50 callers, 50/50. Receipts depth 0, deplete families depth 10.
        for (s, expected_depth) in [
            (s13(u.clone()), 0),  // receipts
            (s17(u.clone()), 10), // builds
            (s21(u.clone()), 10), // mixed
        ] {
            assert_eq!(s.callers, 50);
            let (hp, ht) = pareto_params(&s);
            assert!((hp - 0.50).abs() < 1e-9);
            assert!((ht - 0.50).abs() < 1e-9);
            assert_eq!(s.depth_hint, expected_depth, "{} depth", s.id);
        }
    }
}
