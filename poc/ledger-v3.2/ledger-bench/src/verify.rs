//! The v3.2-native conservation sweep + strict-replay oracle equivalence —
//! the acct-0at4.5 / acct-0at4.7 survivors ported to alt-C semantics (the
//! frozen v3.1 binaries read a v3.1-shaped dump and check a provisional cost
//! plane FL no longer has; the invariants below are the same ideas against
//! this schema).
//!
//! Whole-run invariants (all methods):
//!   V1  qty conservation      aggregate qty == Σ trx_line.qty per pool
//!   V2  value reconciliation  value_sum == the hot path's commit-order fold
//!                             over recorded costs (with the exact-empty
//!                             flush) − the telescoped settlement deltas
//!                             Σ (auth_maxgen − observed) × qty − the swept
//!                             residue (zero-qty adjustment GL)
//!   V5  structural integrity  every trx has ≥1 line; FL physical lines post
//!                             no hot-path GL; wac physical lines post
//!                             exactly one; engine/sweep output
//!                             (cost_adjustment_line) posts exactly one
//!   V6  intent                acked submissions == recorded trx (no silent
//!                             drop, no phantom), when the driver supplies
//!                             its counts
//!   V7  method isolation      wac pools have no settlement state
//!
//! Settled-state invariants (`expect_settled`, meaningful only at quiesce —
//! authoritative costs are NOT prefix-stable under an unsettled backdated
//! tail, so mid-run comparison would be a false alarm):
//!   V3  oracle equivalence    per FL pool, an independent strict layer walk
//!                             in (posted_at, id) order (shares only
//!                             banker_div with the engine) matches every
//!                             depletion's max-generation authoritative cost
//!   V4  layer materialization persisted layers == the walk's open layers;
//!                             after a close sweep (`expect_swept`),
//!                             value_sum == Σ open-layer value exactly
//!
//! Also emits the provisional-vs-authoritative drift distribution per method
//! (the replay oracle's §2 variance table measured on the LIVE engine).

use ledger_core::banker_div;
use serde::Serialize;
use sqlx::PgPool;
use std::collections::HashMap;

#[derive(Debug, Default, Serialize)]
pub struct DriftStats {
    pub method: String,
    pub depletions: u64,
    /// mean(auth − observed), µ-units — bias, signed.
    pub mean_delta: f64,
    pub mean_abs_delta: f64,
    pub p99_abs_delta: i64,
    pub max_abs_delta: i64,
    /// mean|Δ| / mean(authoritative) — the oracle table's `rel`.
    pub rel_pct: f64,
    /// Depletions whose observed provisional cost was already exact.
    pub exact_pct: f64,
}

#[derive(Debug, Default, Serialize)]
pub struct VerifyReport {
    pub pools_checked: u64,
    pub fl_pools: u64,
    pub events: u64,
    pub depletions_settled: u64,
    /// Pools where the exact V2 identity is not offline-reconstructible (an
    /// exact-empty flush wiped engine aggregate corrections); V4 post-close
    /// still covers them exactly.
    pub v2_skipped_flush_wiped: u64,
    pub failures: Vec<String>,
    pub drift: Vec<DriftStats>,
}

impl VerifyReport {
    pub fn ok(&self) -> bool {
        self.failures.is_empty()
    }

    fn fail(&mut self, msg: String) {
        if self.failures.len() < 50 {
            self.failures.push(msg);
        }
    }
}

/// Driver-side ack counts for the V6 intent check.
#[derive(Debug, Clone, Copy, Default)]
pub struct AckCounts {
    pub direct_acked: u64,
    pub staging_done: u64,
}

/// Independent strict layer walk (shares only banker_div with the engine).
/// Events: (trx_line_id, signed qty, recorded unit_cost) in (posted_at, id)
/// order. Returns per-depletion authoritative costs and final open layers.
fn reference_walk(
    newest_first: bool,
    events: &[(i64, i64, i64)],
) -> (Vec<(i64, i64)>, Vec<(i64, i64, i64)>) {
    let mut layers: Vec<(i64, i64, i64)> = Vec::new(); // (layer_id, qty, cost), oldest first
    let mut costings = Vec::new();
    for &(id, qty, cost) in events {
        if qty > 0 {
            layers.push((id, qty, cost));
            continue;
        }
        let want = -qty;
        let mut remaining = want;
        let mut value: i128 = 0;
        while remaining > 0 && !layers.is_empty() {
            let idx = if newest_first { layers.len() - 1 } else { 0 };
            let take = remaining.min(layers[idx].1);
            value += take as i128 * layers[idx].2 as i128;
            layers[idx].1 -= take;
            remaining -= take;
            if layers[idx].1 == 0 {
                layers.remove(idx);
            }
        }
        value += remaining as i128 * cost as i128; // uncovered at observed
        costings.push((id, banker_div(value, want)));
    }
    (costings, layers)
}

/// The hot path's commit-order fold over recorded costs, including the FL/WAC
/// CTE exact-empty flush (qty hitting 0 flushes value_sum to 0). Also reports
/// whether any depletion flushed: a flush wipes whatever recalc value
/// corrections the engine had folded into the live aggregate by that point,
/// so the offline `fold − deltas` identity is not reconstructible for a
/// flushed pool that also has settlement deltas (the wiped amount depends on
/// the run-time interleaving of engine passes and hot-path commits, which the
/// stream does not record). The drift is bounded and self-heals: the close
/// sweep re-trues the aggregate to Σ open-layer value, absorbing the wiped
/// amount into the residue GL.
fn hot_fold(events_by_id: &[(i64, i64, i64)]) -> (i128, bool) {
    let mut qty: i64 = 0;
    let mut value: i128 = 0;
    let mut flushed = false;
    for &(_, q, c) in events_by_id {
        qty += q;
        if q > 0 {
            value += q as i128 * c as i128;
        } else if qty == 0 {
            value = 0;
            flushed = true;
        } else {
            value -= (-q) as i128 * c as i128;
        }
    }
    (value, flushed)
}

pub async fn run_verify(
    pool: &PgPool,
    expect_settled: bool,
    expect_swept: bool,
    acked: Option<AckCounts>,
) -> Result<VerifyReport, sqlx::Error> {
    let mut report = VerifyReport::default();

    let pools: Vec<(i64, String)> =
        sqlx::query_as("SELECT id, method::text FROM pool ORDER BY id").fetch_all(pool).await?;
    report.pools_checked = pools.len() as u64;

    let aggregates: HashMap<i64, (i64, i64)> = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT pool_id, qty, value_sum FROM pool_state WHERE layer_id = 0",
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|(p, q, v)| (p, (q, v)))
    .collect();

    // Physical lines only: the engine's cost_adjustment_line rows carry the
    // re-costed depletion's qty for audit but are valuation output, not
    // physical movement — the aggregate qty never saw them.
    let line_sums: HashMap<i64, i64> = sqlx::query_as::<_, (i64, i64)>(
        "SELECT pool_id, COALESCE(sum(qty), 0)::bigint FROM trx_line \
          WHERE line_type <> 'cost_adjustment_line' GROUP BY pool_id",
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect();

    // Physical events per pool in R-1 (posted_at, id) order.
    let mut events_r1: HashMap<i64, Vec<(i64, i64, i64)>> = HashMap::new();
    for (pid, id, qty, cost) in sqlx::query_as::<_, (i64, i64, i64, i64)>(
        "SELECT pool_id, id, qty, unit_cost FROM trx_line \
          WHERE line_type <> 'cost_adjustment_line' ORDER BY pool_id, posted_at, id",
    )
    .fetch_all(pool)
    .await?
    {
        report.events += 1;
        events_r1.entry(pid).or_default().push((id, qty, cost));
    }

    // Max-generation settlement per depletion.
    let mut settlements: HashMap<i64, Vec<(i64, i64)>> = HashMap::new();
    for (pid, dep_id, auth) in sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT tl.pool_id, cs.depletion_trx_line_id, cs.authoritative_unit_cost \
           FROM (SELECT DISTINCT ON (depletion_trx_line_id) \
                        depletion_trx_line_id, authoritative_unit_cost \
                   FROM cost_settlement \
                  ORDER BY depletion_trx_line_id, recalc_generation DESC) cs \
           JOIN trx_line tl ON tl.id = cs.depletion_trx_line_id",
    )
    .fetch_all(pool)
    .await?
    {
        report.depletions_settled += 1;
        settlements.entry(pid).or_default().push((dep_id, auth));
    }

    // Persisted open layers per pool.
    let mut layers_db: HashMap<i64, Vec<(i64, i64, i64)>> = HashMap::new();
    for (pid, lid, qty, cost) in sqlx::query_as::<_, (i64, i64, i64, i64)>(
        "SELECT pool_id, layer_id, qty, unit_cost FROM pool_state \
          WHERE layer_id > 0 ORDER BY pool_id, layer_id",
    )
    .fetch_all(pool)
    .await?
    {
        layers_db.entry(pid).or_default().push((lid, qty, cost));
    }

    // Signed swept residue per pool (zero-qty adjustment GL; positive = value
    // moved out of inventory into variance).
    let mut swept: HashMap<i64, i128> = HashMap::new();
    for (pid, amount, debit) in sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT tl.pool_id, pl.amount, pl.debit_account FROM posting_line pl \
           JOIN trx_line tl ON tl.id = pl.trx_line_id \
          WHERE tl.line_type = 'cost_adjustment_line' AND tl.qty = 0",
    )
    .fetch_all(pool)
    .await?
    {
        let signed = if debit == crate::universe::VAR_ACCT {
            amount as i128
        } else {
            -(amount as i128)
        };
        *swept.entry(pid).or_default() += signed;
    }

    let mut drift_by_method: HashMap<String, Vec<i64>> = HashMap::new();
    let mut auth_sums: HashMap<String, i128> = HashMap::new();

    for (pid, method) in &pools {
        let is_fl = method == "fifo" || method == "lifo";
        if is_fl {
            report.fl_pools += 1;
        }
        let events = events_r1.remove(pid).unwrap_or_default();
        let (agg_qty, agg_value) = aggregates.get(pid).copied().unwrap_or((0, 0));

        // V1 — qty conservation.
        let sum_qty = line_sums.get(pid).copied().unwrap_or(0);
        if agg_qty != sum_qty {
            report.fail(format!("V1 pool {pid}: aggregate qty {agg_qty} != Σ lines {sum_qty}"));
        }

        // V7 — method isolation.
        let pool_settlements = settlements.remove(pid).unwrap_or_default();
        if !is_fl && !pool_settlements.is_empty() {
            report.fail(format!("V7 pool {pid} ({method}): settlement state on a wac pool"));
            continue;
        }

        // V2 — value reconciliation. Skipped (and counted) when an exact-empty
        // flush coexists with settlement deltas: the flush wiped an
        // interleaving-dependent share of the engine's aggregate corrections
        // (see hot_fold docs); post-close V4 covers those pools exactly.
        let mut by_commit = events.clone();
        by_commit.sort_by_key(|(id, _, _)| *id);
        let (fold, flushed) = hot_fold(&by_commit);
        let observed_of: HashMap<i64, (i64, i64)> =
            events.iter().map(|&(id, q, c)| (id, (q, c))).collect();
        let deltas: i128 = pool_settlements
            .iter()
            .map(|&(dep_id, auth)| {
                let (q, observed) = observed_of.get(&dep_id).copied().unwrap_or((0, 0));
                (auth - observed) as i128 * (-q) as i128
            })
            .sum();
        let residue = swept.get(pid).copied().unwrap_or(0);
        if flushed && deltas != 0 {
            report.v2_skipped_flush_wiped += 1;
        } else if agg_value as i128 != fold - deltas - residue {
            report.fail(format!(
                "V2 pool {pid} ({method}): value_sum {agg_value} != fold {fold} − deltas \
                 {deltas} − swept {residue}"
            ));
        }

        if !is_fl || events.is_empty() {
            continue;
        }

        // V3/V4 + drift — settled-state invariants.
        let (ref_costings, ref_layers) = reference_walk(method == "lifo", &events);
        let settled_of: HashMap<i64, i64> = pool_settlements.iter().copied().collect();
        for &(dep_id, ref_auth) in &ref_costings {
            match settled_of.get(&dep_id) {
                Some(&auth) if expect_settled => {
                    if auth != ref_auth {
                        report.fail(format!(
                            "V3 pool {pid} ({method}) depletion {dep_id}: authoritative {auth} \
                             != reference {ref_auth}"
                        ));
                    }
                    let (_, observed) = observed_of[&dep_id];
                    drift_by_method.entry(method.clone()).or_default().push(auth - observed);
                    *auth_sums.entry(method.clone()).or_default() += auth as i128;
                }
                Some(_) => {} // not at quiesce: settlements exist but are not comparable
                None if expect_settled => {
                    report.fail(format!(
                        "V3 pool {pid} ({method}) depletion {dep_id}: no settlement at quiesce"
                    ));
                }
                None => {}
            }
        }
        if expect_settled {
            let mut db = layers_db.remove(pid).unwrap_or_default();
            let mut expected = ref_layers.clone();
            db.sort();
            expected.sort();
            if db != expected {
                report.fail(format!(
                    "V4 pool {pid} ({method}): persisted layers {db:?} != reference {expected:?}"
                ));
            }
            if expect_swept {
                let layer_value: i128 = expected.iter().map(|&(_, q, c)| q as i128 * c as i128).sum();
                if agg_value as i128 != layer_value {
                    report.fail(format!(
                        "V4 pool {pid} ({method}): post-sweep value_sum {agg_value} != Σ layer \
                         value {layer_value}"
                    ));
                }
            }
        }
    }

    // V5 — structural integrity (global).
    let orphan_trx: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM trx t WHERE NOT EXISTS \
           (SELECT 1 FROM trx_line tl WHERE tl.trx_id = t.id)",
    )
    .fetch_one(pool)
    .await?;
    if orphan_trx != 0 {
        report.fail(format!("V5: {orphan_trx} trx rows with no trx_line"));
    }
    let fl_posted: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM trx_line tl \
           JOIN pool p ON p.id = tl.pool_id AND p.method IN ('fifo', 'lifo') \
          WHERE tl.line_type <> 'cost_adjustment_line' \
            AND EXISTS (SELECT 1 FROM posting_line pl WHERE pl.trx_line_id = tl.id)",
    )
    .fetch_one(pool)
    .await?;
    if fl_posted != 0 {
        report.fail(format!("V5: {fl_posted} FL physical lines with hot-path GL (alt-C violation)"));
    }
    let wac_bad: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM trx_line tl \
           JOIN pool p ON p.id = tl.pool_id AND p.method = 'wac' \
          WHERE tl.line_type <> 'cost_adjustment_line' \
            AND (SELECT count(*) FROM posting_line pl WHERE pl.trx_line_id = tl.id) <> 1",
    )
    .fetch_one(pool)
    .await?;
    if wac_bad != 0 {
        report.fail(format!("V5: {wac_bad} wac physical lines without exactly one posting"));
    }
    let adj_bad: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM trx_line tl \
          WHERE tl.line_type = 'cost_adjustment_line' \
            AND (SELECT count(*) FROM posting_line pl WHERE pl.trx_line_id = tl.id) <> 1",
    )
    .fetch_one(pool)
    .await?;
    if adj_bad != 0 {
        report.fail(format!("V5: {adj_bad} cost_adjustment_lines without exactly one posting"));
    }

    // V6 — intent: acked == recorded.
    if let Some(acked) = acked {
        let trx_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM trx WHERE trx_type <> 'cost_adjustment'",
        )
        .fetch_one(pool)
        .await?;
        let expected = acked.direct_acked + acked.staging_done;
        if trx_count as u64 != expected {
            report.fail(format!(
                "V6: {trx_count} recorded trx != {expected} acked (direct {} + staging done {})",
                acked.direct_acked, acked.staging_done
            ));
        }
    }

    // Drift distribution per method (the oracle §2 table on the live engine).
    for (method, mut deltas) in drift_by_method {
        let n = deltas.len() as u64;
        if n == 0 {
            continue;
        }
        let mean = deltas.iter().map(|&d| d as f64).sum::<f64>() / n as f64;
        let mean_abs = deltas.iter().map(|&d| (d as f64).abs()).sum::<f64>() / n as f64;
        deltas.sort_by_key(|d| d.abs());
        let p99 = deltas[((n as f64 * 0.99) as usize).min(deltas.len() - 1)].abs();
        let max = deltas.last().map(|d| d.abs()).unwrap_or(0);
        let exact = deltas.iter().filter(|&&d| d == 0).count() as f64 / n as f64 * 100.0;
        let mean_auth = auth_sums.get(&method).copied().unwrap_or(0) as f64 / n as f64;
        report.drift.push(DriftStats {
            method,
            depletions: n,
            mean_delta: mean,
            mean_abs_delta: mean_abs,
            p99_abs_delta: p99,
            max_abs_delta: max,
            rel_pct: if mean_auth > 0.0 { mean_abs / mean_auth * 100.0 } else { 0.0 },
            exact_pct: exact,
        });
    }
    report.drift.sort_by(|a, b| a.method.cmp(&b.method));

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_walk_prices_fifo_and_lifo() {
        // 10 @ 100, 5 @ 200; deplete 12 → fifo consumes 10@100 + 2@200,
        // lifo consumes 5@200 + 7@100.
        let events = [(1, 10, 100), (2, 5, 200), (3, -12, 0)];
        let (fifo, fifo_layers) = reference_walk(false, &events);
        assert_eq!(fifo, vec![(3, banker_div(10 * 100 + 2 * 200, 12))]);
        assert_eq!(fifo_layers, vec![(2, 3, 200)]);
        let (lifo, lifo_layers) = reference_walk(true, &events);
        assert_eq!(lifo, vec![(3, banker_div(5 * 200 + 7 * 100, 12))]);
        assert_eq!(lifo_layers, vec![(1, 3, 100)]);
    }

    #[test]
    fn reference_walk_uncovered_at_observed() {
        let events = [(1, 5, 100), (2, -8, 0)];
        let (costs, layers) = reference_walk(false, &events);
        // 5 covered at 100, 3 uncovered at observed 0.
        assert_eq!(costs, vec![(2, banker_div(500, 8))]);
        assert!(layers.is_empty());
    }

    #[test]
    fn hot_fold_flushes_on_exact_empty() {
        // Receipt 10@100 (=1000), deplete 10 at observed 99 (990) → qty 0
        // flushes value to 0 (not 10) and flags the wipe hazard.
        let events = [(1, 10, 100), (2, -10, 99)];
        assert_eq!(hot_fold(&events), (0, true));
        // Without the flush: 3 remaining at cost, no hazard.
        let events = [(1, 10, 100), (2, -7, 100)];
        assert_eq!(hot_fold(&events), (300, false));
    }
}
