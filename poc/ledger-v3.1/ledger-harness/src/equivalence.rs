//! Cross-flavor equivalence check (design-v3.1 §9.4 / §11.1, P4 acct-2ttr.8).
//!
//! Drives `ledger_submit_trx_c` (direct) and `ledger_enqueue_trx_c` (routed)
//! against an IDENTICAL, deterministically-generated input sequence and diffs
//! the resulting `pool_state` aggregate (`layer_id = 0`):
//!
//!   - aggregate **qty** MUST be identical across flavors. Net qty per pool is
//!     the order-independent sum of signed line qtys; if both flavors apply
//!     every submission exactly once, they must agree. A mismatch means a
//!     submission was dropped, duplicated, or misapplied — a real bug.
//!   - aggregate **unit_cost** MAY differ. Routed batches submissions across
//!     callers and may process them in a different within-batch order than the
//!     caller-serial direct flavor; the running-average provisional cost is
//!     order-sensitive. This divergence is by design (recalc/close, deferred,
//!     would converge both to identical authoritative costs).
//!
//! The universe is deep-seeded so depletions always have headroom and neither
//! flavor drops a submission for InsufficientInventory (which would itself make
//! qty diverge — correctly, but it would muddy the cross-flavor signal).

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::SeedableRng;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use crate::cli::MethodMix;
use crate::driver_common::build_lines_json;
use crate::pool_universe;
use crate::scenarios;
use crate::seed;
use crate::workload::{LineParam, Workload};

pub struct EquivalenceOptions {
    pub dsn: String,
    pub scenario: String,
    pub submissions_per_caller: usize,
    pub callers: usize,
    pub method_mix: MethodMix,
    pub depth: usize,
}

/// Small fixed universe for the deterministic replay — keeps the run quick and
/// makes pool overlap dense enough to exercise routed batching.
const EQUIV_POOLS: usize = 20;
const TRX_TYPE: &str = "po_receipt";
const POSTED_AT: &str = "2026-05-25T12:00:00+00:00";

struct Submission {
    source_id: i64,
    lines: Vec<LineParam>,
}

pub async fn run(opts: EquivalenceOptions) -> Result<(), String> {
    let pool = PgPoolOptions::new()
        .max_connections(16)
        .acquire_timeout(Duration::from_secs(15))
        .connect(&opts.dsn)
        .await
        .map_err(|e| format!("connect: {e}"))?;

    // ── Build the deterministic submission sequence from the scenario's
    // workload shape on the small equivalence universe. ──
    let universe = pool_universe::PoolUniverse {
        pool_ids: (1..=EQUIV_POOLS as i64).collect(),
        inv_account: pool_universe::INV_ACCOUNT,
        ap_account: pool_universe::AP_ACCOUNT,
        variance_account: pool_universe::VARIANCE_ACCOUNT,
    };
    let spec = scenarios::by_id(&opts.scenario, universe.clone())
        .ok_or_else(|| format!("unknown scenario '{}' (try s1..s8)", opts.scenario))?;
    let submissions = generate(&spec.workload, opts.callers, opts.submissions_per_caller);
    eprintln!(
        "equivalence: scenario {} — {} submissions ({} callers × {}), method_mix={:?}, depth={}",
        spec.id,
        submissions.len(),
        opts.callers,
        opts.submissions_per_caller,
        opts.method_mix,
        opts.depth
    );

    // ── Flavor 1: direct (equivalence owns the DB; reset any prior fixture) ──
    pool_universe::reset_ledger_tables(&pool)
        .await
        .map_err(|e| format!("reset before direct: {e}"))?;
    setup_universe(&pool, opts.method_mix, opts.depth).await?;
    apply_direct(&pool, &submissions).await?;
    let direct_qty = snapshot_agg(&pool, "qty").await?;
    let direct_cost = snapshot_agg(&pool, "unit_cost").await?;

    // ── Flavor 2: routed (fresh DB, identical input) ──
    pool_universe::reset_ledger_tables(&pool)
        .await
        .map_err(|e| format!("reset before routed: {e}"))?;
    setup_universe(&pool, opts.method_mix, opts.depth).await?;
    apply_routed(&pool, &submissions, Duration::from_secs(30)).await?;
    let routed_qty = snapshot_agg(&pool, "qty").await?;
    let routed_cost = snapshot_agg(&pool, "unit_cost").await?;

    // ── Diff ──
    let mut qty_mismatches: Vec<(i64, i64, i64)> = Vec::new();
    let mut all_pools: Vec<i64> = direct_qty.keys().copied().collect();
    for p in routed_qty.keys() {
        if !direct_qty.contains_key(p) {
            all_pools.push(*p);
        }
    }
    all_pools.sort_unstable();
    all_pools.dedup();
    for p in &all_pools {
        let d = direct_qty.get(p).copied().unwrap_or(0);
        let r = routed_qty.get(p).copied().unwrap_or(0);
        if d != r {
            qty_mismatches.push((*p, d, r));
        }
    }
    let cost_divergences = all_pools
        .iter()
        .filter(|p| direct_cost.get(p).copied() != routed_cost.get(p).copied())
        .count();

    println!(
        "{{\"scenario\":\"{}\",\"pools\":{},\"submissions\":{},\"qty_mismatches\":{},\
        \"unit_cost_divergences\":{},\"verdict\":\"{}\"}}",
        spec.id,
        all_pools.len(),
        submissions.len(),
        qty_mismatches.len(),
        cost_divergences,
        if qty_mismatches.is_empty() { "PASS" } else { "FAIL" }
    );

    if qty_mismatches.is_empty() {
        eprintln!(
            "equivalence PASS: aggregate qty identical across flavors on all {} pools; \
             {} pools show expected provisional unit_cost divergence (§11.1).",
            all_pools.len(),
            cost_divergences
        );
        Ok(())
    } else {
        for (p, d, r) in qty_mismatches.iter().take(20) {
            eprintln!("  pool {p}: direct qty {d} != routed qty {r}");
        }
        Err(format!(
            "equivalence FAIL: {} pool(s) have mismatched aggregate qty",
            qty_mismatches.len()
        ))
    }
}

/// Deterministic submission list: one RNG per caller (seeded as the drivers do),
/// source_id = caller_base + tick. Generated once; replayed against both flavors.
fn generate(workload: &Workload, callers: usize, per_caller: usize) -> Vec<Submission> {
    let mut out = Vec::with_capacity(callers * per_caller);
    for caller_id in 0..callers {
        let mut rng = StdRng::seed_from_u64(0xDEAD_BEEF_u64.wrapping_add(caller_id as u64));
        let caller_base = (caller_id as i64) * 1_000_000;
        for tick in 0..per_caller {
            out.push(Submission {
                source_id: caller_base + tick as i64,
                lines: workload.next_lines(&mut rng, caller_id),
            });
        }
    }
    out
}

async fn setup_universe(pool: &PgPool, mix: MethodMix, depth: usize) -> Result<(), String> {
    let u = pool_universe::seed(pool, EQUIV_POOLS, EQUIV_POOLS, 1, mix)
        .await
        .map_err(|e| format!("seed universe: {e}"))?;
    seed::deepen(pool, &u.pool_ids, depth.max(1))
        .await
        .map_err(|e| format!("deepen universe: {e}"))?;
    Ok(())
}

async fn apply_direct(pool: &PgPool, subs: &[Submission]) -> Result<(), String> {
    for s in subs {
        let lines_json = build_lines_json(&s.lines);
        sqlx::query("SELECT ledger_submit_trx_c($1, $2, $3, $4::jsonb)")
            .bind(TRX_TYPE)
            .bind(s.source_id)
            .bind(POSTED_AT)
            .bind(&lines_json)
            .execute(pool)
            .await
            .map_err(|e| format!("direct submit source_id={}: {e}", s.source_id))?;
    }
    Ok(())
}

async fn apply_routed(pool: &PgPool, subs: &[Submission], drain: Duration) -> Result<(), String> {
    for s in subs {
        let lines_json = build_lines_json(&s.lines);
        sqlx::query("SELECT ledger_enqueue_trx_c($1, $2, $3, $4::jsonb)")
            .bind(TRX_TYPE)
            .bind(s.source_id)
            .bind(POSTED_AT)
            .bind(&lines_json)
            .execute(pool)
            .await
            .map_err(|e| format!("routed enqueue source_id={}: {e}", s.source_id))?;
    }
    // Wait for the committer to materialize all enqueued submissions: poll the
    // trx count until it stops advancing (3 stable reads) or the deadline hits.
    let cap = Instant::now() + drain;
    let mut last: i64 = -1;
    let mut stable = 0u32;
    while Instant::now() < cap {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM trx")
            .fetch_one(pool)
            .await
            .map_err(|e| format!("routed drain poll: {e}"))?;
        if n == last {
            stable += 1;
            if stable >= 3 {
                break;
            }
        } else {
            stable = 0;
            last = n;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Ok(())
}

/// Read the aggregate column (`qty` or `unit_cost`) per pool from layer_id = 0.
async fn snapshot_agg(pool: &PgPool, column: &str) -> Result<BTreeMap<i64, i64>, String> {
    let sql = format!("SELECT pool_id, {column} FROM pool_state WHERE layer_id = 0 ORDER BY pool_id");
    let rows: Vec<(i64, i64)> = sqlx::query_as(&sql)
        .fetch_all(pool)
        .await
        .map_err(|e| format!("snapshot {column}: {e}"))?;
    Ok(rows.into_iter().collect())
}
