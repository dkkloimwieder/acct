//! M10.1 (acct-4d4n.23) — C3 determinism (spec §4.1).
//!
//! "For a fixed event stream and seed (no kills/cancels), the rows
//! written to cost tables are identical across runs."
//!
//! Methodology: drive a deterministic sequence of 200 applies across
//! 5 pre-seeded SKUs using FIFO (uses cost_layers + cost_depletions —
//! the cost method with the most-stateful row trail). Reset state.
//! Re-run the identical sequence. Compare cost-meaningful aggregates
//! per (issue_id, method_used) across the two runs.
//!
//! What we assert identical (the "rows are identical" interpretation
//! that's load-bearing for correctness):
//!   - same row count per table
//!   - same set of (issue_id, method_used) keys
//!   - same SUM(qty) per (issue_id, method_used) in depletions
//!   - same SUM(unit_cost * qty) per (issue_id, method_used) in depletions
//!   - same set of distinct (sku_id, location_id) pairs in layers
//!
//! What we explicitly DON'T compare: committer_tx_id, layer_id,
//! depletion_id, posted_at, *_seq. Those reset to 0 / monotonic-from-0
//! per cluster epoch and may shift if shard election runs differently
//! between runs — that's incidental to the C3 contract, not a violation.
//!
//! Run via:
//!   cargo test --release --test c3_determinism_t1 c3_determinism \
//!     --features pg18 --no-default-features -- --ignored --nocapture

#![cfg(test)]

mod common;

use common::m9_runner::{connect_pool, reset_state, LOCATION_ID};
use sqlx::PgPool;
use std::sync::Arc;

const N_SKUS: i64 = 5;
const N_EVENTS: i64 = 200;

#[derive(Debug, Clone, PartialEq)]
struct CostSnapshot {
    layer_count: i64,
    depletion_count: i64,
    consumption_count: i64,
    /// Per (issue_id, method_used) — SUM(qty) for depletions.
    depletion_qty_per_issue: Vec<(i64, String, i64)>,
    /// Per (issue_id, method_used) — SUM(qty * unit_cost) for depletions.
    depletion_cost_per_issue: Vec<(i64, String, i64)>,
    /// Distinct (sku, location) pairs in layers.
    layer_sku_locs: Vec<(i64, i64)>,
}

async fn pre_seed_fifo(pool: &PgPool) -> Result<(), sqlx::Error> {
    // Five SKUs with 5 layers each, deterministic unit_costs.
    for sku in 1_i64..=N_SKUS {
        for layer in 0..5_i64 {
            // qty=10000 so 200 events × max 50/event never drains.
            sqlx::query(
                "INSERT INTO poc_cost_layers (sku_id, location_id, qty, unit_cost, source_kind) \
                 VALUES ($1, $2, 10000, $3, 'seed')",
            )
            .bind(sku)
            .bind(LOCATION_ID)
            .bind(100_i64 + sku * 10 + layer) // deterministic unit_cost per (sku, layer)
            .execute(pool)
            .await?;
        }
    }
    Ok(())
}

async fn drive_deterministic_sequence(pool: &PgPool) -> Result<(), sqlx::Error> {
    // Deterministic event stream: issue_id iterates 1..=200; sku rotates
    // 1..=5; qty deterministic from issue_id; method = 'fifo' for all.
    for i in 1_i64..=N_EVENTS {
        let sku = ((i - 1) % N_SKUS) + 1;
        let qty = 1_i64 + (i % 50); // 1..=50 deterministic
        sqlx::query("SELECT poc_ledger_apply($1, $2, $3, $4, 'fifo')")
            .bind(sku)
            .bind(LOCATION_ID)
            .bind(qty)
            .bind(i) // issue_id = i, unique per call
            .execute(pool)
            .await?;
    }
    Ok(())
}

async fn snapshot(pool: &PgPool) -> CostSnapshot {
    let layer_count: (i64,) = sqlx::query_as("SELECT COUNT(*)::BIGINT FROM poc_cost_layers")
        .fetch_one(pool)
        .await
        .unwrap();
    let depletion_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*)::BIGINT FROM poc_cost_depletions")
            .fetch_one(pool)
            .await
            .unwrap();
    let consumption_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*)::BIGINT FROM poc_cost_consumptions")
            .fetch_one(pool)
            .await
            .unwrap();
    let qty_rows: Vec<(i64, String, i64)> = sqlx::query_as(
        "SELECT issue_id, method_used::TEXT, COALESCE(SUM(qty), 0)::BIGINT \
           FROM poc_cost_depletions \
          GROUP BY issue_id, method_used \
          ORDER BY issue_id, method_used",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    let cost_rows: Vec<(i64, String, i64)> = sqlx::query_as(
        "SELECT issue_id, method_used::TEXT, COALESCE(SUM(qty * unit_cost), 0)::BIGINT \
           FROM poc_cost_depletions \
          GROUP BY issue_id, method_used \
          ORDER BY issue_id, method_used",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    let sku_loc_rows: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT DISTINCT sku_id, location_id \
           FROM poc_cost_layers \
          ORDER BY sku_id, location_id",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    CostSnapshot {
        layer_count: layer_count.0,
        depletion_count: depletion_count.0,
        consumption_count: consumption_count.0,
        depletion_qty_per_issue: qty_rows,
        depletion_cost_per_issue: cost_rows,
        layer_sku_locs: sku_loc_rows,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn c3_determinism() {
    let pool = Arc::new(connect_pool(4).await);

    // Run 1
    eprintln!("==> C3 determinism run 1");
    reset_state(&pool).await.expect("reset_state");
    pre_seed_fifo(&pool).await.expect("pre_seed run 1");
    drive_deterministic_sequence(&pool)
        .await
        .expect("drive run 1");
    let snap1 = snapshot(&pool).await;
    eprintln!(
        "    layers={} depletions={} consumptions={}",
        snap1.layer_count, snap1.depletion_count, snap1.consumption_count
    );

    // Run 2 — identical event stream against identical fresh state.
    eprintln!("==> C3 determinism run 2");
    reset_state(&pool).await.expect("reset_state");
    pre_seed_fifo(&pool).await.expect("pre_seed run 2");
    drive_deterministic_sequence(&pool)
        .await
        .expect("drive run 2");
    let snap2 = snapshot(&pool).await;
    eprintln!(
        "    layers={} depletions={} consumptions={}",
        snap2.layer_count, snap2.depletion_count, snap2.consumption_count
    );

    // Assertions
    assert_eq!(
        snap1.layer_count, snap2.layer_count,
        "C3 FAIL: cost_layers row count differs ({} vs {})",
        snap1.layer_count, snap2.layer_count
    );
    assert_eq!(
        snap1.depletion_count, snap2.depletion_count,
        "C3 FAIL: cost_depletions row count differs ({} vs {})",
        snap1.depletion_count, snap2.depletion_count
    );
    assert_eq!(
        snap1.consumption_count, snap2.consumption_count,
        "C3 FAIL: cost_consumptions row count differs"
    );
    assert_eq!(
        snap1.depletion_qty_per_issue, snap2.depletion_qty_per_issue,
        "C3 FAIL: SUM(qty) per (issue_id, method) diverged"
    );
    assert_eq!(
        snap1.depletion_cost_per_issue, snap2.depletion_cost_per_issue,
        "C3 FAIL: SUM(qty*unit_cost) per (issue_id, method) diverged"
    );
    assert_eq!(
        snap1.layer_sku_locs, snap2.layer_sku_locs,
        "C3 FAIL: distinct (sku, location) layer set diverged"
    );

    // Persist outcome for BENCHMARK_RESULTS.md.
    let md = format!(
        "# M10.1 C3 determinism (acct-4d4n.23)\n\n\
         Two identical sequences of {n} FIFO applies (issue_id 1..={n}, \
         sku rotates 1..={s}, qty deterministic 1..=50, method='fifo') \
         driven against reset+pre-seeded state. Per-(issue_id, method_used) \
         SUM(qty) and SUM(qty*unit_cost) compared row-by-row. Row counts \
         compared per cost table.\n\n\
         | metric | run 1 | run 2 | match |\n\
         |---|---|---|---|\n\
         | cost_layers rows | {l1} | {l2} | {lm} |\n\
         | cost_depletions rows | {d1} | {d2} | {dm} |\n\
         | cost_consumptions rows | {c1} | {c2} | {cm} |\n\
         | distinct depletion (issue_id, method) keys | {k1} | {k2} | {km} |\n\
         | SUM(qty) per key (compared row-by-row) | — | — | {qm} |\n\
         | SUM(qty*unit_cost) per key (compared row-by-row) | — | — | {sm} |\n\n\
         **C3 verdict: PASS** — every measured aggregate identical across runs.\n\n\
         What was NOT compared: committer_tx_id, layer_id, depletion_id, \
         posted_at, *_seq. These reset per cluster epoch and may shift \
         under non-deterministic shard-election timing — incidental to the \
         C3 contract.\n",
        n = N_EVENTS,
        s = N_SKUS,
        l1 = snap1.layer_count,
        l2 = snap2.layer_count,
        lm = if snap1.layer_count == snap2.layer_count { "✓" } else { "✗" },
        d1 = snap1.depletion_count,
        d2 = snap2.depletion_count,
        dm = if snap1.depletion_count == snap2.depletion_count { "✓" } else { "✗" },
        c1 = snap1.consumption_count,
        c2 = snap2.consumption_count,
        cm = if snap1.consumption_count == snap2.consumption_count { "✓" } else { "✗" },
        k1 = snap1.depletion_qty_per_issue.len(),
        k2 = snap2.depletion_qty_per_issue.len(),
        km = if snap1.depletion_qty_per_issue.len() == snap2.depletion_qty_per_issue.len() { "✓" } else { "✗" },
        qm = if snap1.depletion_qty_per_issue == snap2.depletion_qty_per_issue { "✓" } else { "✗" },
        sm = if snap1.depletion_cost_per_issue == snap2.depletion_cost_per_issue { "✓" } else { "✗" },
    );
    std::fs::write("bench/results-c3-determinism.md", md).expect("write c3 md");
    println!("==> C3 determinism PASS — wrote bench/results-c3-determinism.md");
}
