//! acct-du2.16 — property tests for close_period exercising the three
//! close hooks (wac_periodic_close_hook, wac_retroactive_close_hook,
//! cost_adjust_retroactive_hook).
//!
//! Per scenario: fresh wac_periodic + wac_retroactive SKUs at a fresh
//! location, pre-seeded with non-empty pools. Random sequence of adjust
//! IN (asserted unit cost) and adjust OUT (depletion at running avg —
//! flags posting_lines_provisional). Random retroactive cost-adjust queue
//! rows against the same open period. Then close_period.
//!
//! Post-close assertions:
//!   - close_period succeeded
//!   - all posting_lines_provisional rows targeting this period have
//!     finalized_at NOT NULL (no orphan provisional flags)
//!   - all inventory_cost_adjustments_retroactive rows targeting this
//!     period have flushed_at NOT NULL
//!   - 7 invariants hold post-close (assert_invariants_hold)
//!
//! Regression net for the close hooks' topological replay logic
//! (acct-smn / acct-rso) and for sibling shapes that might leave
//! provisional rows un-finalized or post negative-balance variances.
//!
//! ≥100 scenarios via PROPTEST_CASES env var; --test-threads=1 required.

mod common;

use common::*;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use sqlx::PgPool;

const TEST_PROPTEST_CASES_DEFAULT: u32 = 100;

#[derive(Debug, Clone, Copy)]
enum SkuKind {
    WacPeriodic,
    WacRetroactive,
}

impl SkuKind {
    fn cost_method(&self) -> &'static str {
        match self {
            SkuKind::WacPeriodic => "wac_periodic",
            SkuKind::WacRetroactive => "wac_retroactive",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Op {
    AdjustIn {
        sku: SkuKind,
        qty: i64,
        unit_cost: i64,
        bd_offset: u32,
    },
    AdjustOut {
        sku: SkuKind,
        qty: i64,
        bd_offset: u32,
    },
    QueueRetro {
        sku: SkuKind,
        target_avg: i64,
        bd_offset: u32,
    },
}

fn arb_sku() -> impl Strategy<Value = SkuKind> {
    prop_oneof![Just(SkuKind::WacPeriodic), Just(SkuKind::WacRetroactive)]
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        // 60% adjust IN
        3 => (arb_sku(), 5i64..=50, 5i64..=20, 1u32..=25)
            .prop_map(|(s, q, uc, b)| Op::AdjustIn { sku: s, qty: q, unit_cost: uc, bd_offset: b }),
        // 30% adjust OUT
        2 => (arb_sku(), 5i64..=30, 1u32..=25)
            .prop_map(|(s, q, b)| Op::AdjustOut { sku: s, qty: q, bd_offset: b }),
        // 10% queue retroactive
        1 => (arb_sku(), 1i64..=25, 1u32..=25)
            .prop_map(|(s, t, b)| Op::QueueRetro { sku: s, target_avg: t, bd_offset: b }),
    ]
}

fn arb_op_seq() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(arb_op(), 1..=8)
}

#[derive(Default)]
struct Pool {
    qty: i64,
    value: i64,
}

#[tokio::test(flavor = "current_thread")]
async fn property_period_close_invariants_hold() {
    let pool = connect_test_db().await;

    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(TEST_PROPTEST_CASES_DEFAULT);

    let mut runner = proptest::test_runner::TestRunner::default();
    let strategy = arb_op_seq();

    for case_idx in 0..cases {
        reset_to_fixture(&pool).await;

        let label = format!("close#{case_idx}");
        let pid = period_id(&pool, "2026-04").await;

        // Set up two SKUs (one per cost_method) at a single fresh
        // location, each with stock_available + inv_value_fg pre-seeded
        // generously. fg class chosen because both close hooks support
        // it (wip class still raises P0006 for these methods on direct
        // adjustment).
        let per_sku = fresh_sku(
            &pool,
            &format!("PROP-PER-{case_idx}"),
            SkuKind::WacPeriodic.cost_method(),
        )
        .await;
        let ret_sku = fresh_sku(
            &pool,
            &format!("PROP-RET-{case_idx}"),
            SkuKind::WacRetroactive.cost_method(),
        )
        .await;
        let loc = fresh_location(&pool, &format!("PROP-LOC-{case_idx}")).await;

        let _ = open_account(
            &pool, "stock_available", "qty", None,
            Some(&per_sku), Some(&loc), None, "debit",
        ).await;
        let _ = open_account(
            &pool, "inv_value_fg", "value", Some("USD"),
            Some(&per_sku), Some(&loc), None, "debit",
        ).await;
        let _ = open_account(
            &pool, "stock_available", "qty", None,
            Some(&ret_sku), Some(&loc), None, "debit",
        ).await;
        let _ = open_account(
            &pool, "inv_value_fg", "value", Some("USD"),
            Some(&ret_sku), Some(&loc), None, "debit",
        ).await;

        // Pre-seed both pools: 200 qty @ $10 = $2000. Done before any
        // random op so depletions never hit empty-pool P0010 / P0011 at
        // the start, AND so wac_periodic_close_hook always has at least
        // one in-period receipt (P0020 'no_receipts' otherwise).
        // Pre-seed via adjust-IN inside the period so the receipt
        // counts toward the period avg.
        adjust(&pool, &per_sku, &loc, 200, Some(10), "2026-04-02")
            .await
            .expect("seed per_sku");
        adjust(&pool, &ret_sku, &loc, 200, Some(10), "2026-04-02")
            .await
            .expect("seed ret_sku");

        // Track approximate pool qty per SKU so depletions don't run
        // the pool empty (raises P0010). Seed = 200 each.
        let mut per_pool = Pool { qty: 200, value: 2000 };
        let mut ret_pool = Pool { qty: 200, value: 2000 };

        let tree = strategy.new_tree(&mut runner).expect("strategy.new_tree");
        let ops: Vec<Op> = tree.current();

        for (i, op) in ops.iter().enumerate() {
            let op_label = format!("{label}/op{i}");
            let bd = bd_in_april(*op_bd_offset(op));

            let res = match *op {
                Op::AdjustIn { sku, qty, unit_cost, .. } => {
                    let r = adjust(
                        &pool,
                        match sku {
                            SkuKind::WacPeriodic => &per_sku,
                            SkuKind::WacRetroactive => &ret_sku,
                        },
                        &loc,
                        qty,
                        Some(unit_cost),
                        &bd,
                    )
                    .await;
                    if r.is_ok() {
                        let p = match sku {
                            SkuKind::WacPeriodic => &mut per_pool,
                            SkuKind::WacRetroactive => &mut ret_pool,
                        };
                        p.qty += qty;
                        p.value += qty * unit_cost;
                    }
                    r.map(|_| ())
                }
                Op::AdjustOut { sku, qty, .. } => {
                    let p = match sku {
                        SkuKind::WacPeriodic => &per_pool,
                        SkuKind::WacRetroactive => &ret_pool,
                    };
                    // Skip if depletion would leave qty <= 0 (pool would
                    // empty mid-period and subsequent ops on it would
                    // fail P0010, which is fine — but the depletion
                    // function ALSO raises P0010 when v_qty_balance <= 0
                    // pre-deplete, so depleting the LAST of the pool is
                    // allowed but going below 0 is not).
                    if qty >= p.qty {
                        continue;
                    }
                    let r = adjust(
                        &pool,
                        match sku {
                            SkuKind::WacPeriodic => &per_sku,
                            SkuKind::WacRetroactive => &ret_sku,
                        },
                        &loc,
                        -qty,
                        None,
                        &bd,
                    )
                    .await;
                    if r.is_ok() {
                        let avg = if p.qty > 0 { p.value / p.qty } else { 0 };
                        let pm = match sku {
                            SkuKind::WacPeriodic => &mut per_pool,
                            SkuKind::WacRetroactive => &mut ret_pool,
                        };
                        pm.qty -= qty;
                        pm.value -= qty * avg;
                    }
                    r.map(|_| ())
                }
                Op::QueueRetro { sku, target_avg, .. } => {
                    queue_retro(
                        &pool,
                        match sku {
                            SkuKind::WacPeriodic => &per_sku,
                            SkuKind::WacRetroactive => &ret_sku,
                        },
                        &loc,
                        "fg",
                        target_avg,
                        pid,
                        &bd,
                    )
                    .await
                    .map(|_| ())
                }
            };

            if res.is_err() {
                continue;
            }

            assert_invariants_hold(&pool, &op_label).await;
        }

        // Close the period. Always pass force_provisional=TRUE so we
        // don't trip P0020 (no in-period receipts on a SKU). Pre-seed
        // already covers the common case but force keeps random
        // ordering shifts safe.
        let actor = fresh_uuid(&pool).await;
        let close_res: sqlx::Result<serde_json::Value> = sqlx::query_scalar(
            "SELECT close_period($1, $2::UUID, TRUE, FALSE)",
        )
        .bind(pid)
        .bind(&actor)
        .fetch_one(&pool)
        .await;

        let summary = close_res.unwrap_or_else(|e| {
            panic!("[{label}] close_period failed: {e}")
        });

        // (a) all posting_lines_provisional rows for this period are
        // finalized_at NOT NULL (no orphans).
        let unfinalized: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM posting_lines_provisional WHERE period_id = $1 AND finalized_at IS NULL",
        )
        .bind(pid)
        .fetch_one(&pool)
        .await
        .expect("unfinalized count");
        assert_eq!(
            unfinalized, 0,
            "[{label}] {unfinalized} posting_lines_provisional rows un-finalized after close \
             (summary={summary})",
        );

        // (b) all inventory_cost_adjustments_retroactive rows for this
        // period are finalized_at NOT NULL.
        let unfinalized_retro: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM inventory_cost_adjustments_retroactive
              WHERE target_period_id = $1 AND finalized_at IS NULL",
        )
        .bind(pid)
        .fetch_one(&pool)
        .await
        .expect("unfinalized retro count");
        assert_eq!(
            unfinalized_retro, 0,
            "[{label}] {unfinalized_retro} retro adjustments un-finalized after close",
        );

        // (c) standard 7 invariants hold post-close.
        assert_invariants_hold(&pool, &format!("{label}/post-close")).await;
    }
}

fn op_bd_offset(op: &Op) -> &u32 {
    match op {
        Op::AdjustIn { bd_offset, .. } => bd_offset,
        Op::AdjustOut { bd_offset, .. } => bd_offset,
        Op::QueueRetro { bd_offset, .. } => bd_offset,
    }
}

fn bd_in_april(offset: u32) -> String {
    let day = (offset.min(28).max(2)) as u32;
    format!("2026-04-{day:02}")
}

// ============================================================
// Local scaffolding
// ============================================================

async fn fresh_sku(pool: &PgPool, code: &str, cost_method: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ($1, 'EA', $2::cost_method) RETURNING id::text",
    )
    .bind(code)
    .bind(cost_method)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert sku {code}: {e}"))
}

async fn fresh_location(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO locations (code, name) VALUES ($1, $2) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Loc {code}"))
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert location {code}: {e}"))
}

#[allow(clippy::too_many_arguments)]
async fn open_account(
    pool: &PgPool,
    kind: &str,
    ledger_kind: &str,
    currency: Option<&str>,
    sku_id: Option<&str>,
    loc_id: Option<&str>,
    routing_op: Option<i32>,
    normal_side: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts
            (kind, ledger_kind, currency, sku_id, location_id, routing_op, normal_side)
         VALUES ($1::account_kind, $2::ledger_kind, $3, $4::UUID, $5::UUID, $6, $7::balance_direction)
         RETURNING id",
    )
    .bind(kind)
    .bind(ledger_kind)
    .bind(currency)
    .bind(sku_id)
    .bind(loc_id)
    .bind(routing_op)
    .bind(normal_side)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("open {kind}: {e}"))
}

async fn period_id(pool: &PgPool, code: &str) -> i64 {
    sqlx::query_scalar("SELECT id FROM periods WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .expect("period")
}

async fn adjust(
    pool: &PgPool,
    sku: &str,
    loc: &str,
    qty_delta: i64,
    unit_cost: Option<i64>,
    business_date: &str,
) -> sqlx::Result<()> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
             $1::UUID, $2::UUID, $3, $4, 'USD', 'fg', $5::DATE,
             $6::UUID, $7::UUID, NULL
         )::text",
    )
    .bind(sku)
    .bind(loc)
    .bind(qty_delta)
    .bind(unit_cost)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
    .map(|_| ())
}

async fn queue_retro(
    pool: &PgPool,
    sku: &str,
    loc: &str,
    class: &str,
    target_avg: i64,
    target_period_id: i64,
    business_date: &str,
) -> sqlx::Result<()> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_cost_adjustment_retroactive(
             $1, $2::UUID, $3::UUID, 'USD', $4, $5, $6::DATE,
             $7::UUID, $8::UUID, NULL
         )::text",
    )
    .bind(target_period_id)
    .bind(sku)
    .bind(loc)
    .bind(class)
    .bind(target_avg)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
    .map(|_| ())
}
