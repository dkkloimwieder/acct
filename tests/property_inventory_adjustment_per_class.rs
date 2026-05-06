//! acct-3lkq (sub of acct-1cer) — property test for
//! `post_inventory_adjustment` on `wac_perpetual` SKUs.
//!
//! Pins **R1** (per-class qty divisor): raw and fg classes share
//! `stock_available(sku, location)` qty pool, but maintain
//! INDEPENDENT running averages computed from per-class signed qty
//! SUM on the value account's `transfers.qty` (mig 0030 acct-1vr
//! fix; also acct-fii). A regression in R1 — e.g., reading qty
//! divisor from `stock_available.balance` instead of per-class
//! signed-SUM — would let raw activity pollute the fg avg (or vice
//! versa) once both classes had non-zero history at the same
//! (sku, location).
//!
//! For every successful adjustment:
//!
//! 1. inv_value_<class> debit Δ (when qty_delta > 0) =
//!    qty_delta × effective_uc.
//! 2. inv_value_<class> credit Δ (when qty_delta < 0) =
//!    |qty_delta| × pool_avg_per_class.
//! 3. inventory_adjustments.unit_cost == effective_uc (R7 audit-vs-
//!    ledger consistency on this entry-point).
//! 4. Cross-class isolation: a raw adjustment leaves the fg pool's
//!    running avg unchanged; vice versa.
//! 5. I1–I7 invariants throughout.
//!
//! Random sequences alternate raw / fg adjustments on the same
//! wac_perpetual SKU + location.

mod common;

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use sqlx::PgPool;

const TEST_PROPTEST_CASES_DEFAULT: u32 = 100;

#[derive(Debug, Clone, Copy, PartialEq)]
enum Class {
    Raw,
    Fg,
}

impl Class {
    fn as_pg(self) -> &'static str {
        match self {
            Class::Raw => "raw",
            Class::Fg => "fg",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum AdjOp {
    /// +qty with explicit unit_cost (always valid; either seeds or
    /// asserts a new in-cost).
    InAt { class: Class, qty: i64, unit_cost: i64 },
    /// +qty letting the dispatcher use pool avg (requires non-empty
    /// pool — runtime-skipped if pool is empty).
    InAtAvg { class: Class, qty: i64 },
    /// -qty depleting at pool avg (runtime-skipped if pool is empty
    /// or qty exceeds pool_qty).
    Out { class: Class, qty: i64 },
}

fn arb_class() -> impl Strategy<Value = Class> {
    prop::sample::select(vec![Class::Raw, Class::Fg])
}

fn arb_op() -> impl Strategy<Value = AdjOp> {
    prop_oneof![
        // Bias toward IN ops so the pool stays non-empty.
        3 => (arb_class(), 1i64..=50, 1i64..=200)
            .prop_map(|(c, q, uc)| AdjOp::InAt { class: c, qty: q, unit_cost: uc }),
        2 => (arb_class(), 1i64..=50)
            .prop_map(|(c, q)| AdjOp::InAtAvg { class: c, qty: q }),
        1 => (arb_class(), 1i64..=20)
            .prop_map(|(c, q)| AdjOp::Out { class: c, qty: q }),
    ]
}

fn arb_op_seq() -> impl Strategy<Value = Vec<AdjOp>> {
    prop::collection::vec(arb_op(), 3..=12)
}

#[tokio::test(flavor = "current_thread")]
async fn property_inventory_adjustment_per_class_invariants_hold() {
    let pool = common::connect_test_db().await;

    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(TEST_PROPTEST_CASES_DEFAULT);

    let mut runner = proptest::test_runner::TestRunner::default();
    let strategy = arb_op_seq();

    for case_idx in 0..cases {
        common::reset_to_fixture(&pool).await;

        let tree = strategy
            .new_tree(&mut runner)
            .expect("strategy.new_tree");
        let ops: Vec<AdjOp> = tree.current();

        let label = format!("inv_adj#{case_idx}");
        let s = scaffold(&pool, &label).await;

        // Independent per-class mirrors. Raw and fg are tracked
        // separately to verify R1 cross-class isolation.
        let mut raw = PoolMirror::default();
        let mut fg = PoolMirror::default();
        let mut step = 0usize;

        for op in &ops {
            let class = match *op {
                AdjOp::InAt { class, .. }
                | AdjOp::InAtAvg { class, .. }
                | AdjOp::Out { class, .. } => class,
            };

            // Snapshot the mirror state for both classes before borrowing.
            let mirror_before = match class {
                Class::Raw => raw,
                Class::Fg => fg,
            };
            let other_before = match class {
                Class::Raw => fg,
                Class::Fg => raw,
            };

            let (expected_uc, qty_delta) = match *op {
                AdjOp::InAt { qty, unit_cost, .. } => (unit_cost, qty),
                AdjOp::InAtAvg { qty, .. } => {
                    if mirror_before.qty == 0 {
                        step += 1;
                        continue; // function would raise — empty pool needs explicit uc
                    }
                    (mirror_before.value / mirror_before.qty, qty)
                }
                AdjOp::Out { qty, .. } => {
                    if mirror_before.qty == 0 || qty > mirror_before.qty {
                        step += 1;
                        continue;
                    }
                    (mirror_before.value / mirror_before.qty, -qty)
                }
            };

            let other_qty_before = other_before.qty;
            let other_value_before = other_before.value;

            // Snapshot the OTHER class's value account balance before
            // the call so we can verify cross-class isolation.
            let other_val_acct = match class {
                Class::Raw => s.fg_val_acct,
                Class::Fg => s.raw_val_acct,
            };
            let other_val_balance_before = balance(&pool, other_val_acct).await;

            let val_amount = qty_delta.abs() * expected_uc;
            let doc_id = do_adjustment(&pool, &s, class, qty_delta, op_unit_cost(op), step + 1, &label).await;

            // Verify ledger pool balances match expected.
            let val_acct = match class {
                Class::Raw => s.raw_val_acct,
                Class::Fg => s.fg_val_acct,
            };
            let val_balance_after = balance(&pool, val_acct).await;
            let expected_value_after = mirror_before.value + qty_delta.signum() * val_amount;
            assert_eq!(
                val_balance_after,
                expected_value_after,
                "[{label}/step{step}] {class:?} pool value Δ wrong (expected {expected_value_after}, got {val_balance_after}; qty_delta={qty_delta}, expected_uc={expected_uc}, val_amount={val_amount})",
                class = class.as_pg(),
            );

            // Audit field: inventory_adjustments.unit_cost = effective_uc.
            let audit_uc = audit_unit_cost(&pool, &doc_id).await;
            assert_eq!(
                audit_uc, expected_uc,
                "[{label}/step{step}] inventory_adjustments.unit_cost ({audit_uc}) != expected_uc ({expected_uc})",
            );

            // Cross-class isolation: the OTHER class's value account
            // balance must be unchanged.
            let other_val_balance_after = balance(&pool, other_val_acct).await;
            assert_eq!(
                other_val_balance_after,
                other_val_balance_before,
                "[{label}/step{step}] R1 violation: {class:?} adj changed other-class value balance ({} → {})",
                other_val_balance_before, other_val_balance_after,
                class = class.as_pg(),
            );

            // Update mirror.
            let mirror_mut = match class {
                Class::Raw => &mut raw,
                Class::Fg => &mut fg,
            };
            mirror_mut.qty += qty_delta;
            mirror_mut.value += qty_delta.signum() * val_amount;
            // Sanity: the OTHER mirror unchanged (in-memory state).
            let other_after = match class {
                Class::Raw => fg,
                Class::Fg => raw,
            };
            assert_eq!(other_after.qty, other_qty_before);
            assert_eq!(other_after.value, other_value_before);

            common::assert_invariants_hold(&pool, &format!("{label}/step{step}")).await;
            step += 1;
        }
    }
}

fn op_unit_cost(op: &AdjOp) -> Option<i64> {
    match *op {
        AdjOp::InAt { unit_cost, .. } => Some(unit_cost),
        AdjOp::InAtAvg { .. } | AdjOp::Out { .. } => None,
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct PoolMirror {
    qty: i64,
    value: i64,
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("balance")
}

async fn audit_unit_cost(pool: &PgPool, doc_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT unit_cost FROM inventory_adjustments WHERE id = $1::UUID",
    )
    .bind(doc_id)
    .fetch_one(pool)
    .await
    .expect("audit unit_cost")
}

// ============================================================
// Scaffold
// ============================================================

#[allow(dead_code)]
struct Scaffold {
    sku_id: String,
    loc_id: String,
    qty_acct: i64,        // shared stock_available across raw + fg
    raw_val_acct: i64,    // inv_value_raw
    fg_val_acct: i64,     // inv_value_fg
}

async fn scaffold(pool: &PgPool, label: &str) -> Scaffold {
    let suffix = sanitize(label);
    let sku_id = fresh_sku(pool, &format!("S-{suffix}"), "wac_perpetual").await;
    let loc_id = fresh_location(pool, &format!("L-{suffix}")).await;

    let qty_acct = open_account(
        pool, "stock_available", "qty", None,
        Some(&sku_id), Some(&loc_id), None, "debit",
    )
    .await;
    let raw_val_acct = open_account(
        pool, "inv_value_raw", "value", Some("USD"),
        Some(&sku_id), Some(&loc_id), None, "debit",
    )
    .await;
    let fg_val_acct = open_account(
        pool, "inv_value_fg", "value", Some("USD"),
        Some(&sku_id), Some(&loc_id), None, "debit",
    )
    .await;

    Scaffold {
        sku_id,
        loc_id,
        qty_acct,
        raw_val_acct,
        fg_val_acct,
    }
}

async fn do_adjustment(
    pool: &PgPool,
    s: &Scaffold,
    class: Class,
    qty_delta: i64,
    p_unit_cost: Option<i64>,
    step: usize,
    label: &str,
) -> String {
    let posted_by = common::fresh_uuid(pool).await;
    let key = uuid_for(label, step, "adj");
    let r: sqlx::Result<String> = sqlx::query_scalar(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3::BIGINT, $4::BIGINT, 'USD',
            $5::TEXT, '2026-04-15'::DATE, $6::UUID, $7::UUID, NULL
         )::text",
    )
    .bind(&s.sku_id)
    .bind(&s.loc_id)
    .bind(qty_delta)
    .bind(p_unit_cost)
    .bind(class.as_pg())
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await;
    r.unwrap_or_else(|e| panic!("[{label}/step{step}] post_inventory_adjustment(class={}, qty_delta={qty_delta}, uc={p_unit_cost:?}): {e}", class.as_pg()))
}

// ============================================================
// Local SQL helpers
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
    .unwrap_or_else(|e| panic!("insert loc {code}: {e}"))
}

#[allow(clippy::too_many_arguments)]
async fn open_account(
    pool: &PgPool,
    kind: &str,
    ledger_kind: &str,
    currency: Option<&str>,
    sku_id: Option<&str>,
    loc_id: Option<&str>,
    counterparty_id: Option<&str>,
    normal_side: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts
            (kind, ledger_kind, currency, sku_id, location_id,
             counterparty_id, normal_side)
         VALUES ($1::account_kind, $2, $3, $4::UUID, $5::UUID, $6::UUID,
                 $7::balance_direction)
         RETURNING id",
    )
    .bind(kind)
    .bind(ledger_kind)
    .bind(currency)
    .bind(sku_id)
    .bind(loc_id)
    .bind(counterparty_id)
    .bind(normal_side)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("open_account {kind}: {e}"))
}

// ============================================================
// Misc utilities
// ============================================================

fn sanitize(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(8)
        .collect()
}

fn uuid_for(label: &str, step: usize, kind: &str) -> String {
    let raw = format!("{label}/{step}/{kind}");
    let h = hash(raw.as_bytes());
    format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        h.0,
        (h.1 >> 16) as u16,
        (h.1 & 0xfff) as u16,
        (0x8000 | ((h.2 >> 16) & 0x3fff)) as u16,
        h.3 & 0xffff_ffff_ffff,
    )
}

fn hash(b: &[u8]) -> (u32, u32, u64, u64) {
    let mut h0: u64 = 0xcbf29ce484222325;
    for &x in b {
        h0 ^= x as u64;
        h0 = h0.wrapping_mul(0x100000001b3);
    }
    let h1 = h0.wrapping_mul(0x9e3779b97f4a7c15);
    let h2 = h0.rotate_left(17).wrapping_mul(0x94d049bb133111eb);
    let h3 = h0.rotate_right(13).wrapping_mul(0xc6a4a7935bd1e995);
    (h0 as u32, h1 as u32, h2, h3)
}
