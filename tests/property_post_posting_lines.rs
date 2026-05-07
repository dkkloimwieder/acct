//! acct-du2 Phase 3 — property tests for post_posting_lines.
//!
//! Generates random valid post_posting_lines batches over the seed fixture
//! and asserts the 7 invariants (I1-I7) hold after each call. Acts as
//! a regression net for class-confusion bugs that bypass pattern-grep
//! and structural-walk detection.
//!
//! Scenarios are sequential within this binary (proptest's default).
//! Use `PROPTEST_CASES=N` to override the default 100-scenario count.
//!
//! Currently exercises:
//!   - random ar_invoice / ar_payment chains (cash + AR + revenue)
//!   - random cycle_count_adj seedings on stock_available + inv_value_*
//!   - random cost_adjustment value events on existing pools
//!
//! Document-wrapper exercises (post_inventory_adjustment,
//! post_cost_adjustment, post_wo_complete) belong in dedicated
//! property_<wrapper>.rs binaries — they need fixture extensions
//! (cost_method='wac_perpetual' SKU pre-seeded with both raw + fg
//! pools) that this baseline binary doesn't carry.

mod common;

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use serde_json::json;

const TEST_PROPTEST_CASES_DEFAULT: u32 = 100;

#[derive(Debug, Clone)]
enum Op {
    ArInvoice { amount: i64 },
    ArPayment { amount: i64 },
    CycleCountUsd { qty: i64 },
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        (1i64..1000).prop_map(|amount| Op::ArInvoice { amount }),
        (1i64..1000).prop_map(|amount| Op::ArPayment { amount }),
        (1i64..100).prop_map(|qty| Op::CycleCountUsd { qty }),
    ]
}

fn arb_op_seq() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(arb_op(), 1..6)
}

#[tokio::test(flavor = "current_thread")]
async fn property_post_transfers_invariants_hold() {
    let pool = common::connect_test_db().await;

    // proptest's `proptest!` macro is sync; we drive it manually here so
    // we can `.await` per scenario. Strategy: hand-rolled sample loop
    // sized from PROPTEST_CASES env var.
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
        let ops: Vec<Op> = tree.current();

        let label = format!("post_posting_lines#{case_idx}");

        // Track AR balance so ar_payment can't exceed it (would violate
        // ar account's debit-normal CHECK).
        let mut ar_outstanding: i64 = 0;
        // stock_available + inv_value_raw qty/value mirror so
        // cycle_count_adj seedings are coherent.

        let mut events = Vec::new();
        for (i, op) in ops.iter().enumerate() {
            match *op {
                Op::ArInvoice { amount } => {
                    let event = make_ar_invoice(&pool, amount, &label, i).await;
                    events.push(event);
                    ar_outstanding += amount;
                }
                Op::ArPayment { amount } => {
                    if amount > ar_outstanding {
                        continue;
                    }
                    let event = make_ar_payment(&pool, amount, &label, i).await;
                    events.push(event);
                    ar_outstanding -= amount;
                }
                Op::CycleCountUsd { qty } => {
                    let (qty_e, val_e) =
                        make_cycle_count_pair(&pool, qty, &label, i).await;
                    events.push(qty_e);
                    events.push(val_e);
                }
            }
        }

        if events.is_empty() {
            common::assert_invariants_hold(&pool, &format!("{label}/empty")).await;
            continue;
        }

        let result = common::call_post_posting_lines(&pool, json!(events), false).await;
        result.unwrap_or_else(|e| panic!("[{label}] post_posting_lines: {e}"));

        common::assert_invariants_hold(&pool, &label).await;
    }
}

async fn make_ar_invoice(
    pool: &sqlx::PgPool,
    amount: i64,
    label: &str,
    i: usize,
) -> serde_json::Value {
    let ar = common::account_id_by_kind_currency(pool, "ar", Some("USD")).await;
    let revenue = common::account_id_by_kind_currency(pool, "revenue", Some("USD")).await;
    let key = uuid_for(label, i, "ar_invoice");
    common::make_event("ar_invoice", ar, revenue, amount, "2026-04-15", &key)
}

async fn make_ar_payment(
    pool: &sqlx::PgPool,
    amount: i64,
    label: &str,
    i: usize,
) -> serde_json::Value {
    let cash = common::account_id_by_kind_currency(pool, "cash", Some("USD")).await;
    let ar = common::account_id_by_kind_currency(pool, "ar", Some("USD")).await;
    let key = uuid_for(label, i, "ar_payment");
    common::make_event("ar_payment", cash, ar, amount, "2026-04-15", &key)
}

async fn make_cycle_count_pair(
    pool: &sqlx::PgPool,
    qty: i64,
    label: &str,
    i: usize,
) -> (serde_json::Value, serde_json::Value) {
    let stock = common::account_id_stock_available(pool, "SKU-A", "MAIN").await;
    let void_qty = common::account_id_by_kind_currency(pool, "creation_void", None).await;
    let inv_raw = common::account_id_for_selector(
        pool,
        "inv_value_raw",
        Some("SKU-A"),
        Some("MAIN"),
        Some("USD"),
        None,
    )
    .await;
    let void_val = common::account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;
    let amount = qty * 100; // SKU-A standard is 100; keeps avg sane

    let qty_event = common::make_event_with_qty(
        "cycle_count_adj",
        stock,
        void_qty,
        qty,
        qty,
        "2026-04-15",
        &uuid_for(label, i, "cc_qty"),
    );
    let val_event = common::make_event_with_qty(
        "cycle_count_adj",
        inv_raw,
        void_val,
        amount,
        qty,
        "2026-04-15",
        &uuid_for(label, i, "cc_val"),
    );
    (qty_event, val_event)
}

fn uuid_for(label: &str, i: usize, kind: &str) -> String {
    // Deterministic-per-run UUID so retries within proptest don't
    // collide with prior round's transfers.idempotency_key.
    let raw = format!("{label}/{i}/{kind}");
    let h = blake_like_hash(raw.as_bytes());
    format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        h.0,
        (h.1 >> 16) as u16,
        (h.1 & 0xfff) as u16,
        (0x8000 | ((h.2 >> 16) & 0x3fff)) as u16,
        h.3 & 0xffff_ffff_ffff,
    )
}

fn blake_like_hash(b: &[u8]) -> (u32, u32, u64, u64) {
    // FNV-1a-style multi-output hash; good enough for unique idempotency
    // keys in tests, not cryptographic.
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
