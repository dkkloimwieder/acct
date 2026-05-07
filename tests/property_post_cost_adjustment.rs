//! acct-du2.15 — property tests for post_cost_adjustment +
//! post_inventory_adjustment over multi-class same-location pools.
//!
//! Regression lock for acct-fii (mig 0069). Pre-seeds a fresh
//! wac_perpetual SKU per scenario with BOTH inv_value_raw AND
//! inv_value_fg pools at the same location, runs random op
//! sequences, and asserts:
//!
//!   (a) standard 7 invariants (assert_invariants_hold)
//!   (b) class isolation — a cost_adjust on class X must not change
//!       the per-class qty SUM or value balance of the OTHER class
//!   (c) running-avg consistency — after a cost_adjust on class X
//!       with target=T, inv_value_X.balance must equal T × class_qty(X)
//!       exactly (pre-fix the function over-shot by other_class_qty × T)
//!
//! Use PROPTEST_CASES=N to override the default scenario count.
//! Run with --test-threads=1 to avoid TRUNCATE+seed contention.

mod common;

use common::*;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use serde_json::json;
use sqlx::PgPool;

const TEST_PROPTEST_CASES_DEFAULT: u32 = 100;

#[derive(Debug, Clone)]
enum Op {
    InvAdjRaw {
        qty_delta: i64,
        asserted_uc: Option<i64>,
    },
    InvAdjFg {
        qty_delta: i64,
        asserted_uc: Option<i64>,
    },
    CostAdjRaw {
        target: i64,
    },
    CostAdjFg {
        target: i64,
    },
    CycleCountRaw {
        qty: i64,
        unit_cost: i64,
    },
    CycleCountFg {
        qty: i64,
        unit_cost: i64,
    },
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        ((-5i64..=10), prop::option::of(8i64..15))
            .prop_map(|(q, uc)| Op::InvAdjRaw { qty_delta: q, asserted_uc: uc }),
        ((-5i64..=10), prop::option::of(15i64..25))
            .prop_map(|(q, uc)| Op::InvAdjFg { qty_delta: q, asserted_uc: uc }),
        (5i64..=25).prop_map(|t| Op::CostAdjRaw { target: t }),
        (10i64..=35).prop_map(|t| Op::CostAdjFg { target: t }),
        ((1i64..=8), 5i64..=15)
            .prop_map(|(q, uc)| Op::CycleCountRaw { qty: q, unit_cost: uc }),
        ((1i64..=8), 15i64..=25)
            .prop_map(|(q, uc)| Op::CycleCountFg { qty: q, unit_cost: uc }),
    ]
}

fn arb_op_seq() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(arb_op(), 1..6)
}

#[tokio::test(flavor = "current_thread")]
async fn property_cost_adjustment_class_isolation() {
    let pool = connect_test_db().await;

    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(TEST_PROPTEST_CASES_DEFAULT);

    let mut runner = proptest::test_runner::TestRunner::default();
    let strategy = arb_op_seq();

    for case_idx in 0..cases {
        reset_to_fixture(&pool).await;

        // Fresh wac_perpetual SKU + location with both raw + fg at same
        // location. This is the multi-class same-location layout that
        // surfaced acct-fii.
        let sku_code = format!("PROP-WAC-{case_idx}");
        let loc_code = format!("PROP-LOC-{case_idx}");
        let sku = fresh_sku(&pool, &sku_code, "wac_perpetual").await;
        let loc = fresh_location(&pool, &loc_code).await;

        let qty_acct = open_account_acct(
            &pool, "stock_available", "qty", None,
            Some(&sku), Some(&loc), None, "debit",
        ).await;
        let raw_v = open_account_acct(
            &pool, "inv_value_raw", "value", Some("USD"),
            Some(&sku), Some(&loc), None, "debit",
        ).await;
        let fg_v = open_account_acct(
            &pool, "inv_value_fg", "value", Some("USD"),
            Some(&sku), Some(&loc), None, "debit",
        ).await;

        // Pre-seed both classes so cost_adjust never hits empty-pool
        // P0010 in the steady state, and inv_adjust on wac_perpetual
        // can use the running pool avg on adjustment-out without an
        // asserted unit cost.
        seed_class(&pool, qty_acct, raw_v, 20, 200, "2026-04-15").await; // raw avg=$10
        seed_class(&pool, qty_acct, fg_v, 10, 200, "2026-04-15").await; // fg avg=$20

        let tree = strategy.new_tree(&mut runner).expect("strategy.new_tree");
        let ops: Vec<Op> = tree.current();
        let label = format!("ccu#{case_idx}");

        for (i, op) in ops.iter().enumerate() {
            let op_label = format!("{label}/op{i}");

            // Snapshot per-class state BEFORE the op so we can verify
            // class isolation on cost_adjust.
            let raw_before = (class_qty(&pool, raw_v).await, balance(&pool, raw_v).await);
            let fg_before = (class_qty(&pool, fg_v).await, balance(&pool, fg_v).await);

            let res: sqlx::Result<()> = match *op {
                Op::CycleCountRaw { qty, unit_cost } => {
                    seed_class_safe(&pool, qty_acct, raw_v, qty, qty * unit_cost, "2026-04-15", &op_label, i).await
                }
                Op::CycleCountFg { qty, unit_cost } => {
                    seed_class_safe(&pool, qty_acct, fg_v, qty, qty * unit_cost, "2026-04-15", &op_label, i).await
                }
                Op::InvAdjRaw { qty_delta, asserted_uc } => {
                    inv_adjust(&pool, &sku, &loc, "USD", "raw", qty_delta, asserted_uc, "2026-04-15").await
                }
                Op::InvAdjFg { qty_delta, asserted_uc } => {
                    inv_adjust(&pool, &sku, &loc, "USD", "fg", qty_delta, asserted_uc, "2026-04-15").await
                }
                Op::CostAdjRaw { target } => {
                    cost_adjust(&pool, &sku, &loc, "USD", "raw", target, "2026-04-15").await
                }
                Op::CostAdjFg { target } => {
                    cost_adjust(&pool, &sku, &loc, "USD", "fg", target, "2026-04-15").await
                }
            };

            // Some ops legitimately fail (e.g. depletion past zero,
            // wac_perpetual on empty without asserted_uc). Invariants
            // only have to hold after successful posts.
            if res.is_err() {
                continue;
            }

            // Class-isolation lock: cost_adjust on one class must NOT
            // mutate the per-class qty SUM or value balance of the
            // other class. This is the heart of the acct-fii regression:
            // pre-fix, cost_adjust on raw read stock_available qty
            // (cross-class) and posted a delta that knocked the OTHER
            // class's value pool sideways via the shared variance
            // contra-leg. Note the contra leg lands on
            // variance_cost_adjustment, not on the other class's value
            // pool — so the OTHER class's balance stays put even
            // pre-fix; what gets clobbered is THIS class's running
            // avg (caught by the running-avg-consistency check below).
            // We still snapshot+compare to surface any sibling-bug
            // regression that might add cross-class side effects.
            match *op {
                Op::CostAdjRaw { .. } => {
                    let fg_after = (class_qty(&pool, fg_v).await, balance(&pool, fg_v).await);
                    assert_eq!(
                        fg_before, fg_after,
                        "[{op_label}] cost_adjust on raw mutated fg state"
                    );
                }
                Op::CostAdjFg { .. } => {
                    let raw_after = (class_qty(&pool, raw_v).await, balance(&pool, raw_v).await);
                    assert_eq!(
                        raw_before, raw_after,
                        "[{op_label}] cost_adjust on fg mutated raw state"
                    );
                }
                _ => {}
            }

            // Running-avg consistency: after a cost_adjust(class=X,
            // target=T), inv_value_X.balance MUST equal T × class_qty(X).
            // Pre-fix (acct-fii), the function read v_pool_qty from
            // stock_available (cross-class) and computed
            //   v_new_value = T × cross_qty
            // resulting in inv_value_X.balance = T × cross_qty,
            // which makes inv_value_X / class_qty(X) = T × (cross/class)
            // — the exact over-shoot. This assertion catches it.
            match *op {
                Op::CostAdjRaw { target } => {
                    let q = class_qty(&pool, raw_v).await;
                    let v = balance(&pool, raw_v).await;
                    assert_eq!(
                        v,
                        target * q,
                        "[{op_label}] raw inv_value != target*class_qty: \
                         target={target} q={q} v={v}"
                    );
                }
                Op::CostAdjFg { target } => {
                    let q = class_qty(&pool, fg_v).await;
                    let v = balance(&pool, fg_v).await;
                    assert_eq!(
                        v,
                        target * q,
                        "[{op_label}] fg inv_value != target*class_qty: \
                         target={target} q={q} v={v}"
                    );
                }
                _ => {}
            }

            assert_invariants_hold(&pool, &op_label).await;
        }
    }
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
async fn open_account_acct(
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

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("balance")
}

async fn class_qty(pool: &PgPool, val_acct: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT COALESCE(SUM(CASE
                  WHEN t.debit_account_id  = $1 THEN  t.qty
                  WHEN t.credit_account_id = $1 THEN -t.qty
                END), 0)::BIGINT
           FROM posting_lines t
          WHERE $1 IN (t.debit_account_id, t.credit_account_id)
            AND t.qty IS NOT NULL",
    )
    .bind(val_acct)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Pre-seed pattern (panics on failure — used only for the deterministic
/// up-front seeding before the random ops run).
async fn seed_class(
    pool: &PgPool,
    qty_acct: i64,
    val_acct: i64,
    qty: i64,
    value: i64,
    business_date: &str,
) {
    seed_class_inner(pool, qty_acct, val_acct, qty, value, business_date)
        .await
        .unwrap_or_else(|e| panic!("seed_class qty={qty} val={value}: {e}"));
}

/// Mid-scenario seed that returns sqlx::Result so we can swallow
/// CHECK violations from random shrinking (e.g. value pool would go
/// negative on signed-qty drift).
async fn seed_class_safe(
    pool: &PgPool,
    qty_acct: i64,
    val_acct: i64,
    qty: i64,
    value: i64,
    business_date: &str,
    _label: &str,
    _op_idx: usize,
) -> sqlx::Result<()> {
    seed_class_inner(pool, qty_acct, val_acct, qty, value, business_date).await
}

async fn seed_class_inner(
    pool: &PgPool,
    qty_acct: i64,
    val_acct: i64,
    qty: i64,
    value: i64,
    business_date: &str,
) -> sqlx::Result<()> {
    let posted_by = fresh_uuid(pool).await;
    let void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let val_currency: String = sqlx::query_scalar(
        "SELECT currency FROM accounts WHERE id = $1",
    )
    .bind(val_acct)
    .fetch_one(pool)
    .await
    .unwrap();
    let void_val =
        account_id_by_kind_currency(pool, "creation_void", Some(&val_currency)).await;
    let doc_id = fresh_uuid(pool).await;
    let events = json!([
        {
            "reason": "cycle_count_adj", "document_kind": "seed", "document_id": doc_id,
            "debit_account_id": qty_acct, "credit_account_id": void_qty,
            "amount": qty, "qty": qty, "business_date": business_date,
            "idempotency_key": fresh_uuid(pool).await, "posted_by": posted_by
        },
        {
            "reason": "cycle_count_adj", "document_kind": "seed", "document_id": doc_id,
            "debit_account_id": val_acct, "credit_account_id": void_val,
            "amount": value, "qty": qty, "business_date": business_date,
            "idempotency_key": fresh_uuid(pool).await, "posted_by": posted_by
        },
    ]);
    sqlx::query("SELECT post_posting_lines($1, FALSE)")
        .bind(events)
        .execute(pool)
        .await
        .map(|_| ())
}

#[allow(clippy::too_many_arguments)]
async fn inv_adjust(
    pool: &PgPool,
    sku: &str,
    loc: &str,
    currency: &str,
    class: &str,
    qty_delta: i64,
    asserted_uc: Option<i64>,
    business_date: &str,
) -> sqlx::Result<()> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
             $1::UUID, $2::UUID, $3, $4, $5, $6, $7::DATE,
             $8::UUID, $9::UUID, NULL
         )::text",
    )
    .bind(sku)
    .bind(loc)
    .bind(qty_delta)
    .bind(asserted_uc)
    .bind(currency)
    .bind(class)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
    .map(|_| ())
}

async fn cost_adjust(
    pool: &PgPool,
    sku: &str,
    loc: &str,
    currency: &str,
    class: &str,
    target: i64,
    business_date: &str,
) -> sqlx::Result<()> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_cost_adjustment(
             $1::UUID, $2::UUID, $3, $4, $5, $6::DATE,
             $7::UUID, $8::UUID, NULL
         )::text",
    )
    .bind(sku)
    .bind(loc)
    .bind(currency)
    .bind(class)
    .bind(target)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
    .map(|_| ())
}
