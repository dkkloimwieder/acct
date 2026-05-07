//! acct-uoub (sub of acct-1cer) — property test for
//! post_ar_payment + post_ap_payment.
//!
//! Both functions are caller-supplied amount, single-event drains:
//!   * post_ar_payment: cash DR / ar CR per (customer, currency)
//!   * post_ap_payment: ap DR / cash CR per (vendor, currency)
//!
//! No cost dispatch, no per-class qty divisor, no R1/R2 risk. The bug
//! class these would catch is per-(counterparty, currency) balance
//! reconciliation drift (e.g. a prior over-pay or wrong-vendor route)
//! and idempotency races.
//!
//! Random ops mix ArPay / ApPay across N customers + N vendors. After
//! each op:
//!
//!   1. Per (counterparty, currency): mirror tracker matches
//!      accounts.balance for ar (debit-normal) / ap (credit-normal).
//!   2. Cash USD account net delta == Σ(ArPay) − Σ(ApPay) for the
//!      currency (single-currency property test for tractability).
//!   3. Idempotency replay returns the same id, no duplicate cash leg.
//!   4. Validation gates fire on amount=0 / amount<0.
//!   5. Standard I1–I7 invariants.
//!
//! Use PROPTEST_CASES=N to override the default scenario count.
//! Run with --test-threads=1 to avoid TRUNCATE+seed contention.

mod common;

use common::*;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use sqlx::PgPool;

const TEST_PROPTEST_CASES_DEFAULT: u32 = 100;
const N_CUSTOMERS: usize = 3;
const N_VENDORS: usize = 3;
const SEED_AR_PER_CUSTOMER: i64 = 1_000;
const SEED_AP_PER_VENDOR: i64 = 1_000;

#[derive(Debug, Clone, Copy)]
enum Op {
    /// Pay against customer N's ar balance.
    ArPay { idx: usize, amount: i64 },
    /// Pay vendor N out of cash, draining ap.
    ApPay { idx: usize, amount: i64 },
    /// Replay the most recent ArPay or ApPay (same idempotency key).
    /// Runtime-skipped if no payment yet posted.
    Replay,
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        // Bias toward live ops over replays (replays exercise idempotency
        // dual-check; we want plenty of new payment material first).
        4 => (0usize..N_CUSTOMERS, 1i64..=300)
            .prop_map(|(i, a)| Op::ArPay { idx: i, amount: a }),
        4 => (0usize..N_VENDORS, 1i64..=300)
            .prop_map(|(i, a)| Op::ApPay { idx: i, amount: a }),
        1 => Just(Op::Replay),
    ]
}

fn arb_op_seq() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(arb_op(), 4..=15)
}

#[derive(Debug, Default)]
struct CustomerMirror {
    cust_id: String,
    cust_ar: i64, // account id
    paid: i64,    // cumulative paid_in
}

#[derive(Debug, Default)]
struct VendorMirror {
    vend_id: String,
    vend_ap: i64,
    paid: i64,
}

#[derive(Debug)]
struct Scaffold {
    customers: Vec<CustomerMirror>,
    vendors: Vec<VendorMirror>,
    cash_usd: i64,
    cash_initial: i64,
    /// Cumulative cash delta from this run's payments (mirror of
    /// expected (debits − credits) change vs cash_initial). Positive
    /// means net inflow (Σ ArPay > Σ ApPay).
    net_cash_delta: i64,
    /// Last (kind, idx, amount, posted_by, key, currency) payment to
    /// support the Replay op's idempotency-dual-check probe.
    last_payment: Option<LastPayment>,
}

#[derive(Debug, Clone)]
struct LastPayment {
    kind: PayKind,
    idx: usize,
    amount: i64,
    posted_by: String,
    key: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum PayKind {
    Ar,
    Ap,
}

async fn fresh_customer(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO customers (code, name, default_currency)
         VALUES ($1, $2, 'USD') RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Cust {code}"))
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert customer: {e}"))
}

async fn fresh_vendor(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO vendors (code, name, currency)
         VALUES ($1, $2, 'USD') RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Vend {code}"))
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert vendor: {e}"))
}

#[allow(clippy::too_many_arguments)]
async fn open_account(
    pool: &PgPool,
    kind: &str,
    counterparty_id: &str,
    normal_side: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts
            (kind, ledger_kind, currency, counterparty_id, normal_side)
         VALUES ($1::account_kind, 'value', 'USD', $2::UUID, $3::balance_direction)
         RETURNING id",
    )
    .bind(kind)
    .bind(counterparty_id)
    .bind(normal_side)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("open_account {kind}: {e}"))
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("balance")
}

async fn build_scaffold(pool: &PgPool, label: &str) -> Scaffold {
    let void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;
    let cash_usd = account_id_by_kind_currency(pool, "cash", Some("USD")).await;

    let mut customers: Vec<CustomerMirror> = Vec::with_capacity(N_CUSTOMERS);
    for i in 0..N_CUSTOMERS {
        let cust_id = fresh_customer(pool, &format!("{label}-c{i}")).await;
        let cust_ar = open_account(pool, "ar", &cust_id, "debit").await;
        let cust_unsettled = open_account(pool, "ar_unsettled", &cust_id, "debit").await;
        let posted_by = fresh_uuid(pool).await;

        // Mint ar balance: creation_void → ar_unsettled (cycle_count_adj),
        // then ar_unsettled → ar (ar_invoice). Mirrors what a real
        // ship + invoice would do.
        let mint = serde_json::json!([
            {"reason":"cycle_count_adj","document_kind":"seed",
             "document_id":fresh_uuid(pool).await,
             "debit_account_id":cust_unsettled,"credit_account_id":void_val,
             "amount":SEED_AR_PER_CUSTOMER,"qty":SEED_AR_PER_CUSTOMER,
             "business_date":"2026-04-15",
             "idempotency_key":fresh_uuid(pool).await,
             "posted_by":posted_by,
             "counterparty_id":cust_id},
            {"reason":"ar_invoice","document_kind":"seed",
             "document_id":fresh_uuid(pool).await,
             "debit_account_id":cust_ar,"credit_account_id":cust_unsettled,
             "amount":SEED_AR_PER_CUSTOMER,
             "business_date":"2026-04-15",
             "idempotency_key":fresh_uuid(pool).await,
             "posted_by":posted_by,
             "counterparty_id":cust_id},
        ]);
        sqlx::query("SELECT post_posting_lines($1, FALSE)")
            .bind(mint)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("seed customer {i}: {e}"));

        customers.push(CustomerMirror {
            cust_id,
            cust_ar,
            paid: 0,
        });
    }

    let mut vendors: Vec<VendorMirror> = Vec::with_capacity(N_VENDORS);
    for i in 0..N_VENDORS {
        let vend_id = fresh_vendor(pool, &format!("{label}-v{i}")).await;
        let vend_ap = open_account(pool, "ap", &vend_id, "credit").await;
        let vend_unsettled = open_account(pool, "ap_unsettled", &vend_id, "credit").await;
        let posted_by = fresh_uuid(pool).await;

        // Mint ap balance: creation_void → ap_unsettled then
        // ap_unsettled → ap. Plus mint cash to fund the eventual
        // outflows so cash stays >= 0 throughout the run.
        let mint = serde_json::json!([
            {"reason":"cycle_count_adj","document_kind":"seed",
             "document_id":fresh_uuid(pool).await,
             "debit_account_id":void_val,"credit_account_id":vend_unsettled,
             "amount":SEED_AP_PER_VENDOR,"qty":SEED_AP_PER_VENDOR,
             "business_date":"2026-04-15",
             "idempotency_key":fresh_uuid(pool).await,
             "posted_by":posted_by,
             "counterparty_id":vend_id},
            {"reason":"ap_bill","document_kind":"seed",
             "document_id":fresh_uuid(pool).await,
             "debit_account_id":vend_unsettled,"credit_account_id":vend_ap,
             "amount":SEED_AP_PER_VENDOR,
             "business_date":"2026-04-15",
             "idempotency_key":fresh_uuid(pool).await,
             "posted_by":posted_by,
             "counterparty_id":vend_id},
            {"reason":"cycle_count_adj","document_kind":"seed",
             "document_id":fresh_uuid(pool).await,
             "debit_account_id":cash_usd,"credit_account_id":void_val,
             "amount":SEED_AP_PER_VENDOR,
             "business_date":"2026-04-15",
             "idempotency_key":fresh_uuid(pool).await,
             "posted_by":posted_by},
        ]);
        sqlx::query("SELECT post_posting_lines($1, FALSE)")
            .bind(mint)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("seed vendor {i}: {e}"));

        vendors.push(VendorMirror {
            vend_id,
            vend_ap,
            paid: 0,
        });
    }

    let cash_after_seed = balance(pool, cash_usd).await;
    Scaffold {
        customers,
        vendors,
        cash_usd,
        cash_initial: cash_after_seed,
        net_cash_delta: 0,
        last_payment: None,
    }
}

async fn call_ar_payment(
    pool: &PgPool,
    cust_id: &str,
    amount: i64,
    posted_by: &str,
    key: &str,
) -> sqlx::Result<String> {
    sqlx::query_scalar(
        "SELECT post_ar_payment($1::UUID, 'USD', $2::BIGINT,
                                 '2026-04-25'::DATE, $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(cust_id)
    .bind(amount)
    .bind(posted_by)
    .bind(key)
    .fetch_one(pool)
    .await
}

async fn call_ap_payment(
    pool: &PgPool,
    vend_id: &str,
    amount: i64,
    posted_by: &str,
    key: &str,
) -> sqlx::Result<String> {
    sqlx::query_scalar(
        "SELECT post_ap_payment($1::UUID, 'USD', $2::BIGINT,
                                 '2026-04-25'::DATE, $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(vend_id)
    .bind(amount)
    .bind(posted_by)
    .bind(key)
    .fetch_one(pool)
    .await
}

async fn assert_payment_invariants(pool: &PgPool, s: &Scaffold, label: &str) {
    // Per-customer: ar.balance == SEED_AR_PER_CUSTOMER − paid.
    for (i, c) in s.customers.iter().enumerate() {
        let expected = SEED_AR_PER_CUSTOMER - c.paid;
        let actual = balance(pool, c.cust_ar).await;
        assert_eq!(
            actual, expected,
            "[{label}] customer {i} ar drift: expected {expected} actual {actual} (paid={})",
            c.paid
        );
    }
    // Per-vendor: ap is credit-normal, balance is debits−credits, so
    // a remaining liability of X shows as −X.
    for (i, v) in s.vendors.iter().enumerate() {
        let expected = -(SEED_AP_PER_VENDOR - v.paid);
        let actual = balance(pool, v.vend_ap).await;
        assert_eq!(
            actual, expected,
            "[{label}] vendor {i} ap drift: expected {expected} actual {actual} (paid={})",
            v.paid
        );
    }
    // Cash: initial + net_delta. Cash is debit-normal so balance is
    // straightforward (debits − credits). ArPay credits cash debits +amt;
    // ApPay credits cash, so balance −= amt.
    let expected_cash = s.cash_initial + s.net_cash_delta;
    let actual_cash = balance(pool, s.cash_usd).await;
    assert_eq!(
        actual_cash, expected_cash,
        "[{label}] cash drift: expected {expected_cash} actual {actual_cash} (delta={})",
        s.net_cash_delta
    );

    assert_invariants_hold(pool, label).await;
}

#[tokio::test(flavor = "current_thread")]
async fn property_payment_workflows_invariants_hold() {
    let pool = connect_test_db().await;

    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(TEST_PROPTEST_CASES_DEFAULT);

    let mut runner = proptest::test_runner::TestRunner::default();
    let strategy = arb_op_seq();

    for case_idx in 0..cases {
        reset_to_fixture(&pool).await;

        let tree = strategy.new_tree(&mut runner).expect("strategy.new_tree");
        let ops: Vec<Op> = tree.current();

        let label = format!("pay#{case_idx}");
        let mut s = build_scaffold(&pool, &label).await;
        assert_payment_invariants(&pool, &s, &format!("{label}.seed")).await;

        for (step, op) in ops.iter().enumerate() {
            let step_label = format!("{label}.step{step}");
            match *op {
                Op::ArPay { idx, amount } => {
                    let remaining = SEED_AR_PER_CUSTOMER - s.customers[idx].paid;
                    if amount > remaining {
                        // Skip — would push ar below 0 and trip
                        // accounts_check (debit-normal).
                        continue;
                    }
                    let posted_by = fresh_uuid(&pool).await;
                    let key = fresh_uuid(&pool).await;
                    let cust_id = s.customers[idx].cust_id.clone();
                    let id = call_ar_payment(&pool, &cust_id, amount, &posted_by, &key)
                        .await
                        .unwrap_or_else(|e| {
                            panic!(
                                "[{step_label}] ar_payment cust={idx} amt={amount}: {e}"
                            )
                        });
                    assert!(!id.is_empty());
                    s.customers[idx].paid += amount;
                    s.net_cash_delta += amount;
                    s.last_payment = Some(LastPayment {
                        kind: PayKind::Ar,
                        idx,
                        amount,
                        posted_by,
                        key,
                    });
                    assert_payment_invariants(&pool, &s, &step_label).await;
                }
                Op::ApPay { idx, amount } => {
                    let remaining = SEED_AP_PER_VENDOR - s.vendors[idx].paid;
                    if amount > remaining {
                        // Skip — would push ap above 0 and trip
                        // accounts_check (credit-normal).
                        continue;
                    }
                    // Cash must stay >= 0 too — debit-normal check.
                    if s.cash_initial + s.net_cash_delta - amount < 0 {
                        continue;
                    }
                    let posted_by = fresh_uuid(&pool).await;
                    let key = fresh_uuid(&pool).await;
                    let vend_id = s.vendors[idx].vend_id.clone();
                    let id = call_ap_payment(&pool, &vend_id, amount, &posted_by, &key)
                        .await
                        .unwrap_or_else(|e| {
                            panic!(
                                "[{step_label}] ap_payment vend={idx} amt={amount}: {e}"
                            )
                        });
                    assert!(!id.is_empty());
                    s.vendors[idx].paid += amount;
                    s.net_cash_delta -= amount;
                    s.last_payment = Some(LastPayment {
                        kind: PayKind::Ap,
                        idx,
                        amount,
                        posted_by,
                        key,
                    });
                    assert_payment_invariants(&pool, &s, &step_label).await;
                }
                Op::Replay => {
                    // Idempotency dual-check probe: re-call the previous
                    // payment with the SAME (posted_by, idempotency_key).
                    // Should return the same id, no balance drift, no
                    // duplicate transfers.
                    let Some(last) = s.last_payment.clone() else {
                        continue;
                    };
                    let id = match last.kind {
                        PayKind::Ar => {
                            let cust_id = s.customers[last.idx].cust_id.clone();
                            call_ar_payment(
                                &pool,
                                &cust_id,
                                last.amount,
                                &last.posted_by,
                                &last.key,
                            )
                            .await
                        }
                        PayKind::Ap => {
                            let vend_id = s.vendors[last.idx].vend_id.clone();
                            call_ap_payment(
                                &pool,
                                &vend_id,
                                last.amount,
                                &last.posted_by,
                                &last.key,
                            )
                            .await
                        }
                    }
                    .unwrap_or_else(|e| panic!("[{step_label}] replay: {e}"));
                    assert!(!id.is_empty());
                    // Balances should be UNCHANGED — no mirror update.
                    assert_payment_invariants(&pool, &s, &step_label).await;
                }
            }
        }
    }
}

// ============================================================
// Validation gate property tests (small, deterministic — not random
// scenarios since these are just per-call gates).
// ============================================================

#[tokio::test]
async fn property_amount_zero_rejected_on_both() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = build_scaffold(&pool, "amt-zero").await;

    expect_sqlstate("P0039", || async {
        let posted_by = fresh_uuid(&pool).await;
        let key = fresh_uuid(&pool).await;
        call_ar_payment(&pool, &s.customers[0].cust_id, 0, &posted_by, &key).await
    })
    .await;

    expect_sqlstate("P0042", || async {
        let posted_by = fresh_uuid(&pool).await;
        let key = fresh_uuid(&pool).await;
        call_ap_payment(&pool, &s.vendors[0].vend_id, 0, &posted_by, &key).await
    })
    .await;

    // Failed calls must NOT have moved any balance.
    assert_payment_invariants(&pool, &s, "amt-zero.post").await;
}

#[tokio::test]
async fn property_amount_negative_rejected_on_both() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = build_scaffold(&pool, "amt-neg").await;

    expect_sqlstate("P0039", || async {
        let posted_by = fresh_uuid(&pool).await;
        let key = fresh_uuid(&pool).await;
        call_ar_payment(&pool, &s.customers[0].cust_id, -50, &posted_by, &key).await
    })
    .await;

    expect_sqlstate("P0042", || async {
        let posted_by = fresh_uuid(&pool).await;
        let key = fresh_uuid(&pool).await;
        call_ap_payment(&pool, &s.vendors[0].vend_id, -50, &posted_by, &key).await
    })
    .await;

    assert_payment_invariants(&pool, &s, "amt-neg.post").await;
}

#[tokio::test]
async fn property_idempotency_no_duplicate_transfer() {
    // Pin: a successful payment, replayed, leaves transfers count at
    // exactly the seed-count + 1 (not +2). Single-event single-transfer
    // shape so this is direct.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let mut s = build_scaffold(&pool, "idemp").await;

    let cust_id = s.customers[0].cust_id.clone();
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;

    let baseline_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM posting_lines WHERE reason = 'ar_payment'")
            .fetch_one(&pool)
            .await
            .expect("count");

    let id1 = call_ar_payment(&pool, &cust_id, 250, &posted_by, &key)
        .await
        .expect("first call");

    let after_first: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM posting_lines WHERE reason = 'ar_payment'")
            .fetch_one(&pool)
            .await
            .expect("count after first");
    assert_eq!(
        after_first - baseline_count,
        1,
        "first ar_payment must emit exactly one transfer"
    );

    let id2 = call_ar_payment(&pool, &cust_id, 250, &posted_by, &key)
        .await
        .expect("replay call");
    assert_eq!(id1, id2, "idempotent replay returns same id");

    let after_replay: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM posting_lines WHERE reason = 'ar_payment'")
            .fetch_one(&pool)
            .await
            .expect("count after replay");
    assert_eq!(
        after_replay, after_first,
        "replay must NOT emit a duplicate transfer"
    );

    s.customers[0].paid += 250;
    s.net_cash_delta += 250;
    assert_payment_invariants(&pool, &s, "idemp.post").await;
}
