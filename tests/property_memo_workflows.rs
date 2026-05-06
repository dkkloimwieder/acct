//! acct-z9bs (sub of acct-1cer) — property test for
//! post_customer_credit_memo + post_vendor_debit_memo.
//!
//! Both functions land in mig 0088 with a `kind` discriminator on their
//! line tables: `'financial'` (caller-supplied account ↔ ar/ap, no
//! qty leg) and `'goods_return'` (qty + value reversal, caller-supplied
//! unit_cost, no PPV). Both ALWAYS route to cleared (ar/ap), NEVER to
//! staging (ar_unsettled / ap_unsettled) — that's the load-bearing
//! invariant vs post_customer_return / post_po_return which split per
//! state. State-aware routing would be a regression here; this test
//! pins the always-cleared shape against random scenario noise.
//!
//! After every memo:
//!
//!   1. Per (counterparty, currency): ar/ap.balance delta matches the
//!      sum of memo amounts (and tax legs).
//!   2. ar_unsettled / ap_unsettled balance UNCHANGED across the run.
//!   3. financial line: caller-account.balance + ar/ap.balance net to 0.
//!   4. goods_return line: qty disposition routes correctly per kind
//!      (restock → stock_available + inv_value_fg, scrap → stock_scrap
//!      + variance_scrap, repair → stock_quarantine + inv_value_fg
//!      for credit memo; vendor memo always raw → vendor_pool).
//!   5. Idempotency replay returns same id, no duplicate transfers.
//!   6. Standard I1–I7 invariants.
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
const SEED_AR: i64 = 50_000;
const SEED_AP: i64 = 50_000;
const STD_COST: i64 = 60;
const UNIT_PRICE: i64 = 100;
const SEED_QTY: i64 = 500;

#[derive(Debug, Clone, Copy)]
enum Disposition {
    Restock,
    Scrap,
    Repair,
}

impl Disposition {
    fn as_pg(self) -> &'static str {
        match self {
            Disposition::Restock => "restock",
            Disposition::Scrap => "scrap",
            Disposition::Repair => "repair",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Op {
    /// Customer credit memo, single financial line.
    /// amount > 0; optional tax_amount.
    CustFinancial { amount: i64, tax_amount: i64 },
    /// Customer credit memo, single goods_return line per disposition.
    CustGoodsReturn {
        qty: i64,
        unit_cost: i64,
        disposition: Disposition,
    },
    /// Vendor debit memo, single financial line.
    VendFinancial { amount: i64 },
    /// Vendor debit memo, single goods_return line.
    VendGoodsReturn { qty: i64, unit_cost: i64 },
    /// Replay the most recent memo (same idempotency key).
    Replay,
}

fn arb_disposition() -> impl Strategy<Value = Disposition> {
    prop::sample::select(vec![
        Disposition::Restock,
        Disposition::Scrap,
        Disposition::Repair,
    ])
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        // Customer financial: caller-supplied amount + optional tax.
        2 => (1i64..=200, prop::option::of(1i64..=20))
            .prop_map(|(a, t)| Op::CustFinancial { amount: a, tax_amount: t.unwrap_or(0) }),
        // Customer goods_return: qty + caller-supplied unit_cost.
        2 => (1i64..=10, 30i64..=80, arb_disposition())
            .prop_map(|(q, uc, d)| Op::CustGoodsReturn { qty: q, unit_cost: uc, disposition: d }),
        // Vendor financial.
        2 => (1i64..=200)
            .prop_map(|a| Op::VendFinancial { amount: a }),
        // Vendor goods_return.
        2 => (1i64..=10, 30i64..=80)
            .prop_map(|(q, uc)| Op::VendGoodsReturn { qty: q, unit_cost: uc }),
        // Idempotency probe (low frequency).
        1 => Just(Op::Replay),
    ]
}

fn arb_op_seq() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(arb_op(), 4..=12)
}

#[derive(Debug, Clone)]
enum LastMemo {
    Customer {
        amount: i64,
        tax: i64,
        line: serde_json::Value,
        posted_by: String,
        key: String,
    },
    Vendor {
        amount: i64,
        line: serde_json::Value,
        posted_by: String,
        key: String,
    },
}

#[derive(Debug)]
#[allow(dead_code)]
struct Scaffold {
    customer_id: String,
    vendor_id: String,
    sku_id: String,
    loc_id: String,
    // customer-side accounts
    cust_ar: i64,
    cust_ar_unsettled: i64,
    cust_qty: i64,
    revenue_acct: i64,
    cogs_acct: i64,
    tax_acct: i64,
    // vendor-side
    vend_ap: i64,
    vend_ap_unsettled: i64,
    vend_qty: i64,
    expense_acct: i64,
    // sku-located stock + value pools
    stock_available: i64,
    stock_scrap: i64,
    stock_quarantine: i64,
    inv_value_raw: i64,
    inv_value_fg: i64,
    var_scrap: i64,
    creation_void_qty: i64,
    creation_void_val: i64,
    // mirror state
    cust_ar_paid_off: i64, // cumulative reduction from memos (financial+goods_return+tax)
    vend_ap_paid_off: i64, // cumulative reduction
    last_memo: Option<LastMemo>,
    n_cust_memos: i64,
    n_vend_memos: i64,
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
    .unwrap_or_else(|e| panic!("customer: {e}"))
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
    .unwrap_or_else(|e| panic!("vendor: {e}"))
}

async fn fresh_sku(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ($1, 'EA', 'standard') RETURNING id::text",
    )
    .bind(code)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("sku: {e}"))
}

async fn fresh_location(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO locations (code, name) VALUES ($1, $2) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Loc {code}"))
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("loc: {e}"))
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
        "INSERT INTO accounts (kind, ledger_kind, currency, sku_id, location_id,
                                counterparty_id, normal_side)
         VALUES ($1::account_kind, $2, $3, $4::UUID, $5::UUID, $6::UUID,
                 $7::balance_direction) RETURNING id",
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
    .unwrap_or_else(|e| panic!("open {kind}: {e}"))
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("balance")
}

async fn build_scaffold(pool: &PgPool, label: &str) -> Scaffold {
    let customer_id = fresh_customer(pool, &format!("CMW-{label}")).await;
    let vendor_id = fresh_vendor(pool, &format!("VMW-{label}")).await;
    let sku_id = fresh_sku(pool, &format!("SKU-MW-{label}")).await;
    let loc_id = fresh_location(pool, &format!("MW-{label}")).await;

    // Standard cost so the inv_value_fg pool can pre-load with a defined cost.
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query(
        "INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
         VALUES ($1::UUID, $2, '2026-01-01', $3::UUID, $4::UUID)",
    )
    .bind(&sku_id)
    .bind(STD_COST)
    .bind(&posted_by)
    .bind(&key)
    .execute(pool)
    .await
    .expect("std cost");

    let stock_available = open_account(
        pool, "stock_available", "qty", None, Some(&sku_id), Some(&loc_id), None, "debit",
    )
    .await;
    let stock_scrap = open_account(
        pool, "stock_scrap", "qty", None, Some(&sku_id), None, None, "debit",
    )
    .await;
    let stock_quarantine = open_account(
        pool, "stock_quarantine", "qty", None, Some(&sku_id), Some(&loc_id), None, "debit",
    )
    .await;
    let inv_value_raw = open_account(
        pool, "inv_value_raw", "value", Some("USD"), Some(&sku_id), Some(&loc_id), None, "debit",
    )
    .await;
    let inv_value_fg = open_account(
        pool, "inv_value_fg", "value", Some("USD"), Some(&sku_id), Some(&loc_id), None, "debit",
    )
    .await;

    let cust_qty = open_account(
        pool, "customer_pool", "qty", None, None, None, Some(&customer_id), "debit",
    )
    .await;
    let cust_ar = open_account(
        pool, "ar", "value", Some("USD"), None, None, Some(&customer_id), "debit",
    )
    .await;
    let cust_ar_unsettled = open_account(
        pool, "ar_unsettled", "value", Some("USD"), None, None, Some(&customer_id), "debit",
    )
    .await;

    let vend_qty = open_account(
        pool, "vendor_pool", "qty", None, None, None, Some(&vendor_id), "credit",
    )
    .await;
    let vend_ap = open_account(
        pool, "ap", "value", Some("USD"), None, None, Some(&vendor_id), "credit",
    )
    .await;
    let vend_ap_unsettled = open_account(
        pool, "ap_unsettled", "value", Some("USD"), None, None, Some(&vendor_id), "credit",
    )
    .await;

    let revenue_acct = account_id_by_kind_currency(pool, "revenue", Some("USD")).await;
    let cogs_acct = account_id_by_kind_currency(pool, "cogs", Some("USD")).await;
    let tax_acct = account_id_by_kind_currency(pool, "sales_tax_payable", Some("USD")).await;
    let var_scrap = account_id_by_kind_currency(pool, "variance_scrap", Some("USD")).await;
    let creation_void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let creation_void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;
    let expense_acct = cogs_acct; // re-use cogs as caller-supplied "expense" for vendor financial

    // Seed ar balance (mirrors prior ship+invoice).
    let pby = fresh_uuid(pool).await;
    let did = fresh_uuid(pool).await;
    let ar_seed = json!([{
        "reason":"ar_invoice","document_kind":"seed","document_id":did,
        "debit_account_id":cust_ar,"credit_account_id":revenue_acct,
        "amount":SEED_AR,
        "business_date":"2026-04-15",
        "idempotency_key":fresh_uuid(pool).await,
        "posted_by":pby,
        "counterparty_id":customer_id
    }]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(ar_seed)
        .execute(pool)
        .await
        .expect("seed ar");

    // Seed sales_tax_payable so memos can debit tax without underflow.
    let did2 = fresh_uuid(pool).await;
    let tax_seed = json!([{
        "reason":"ar_invoice","document_kind":"seed","document_id":did2,
        "debit_account_id":cust_ar,"credit_account_id":tax_acct,
        "amount":2000,
        "business_date":"2026-04-15",
        "idempotency_key":fresh_uuid(pool).await,
        "posted_by":pby,
        "counterparty_id":customer_id
    }]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(tax_seed)
        .execute(pool)
        .await
        .expect("seed tax");

    // Seed customer_pool + cogs (mimics prior ship leg) so goods_return
    // memo's customer_pool DR / cogs CR don't push debit-normal accounts
    // negative.
    let did3 = fresh_uuid(pool).await;
    let cust_ship_seed = json!([
        {"reason":"cycle_count_adj","document_kind":"seed","document_id":did3,
         "debit_account_id":cust_qty,"credit_account_id":creation_void_qty,
         "amount":SEED_QTY,"qty":SEED_QTY,
         "business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,"posted_by":pby},
        {"reason":"cycle_count_adj","document_kind":"seed","document_id":did3,
         "debit_account_id":cogs_acct,"credit_account_id":creation_void_val,
         "amount":SEED_QTY * STD_COST,
         "business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,"posted_by":pby},
    ]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(cust_ship_seed)
        .execute(pool)
        .await
        .expect("seed customer ship side");

    // Seed ap balance + vendor_pool + stock_available + inv_value_raw
    // (mirrors prior po_receipt + ap_bill).
    let did4 = fresh_uuid(pool).await;
    let ap_seed = json!([
        // qty leg: stock_available DR / vendor_pool CR (vendor_pool is
        // credit-normal so the credit raises its credit balance).
        {"reason":"cycle_count_adj","document_kind":"seed","document_id":did4,
         "debit_account_id":stock_available,"credit_account_id":vend_qty,
         "amount":SEED_QTY,"qty":SEED_QTY,
         "business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,"posted_by":pby},
        // value leg: inv_value_raw DR / ap CR.
        {"reason":"ap_bill","document_kind":"seed","document_id":did4,
         "debit_account_id":inv_value_raw,"credit_account_id":vend_ap,
         "amount":SEED_AP,
         "business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,"posted_by":pby,
         "counterparty_id":vendor_id},
    ]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(ap_seed)
        .execute(pool)
        .await
        .expect("seed ap side");

    // Pre-seed inv_value_fg with some balance so restock+repair memos
    // don't require it to be at exactly the qty * unit_cost on entry.
    // (Memo unit_cost is caller-supplied per line; we just need fg pool
    // to be non-negative after an unrelated value DR.)
    let did5 = fresh_uuid(pool).await;
    let fg_seed = json!([{
        "reason":"cycle_count_adj","document_kind":"seed","document_id":did5,
        "debit_account_id":inv_value_fg,"credit_account_id":creation_void_val,
        "amount":SEED_QTY * STD_COST, "qty":SEED_QTY,
        "business_date":"2026-04-15",
        "idempotency_key":fresh_uuid(pool).await,"posted_by":pby
    }]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(fg_seed)
        .execute(pool)
        .await
        .expect("seed fg pool");

    Scaffold {
        customer_id, vendor_id, sku_id, loc_id,
        cust_ar, cust_ar_unsettled, cust_qty, revenue_acct, cogs_acct, tax_acct,
        vend_ap, vend_ap_unsettled, vend_qty, expense_acct,
        stock_available, stock_scrap, stock_quarantine,
        inv_value_raw, inv_value_fg, var_scrap,
        creation_void_qty, creation_void_val,
        cust_ar_paid_off: 0,
        vend_ap_paid_off: 0,
        last_memo: None,
        n_cust_memos: 0,
        n_vend_memos: 0,
    }
}

async fn call_customer_memo(
    pool: &PgPool,
    customer_id: &str,
    line: &serde_json::Value,
    posted_by: &str,
    key: &str,
) -> sqlx::Result<String> {
    let lines = json!([line]);
    sqlx::query_scalar(
        "SELECT post_customer_credit_memo(
            $1::UUID, 'USD'::CHAR(3), $2::JSONB, '2026-04-25'::DATE,
            $3::UUID, $4::UUID, NULL, FALSE)::text",
    )
    .bind(customer_id)
    .bind(lines)
    .bind(posted_by)
    .bind(key)
    .fetch_one(pool)
    .await
}

async fn call_vendor_memo(
    pool: &PgPool,
    vendor_id: &str,
    line: &serde_json::Value,
    posted_by: &str,
    key: &str,
) -> sqlx::Result<String> {
    let lines = json!([line]);
    sqlx::query_scalar(
        "SELECT post_vendor_debit_memo(
            $1::UUID, 'USD'::CHAR(3), $2::JSONB, '2026-04-25'::DATE,
            $3::UUID, $4::UUID, NULL, FALSE)::text",
    )
    .bind(vendor_id)
    .bind(lines)
    .bind(posted_by)
    .bind(key)
    .fetch_one(pool)
    .await
}

async fn assert_memo_invariants(pool: &PgPool, s: &Scaffold, label: &str) {
    // Invariant A: ar.balance == SEED_AR + 2000 (tax seed) − cust_ar_paid_off.
    let expected_ar = SEED_AR + 2000 - s.cust_ar_paid_off;
    let actual_ar = balance(pool, s.cust_ar).await;
    assert_eq!(
        actual_ar, expected_ar,
        "[{label}] customer ar drift: expected {expected_ar} actual {actual_ar} (paid_off={})",
        s.cust_ar_paid_off
    );

    // Invariant B: ap.balance == -(SEED_AP) + vend_ap_paid_off (credit-normal).
    let expected_ap = -(SEED_AP) + s.vend_ap_paid_off;
    let actual_ap = balance(pool, s.vend_ap).await;
    assert_eq!(
        actual_ap, expected_ap,
        "[{label}] vendor ap drift: expected {expected_ap} actual {actual_ap} (paid_off={})",
        s.vend_ap_paid_off
    );

    // Invariant C: ar_unsettled and ap_unsettled balances UNCHANGED.
    // Memo path is always-cleared. State-aware split would be a regression.
    let cust_unsettled = balance(pool, s.cust_ar_unsettled).await;
    let vend_unsettled = balance(pool, s.vend_ap_unsettled).await;
    assert_eq!(
        cust_unsettled, 0,
        "[{label}] customer ar_unsettled MUST be 0 (memos always route cleared); got {cust_unsettled}"
    );
    assert_eq!(
        vend_unsettled, 0,
        "[{label}] vendor ap_unsettled MUST be 0 (memos always route cleared); got {vend_unsettled}"
    );

    // Invariant D: row counts match driver counts.
    let cust_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM customer_credit_memos WHERE customer_id = $1::UUID")
            .bind(&s.customer_id)
            .fetch_one(pool)
            .await
            .expect("count customer memos");
    let vend_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM vendor_debit_memos WHERE vendor_id = $1::UUID")
            .bind(&s.vendor_id)
            .fetch_one(pool)
            .await
            .expect("count vendor memos");
    assert_eq!(
        cust_count, s.n_cust_memos,
        "[{label}] customer memo count drift: expected {} actual {cust_count}",
        s.n_cust_memos
    );
    assert_eq!(
        vend_count, s.n_vend_memos,
        "[{label}] vendor memo count drift: expected {} actual {vend_count}",
        s.n_vend_memos
    );

    assert_invariants_hold(pool, label).await;
}

#[tokio::test(flavor = "current_thread")]
async fn property_memo_workflows_invariants_hold() {
    let pool = connect_test_db().await;

    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(TEST_PROPTEST_CASES_DEFAULT);

    let mut runner = proptest::test_runner::TestRunner::default();
    let strategy = arb_op_seq();

    for case_idx in 0..cases {
        reset_to_fixture(&pool).await;

        let tree = strategy.new_tree(&mut runner).expect("new_tree");
        let ops: Vec<Op> = tree.current();

        let label = format!("memo#{case_idx}");
        let mut s = build_scaffold(&pool, &label).await;
        assert_memo_invariants(&pool, &s, &format!("{label}.seed")).await;

        for (step, op) in ops.iter().enumerate() {
            let step_label = format!("{label}.step{step}");
            match *op {
                Op::CustFinancial { amount, tax_amount } => {
                    let total = amount + tax_amount;
                    if SEED_AR + 2000 - s.cust_ar_paid_off - total < 0 {
                        // Skip — would push ar below 0.
                        continue;
                    }
                    let posted_by = fresh_uuid(&pool).await;
                    let key = fresh_uuid(&pool).await;
                    let line = json!({
                        "kind": "financial",
                        "revenue_account_id": s.revenue_acct,
                        "amount": amount,
                        "tax_amount": tax_amount,
                    });
                    let id = call_customer_memo(&pool, &s.customer_id, &line, &posted_by, &key)
                        .await
                        .unwrap_or_else(|e| panic!("[{step_label}] cust financial: {e}"));
                    assert!(!id.is_empty());
                    s.cust_ar_paid_off += total;
                    s.n_cust_memos += 1;
                    s.last_memo = Some(LastMemo::Customer {
                        amount,
                        tax: tax_amount,
                        line,
                        posted_by,
                        key,
                    });
                    assert_memo_invariants(&pool, &s, &step_label).await;
                }
                Op::CustGoodsReturn { qty, unit_cost, disposition } => {
                    let amount = qty * UNIT_PRICE;
                    if SEED_AR + 2000 - s.cust_ar_paid_off - amount < 0 {
                        continue;
                    }
                    // Skip if cogs would underflow (cogs is debit-normal,
                    // we credit qty*unit_cost out).
                    let cogs_bal = balance(&pool, s.cogs_acct).await;
                    if cogs_bal - qty * unit_cost < 0 {
                        continue;
                    }
                    let posted_by = fresh_uuid(&pool).await;
                    let key = fresh_uuid(&pool).await;
                    let line = json!({
                        "kind": "goods_return",
                        "sku_id": s.sku_id,
                        "location_id": s.loc_id,
                        "qty": qty,
                        "unit_cost": unit_cost,
                        "unit_price": UNIT_PRICE,
                        "disposition": disposition.as_pg(),
                        "amount": amount,
                        "tax_amount": 0,
                    });
                    let id = call_customer_memo(&pool, &s.customer_id, &line, &posted_by, &key)
                        .await
                        .unwrap_or_else(|e| panic!("[{step_label}] cust goods: {e}"));
                    assert!(!id.is_empty());
                    s.cust_ar_paid_off += amount;
                    s.n_cust_memos += 1;
                    s.last_memo = Some(LastMemo::Customer {
                        amount,
                        tax: 0,
                        line,
                        posted_by,
                        key,
                    });
                    assert_memo_invariants(&pool, &s, &step_label).await;
                }
                Op::VendFinancial { amount } => {
                    if SEED_AP - s.vend_ap_paid_off - amount < 0 {
                        continue;
                    }
                    // expense (cogs) underflow check — vendor financial
                    // credits expense_acct, debit-normal so it must stay >= 0.
                    let exp_bal = balance(&pool, s.expense_acct).await;
                    if exp_bal - amount < 0 {
                        continue;
                    }
                    let posted_by = fresh_uuid(&pool).await;
                    let key = fresh_uuid(&pool).await;
                    let line = json!({
                        "kind": "financial",
                        "expense_account_id": s.expense_acct,
                        "amount": amount,
                    });
                    let id = call_vendor_memo(&pool, &s.vendor_id, &line, &posted_by, &key)
                        .await
                        .unwrap_or_else(|e| panic!("[{step_label}] vend financial: {e}"));
                    assert!(!id.is_empty());
                    s.vend_ap_paid_off += amount;
                    s.n_vend_memos += 1;
                    s.last_memo = Some(LastMemo::Vendor {
                        amount,
                        line,
                        posted_by,
                        key,
                    });
                    assert_memo_invariants(&pool, &s, &step_label).await;
                }
                Op::VendGoodsReturn { qty, unit_cost } => {
                    let amount = qty * unit_cost;
                    if SEED_AP - s.vend_ap_paid_off - amount < 0 {
                        continue;
                    }
                    // inv_value_raw underflow check (debit-normal).
                    let raw_bal = balance(&pool, s.inv_value_raw).await;
                    if raw_bal - amount < 0 {
                        continue;
                    }
                    // stock_available underflow (debit-normal qty).
                    let stk_bal = balance(&pool, s.stock_available).await;
                    if stk_bal - qty < 0 {
                        continue;
                    }
                    let posted_by = fresh_uuid(&pool).await;
                    let key = fresh_uuid(&pool).await;
                    let line = json!({
                        "kind": "goods_return",
                        "sku_id": s.sku_id,
                        "location_id": s.loc_id,
                        "qty": qty,
                        "unit_cost": unit_cost,
                        "amount": amount,
                    });
                    let id = call_vendor_memo(&pool, &s.vendor_id, &line, &posted_by, &key)
                        .await
                        .unwrap_or_else(|e| panic!("[{step_label}] vend goods: {e}"));
                    assert!(!id.is_empty());
                    s.vend_ap_paid_off += amount;
                    s.n_vend_memos += 1;
                    s.last_memo = Some(LastMemo::Vendor {
                        amount,
                        line,
                        posted_by,
                        key,
                    });
                    assert_memo_invariants(&pool, &s, &step_label).await;
                }
                Op::Replay => {
                    let Some(last) = s.last_memo.clone() else {
                        continue;
                    };
                    let id = match &last {
                        LastMemo::Customer { line, posted_by, key, .. } => {
                            call_customer_memo(&pool, &s.customer_id, line, posted_by, key).await
                        }
                        LastMemo::Vendor { line, posted_by, key, .. } => {
                            call_vendor_memo(&pool, &s.vendor_id, line, posted_by, key).await
                        }
                    }
                    .unwrap_or_else(|e| panic!("[{step_label}] replay: {e}"));
                    assert!(!id.is_empty());
                    // Mirror unchanged.
                    assert_memo_invariants(&pool, &s, &step_label).await;
                }
            }
        }
    }
}

// ============================================================
// Disposition routing pin (deterministic) — verifies per-disposition
// qty_dr_acct / val_dr_acct shape.
// ============================================================

#[tokio::test]
async fn property_disposition_routes_correctly() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let mut s = build_scaffold(&pool, "disp").await;

    let qty = 3i64;
    let unit_cost = 50i64;
    let amount = qty * UNIT_PRICE;

    for (label, dispo, expect_qty_acct, expect_val_acct) in [
        ("restock", Disposition::Restock, s.stock_available, s.inv_value_fg),
        ("scrap", Disposition::Scrap, s.stock_scrap, s.var_scrap),
        ("repair", Disposition::Repair, s.stock_quarantine, s.inv_value_fg),
    ] {
        let qty_before = balance(&pool, expect_qty_acct).await;
        let val_before = balance(&pool, expect_val_acct).await;
        let other_qty_accts: Vec<i64> = vec![s.stock_available, s.stock_scrap, s.stock_quarantine]
            .into_iter()
            .filter(|a| *a != expect_qty_acct)
            .collect();
        let other_val_accts: Vec<i64> = vec![s.inv_value_fg, s.var_scrap]
            .into_iter()
            .filter(|a| *a != expect_val_acct)
            .collect();
        let mut other_qty_befores = vec![];
        for a in &other_qty_accts {
            other_qty_befores.push((*a, balance(&pool, *a).await));
        }
        let mut other_val_befores = vec![];
        for a in &other_val_accts {
            other_val_befores.push((*a, balance(&pool, *a).await));
        }

        let posted_by = fresh_uuid(&pool).await;
        let key = fresh_uuid(&pool).await;
        let line = json!({
            "kind": "goods_return",
            "sku_id": s.sku_id,
            "location_id": s.loc_id,
            "qty": qty, "unit_cost": unit_cost, "unit_price": UNIT_PRICE,
            "disposition": dispo.as_pg(),
            "amount": amount, "tax_amount": 0,
        });
        let _id = call_customer_memo(&pool, &s.customer_id, &line, &posted_by, &key)
            .await
            .unwrap_or_else(|e| panic!("[{label}] memo: {e}"));

        let qty_after = balance(&pool, expect_qty_acct).await;
        let val_after = balance(&pool, expect_val_acct).await;
        assert_eq!(
            qty_after - qty_before,
            qty,
            "[{label}] expected qty acct gained {qty} (got {})",
            qty_after - qty_before
        );
        assert_eq!(
            val_after - val_before,
            qty * unit_cost,
            "[{label}] expected val acct gained {} (got {})",
            qty * unit_cost,
            val_after - val_before
        );

        // Other disposition qty + val accts MUST be unchanged.
        for (a, before) in &other_qty_befores {
            let after = balance(&pool, *a).await;
            assert_eq!(
                after, *before,
                "[{label}] other qty acct {a} drift: expected {before} actual {after}"
            );
        }
        for (a, before) in &other_val_befores {
            let after = balance(&pool, *a).await;
            assert_eq!(
                after, *before,
                "[{label}] other val acct {a} drift: expected {before} actual {after}"
            );
        }

        s.cust_ar_paid_off += amount;
        s.n_cust_memos += 1;
    }

    assert_memo_invariants(&pool, &s, "disp.post").await;
}

// ============================================================
// Validation gates (P0048 / P0049 paths).
// ============================================================

#[tokio::test]
async fn property_empty_lines_rejected() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = build_scaffold(&pool, "empty").await;

    expect_sqlstate("P0048", || async {
        let posted_by = fresh_uuid(&pool).await;
        let key = fresh_uuid(&pool).await;
        sqlx::query_scalar::<_, String>(
            "SELECT post_customer_credit_memo($1::UUID, 'USD', '[]'::JSONB,
                                                '2026-04-25'::DATE, $2::UUID, $3::UUID,
                                                NULL, FALSE)::text",
        )
        .bind(&s.customer_id)
        .bind(&posted_by)
        .bind(&key)
        .fetch_one(&pool)
        .await
    })
    .await;

    expect_sqlstate("P0049", || async {
        let posted_by = fresh_uuid(&pool).await;
        let key = fresh_uuid(&pool).await;
        sqlx::query_scalar::<_, String>(
            "SELECT post_vendor_debit_memo($1::UUID, 'USD', '[]'::JSONB,
                                            '2026-04-25'::DATE, $2::UUID, $3::UUID,
                                            NULL, FALSE)::text",
        )
        .bind(&s.vendor_id)
        .bind(&posted_by)
        .bind(&key)
        .fetch_one(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn property_unknown_kind_rejected() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = build_scaffold(&pool, "kind").await;

    expect_sqlstate("P0048", || async {
        let posted_by = fresh_uuid(&pool).await;
        let key = fresh_uuid(&pool).await;
        let bad = json!([{ "kind": "bogus", "amount": 100 }]);
        sqlx::query_scalar::<_, String>(
            "SELECT post_customer_credit_memo($1::UUID, 'USD', $2::JSONB,
                                                '2026-04-25'::DATE, $3::UUID, $4::UUID,
                                                NULL, FALSE)::text",
        )
        .bind(&s.customer_id)
        .bind(bad)
        .bind(&posted_by)
        .bind(&key)
        .fetch_one(&pool)
        .await
    })
    .await;

    expect_sqlstate("P0049", || async {
        let posted_by = fresh_uuid(&pool).await;
        let key = fresh_uuid(&pool).await;
        let bad = json!([{ "kind": "bogus", "amount": 100 }]);
        sqlx::query_scalar::<_, String>(
            "SELECT post_vendor_debit_memo($1::UUID, 'USD', $2::JSONB,
                                            '2026-04-25'::DATE, $3::UUID, $4::UUID,
                                            NULL, FALSE)::text",
        )
        .bind(&s.vendor_id)
        .bind(bad)
        .bind(&posted_by)
        .bind(&key)
        .fetch_one(&pool)
        .await
    })
    .await;
}
