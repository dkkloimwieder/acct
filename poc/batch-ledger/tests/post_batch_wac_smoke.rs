//! P4 — WAC perpetual correctness tests for post_batch.

use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

async fn pool() -> PgPool {
    let url = std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());
    PgPool::connect(&url).await.expect("connect")
}

/// Seeds 4 accounts:
///   `ap`   (credit_normal) — vendor liability
///   `cogs` (debit_normal)  — cost of goods sold
///   `raw`  (inv_value_raw) — WAC pool
///   `fg`   (inv_value_fg)  — second WAC pool (for cross-pool tests)
async fn seed_wac_fixture(pool: &PgPool, tag: &str) -> (i64, i64, i64, i64) {
    let row: (i64, i64, i64, i64) = sqlx::query_as(
        "WITH ins AS (
             INSERT INTO accounts (code, currency, kind)
                 VALUES ($1, 'USD', 'credit_normal'),
                        ($2, 'USD', 'debit_normal'),
                        ($3, 'USD', 'inv_value_raw'),
                        ($4, 'USD', 'inv_value_fg')
             RETURNING id, code
         )
         SELECT
             (SELECT id FROM ins WHERE code = $1),
             (SELECT id FROM ins WHERE code = $2),
             (SELECT id FROM ins WHERE code = $3),
             (SELECT id FROM ins WHERE code = $4)",
    )
    .bind(format!("p4-ap-{tag}"))
    .bind(format!("p4-cogs-{tag}"))
    .bind(format!("p4-raw-{tag}"))
    .bind(format!("p4-fg-{tag}"))
    .fetch_one(pool)
    .await
    .expect("seed");
    row
}

#[derive(sqlx::FromRow, Debug)]
struct BatchRow {
    envelope_idx: i32,
    status: String,
    posting_line_id: Option<i64>,
    #[allow(dead_code)] error_code: Option<String>,
    #[allow(dead_code)] error_message: Option<String>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wac_receipt_single_inflow() {
    let pool = pool().await;
    let tag = Uuid::new_v4().to_string();
    let (ap, _cogs, raw, _fg) = seed_wac_fixture(&pool, &tag).await;

    // Receipt: 10 units @ 100 each → pool value 1000, qty 10.
    let envs = json!([{
        "envelope_idx": 0,
        "kind": "wac_receipt",
        "debit_account_id": raw,
        "credit_account_id": ap,
        "qty": 10,
        "unit_cost": 100,
        "idempotency_key": Uuid::new_v4().to_string(),
        "business_date": "2026-05-11",
    }]);
    let rows = sqlx::query_as::<_, BatchRow>("SELECT * FROM post_batch($1)")
        .bind(envs).fetch_all(&pool).await.expect("post_batch");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, "committed");

    let (bal, qty): (i64, i64) = sqlx::query_as(
        "SELECT balance, qty FROM accounts WHERE id = $1")
        .bind(raw).fetch_one(&pool).await.expect("read raw");
    assert_eq!(bal, 1000);
    assert_eq!(qty, 10);

    let ap_bal: i64 = sqlx::query_scalar(
        "SELECT balance FROM accounts WHERE id = $1")
        .bind(ap).fetch_one(&pool).await.expect("read ap");
    assert_eq!(ap_bal, -1000); // credit-normal: -value-delta in our naive sign convention
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wac_issue_prices_at_running_average() {
    let pool = pool().await;
    let tag = Uuid::new_v4().to_string();
    let (ap, cogs, raw, _fg) = seed_wac_fixture(&pool, &tag).await;

    // First batch: receipt 10 @ 100 → pool 1000 / 10.
    let _: Vec<BatchRow> = sqlx::query_as("SELECT * FROM post_batch($1)")
        .bind(json!([{
            "envelope_idx": 0, "kind": "wac_receipt",
            "debit_account_id": raw, "credit_account_id": ap,
            "qty": 10, "unit_cost": 100,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-11",
        }]))
        .fetch_all(&pool).await.expect("recv1");

    // Second batch: receipt 10 @ 200 → pool 3000 / 20 → unit cost 150.
    let _: Vec<BatchRow> = sqlx::query_as("SELECT * FROM post_batch($1)")
        .bind(json!([{
            "envelope_idx": 0, "kind": "wac_receipt",
            "debit_account_id": raw, "credit_account_id": ap,
            "qty": 10, "unit_cost": 200,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-11",
        }]))
        .fetch_all(&pool).await.expect("recv2");

    // Issue 4 units. Expected amount = 4 * (3000/20) = 4 * 150 = 600.
    let rows = sqlx::query_as::<_, BatchRow>("SELECT * FROM post_batch($1)")
        .bind(json!([{
            "envelope_idx": 0, "kind": "wac_issue",
            "debit_account_id": cogs, "credit_account_id": raw,
            "qty": 4,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-11",
        }]))
        .fetch_all(&pool).await.expect("issue");
    assert_eq!(rows[0].status, "committed");

    let pl_amount: i64 = sqlx::query_scalar(
        "SELECT amount FROM posting_lines WHERE id = $1")
        .bind(rows[0].posting_line_id.unwrap())
        .fetch_one(&pool).await.expect("amount");
    assert_eq!(pl_amount, 600, "wac_issue should price at running avg 150 * 4 = 600");

    let (raw_bal, raw_qty): (i64, i64) = sqlx::query_as(
        "SELECT balance, qty FROM accounts WHERE id = $1")
        .bind(raw).fetch_one(&pool).await.expect("read raw");
    assert_eq!(raw_bal, 2400); // 3000 - 600
    assert_eq!(raw_qty, 16);   // 20 - 4
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_inflow_outflow_in_same_batch_uses_running_avg() {
    let pool = pool().await;
    let tag = Uuid::new_v4().to_string();
    let (ap, cogs, raw, _fg) = seed_wac_fixture(&pool, &tag).await;

    // Seed pool: 10 @ 100 = 1000 value.
    let _: Vec<BatchRow> = sqlx::query_as("SELECT * FROM post_batch($1)")
        .bind(json!([{
            "envelope_idx": 0, "kind": "wac_receipt",
            "debit_account_id": raw, "credit_account_id": ap,
            "qty": 10, "unit_cost": 100,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-11",
        }]))
        .fetch_all(&pool).await.expect("seed pool");

    // Single batch: receipt 10 @ 200, then issue 5.
    //   After receipt: pool = 3000 / 20 → avg 150.
    //   Issue 5: amount = 5 * 150 = 750.
    let rows = sqlx::query_as::<_, BatchRow>("SELECT * FROM post_batch($1)")
        .bind(json!([
            {
                "envelope_idx": 0, "kind": "wac_receipt",
                "debit_account_id": raw, "credit_account_id": ap,
                "qty": 10, "unit_cost": 200,
                "idempotency_key": Uuid::new_v4().to_string(),
                "business_date": "2026-05-11",
            },
            {
                "envelope_idx": 1, "kind": "wac_issue",
                "debit_account_id": cogs, "credit_account_id": raw,
                "qty": 5,
                "idempotency_key": Uuid::new_v4().to_string(),
                "business_date": "2026-05-11",
            },
        ]))
        .fetch_all(&pool).await.expect("mixed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].status, "committed");
    assert_eq!(rows[1].status, "committed");

    let issue_amount: i64 = sqlx::query_scalar(
        "SELECT amount FROM posting_lines WHERE id = $1")
        .bind(rows[1].posting_line_id.unwrap())
        .fetch_one(&pool).await.expect("amount");
    assert_eq!(issue_amount, 750,
        "issue should price at avg 150 (after in-batch receipt) * 5 = 750");

    let (raw_bal, raw_qty): (i64, i64) = sqlx::query_as(
        "SELECT balance, qty FROM accounts WHERE id = $1")
        .bind(raw).fetch_one(&pool).await.expect("read raw");
    assert_eq!(raw_bal, 2250); // 1000 + 2000 - 750
    assert_eq!(raw_qty, 15);   // 10 + 10 - 5
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn issue_from_empty_pool_rejects_whole_batch() {
    let pool = pool().await;
    let tag = Uuid::new_v4().to_string();
    let (_ap, cogs, raw, _fg) = seed_wac_fixture(&pool, &tag).await;

    let res = sqlx::query_as::<_, BatchRow>("SELECT * FROM post_batch($1)")
        .bind(json!([{
            "envelope_idx": 0, "kind": "wac_issue",
            "debit_account_id": cogs, "credit_account_id": raw,
            "qty": 5,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-11",
        }]))
        .fetch_all(&pool).await;
    assert!(res.is_err(), "issue from empty pool must error");

    let (bal, qty): (i64, i64) = sqlx::query_as(
        "SELECT balance, qty FROM accounts WHERE id = $1")
        .bind(raw).fetch_one(&pool).await.expect("read raw");
    assert_eq!(bal, 0);
    assert_eq!(qty, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn issue_exceeding_running_qty_rejects() {
    let pool = pool().await;
    let tag = Uuid::new_v4().to_string();
    let (ap, cogs, raw, _fg) = seed_wac_fixture(&pool, &tag).await;

    // 5 in pool.
    let _: Vec<BatchRow> = sqlx::query_as("SELECT * FROM post_batch($1)")
        .bind(json!([{
            "envelope_idx": 0, "kind": "wac_receipt",
            "debit_account_id": raw, "credit_account_id": ap,
            "qty": 5, "unit_cost": 100,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-11",
        }]))
        .fetch_all(&pool).await.expect("seed");

    // Issue 10 — exceeds.
    let res = sqlx::query_as::<_, BatchRow>("SELECT * FROM post_batch($1)")
        .bind(json!([{
            "envelope_idx": 0, "kind": "wac_issue",
            "debit_account_id": cogs, "credit_account_id": raw,
            "qty": 10,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-11",
        }]))
        .fetch_all(&pool).await;
    assert!(res.is_err(), "overshot issue must error");

    // Pool unchanged after rollback.
    let (bal, qty): (i64, i64) = sqlx::query_as(
        "SELECT balance, qty FROM accounts WHERE id = $1")
        .bind(raw).fetch_one(&pool).await.expect("read");
    assert_eq!(bal, 500);
    assert_eq!(qty, 5);
}
