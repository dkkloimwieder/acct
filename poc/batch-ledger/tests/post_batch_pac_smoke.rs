//! P4 PAC variant smoke tests — preset-snapshot-and-fixed-cost-per-batch.

use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

async fn pool() -> PgPool {
    let url = std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());
    PgPool::connect(&url).await.expect("connect")
}

async fn seed_wac_fixture(pool: &PgPool, tag: &str) -> (i64, i64, i64) {
    sqlx::query_as::<_, (i64, i64, i64)>(
        "WITH ins AS (
             INSERT INTO accounts (code, currency, kind)
                 VALUES ($1, 'USD', 'credit_normal'),
                        ($2, 'USD', 'debit_normal'),
                        ($3, 'USD', 'inv_value_raw')
             RETURNING id, code
         )
         SELECT
             (SELECT id FROM ins WHERE code = $1),
             (SELECT id FROM ins WHERE code = $2),
             (SELECT id FROM ins WHERE code = $3)",
    )
    .bind(format!("pac-ap-{tag}"))
    .bind(format!("pac-cogs-{tag}"))
    .bind(format!("pac-raw-{tag}"))
    .fetch_one(pool).await.expect("seed")
}

#[derive(sqlx::FromRow, Debug)]
struct BatchRow { envelope_idx: i32, status: String, posting_line_id: Option<i64>,
    #[allow(dead_code)] error_code: Option<String>,
    #[allow(dead_code)] error_message: Option<String> }

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pac_receipt_then_issue_uses_snapshot_avg() {
    let pool = pool().await;
    let tag = Uuid::new_v4().to_string();
    let (ap, cogs, raw) = seed_wac_fixture(&pool, &tag).await;

    // Seed pool: 10 @ 100. After this batch: balance=1000, qty=10, avg=100.
    let _: Vec<BatchRow> = sqlx::query_as("SELECT * FROM post_batch($1)")
        .bind(json!([{
            "envelope_idx": 0, "kind": "wac_pac_receipt",
            "debit_account_id": raw, "credit_account_id": ap,
            "qty": 10, "unit_cost": 100,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-12",
        }])).fetch_all(&pool).await.expect("seed");

    // New batch: receipt 10 @ 200 + issue 5.
    //   Snapshot avg at batch start = 1000/10 = 100.
    //   PAC: issue prices at snapshot (100), NOT post-receipt (150).
    //   Issue amount = 5 * 100 = 500.
    let r = sqlx::query_as::<_, BatchRow>("SELECT * FROM post_batch($1)")
        .bind(json!([
            { "envelope_idx": 0, "kind": "wac_pac_receipt",
              "debit_account_id": raw, "credit_account_id": ap,
              "qty": 10, "unit_cost": 200,
              "idempotency_key": Uuid::new_v4().to_string(),
              "business_date": "2026-05-12" },
            { "envelope_idx": 1, "kind": "wac_pac_issue",
              "debit_account_id": cogs, "credit_account_id": raw,
              "qty": 5,
              "idempotency_key": Uuid::new_v4().to_string(),
              "business_date": "2026-05-12" },
        ])).fetch_all(&pool).await.expect("pac batch");
    assert_eq!(r.len(), 2);
    assert_eq!(r[0].status, "committed");
    assert_eq!(r[1].status, "committed");

    let issue_amt: i64 = sqlx::query_scalar(
        "SELECT amount FROM posting_lines WHERE id = $1")
        .bind(r[1].posting_line_id.unwrap()).fetch_one(&pool).await.expect("amt");
    assert_eq!(issue_amt, 500, "PAC issue should price at pre-batch snapshot avg 100, not running 150");

    let (bal, qty): (i64, i64) = sqlx::query_as(
        "SELECT balance, qty FROM accounts WHERE id = $1")
        .bind(raw).fetch_one(&pool).await.expect("bal");
    // 1000 + 2000 - 500 = 2500. qty = 10 + 10 - 5 = 15. Real avg = 166.67, drifted from PAC's 100.
    assert_eq!(bal, 2500);
    assert_eq!(qty, 15);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pac_issue_from_empty_pool_rejects() {
    let pool = pool().await;
    let tag = Uuid::new_v4().to_string();
    let (_ap, cogs, raw) = seed_wac_fixture(&pool, &tag).await;
    let res = sqlx::query_as::<_, BatchRow>("SELECT * FROM post_batch($1)")
        .bind(json!([{
            "envelope_idx": 0, "kind": "wac_pac_issue",
            "debit_account_id": cogs, "credit_account_id": raw,
            "qty": 5,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-12",
        }])).fetch_all(&pool).await;
    assert!(res.is_err(), "empty pool must reject");
}
