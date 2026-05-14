//! P5 FIFO correctness tests for post_batch.

use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

async fn pool() -> PgPool {
    let url = std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());
    PgPool::connect(&url).await.expect("connect")
}

/// (ap, cogs, pool_raw)
async fn seed_fifo_fixture(pool: &PgPool, tag: &str) -> (i64, i64, i64) {
    let row: (i64, i64, i64) = sqlx::query_as(
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
    .bind(format!("p5-ap-{tag}"))
    .bind(format!("p5-cogs-{tag}"))
    .bind(format!("p5-raw-{tag}"))
    .fetch_one(pool).await.expect("seed");
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
async fn fifo_receipt_creates_cost_layer() {
    let pool = pool().await;
    let tag = Uuid::new_v4().to_string();
    let (ap, _cogs, raw) = seed_fifo_fixture(&pool, &tag).await;

    let r = sqlx::query_as::<_, BatchRow>("SELECT * FROM post_batch_fifo($1)")
        .bind(json!([{
            "envelope_idx": 0, "kind": "fifo_receipt",
            "debit_account_id": raw, "credit_account_id": ap,
            "qty": 100, "unit_cost": 50,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-11",
        }]))
        .fetch_all(&pool).await.expect("post");
    assert_eq!(r[0].status, "committed");

    let (layer_count, layer_qty, layer_cost): (i64, i64, i64) = sqlx::query_as(
        "SELECT COUNT(*), SUM(qty_remaining)::BIGINT, MAX(unit_cost) FROM cost_layers WHERE pool_account_id = $1")
        .bind(raw).fetch_one(&pool).await.expect("count");
    assert_eq!(layer_count, 1);
    assert_eq!(layer_qty, 100);
    assert_eq!(layer_cost, 50);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fifo_issue_walks_layers_in_receipt_date_order() {
    let pool = pool().await;
    let tag = Uuid::new_v4().to_string();
    let (ap, cogs, raw) = seed_fifo_fixture(&pool, &tag).await;

    // Layer 1: 10 @ 100, business_date 2026-05-01
    // Layer 2: 10 @ 200, business_date 2026-05-02
    // Issue 15: should take all 10 from L1 (cost 1000) + 5 from L2 (cost 1000) = 2000
    let _: Vec<BatchRow> = sqlx::query_as("SELECT * FROM post_batch_fifo($1)")
        .bind(json!([{
            "envelope_idx": 0, "kind": "fifo_receipt",
            "debit_account_id": raw, "credit_account_id": ap,
            "qty": 10, "unit_cost": 100,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-01",
        }]))
        .fetch_all(&pool).await.expect("L1");
    let _: Vec<BatchRow> = sqlx::query_as("SELECT * FROM post_batch_fifo($1)")
        .bind(json!([{
            "envelope_idx": 0, "kind": "fifo_receipt",
            "debit_account_id": raw, "credit_account_id": ap,
            "qty": 10, "unit_cost": 200,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-02",
        }]))
        .fetch_all(&pool).await.expect("L2");

    let r = sqlx::query_as::<_, BatchRow>("SELECT * FROM post_batch_fifo($1)")
        .bind(json!([{
            "envelope_idx": 0, "kind": "fifo_issue",
            "debit_account_id": cogs, "credit_account_id": raw,
            "qty": 15,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-03",
        }]))
        .fetch_all(&pool).await.expect("issue");
    assert_eq!(r[0].status, "committed");

    let amount: i64 = sqlx::query_scalar("SELECT amount FROM posting_lines WHERE id = $1")
        .bind(r[0].posting_line_id.unwrap()).fetch_one(&pool).await.expect("amt");
    assert_eq!(amount, 2000, "10*100 + 5*200 = 2000");

    let (dep_count, dep_total): (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*), SUM(cost_amount)::BIGINT FROM cost_layer_depletions WHERE issue_posting_line_id = $1")
        .bind(r[0].posting_line_id.unwrap()).fetch_one(&pool).await.expect("dep");
    assert_eq!(dep_count, 2);
    assert_eq!(dep_total, 2000);

    let layer_state: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT unit_cost, qty_remaining FROM cost_layers WHERE pool_account_id = $1 ORDER BY receipt_date")
        .bind(raw).fetch_all(&pool).await.expect("layers");
    assert_eq!(layer_state, vec![(100, 0), (200, 5)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fifo_in_batch_receipt_visible_to_later_issue() {
    let pool = pool().await;
    let tag = Uuid::new_v4().to_string();
    let (ap, cogs, raw) = seed_fifo_fixture(&pool, &tag).await;

    // Single batch: receipt 20 @ 50, then issue 5. Issue should consume 5 @ 50 = 250.
    let r = sqlx::query_as::<_, BatchRow>("SELECT * FROM post_batch_fifo($1)")
        .bind(json!([
            {
                "envelope_idx": 0, "kind": "fifo_receipt",
                "debit_account_id": raw, "credit_account_id": ap,
                "qty": 20, "unit_cost": 50,
                "idempotency_key": Uuid::new_v4().to_string(),
                "business_date": "2026-05-01",
            },
            {
                "envelope_idx": 1, "kind": "fifo_issue",
                "debit_account_id": cogs, "credit_account_id": raw,
                "qty": 5,
                "idempotency_key": Uuid::new_v4().to_string(),
                "business_date": "2026-05-02",
            },
        ]))
        .fetch_all(&pool).await.expect("mixed");
    assert_eq!(r[0].status, "committed");
    assert_eq!(r[1].status, "committed");

    let issue_amount: i64 = sqlx::query_scalar("SELECT amount FROM posting_lines WHERE id = $1")
        .bind(r[1].posting_line_id.unwrap()).fetch_one(&pool).await.expect("amt");
    assert_eq!(issue_amount, 250);

    let layer_remaining: i64 = sqlx::query_scalar(
        "SELECT SUM(qty_remaining)::BIGINT FROM cost_layers WHERE pool_account_id = $1")
        .bind(raw).fetch_one(&pool).await.expect("rem");
    assert_eq!(layer_remaining, 15);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fifo_issue_exceeding_pool_rejects_whole_batch() {
    let pool = pool().await;
    let tag = Uuid::new_v4().to_string();
    let (ap, cogs, raw) = seed_fifo_fixture(&pool, &tag).await;

    let _: Vec<BatchRow> = sqlx::query_as("SELECT * FROM post_batch_fifo($1)")
        .bind(json!([{
            "envelope_idx": 0, "kind": "fifo_receipt",
            "debit_account_id": raw, "credit_account_id": ap,
            "qty": 5, "unit_cost": 100,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-01",
        }]))
        .fetch_all(&pool).await.expect("seed");

    let res = sqlx::query_as::<_, BatchRow>("SELECT * FROM post_batch_fifo($1)")
        .bind(json!([{
            "envelope_idx": 0, "kind": "fifo_issue",
            "debit_account_id": cogs, "credit_account_id": raw,
            "qty": 10,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-02",
        }]))
        .fetch_all(&pool).await;
    assert!(res.is_err());

    let (bal, qty, rem): (i64, i64, i64) = sqlx::query_as(
        "SELECT a.balance, a.qty,
                COALESCE((SELECT SUM(qty_remaining) FROM cost_layers WHERE pool_account_id = a.id), 0)::BIGINT
         FROM accounts a WHERE a.id = $1")
        .bind(raw).fetch_one(&pool).await.expect("read");
    assert_eq!(bal, 500); assert_eq!(qty, 5); assert_eq!(rem, 5);
}
