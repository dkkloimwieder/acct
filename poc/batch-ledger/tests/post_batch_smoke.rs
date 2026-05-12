//! P3 correctness tests for post_batch.

use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

async fn pool() -> PgPool {
    let url = std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());
    PgPool::connect(&url).await.expect("connect")
}

async fn seed_pair(pool: &PgPool, tag: &str) -> (i64, i64) {
    sqlx::query_as::<_, (i64, i64)>(
        "WITH ins AS (
             INSERT INTO accounts (code, currency, kind)
                 VALUES ($1, 'USD', 'debit_normal'),
                        ($2, 'USD', 'credit_normal')
             RETURNING id
         )
         SELECT MIN(id), MAX(id) FROM ins",
    )
    .bind(format!("p3-cash-{tag}"))
    .bind(format!("p3-rev-{tag}"))
    .fetch_one(pool)
    .await
    .expect("seed")
}

#[derive(sqlx::FromRow, Debug)]
struct BatchRow {
    envelope_idx: i32,
    status: String,
    posting_line_id: Option<i64>,
    #[allow(dead_code)]
    error_code: Option<String>,
    #[allow(dead_code)]
    error_message: Option<String>,
}

fn envelope(idx: i32, debit: i64, credit: i64, amount: i64, idem: Uuid) -> Value {
    json!({
        "envelope_idx": idx,
        "debit_account_id": debit,
        "credit_account_id": credit,
        "amount": amount,
        "idempotency_key": idem.to_string(),
        "business_date": "2026-05-11",
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_of_one_commits() {
    let pool = pool().await;
    let tag = Uuid::new_v4().to_string();
    let (a, b) = seed_pair(&pool, &tag).await;
    let envs = json!([envelope(0, a, b, 100, Uuid::new_v4())]);

    let rows = sqlx::query_as::<_, BatchRow>("SELECT * FROM post_batch($1)")
        .bind(envs)
        .fetch_all(&pool)
        .await
        .expect("post_batch");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].envelope_idx, 0);
    assert_eq!(rows[0].status, "committed");
    assert!(rows[0].posting_line_id.is_some());

    // Balance moved.
    let (ba, bb): (i64, i64) = sqlx::query_as(
        "SELECT (SELECT balance FROM accounts WHERE id = $1),
                (SELECT balance FROM accounts WHERE id = $2)",
    )
    .bind(a)
    .bind(b)
    .fetch_one(&pool)
    .await
    .expect("read");
    assert_eq!(ba, 100);
    assert_eq!(bb, -100);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_of_many_commits_atomically() {
    let pool = pool().await;
    let tag = Uuid::new_v4().to_string();
    let (a, b) = seed_pair(&pool, &tag).await;

    let n = 50_i64;
    let mut envs = Vec::new();
    for i in 0..n {
        envs.push(envelope(i as i32, a, b, 10, Uuid::new_v4()));
    }
    let rows = sqlx::query_as::<_, BatchRow>("SELECT * FROM post_batch($1)")
        .bind(Value::Array(envs))
        .fetch_all(&pool)
        .await
        .expect("post_batch");

    assert_eq!(rows.len(), n as usize);
    assert!(rows.iter().all(|r| r.status == "committed"));
    let unique_ids: std::collections::HashSet<_> =
        rows.iter().filter_map(|r| r.posting_line_id).collect();
    assert_eq!(unique_ids.len(), n as usize);

    let (ba, bb): (i64, i64) = sqlx::query_as(
        "SELECT (SELECT balance FROM accounts WHERE id = $1),
                (SELECT balance FROM accounts WHERE id = $2)",
    )
    .bind(a)
    .bind(b)
    .fetch_one(&pool)
    .await
    .expect("read");
    assert_eq!(ba, n * 10);
    assert_eq!(bb, -n * 10);

    let row_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM posting_lines WHERE debit_account_id = $1 OR credit_account_id = $1",
    )
    .bind(a)
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(row_count, n);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replays_return_existing_posting_line_ids() {
    let pool = pool().await;
    let tag = Uuid::new_v4().to_string();
    let (a, b) = seed_pair(&pool, &tag).await;

    let idem = Uuid::new_v4();
    let env = envelope(0, a, b, 100, idem);

    let r1 = sqlx::query_as::<_, BatchRow>("SELECT * FROM post_batch($1)")
        .bind(json!([env.clone()]))
        .fetch_all(&pool)
        .await
        .expect("first");
    assert_eq!(r1[0].status, "committed");
    let pl_id_1 = r1[0].posting_line_id.unwrap();

    // Replay: same envelope, same idempotency_key.
    let r2 = sqlx::query_as::<_, BatchRow>("SELECT * FROM post_batch($1)")
        .bind(json!([env]))
        .fetch_all(&pool)
        .await
        .expect("second");
    assert_eq!(r2[0].status, "idempotent_replay");
    assert_eq!(r2[0].posting_line_id.unwrap(), pl_id_1);

    // Balance only moved ONCE.
    let ba: i64 = sqlx::query_scalar("SELECT balance FROM accounts WHERE id = $1")
        .bind(a)
        .fetch_one(&pool)
        .await
        .expect("balance");
    assert_eq!(ba, 100);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_replay_and_new_in_same_batch() {
    let pool = pool().await;
    let tag = Uuid::new_v4().to_string();
    let (a, b) = seed_pair(&pool, &tag).await;

    // First batch: idem_1, idem_2.
    let idem_1 = Uuid::new_v4();
    let idem_2 = Uuid::new_v4();
    let _: Vec<BatchRow> = sqlx::query_as("SELECT * FROM post_batch($1)")
        .bind(json!([
            envelope(0, a, b, 100, idem_1),
            envelope(1, a, b, 200, idem_2),
        ]))
        .fetch_all(&pool)
        .await
        .expect("first batch");

    // Second batch: replay idem_1, NEW idem_3.
    let idem_3 = Uuid::new_v4();
    let r = sqlx::query_as::<_, BatchRow>("SELECT * FROM post_batch($1)")
        .bind(json!([
            envelope(0, a, b, 100, idem_1),  // replay
            envelope(1, a, b, 500, idem_3),  // new
        ]))
        .fetch_all(&pool)
        .await
        .expect("second batch");
    assert_eq!(r.len(), 2);
    assert_eq!(r[0].status, "idempotent_replay");
    assert_eq!(r[1].status, "committed");

    // Balance: 100 + 200 + 500 = 800.
    let ba: i64 = sqlx::query_scalar("SELECT balance FROM accounts WHERE id = $1")
        .bind(a)
        .fetch_one(&pool)
        .await
        .expect("balance");
    assert_eq!(ba, 800);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn whole_batch_rollback_on_check_violation() {
    let pool = pool().await;
    let tag = Uuid::new_v4().to_string();
    let (a, b) = seed_pair(&pool, &tag).await;

    // Envelope 1 has amount=0 which violates CHECK(amount > 0). Whole batch
    // must roll back; balance unchanged.
    let bad_batch = json!([
        envelope(0, a, b, 100, Uuid::new_v4()),
        envelope(1, a, b, 0,   Uuid::new_v4()),  // violates CHECK
        envelope(2, a, b, 300, Uuid::new_v4()),
    ]);
    let res = sqlx::query_as::<_, BatchRow>("SELECT * FROM post_batch($1)")
        .bind(bad_batch)
        .fetch_all(&pool)
        .await;
    assert!(res.is_err(), "expected CHECK violation");

    let ba: i64 = sqlx::query_scalar("SELECT balance FROM accounts WHERE id = $1")
        .bind(a)
        .fetch_one(&pool)
        .await
        .expect("balance");
    assert_eq!(ba, 0, "balance must be unchanged after whole-batch rollback");

    let row_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM posting_lines WHERE debit_account_id = $1 OR credit_account_id = $1",
    )
    .bind(a)
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(row_count, 0);
}
