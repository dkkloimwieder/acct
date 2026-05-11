//! P1 smoke test — schema applies + INSERT/SELECT round-trip + idempotency UNIQUE.
//!
//! Requires the dev container (localhost:5111) up and `scripts/setup.sh` to have
//! created `acct_poc`. Override DB via `POC_DATABASE_URL` env if needed.
//!
//! Tests use per-test Uuid-tagged account codes so they can run in parallel
//! without colliding on `accounts.code` UNIQUE.

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
    .bind(format!("cash-{tag}"))
    .bind(format!("rev-{tag}"))
    .fetch_one(pool)
    .await
    .expect("seed")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_round_trip() {
    let pool = pool().await;
    let tag = Uuid::new_v4().to_string();
    let (a, b) = seed_pair(&pool, &tag).await;

    let idem = Uuid::new_v4();
    let pl_id: i64 = sqlx::query_scalar(
        "INSERT INTO posting_lines
             (debit_account_id, credit_account_id, amount, currency,
              idempotency_key, business_date)
         VALUES ($1, $2, $3, $4, $5, CURRENT_DATE)
         RETURNING id",
    )
    .bind(a)
    .bind(b)
    .bind(1_000_i64)
    .bind("USD")
    .bind(idem)
    .fetch_one(&pool)
    .await
    .expect("insert");

    assert!(pl_id > 0);

    let (amount, currency): (i64, String) =
        sqlx::query_as("SELECT amount, currency FROM posting_lines WHERE id = $1")
            .bind(pl_id)
            .fetch_one(&pool)
            .await
            .expect("read");

    assert_eq!(amount, 1_000);
    assert_eq!(currency, "USD");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotency_key_unique() {
    let pool = pool().await;
    let tag = Uuid::new_v4().to_string();
    let (a, b) = seed_pair(&pool, &tag).await;

    let idem = Uuid::new_v4();

    let _: i64 = sqlx::query_scalar(
        "INSERT INTO posting_lines
             (debit_account_id, credit_account_id, amount, currency,
              idempotency_key, business_date)
         VALUES ($1, $2, $3, $4, $5, CURRENT_DATE)
         RETURNING id",
    )
    .bind(a)
    .bind(b)
    .bind(500_i64)
    .bind("USD")
    .bind(idem)
    .fetch_one(&pool)
    .await
    .expect("first insert");

    let err = sqlx::query(
        "INSERT INTO posting_lines
             (debit_account_id, credit_account_id, amount, currency,
              idempotency_key, business_date)
         VALUES ($1, $2, $3, $4, $5, CURRENT_DATE)",
    )
    .bind(a)
    .bind(b)
    .bind(500_i64)
    .bind("USD")
    .bind(idem)
    .execute(&pool)
    .await
    .expect_err("second insert with same key must violate UNIQUE");

    let msg = format!("{err}");
    assert!(
        msg.contains("idempotency_key") || msg.contains("duplicate") || msg.contains("23505"),
        "expected unique-violation error, got: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amount_must_be_positive() {
    let pool = pool().await;
    let tag = Uuid::new_v4().to_string();
    let (a, b) = seed_pair(&pool, &tag).await;

    let err = sqlx::query(
        "INSERT INTO posting_lines
             (debit_account_id, credit_account_id, amount, currency,
              idempotency_key, business_date)
         VALUES ($1, $2, $3, $4, $5, CURRENT_DATE)",
    )
    .bind(a)
    .bind(b)
    .bind(0_i64)
    .bind("USD")
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await
    .expect_err("amount=0 must violate CHECK");

    let msg = format!("{err}");
    assert!(msg.contains("amount") || msg.contains("check"), "got: {msg}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn self_loop_rejected() {
    let pool = pool().await;
    let tag = Uuid::new_v4().to_string();
    let (a, _b) = seed_pair(&pool, &tag).await;

    let err = sqlx::query(
        "INSERT INTO posting_lines
             (debit_account_id, credit_account_id, amount, currency,
              idempotency_key, business_date)
         VALUES ($1, $1, $2, $3, $4, CURRENT_DATE)",
    )
    .bind(a)
    .bind(100_i64)
    .bind("USD")
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await
    .expect_err("self-loop must violate CHECK");

    let msg = format!("{err}");
    assert!(msg.contains("check") || msg.contains("debit_account_id"), "got: {msg}");
}
