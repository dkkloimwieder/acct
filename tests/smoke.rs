mod common;

use common::connect_test_db;

/// Smoke test: verify the harness can reach the test DB, that migrations
/// and the fixture have been applied (run-tests.sh seeds before invoking
/// cargo test), and that a basic SELECT round-trips. Read-only.
#[tokio::test]
async fn fixture_is_loaded() {
    let pool = connect_test_db().await;

    let sku_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM skus")
        .fetch_one(&pool)
        .await
        .expect("query skus count");
    assert_eq!(sku_count, 10, "fixture should seed 10 SKUs");

    let acct_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM accounts")
        .fetch_one(&pool)
        .await
        .expect("query accounts count");
    assert!(
        acct_count >= 15,
        "fixture should seed at least 15 accounts, got {acct_count}"
    );

    let period_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM periods")
        .fetch_one(&pool)
        .await
        .expect("query periods count");
    assert_eq!(period_count, 4, "fixture should seed 4 periods");
}
