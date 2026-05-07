//! Idempotency invariant: replaying an event with the same
//! `idempotency_key` returns `result='exists'` instead of posting a
//! duplicate. Balances mutate exactly once. The batch is **not**
//! rolled back on duplicates — the duplicate event simply skips.

mod common;

use common::*;
use serde_json::json;

#[tokio::test]
async fn duplicate_idempotency_key_skips_without_double_post() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let revenue = account_id_by_kind_currency(&pool, "revenue", Some("USD")).await;

    let key = fresh_uuid(&pool).await;
    let event = make_event("ar_payment", cash, revenue, 500, "2026-04-15", &key);

    let r1 = call_post_posting_lines(&pool, json!([event.clone()]), false)
        .await
        .expect("first post");
    assert_eq!(r1[0]["result"], "ok", "first call should be ok: {r1}");

    let r2 = call_post_posting_lines(&pool, json!([event]), false)
        .await
        .expect("second post");
    assert_eq!(
        r2[0]["result"], "exists",
        "second call should be exists: {r2}"
    );

    let row_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM posting_lines WHERE idempotency_key = $1::UUID")
            .bind(&key)
            .fetch_one(&pool)
            .await
            .expect("count transfers");
    assert_eq!(row_count, 1, "exactly one transfer row should exist");

    let cash_debits: i64 = sqlx::query_scalar("SELECT debits_total FROM accounts WHERE id = $1")
        .bind(cash)
        .fetch_one(&pool)
        .await
        .expect("cash debits");
    let rev_credits: i64 = sqlx::query_scalar("SELECT credits_total FROM accounts WHERE id = $1")
        .bind(revenue)
        .fetch_one(&pool)
        .await
        .expect("revenue credits");
    assert_eq!(cash_debits, 500);
    assert_eq!(rev_credits, 500);
}
