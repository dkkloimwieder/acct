//! P0001 account_closed: post_posting_lines refuses any event whose
//! debit OR credit account has `is_closed = TRUE`. Closure is
//! permanent (the account is never reopened — a new account is
//! created instead).

mod common;

use common::*;
use serde_json::json;

#[tokio::test]
async fn post_against_closed_account_raises_p0001() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let revenue = account_id_by_kind_currency(&pool, "revenue", Some("USD")).await;

    sqlx::query(
        "UPDATE accounts SET is_closed = TRUE, closed_at = clock_timestamp() WHERE id = $1",
    )
    .bind(cash)
    .execute(&pool)
    .await
    .expect("close cash account");

    let key = fresh_uuid(&pool).await;
    let event = make_event("ar_payment", cash, revenue, 100, "2026-04-15", &key);

    expect_sqlstate("P0001", || call_post_posting_lines(&pool, json!([event]), false)).await;
}
