//! Transfers are append-only (doc Part IV §1.9). The
//! `trg_transfers_append_only` BEFORE UPDATE OR DELETE trigger
//! installed in 0008 raises P9999 on any modification. Reversals are
//! new transfers — never edits.

mod common;

use common::*;
use serde_json::json;

#[tokio::test]
async fn update_and_delete_transfers_raise_p9999() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let revenue = account_id_by_kind_currency(&pool, "revenue", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let event = make_event("ar_payment", cash, revenue, 100, "2026-04-15", &key);
    call_post_transfers(&pool, json!([event]), false)
        .await
        .expect("seed transfer row");

    expect_sqlstate("P9999", || async {
        sqlx::query("UPDATE transfers SET amount = 1 WHERE idempotency_key = $1::UUID")
            .bind(&key)
            .execute(&pool)
            .await
    })
    .await;

    expect_sqlstate("P9999", || async {
        sqlx::query("DELETE FROM transfers WHERE idempotency_key = $1::UUID")
            .bind(&key)
            .execute(&pool)
            .await
    })
    .await;
}
