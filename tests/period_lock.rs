//! Period lock is a schema invariant (doc Part IV §6 / §2.1):
//! `post_transfers` raises P0005 when the resolved period is closed,
//! unless the caller explicitly passes `p_override_closed_period =
//! TRUE`. The fixture seeds `2026-03` as the closed period.

mod common;

use common::*;
use serde_json::json;

#[tokio::test]
async fn closed_period_blocks_post_unless_override() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let revenue = account_id_by_kind_currency(&pool, "revenue", Some("USD")).await;

    let key_blocked = fresh_uuid(&pool).await;
    let blocked = make_event("ar_payment", cash, revenue, 100, "2026-03-15", &key_blocked);
    expect_sqlstate("P0005", || {
        call_post_transfers(&pool, json!([blocked]), false)
    })
    .await;

    let key_override = fresh_uuid(&pool).await;
    let allowed = make_event("ar_payment", cash, revenue, 100, "2026-03-15", &key_override);
    let result = call_post_transfers(&pool, json!([allowed]), true)
        .await
        .expect("override post");
    assert_eq!(result[0]["result"], "ok", "override should succeed: {result}");
}
