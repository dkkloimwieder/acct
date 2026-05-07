//! P0002 ledger_mismatch: a transfer cannot cross ledger_kinds.
//! qty ↔ value is rejected. This is what keeps the per-ledger
//! double-entry sum from leaking across kinds (doc Part IV §7).

mod common;

use common::*;
use serde_json::json;

#[tokio::test]
async fn qty_to_value_transfer_raises_p0002() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let stock = account_id_stock_available(&pool, "SKU-A", "MAIN").await;

    let key = fresh_uuid(&pool).await;
    let event = make_event("cycle_count_adj", cash, stock, 100, "2026-04-15", &key);

    expect_sqlstate("P0002", || call_post_posting_lines(&pool, json!([event]), false)).await;
}
