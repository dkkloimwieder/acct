//! P0003 currency_mismatch: within the value ledger, a transfer
//! requires both accounts to share the same currency. Cross-currency
//! flows go through paired fx_leg + fx_spread transfers — never a
//! direct USD↔EUR post.

mod common;

use common::*;
use serde_json::json;

#[tokio::test]
async fn cross_currency_value_transfer_raises_p0003() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let cash_usd = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let cash_eur = account_id_by_kind_currency(&pool, "cash", Some("EUR")).await;

    let key = fresh_uuid(&pool).await;
    let event = make_event("ar_payment", cash_usd, cash_eur, 100, "2026-04-15", &key);

    expect_sqlstate("P0003", || call_post_transfers(&pool, json!([event]), false)).await;
}
