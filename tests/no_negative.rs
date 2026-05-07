//! L2 balance-respects-normal-side CHECK rejects overdraw (SQLSTATE
//! 23514). Doc Part IV §1.5: a debit-normal account's credits_total
//! must never exceed its debits_total (and vice versa for credit-
//! normal). Posting a credit against a debit-normal stock_available
//! that has zero debits violates the invariant.

mod common;

use common::*;
use serde_json::json;

#[tokio::test]
async fn overdraw_debit_normal_account_raises_23514() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // creation_void (qty, unrestricted) on the debit side is the
    // canonical "balance from nothing" pattern; stock_available
    // (qty, debit-normal, debits=0) on the credit side trips the CHECK.
    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let stock = account_id_stock_available(&pool, "SKU-A", "MAIN").await;

    let key = fresh_uuid(&pool).await;
    let events = json!([make_event(
        "cycle_count_adj",
        void_qty,
        stock,
        100,
        "2026-04-15",
        &key,
    )]);

    expect_sqlstate("23514", || call_post_posting_lines(&pool, events, false)).await;
}
