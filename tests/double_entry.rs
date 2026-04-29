//! Per-ledger double-entry invariant (B3 fix; doc Part IV §7).
//!
//! After arbitrary valid posts, `SUM(debits_total - credits_total)`
//! grouped by `(ledger_kind, currency)` is zero on every group. The
//! sum is taken **per ledger** — never as a single global sum across
//! ledger_kinds, which would silently mask qty-vs-value imbalances.

mod common;

use common::*;
use serde_json::json;

#[tokio::test]
async fn per_ledger_sum_is_zero_after_posts() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let ar = account_id_by_kind_currency(&pool, "ar", Some("USD")).await;
    let revenue = account_id_by_kind_currency(&pool, "revenue", Some("USD")).await;

    let key1 = fresh_uuid(&pool).await;
    let key2 = fresh_uuid(&pool).await;
    let events = json!([
        make_event("ar_invoice", ar, revenue, 1_000, "2026-04-15", &key1),
        make_event("ar_payment", cash, ar, 1_000, "2026-04-15", &key2),
    ]);

    let result = call_post_transfers(&pool, events, false)
        .await
        .expect("post_transfers ok");
    assert_eq!(
        result.as_array().map(|a| a.len()),
        Some(2),
        "expected 2 results, got: {result}"
    );

    let imbalances: Vec<(String, Option<String>, i64)> = sqlx::query_as(
        "SELECT ledger_kind, currency, SUM(debits_total - credits_total)::BIGINT
           FROM accounts
          GROUP BY ledger_kind, currency
         HAVING SUM(debits_total - credits_total) <> 0",
    )
    .fetch_all(&pool)
    .await
    .expect("imbalance query");

    assert!(
        imbalances.is_empty(),
        "per-ledger imbalance detected: {imbalances:?}"
    );
}
