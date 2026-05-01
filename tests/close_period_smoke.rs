//! `acct-v51` / `acct-s6n.2` — smoke test for close_period() and hooks.
//!
//! Acceptance scope (per the bd issue):
//!   * close_period() applies and round-trips
//!   * Stub hooks all callable and return 0
//!   * Happy-path close on a clean open period succeeds and stamps
//!     periods.closed_at + closed_by.
//!
//! The full P0014 / P0015 / P0016 gate matrix is acct-dno (s6n.3).

mod common;

use common::*;

async fn period_id(pool: &sqlx::PgPool, code: &str) -> i64 {
    sqlx::query_scalar("SELECT id FROM periods WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("period {code}: {e}"))
}

#[tokio::test]
async fn hook_stubs_all_return_zero() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pid = period_id(&pool, "2026-04").await;

    for hook in [
        "wac_periodic_close_hook",
        "wac_retroactive_close_hook",
        "cost_adjust_retroactive_hook",
    ] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT {hook}($1)"))
            .bind(pid)
            .fetch_one(&pool)
            .await
            .unwrap_or_else(|e| panic!("{hook}: {e}"));
        assert_eq!(count, 0, "{hook} stub should return 0");
    }
}

#[tokio::test]
async fn close_period_happy_path_stamps_period() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;

    let summary: serde_json::Value = sqlx::query_scalar(
        "SELECT close_period($1, $2::UUID, FALSE, FALSE)",
    )
    .bind(pid)
    .bind(&actor)
    .fetch_one(&pool)
    .await
    .expect("close_period happy path");

    assert_eq!(summary["period_id"].as_i64(), Some(pid));
    assert_eq!(summary["period_code"].as_str(), Some("2026-04"));
    assert_eq!(summary["finalized_count"].as_i64(), Some(0));
    assert_eq!(summary["hook_results"]["wac_periodic"].as_i64(), Some(0));
    assert_eq!(
        summary["hook_results"]["wac_retroactive"].as_i64(),
        Some(0)
    );
    assert_eq!(
        summary["hook_results"]["cost_adjust_retroactive"].as_i64(),
        Some(0)
    );
    assert_eq!(summary["unfinalized_remaining"].as_i64(), Some(0));
    assert_eq!(summary["alerts"].as_i64(), Some(0));
    assert_eq!(summary["forced"]["provisional"].as_bool(), Some(false));
    assert_eq!(summary["forced"]["recon"].as_bool(), Some(false));
    assert!(summary["closed_at"].is_string());
    assert_eq!(summary["closed_by"].as_str(), Some(actor.as_str()));

    // periods row was actually stamped.
    let (closed_at, closed_by): (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT closed_at::text, closed_by::text FROM periods WHERE id = $1",
    )
    .bind(pid)
    .fetch_one(&pool)
    .await
    .expect("re-read period");
    assert!(closed_at.is_some(), "closed_at should be stamped");
    assert_eq!(closed_by, Some(actor));
}

#[tokio::test]
async fn close_period_default_force_flags_omittable() {
    // Verify the DEFAULT FALSE on the force flags works — caller can
    // pass just (period_id, actor) when both gates are satisfied.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pid = period_id(&pool, "2026-05").await;
    let actor = fresh_uuid(&pool).await;

    let _: serde_json::Value =
        sqlx::query_scalar("SELECT close_period($1, $2::UUID)")
            .bind(pid)
            .bind(&actor)
            .fetch_one(&pool)
            .await
            .expect("close_period with defaulted flags");
}
