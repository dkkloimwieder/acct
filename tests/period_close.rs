//! `acct-dno` / `acct-s6n.3` — close_period() gate behavior matrix.
//!
//! Acceptance:
//!   * P0014 fires for missing or already-closed periods.
//!   * P0015 fires when transfers_provisional has un-finalized rows in
//!     the period; p_force_provisional bypasses it.
//!   * P0016 fires when run_daily_reconciliation() raises new alerts;
//!     p_force_recon bypasses it.
//!   * Force flags act independently of each other.
//!   * All three close-hooks are invoked, in the spec'd order.
//!   * Concurrent callers serialize via FOR UPDATE: exactly one
//!     succeeds, the other raises P0014.
//!   * Once closed, post_transfers' P0005 still fires on writes into
//!     that period.

mod common;

use common::*;
use serde_json::Value;

async fn period_id(pool: &sqlx::PgPool, code: &str) -> i64 {
    sqlx::query_scalar("SELECT id FROM periods WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("period {code}: {e}"))
}

async fn period_closed_at(pool: &sqlx::PgPool, pid: i64) -> Option<String> {
    sqlx::query_scalar("SELECT closed_at::text FROM periods WHERE id = $1")
        .bind(pid)
        .fetch_one(pool)
        .await
        .expect("period closed_at")
}

/// Wrapper around `SELECT close_period(...)` that returns the JSONB on
/// success or surfaces the sqlx::Error so callers can assert SQLSTATE.
async fn call_close_period(
    pool: &sqlx::PgPool,
    pid: i64,
    actor: &str,
    force_provisional: bool,
    force_recon: bool,
) -> sqlx::Result<Value> {
    sqlx::query_scalar("SELECT close_period($1, $2::UUID, $3, $4)")
        .bind(pid)
        .bind(actor)
        .bind(force_provisional)
        .bind(force_recon)
        .fetch_one(pool)
        .await
}

/// Insert a single un-finalized transfers_provisional row in the named
/// period. Posts a placeholder transfer first so we have an FK-able id.
async fn make_unfinalized_provisional(pool: &sqlx::PgPool, pid: i64) {
    let stock = account_id_stock_available(pool, "SKU-A", "MAIN").await;
    let void = account_id_by_kind_currency(pool, "creation_void", None).await;
    let key = fresh_uuid(pool).await;
    let event = make_event("cycle_count_adj", stock, void, 1, "2026-04-15", &key);
    let _ = call_post_transfers(pool, serde_json::json!([event]), false)
        .await
        .expect("seed transfer");
    let tid: i64 = sqlx::query_scalar(
        "SELECT id FROM transfers WHERE idempotency_key = $1::UUID",
    )
    .bind(&key)
    .fetch_one(pool)
    .await
    .expect("transfer id");
    sqlx::query(
        "INSERT INTO transfers_provisional (transfer_id, period_id, cost_method)
         VALUES ($1, $2, 'wac_periodic')",
    )
    .bind(tid)
    .bind(pid)
    .execute(pool)
    .await
    .expect("insert un-finalized");
}

/// Engineer a reservation_over_promise alert: insert an active
/// reservation that exceeds on-hand on (SKU-A, MAIN). Bypasses
/// reserve_inventory(), which is what the recon check is designed to
/// catch.
async fn engineer_recon_alert(pool: &sqlx::PgPool) {
    let so_id = fresh_sales_order(pool).await;
    let so_line_id = fresh_uuid(pool).await;
    sqlx::query(
        "INSERT INTO inventory_reservations
            (sku_id, location_id, qty, so_id, so_line_id, expires_at, status)
         SELECT s.id, l.id, 100, $1::UUID, $2::UUID,
                clock_timestamp() + INTERVAL '1 hour', 'active'
           FROM skus s, locations l
          WHERE s.code = 'SKU-A' AND l.code = 'MAIN'",
    )
    .bind(&so_id)
    .bind(&so_line_id)
    .execute(pool)
    .await
    .expect("engineer reservation");
}

// ---- Happy paths ---------------------------------------------------

#[tokio::test]
async fn close_period_clean_state_succeeds() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;

    let _summary = call_close_period(&pool, pid, &actor, false, false)
        .await
        .expect("clean close");

    assert!(period_closed_at(&pool, pid).await.is_some());
}

#[tokio::test]
async fn close_period_returns_summary_jsonb() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;

    let summary = call_close_period(&pool, pid, &actor, false, false)
        .await
        .unwrap();

    assert_eq!(summary["period_id"].as_i64(), Some(pid));
    assert_eq!(summary["period_code"].as_str(), Some("2026-04"));
    assert_eq!(summary["finalized_count"].as_i64(), Some(0));
    assert_eq!(summary["alerts"].as_i64(), Some(0));
    assert_eq!(summary["unfinalized_remaining"].as_i64(), Some(0));
    assert_eq!(summary["forced"]["provisional"].as_bool(), Some(false));
    assert_eq!(summary["forced"]["recon"].as_bool(), Some(false));
    assert_eq!(summary["closed_by"].as_str(), Some(actor.as_str()));
    assert!(summary["closed_at"].is_string());
    let hooks = &summary["hook_results"];
    assert_eq!(hooks["wac_periodic"].as_i64(), Some(0));
    assert_eq!(hooks["wac_retroactive"].as_i64(), Some(0));
    assert_eq!(hooks["cost_adjust_retroactive"].as_i64(), Some(0));
}

// ---- P0014 ---------------------------------------------------------

#[tokio::test]
async fn close_period_unknown_id_raises_p0014() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let actor = fresh_uuid(&pool).await;

    expect_sqlstate("P0014", || async {
        call_close_period(&pool, 9_999_999, &actor, false, false)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn close_period_already_closed_raises_p0014() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    // Fixture seeds 2026-03 as closed.
    let pid = period_id(&pool, "2026-03").await;
    let actor = fresh_uuid(&pool).await;

    expect_sqlstate("P0014", || async {
        call_close_period(&pool, pid, &actor, false, false)
            .await
            .map(|_| ())
    })
    .await;
}

// ---- P0015 (provisional gate) --------------------------------------

#[tokio::test]
async fn close_period_blocks_on_unfinalized_provisional() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;
    make_unfinalized_provisional(&pool, pid).await;

    expect_sqlstate("P0015", || async {
        call_close_period(&pool, pid, &actor, false, false)
            .await
            .map(|_| ())
    })
    .await;

    // Period MUST still be open after a refused close.
    assert!(period_closed_at(&pool, pid).await.is_none());
}

#[tokio::test]
async fn close_period_force_provisional_bypasses_p0015() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;
    make_unfinalized_provisional(&pool, pid).await;

    let summary = call_close_period(&pool, pid, &actor, true, false)
        .await
        .expect("force_provisional should bypass P0015");

    assert_eq!(summary["unfinalized_remaining"].as_i64(), Some(1));
    assert_eq!(summary["forced"]["provisional"].as_bool(), Some(true));
    assert!(period_closed_at(&pool, pid).await.is_some());

    // Documented behavior: forced close does NOT auto-finalize the
    // un-finalized row. It stays as-is for forensics.
    let still_unfin: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM transfers_provisional
          WHERE period_id = $1 AND finalized_at IS NULL",
    )
    .bind(pid)
    .fetch_one(&pool)
    .await
    .expect("count un-finalized");
    assert_eq!(still_unfin, 1);
}

// ---- P0016 (reconciliation gate) -----------------------------------

#[tokio::test]
async fn close_period_blocks_on_recon_alerts() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;
    engineer_recon_alert(&pool).await;

    expect_sqlstate("P0016", || async {
        call_close_period(&pool, pid, &actor, false, false)
            .await
            .map(|_| ())
    })
    .await;

    assert!(period_closed_at(&pool, pid).await.is_none());
}

#[tokio::test]
async fn close_period_force_recon_bypasses_p0016() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;
    engineer_recon_alert(&pool).await;

    let summary = call_close_period(&pool, pid, &actor, false, true)
        .await
        .expect("force_recon should bypass P0016");

    assert!(summary["alerts"].as_i64().unwrap() >= 1);
    assert_eq!(summary["forced"]["recon"].as_bool(), Some(true));
    assert!(period_closed_at(&pool, pid).await.is_some());
}

// ---- Force-flag independence ---------------------------------------

#[tokio::test]
async fn close_period_force_provisional_does_not_bypass_recon() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;
    make_unfinalized_provisional(&pool, pid).await;
    engineer_recon_alert(&pool).await;

    expect_sqlstate("P0016", || async {
        call_close_period(&pool, pid, &actor, true, false)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn close_period_force_recon_does_not_bypass_provisional() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;
    make_unfinalized_provisional(&pool, pid).await;
    engineer_recon_alert(&pool).await;

    // Provisional gate runs BEFORE recon, so P0015 surfaces first.
    expect_sqlstate("P0015", || async {
        call_close_period(&pool, pid, &actor, false, true)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn close_period_both_force_flags_bypass_both_gates() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;
    make_unfinalized_provisional(&pool, pid).await;
    engineer_recon_alert(&pool).await;

    let summary = call_close_period(&pool, pid, &actor, true, true)
        .await
        .expect("both force flags should bypass both gates");

    assert_eq!(summary["forced"]["provisional"].as_bool(), Some(true));
    assert_eq!(summary["forced"]["recon"].as_bool(), Some(true));
    assert!(summary["unfinalized_remaining"].as_i64().unwrap() >= 1);
    assert!(summary["alerts"].as_i64().unwrap() >= 1);
    assert!(period_closed_at(&pool, pid).await.is_some());
}

// ---- Hook dispatch -------------------------------------------------

/// Spy: redefines the three hooks to log into a side table, runs
/// close_period, and asserts the call order. Restores the original
/// stub bodies on the way out so other tests aren't affected.
#[tokio::test]
async fn close_period_calls_all_three_hook_stubs() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;

    sqlx::raw_sql(
        "CREATE TABLE _hook_log (n BIGSERIAL PRIMARY KEY, hook_name TEXT NOT NULL);

         CREATE OR REPLACE FUNCTION wac_periodic_close_hook(p_period_id BIGINT)
         RETURNS BIGINT LANGUAGE plpgsql AS $$
         BEGIN INSERT INTO _hook_log (hook_name) VALUES ('wac_periodic'); RETURN 0; END;
         $$;

         CREATE OR REPLACE FUNCTION wac_retroactive_close_hook(p_period_id BIGINT)
         RETURNS BIGINT LANGUAGE plpgsql AS $$
         BEGIN INSERT INTO _hook_log (hook_name) VALUES ('wac_retroactive'); RETURN 0; END;
         $$;

         CREATE OR REPLACE FUNCTION cost_adjust_retroactive_hook(p_period_id BIGINT)
         RETURNS BIGINT LANGUAGE plpgsql AS $$
         BEGIN INSERT INTO _hook_log (hook_name) VALUES ('cost_adjust_retroactive'); RETURN 0; END;
         $$;",
    )
    .execute(&pool)
    .await
    .expect("install hook spies");

    let _ = call_close_period(&pool, pid, &actor, false, false)
        .await
        .expect("close with spies");

    let order: Vec<String> =
        sqlx::query_scalar("SELECT hook_name FROM _hook_log ORDER BY n")
            .fetch_all(&pool)
            .await
            .expect("read hook log");
    assert_eq!(
        order,
        vec!["wac_periodic", "wac_retroactive", "cost_adjust_retroactive"],
        "hooks must be invoked in the documented order"
    );

    // Restore the stub bodies so subsequent tests in this binary
    // (and other binaries sharing the DB) see the canonical no-op
    // hooks again.
    sqlx::raw_sql(
        "DROP TABLE _hook_log;

         CREATE OR REPLACE FUNCTION wac_periodic_close_hook(p_period_id BIGINT)
         RETURNS BIGINT LANGUAGE plpgsql AS $$ BEGIN RETURN 0; END; $$;

         CREATE OR REPLACE FUNCTION wac_retroactive_close_hook(p_period_id BIGINT)
         RETURNS BIGINT LANGUAGE plpgsql AS $$ BEGIN RETURN 0; END; $$;

         CREATE OR REPLACE FUNCTION cost_adjust_retroactive_hook(p_period_id BIGINT)
         RETURNS BIGINT LANGUAGE plpgsql AS $$ BEGIN RETURN 0; END; $$;",
    )
    .execute(&pool)
    .await
    .expect("restore stubs");
}

// ---- Idempotency / safety ------------------------------------------

#[tokio::test]
async fn close_period_concurrent_callers_serialize() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pid = period_id(&pool, "2026-04").await;
    let actor1 = fresh_uuid(&pool).await;
    let actor2 = fresh_uuid(&pool).await;

    let (r1, r2) = tokio::join!(
        call_close_period(&pool, pid, &actor1, false, false),
        call_close_period(&pool, pid, &actor2, false, false),
    );

    let succ = r1.is_ok() as i32 + r2.is_ok() as i32;
    assert_eq!(succ, 1, "exactly one of two parallel closes must win");

    let err = match (r1, r2) {
        (Err(e), Ok(_)) | (Ok(_), Err(e)) => e,
        _ => unreachable!(),
    };
    let db_err = err.as_database_error().expect("db error");
    assert_eq!(
        db_err.code().unwrap().as_ref(),
        "P0014",
        "loser must surface period_close_invalid (already closed): {}",
        db_err.message()
    );

    // Period must end up closed.
    assert!(period_closed_at(&pool, pid).await.is_some());
}

#[tokio::test]
async fn closed_period_blocks_subsequent_post_transfers_per_p0005() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;

    let _ = call_close_period(&pool, pid, &actor, false, false)
        .await
        .expect("clean close");
    assert!(period_closed_at(&pool, pid).await.is_some());

    // Subsequent post_transfers into 2026-04 must hit the existing
    // P0005 gate. The qty leg picks any in-period business_date.
    let stock = account_id_stock_available(&pool, "SKU-A", "MAIN").await;
    let void = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let key = fresh_uuid(&pool).await;
    let event = make_event("cycle_count_adj", stock, void, 1, "2026-04-15", &key);

    expect_sqlstate("P0005", || async {
        call_post_transfers(&pool, serde_json::json!([event]), false)
            .await
            .map(|_| ())
    })
    .await;
}
