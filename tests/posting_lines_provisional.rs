//! `acct-4mt` / `acct-s6n.1` — Schema smoke test for posting_lines_provisional.
//!
//! This is the foundational schema layer for the period-close machinery
//! (acct-s6n). The orchestration function close_period() and its hooks
//! land in s6n.2 (acct-v51) — this test verifies only the table and
//! its CHECK constraint shape, plus the three valid lifecycle states:
//!   1. un-finalized           (insertable from the writer side)
//!   2. finalized no-variance  (close_period found nothing to correct)
//!   3. finalized w/ variance  (close_period posted a variance transfer)
//!
//! Plus the negative cases that the CHECK constraint must reject.

mod common;

use common::*;

/// Post a one-off transfer between two qty accounts so we have a real
/// `transfers.id` to FK against. Returns the transfer's BIGSERIAL id.
async fn post_one_transfer(pool: &sqlx::PgPool, qty: i64) -> i64 {
    let stock = account_id_stock_available(pool, "SKU-A", "MAIN").await;
    let void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let key = fresh_uuid(pool).await;
    let event = make_event("cycle_count_adj", stock, void_qty, qty, "2026-04-15", &key);
    let _ = call_post_posting_lines(pool, serde_json::json!([event]), false)
        .await
        .expect("post_posting_lines");
    sqlx::query_scalar::<_, i64>(
        "SELECT id FROM posting_lines WHERE idempotency_key = $1::UUID",
    )
    .bind(&key)
    .fetch_one(pool)
    .await
    .expect("lookup transfer id")
}

async fn period_id(pool: &sqlx::PgPool, code: &str) -> i64 {
    sqlx::query_scalar("SELECT id FROM periods WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("period {code}: {e}"))
}

#[tokio::test]
async fn three_lifecycle_states_round_trip() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pid = period_id(&pool, "2026-04").await;

    // Four transfers: one per provisional row + one variance posting.
    let t_unfinalized = post_one_transfer(&pool, 10).await;
    let t_finalized_zero = post_one_transfer(&pool, 20).await;
    let t_finalized_var = post_one_transfer(&pool, 30).await;
    let t_variance_posting = post_one_transfer(&pool, 40).await;

    // State 1: un-finalized.
    sqlx::query(
        "INSERT INTO posting_lines_provisional
            (posting_line_id, period_id, cost_method)
         VALUES ($1, $2, 'wac_periodic')",
    )
    .bind(t_unfinalized)
    .bind(pid)
    .execute(&pool)
    .await
    .expect("insert un-finalized");

    // State 2: finalized, no variance.
    sqlx::query(
        "INSERT INTO posting_lines_provisional
            (posting_line_id, period_id, cost_method,
             finalized_at, variance_amount, variance_posting_line_id)
         VALUES ($1, $2, 'wac_retroactive',
                 clock_timestamp(), 0, NULL)",
    )
    .bind(t_finalized_zero)
    .bind(pid)
    .execute(&pool)
    .await
    .expect("insert finalized no-variance");

    // State 3: finalized with variance.
    sqlx::query(
        "INSERT INTO posting_lines_provisional
            (posting_line_id, period_id, cost_method,
             finalized_at, variance_amount, variance_posting_line_id)
         VALUES ($1, $2, 'wac_periodic',
                 clock_timestamp(), 500, $3)",
    )
    .bind(t_finalized_var)
    .bind(pid)
    .bind(t_variance_posting)
    .execute(&pool)
    .await
    .expect("insert finalized with variance");

    // Partial index hot path: un-finalized rows for this period.
    let unfin: Vec<i64> = sqlx::query_scalar(
        "SELECT posting_line_id FROM posting_lines_provisional
          WHERE period_id = $1 AND finalized_at IS NULL
          ORDER BY posting_line_id",
    )
    .bind(pid)
    .fetch_all(&pool)
    .await
    .expect("scan un-finalized");
    assert_eq!(unfin, vec![t_unfinalized]);

    // Total round-trip: 3 rows.
    let total: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM posting_lines_provisional WHERE period_id = $1")
            .bind(pid)
            .fetch_one(&pool)
            .await
            .expect("count");
    assert_eq!(total, 3);

    // Spot-check the variance-bearing row's payload.
    let (var_amt, var_tid): (Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT variance_amount, variance_posting_line_id
           FROM posting_lines_provisional WHERE posting_line_id = $1",
    )
    .bind(t_finalized_var)
    .fetch_one(&pool)
    .await
    .expect("read variance row");
    assert_eq!(var_amt, Some(500));
    assert_eq!(var_tid, Some(t_variance_posting));
}

#[tokio::test]
async fn check_rejects_unfinalized_with_variance_amount() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pid = period_id(&pool, "2026-04").await;
    let tid = post_one_transfer(&pool, 5).await;

    // Un-finalized (finalized_at NULL) cannot carry a variance_amount.
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO posting_lines_provisional
                (posting_line_id, period_id, cost_method, variance_amount)
             VALUES ($1, $2, 'wac_periodic', 100)",
        )
        .bind(tid)
        .bind(pid)
        .execute(&pool)
        .await
        .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn check_rejects_finalized_without_variance_amount() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pid = period_id(&pool, "2026-04").await;
    let tid = post_one_transfer(&pool, 5).await;

    // Finalized rows must record the variance amount (even if zero).
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO posting_lines_provisional
                (posting_line_id, period_id, cost_method, finalized_at)
             VALUES ($1, $2, 'wac_periodic', clock_timestamp())",
        )
        .bind(tid)
        .bind(pid)
        .execute(&pool)
        .await
        .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn check_rejects_zero_variance_with_transfer_id() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pid = period_id(&pool, "2026-04").await;
    let tid = post_one_transfer(&pool, 5).await;
    let other = post_one_transfer(&pool, 6).await;

    // A zero-variance close shouldn't have posted a transfer.
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO posting_lines_provisional
                (posting_line_id, period_id, cost_method,
                 finalized_at, variance_amount, variance_posting_line_id)
             VALUES ($1, $2, 'wac_periodic',
                     clock_timestamp(), 0, $3)",
        )
        .bind(tid)
        .bind(pid)
        .bind(other)
        .execute(&pool)
        .await
        .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn finalized_nonzero_variance_without_transfer_id_is_allowed() {
    // Tier 2 (acct-smn, mig 0065): the close hook's internal-chain
    // op_move_v finalization records a non-zero variance_amount with
    // variance_posting_line_id NULL — the cost shift propagates via the
    // topological cache, no audit transfer is posted. The CHECK was
    // relaxed to permit this. Zero-variance + non-NULL posting_line_id and
    // finalized + NULL variance are still rejected (verified by
    // siblings above).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pid = period_id(&pool, "2026-04").await;
    let tid = post_one_transfer(&pool, 5).await;

    sqlx::query(
        "INSERT INTO posting_lines_provisional
            (posting_line_id, period_id, cost_method,
             finalized_at, variance_amount, variance_posting_line_id)
         VALUES ($1, $2, 'wac_periodic',
                 clock_timestamp(), 500, NULL)",
    )
    .bind(tid)
    .bind(pid)
    .execute(&pool)
    .await
    .expect("non-zero variance with NULL posting_line_id is allowed post-tier-2");
}

#[tokio::test]
async fn check_rejects_self_referencing_variance_transfer() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pid = period_id(&pool, "2026-04").await;
    let tid = post_one_transfer(&pool, 5).await;

    // The variance posting must be a *different* transfer from the
    // provisional one being closed out.
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO posting_lines_provisional
                (posting_line_id, period_id, cost_method,
                 finalized_at, variance_amount, variance_posting_line_id)
             VALUES ($1, $2, 'wac_periodic',
                     clock_timestamp(), 500, $1)",
        )
        .bind(tid)
        .bind(pid)
        .execute(&pool)
        .await
        .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn variance_account_kinds_are_seeded_in_both_currencies() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // All three new account_kinds must exist in USD and EUR with
    // normal_side='unrestricted', so close_period() can resolve them
    // by (kind, currency) without further configuration.
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM accounts
          WHERE kind::text IN (
                  'variance_wac_periodic',
                  'variance_wac_retroactive',
                  'variance_cost_adjust_retroactive')
            AND ledger_kind = 'value'
            AND currency IN ('USD', 'EUR')
            AND normal_side = 'unrestricted'
            AND NOT is_closed",
    )
    .fetch_one(&pool)
    .await
    .expect("count variance accounts");
    assert_eq!(count, 6, "3 kinds × 2 currencies = 6 unrestricted accounts");
}
