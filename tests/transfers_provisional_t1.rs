//! T1 probes for `transfers_provisional` (migration 0025). Audit
//! `acct-ool` fold-in: the matrix tests exercise the workflow; these
//! pin the table-level CHECK / FK constraints so a regression in the
//! schema surfaces here.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

/// Insert one transfer the cleanest way and return its id. Used as the
/// FK target for the probe rows.
async fn one_transfer(pool: &PgPool) -> i64 {
    let cash = account_id_by_kind_currency(pool, "cash", Some("USD")).await;
    let revenue = account_id_by_kind_currency(pool, "revenue", Some("USD")).await;
    let key = fresh_uuid(pool).await;
    let event = make_event("ar_payment", cash, revenue, 100, "2026-04-15", &key);
    call_post_transfers(pool, json!([event]), false).await.expect("seed transfer");
    sqlx::query_scalar("SELECT id FROM transfers WHERE idempotency_key = $1::UUID")
        .bind(&key)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn one_period_id(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT id FROM periods WHERE code = '2026-04'")
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn unfinalized_with_variance_amount_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let xfer = one_transfer(&pool).await;
    let period = one_period_id(&pool).await;

    // finalized_at IS NULL but variance_amount is set → CHECK fail (23514).
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO transfers_provisional
                (transfer_id, period_id, cost_method, finalized_at,
                 variance_amount, variance_transfer_id)
             VALUES ($1, $2, 'wac_periodic', NULL, 100, NULL)",
        )
        .bind(xfer)
        .bind(period)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn finalized_zero_variance_with_transfer_id_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let xfer = one_transfer(&pool).await;
    let other_xfer = one_transfer(&pool).await;
    let period = one_period_id(&pool).await;

    // finalized + variance=0 must have variance_transfer_id NULL.
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO transfers_provisional
                (transfer_id, period_id, cost_method, finalized_at,
                 variance_amount, variance_transfer_id)
             VALUES ($1, $2, 'wac_periodic', clock_timestamp(), 0, $3)",
        )
        .bind(xfer)
        .bind(period)
        .bind(other_xfer)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn finalized_nonzero_variance_without_transfer_id_is_allowed() {
    // Tier 2 (acct-smn, mig 0065): the close hook's internal-chain
    // op_move_v finalization records non-zero variance_amount with
    // variance_transfer_id NULL. The CHECK was relaxed to permit this.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let xfer = one_transfer(&pool).await;
    let period = one_period_id(&pool).await;

    sqlx::query(
        "INSERT INTO transfers_provisional
            (transfer_id, period_id, cost_method, finalized_at,
             variance_amount, variance_transfer_id)
         VALUES ($1, $2, 'wac_periodic', clock_timestamp(), 50, NULL)",
    )
    .bind(xfer)
    .bind(period)
    .execute(&pool)
    .await
    .expect("non-zero variance with NULL transfer_id allowed post-tier-2");
}

#[tokio::test]
async fn variance_transfer_id_self_reference_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let xfer = one_transfer(&pool).await;
    let period = one_period_id(&pool).await;

    // variance_transfer_id == transfer_id is rejected (would self-loop).
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO transfers_provisional
                (transfer_id, period_id, cost_method, finalized_at,
                 variance_amount, variance_transfer_id)
             VALUES ($1, $2, 'wac_periodic', clock_timestamp(), 50, $1)",
        )
        .bind(xfer)
        .bind(period)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn unknown_transfer_id_fk_violation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let period = one_period_id(&pool).await;

    // transfer_id pointing to a non-existent transfer → FK violation
    // (23503).
    expect_sqlstate("23503", || async {
        sqlx::query(
            "INSERT INTO transfers_provisional (transfer_id, period_id, cost_method)
             VALUES (999999999, $1, 'wac_periodic')",
        )
        .bind(period)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn duplicate_transfer_id_violates_pk() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let xfer = one_transfer(&pool).await;
    let period = one_period_id(&pool).await;

    sqlx::query(
        "INSERT INTO transfers_provisional (transfer_id, period_id, cost_method)
         VALUES ($1, $2, 'wac_periodic')",
    )
    .bind(xfer)
    .bind(period)
    .execute(&pool)
    .await
    .unwrap();

    // PK violation — transfer_id is the PK of transfers_provisional, so
    // a transfer can be flagged at most once.
    expect_sqlstate("23505", || async {
        sqlx::query(
            "INSERT INTO transfers_provisional (transfer_id, period_id, cost_method)
             VALUES ($1, $2, 'wac_retroactive')",
        )
        .bind(xfer)
        .bind(period)
        .execute(&pool)
        .await
    })
    .await;
}
