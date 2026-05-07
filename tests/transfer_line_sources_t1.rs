//! T1 probes for `transfer_line_sources` (mig 0104, acct-wb75.1.1).
//! Phase B1 of the convergence plan (acct-wb75). Pin the table-level
//! constraints — CHECK (not-all-NULL), CHECK (no-self-reversal),
//! FK to transfers, PK uniqueness — so a regression in the schema
//! surfaces here.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

async fn one_transfer(pool: &PgPool) -> i64 {
    let cash = account_id_by_kind_currency(pool, "cash", Some("USD")).await;
    let revenue = account_id_by_kind_currency(pool, "revenue", Some("USD")).await;
    let key = fresh_uuid(pool).await;
    let event = make_event("ar_payment", cash, revenue, 100, "2026-04-15", &key);
    call_post_transfers(pool, json!([event]), false)
        .await
        .expect("seed transfer");
    sqlx::query_scalar("SELECT id FROM transfers WHERE idempotency_key = $1::UUID")
        .bind(&key)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn all_null_extension_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let xfer = one_transfer(&pool).await;

    // Pure-NULL extension row is forbidden (CHECK 23514).
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO transfer_line_sources (transfer_id) VALUES ($1)",
        )
        .bind(xfer)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn self_reversal_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let xfer = one_transfer(&pool).await;

    // reverses_transfer_id = transfer_id is rejected.
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO transfer_line_sources
                (transfer_id, reverses_transfer_id) VALUES ($1, $1)",
        )
        .bind(xfer)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn unknown_transfer_id_fk_violation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    expect_sqlstate("23503", || async {
        sqlx::query(
            "INSERT INTO transfer_line_sources
                (transfer_id, created_by_process)
             VALUES (999999999, 'test')",
        )
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn unknown_reverses_transfer_id_fk_violation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let xfer = one_transfer(&pool).await;

    // reverses_transfer_id pointing to a nonexistent transfer → FK fail.
    expect_sqlstate("23503", || async {
        sqlx::query(
            "INSERT INTO transfer_line_sources
                (transfer_id, reverses_transfer_id)
             VALUES ($1, 999999999)",
        )
        .bind(xfer)
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

    sqlx::query(
        "INSERT INTO transfer_line_sources
            (transfer_id, created_by_process)
         VALUES ($1, 'first')",
    )
    .bind(xfer)
    .execute(&pool)
    .await
    .unwrap();

    expect_sqlstate("23505", || async {
        sqlx::query(
            "INSERT INTO transfer_line_sources
                (transfer_id, created_by_process)
             VALUES ($1, 'second')",
        )
        .bind(xfer)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn any_single_field_satisfies_not_all_null() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let x1 = one_transfer(&pool).await;
    let x2 = one_transfer(&pool).await;
    let x3 = one_transfer(&pool).await;
    let x4 = one_transfer(&pool).await;
    let x5 = one_transfer(&pool).await;

    // process-only.
    sqlx::query(
        "INSERT INTO transfer_line_sources
            (transfer_id, created_by_process)
         VALUES ($1, 'unit_test')",
    )
    .bind(x1)
    .execute(&pool)
    .await
    .unwrap();

    // reverses-only.
    sqlx::query(
        "INSERT INTO transfer_line_sources
            (transfer_id, reverses_transfer_id)
         VALUES ($1, $2)",
    )
    .bind(x2)
    .bind(x1)
    .execute(&pool)
    .await
    .unwrap();

    // parent-doc-only.
    sqlx::query(
        "INSERT INTO transfer_line_sources
            (transfer_id, parent_document_id)
         VALUES ($1, gen_random_uuid())",
    )
    .bind(x3)
    .execute(&pool)
    .await
    .unwrap();

    // intercompany-pair-only.
    sqlx::query(
        "INSERT INTO transfer_line_sources
            (transfer_id, intercompany_pair_id)
         VALUES ($1, gen_random_uuid())",
    )
    .bind(x4)
    .execute(&pool)
    .await
    .unwrap();

    // multi-field combination.
    sqlx::query(
        "INSERT INTO transfer_line_sources
            (transfer_id, reverses_transfer_id,
             parent_document_id, intercompany_pair_id, created_by_process)
         VALUES ($1, $2, gen_random_uuid(), gen_random_uuid(), 'multi')",
    )
    .bind(x5)
    .bind(x1)
    .execute(&pool)
    .await
    .unwrap();

    let n: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM transfer_line_sources")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(n, 5);
}

#[tokio::test]
async fn dispatcher_writes_extension_when_field_present() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let revenue = account_id_by_kind_currency(&pool, "revenue", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let mut event = make_event("ar_payment", cash, revenue, 100, "2026-04-15", &key);
    let ic_pair = fresh_uuid(&pool).await;
    event["created_by_process"] = json!("post_ar_payment");
    event["intercompany_pair_id"] = json!(ic_pair);

    call_post_transfers(&pool, json!([event]), false)
        .await
        .expect("post");

    let xfer: i64 =
        sqlx::query_scalar("SELECT id FROM transfers WHERE idempotency_key = $1::UUID")
            .bind(&key)
            .fetch_one(&pool)
            .await
            .unwrap();

    let proc: String = sqlx::query_scalar(
        "SELECT created_by_process FROM transfer_line_sources WHERE transfer_id = $1",
    )
    .bind(xfer)
    .fetch_one(&pool)
    .await
    .expect("extension row exists");
    assert_eq!(proc, "post_ar_payment");
}

#[tokio::test]
async fn dispatcher_skips_extension_when_no_fields_present() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let revenue = account_id_by_kind_currency(&pool, "revenue", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let event = make_event("ar_payment", cash, revenue, 100, "2026-04-15", &key);
    // No B1 extension fields on the event JSONB.
    call_post_transfers(&pool, json!([event]), false)
        .await
        .expect("post");

    let xfer: i64 =
        sqlx::query_scalar("SELECT id FROM transfers WHERE idempotency_key = $1::UUID")
            .bind(&key)
            .fetch_one(&pool)
            .await
            .unwrap();

    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM transfer_line_sources WHERE transfer_id = $1",
    )
    .bind(xfer)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n, 0);
}

#[tokio::test]
async fn dispatcher_writes_reverses_pointer() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let revenue = account_id_by_kind_currency(&pool, "revenue", Some("USD")).await;

    // First transfer.
    let k1 = fresh_uuid(&pool).await;
    let e1 = make_event("ar_payment", cash, revenue, 100, "2026-04-15", &k1);
    call_post_transfers(&pool, json!([e1]), false).await.unwrap();
    let t1: i64 =
        sqlx::query_scalar("SELECT id FROM transfers WHERE idempotency_key = $1::UUID")
            .bind(&k1)
            .fetch_one(&pool)
            .await
            .unwrap();

    // Reversing transfer (debit/credit swapped, reverses_transfer_id set).
    let k2 = fresh_uuid(&pool).await;
    let mut e2 = make_event("ar_payment", revenue, cash, 100, "2026-04-15", &k2);
    e2["reverses_transfer_id"] = json!(t1);
    call_post_transfers(&pool, json!([e2]), false).await.unwrap();
    let t2: i64 =
        sqlx::query_scalar("SELECT id FROM transfers WHERE idempotency_key = $1::UUID")
            .bind(&k2)
            .fetch_one(&pool)
            .await
            .unwrap();

    let reversed: i64 = sqlx::query_scalar(
        "SELECT reverses_transfer_id FROM transfer_line_sources WHERE transfer_id = $1",
    )
    .bind(t2)
    .fetch_one(&pool)
    .await
    .expect("extension row");
    assert_eq!(reversed, t1);
}
