//! Acceptance tests for the `posted_at` write contract (acct-1vur.6; 0027,
//! design-v3.2 §3a).
//!
//! The recalc engine replays each pool in `(pool_id, posted_at, id)` order, so
//! R-1 correctness rests on `posted_at` carrying the event's TRUE business
//! time. No constraint can enforce truthfulness — the database cannot know
//! what actually happened when — so these cases pin the parts that ARE
//! mechanical: the column can never be omitted or silently defaulted, the
//! staging path validates it, and business time genuinely drives replay order
//! rather than commit order.
//!
//! They also pin the one concrete hazard future-dating created: the engine's
//! own adjustment output must never lower a recost floor, even when a pool's
//! frontier sits above `now()`.

mod common;

use common::*;
use serde_json::json;

/// Business time — not commit order — decides the fold. Committing in reverse
/// business order must produce the same authoritative costs as committing in
/// order; if a writer stamped a constant (the v3.1 failure mode) both would
/// collapse to id order and this would not hold.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn replay_follows_business_time_not_commit_order() {
    let pool = connect_pool().await;

    // In business order: cheap lot first.
    reset_state(&pool).await;
    let a = seed_fixture(&pool, "fifo", "running_avg").await;
    receipt_at(&pool, a.pool_id, 1, T09, 10, 100).await;
    receipt_at(&pool, a.pool_id, 2, T10, 10, 300).await;
    let da = deplete_at(&pool, a.pool_id, 3, T11, 15).await;
    mark_dirty(&pool, a.pool_id).await;
    drain_recalc(&pool).await;
    let in_order = authoritative_of(&pool, da).await;

    // Same business dates, committed in REVERSE order.
    reset_state(&pool).await;
    let b = seed_fixture(&pool, "fifo", "running_avg").await;
    let db = deplete_at(&pool, b.pool_id, 3, T11, 15).await;
    receipt_at(&pool, b.pool_id, 2, T10, 10, 300).await;
    receipt_at(&pool, b.pool_id, 1, T09, 10, 100).await;
    mark_dirty(&pool, b.pool_id).await;
    drain_recalc(&pool).await;

    assert_eq!(
        authoritative_of(&pool, db).await,
        in_order,
        "business time drives the fold; commit order must not change it"
    );
    assert_eq!(in_order, Some(167), "FIFO draws 10@100 then 5@300");
}

/// `posted_at` can never be omitted: no column carries a default, so a writer
/// that forgets it fails loud instead of silently inheriting now() — which is
/// exactly how a constant/wall-clock stamp would enter the system unnoticed.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn posted_at_is_never_defaulted_or_nullable() {
    let pool = connect_pool().await;

    let defaults: Vec<(String, Option<String>, String)> = sqlx::query_as(
        "SELECT table_name::text, column_default, is_nullable::text \
           FROM information_schema.columns \
          WHERE column_name = 'posted_at' AND table_schema = 'public' \
          ORDER BY table_name",
    )
    .fetch_all(&pool)
    .await
    .expect("read posted_at columns");

    assert!(!defaults.is_empty());
    for (table, default, nullable) in &defaults {
        assert_eq!(default.as_deref(), None, "{table}.posted_at must have NO default");
        assert_eq!(nullable, "NO", "{table}.posted_at must be NOT NULL");
    }
}

/// The staging path is the one place a producer writes the ledger without
/// going through `ledger_submit_trx`, so the envelope's business time is
/// validated there rather than trusted.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn staging_envelopes_must_carry_a_business_time() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;

    let err = sqlx::query(
        "INSERT INTO ledger_inbox (trx_type, source_id, posted_at, lines) \
         VALUES ('po_receipt', 1, NULL, $1::jsonb)",
    )
    .bind(json!([line(f.pool_id, "po_receipt_line", 5, 100)]))
    .execute(&pool)
    .await
    .unwrap_err();
    assert!(
        ["23502", "22004"].contains(&sqlstate(&err).as_str()),
        "a null business time is refused: {err}"
    );

    // A real one flows.
    enqueue(&pool, "po_receipt", 1, json!([line(f.pool_id, "po_receipt_line", 5, 100)])).await;
    assert_eq!(drain(&pool, 10).await, 1);
}

/// Backdating is admitted BY DESIGN (R-2), so no recency bound may exist —
/// and future-dating is likewise permitted, because business events carry
/// legitimate future effective dates.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn business_time_may_run_behind_or_ahead_of_wall_clock() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;

    // Far past and far future both land: the contract is truthfulness, not
    // recency, and there is no horizon.
    receipt_at(&pool, f.pool_id, 1, "2001-03-04T09:00:00+00:00", 10, 100).await;
    receipt_at(&pool, f.pool_id, 2, "2099-12-31T23:00:00+00:00", 10, 300).await;
    assert_eq!(count(&pool, "SELECT count(*) FROM trx_line").await, 2);
}

/// The hazard future-dating created, fixed at its root: a pool whose frontier
/// sits ABOVE now() must not have its own `cost_adjustment_line` output lower
/// a recost floor. Otherwise the engine re-folds, emits another adjustment,
/// and loops forever.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn engine_output_never_lowers_a_floor_on_a_future_dated_pool() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let consumer = reset_feed(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;

    // Every event is future-dated, so the settled frontier ends up ABOVE
    // now() — which is what makes the engine's own now()-stamped adjustment
    // sort BELOW the frontier.
    receipt_at(&pool, f.pool_id, 1, "2099-01-01T09:00:00+00:00", 10, 100).await;
    receipt_at(&pool, f.pool_id, 2, "2099-01-01T10:00:00+00:00", 10, 300).await;
    let d = deplete_at(&pool, f.pool_id, 3, "2099-01-01T11:00:00+00:00", 15).await;
    ingest_all(&consumer).await;
    drain_recalc(&pool).await;
    assert_eq!(authoritative_of(&pool, d).await, Some(167));
    assert!(
        count(&pool, "SELECT count(*) FROM trx_line WHERE line_type = 'cost_adjustment_line'")
            .await
            > 0,
        "the pass posted an adjustment, which is the row that must not lower a floor"
    );

    // Deliver the engine's own output. It must lower nothing and settle into
    // a fixed point rather than a re-fold loop.
    let loopback = consumer.ingest_once(10_000).await.expect("loopback ingest");
    assert!(loopback.inserts >= 1, "the adjustment line was delivered");
    assert_eq!(loopback.floors_lowered, 0, "engine output must never lower a floor");
    assert!(!settlement_of(&pool, f.pool_id).await.unwrap().2, "no floor set");

    drain_recalc(&pool).await;
    ingest_all(&consumer).await;
    drain_recalc(&pool).await;
    assert!(!settlement_of(&pool, f.pool_id).await.unwrap().2, "still no floor: converged");
    assert_eq!(count(&pool, "SELECT count(*) FROM recalc_queue").await, 0, "engine at rest");

    drop_feed_slot(&pool).await;
}
