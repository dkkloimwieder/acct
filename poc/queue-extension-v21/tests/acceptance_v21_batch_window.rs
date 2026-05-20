//! acct-0sc4 acceptance: `poc_v21.batch_window_us` wires into
//! `router_tick()` as a time-coalesce emission gate. Two scenarios:
//!
//!   1. **window=0**: gate disabled. A single envelope enqueued in
//!      isolation emits on the next router_tick.
//!      `router_window_defers_total` stays 0.
//!
//!   2. **window=2_000_000 (2s)**: gate active. An envelope enqueued
//!      immediately before a router_tick is deferred — the tick returns
//!      0 and `router_window_defers_total` increments. After waiting
//!      past the window, the next router_tick emits.
//!
//! Test pauses the BGWorker so only the explicit
//! `poc_v21_test_router_tick()` calls drive the router. ALTER SYSTEM
//! manipulates the GUC; pg_reload_conf publishes Sighup-scope changes
//! to the running BGWorker (it reads via `BATCH_WINDOW_US.get()` per
//! tick, so the change lands within ~50ms after reload).
//!
//! Run via:
//!   cargo test --release --test acceptance_v21_batch_window \
//!     --features pg18,test_hooks --no-default-features -- --ignored --nocapture

#![cfg(test)]

mod common;

use common::{connect_pool, pause_bgworker, reset_state, resume_bgworker};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use uuid::Uuid;

const ADMIN_DSN: &str = "postgres://acct:acct_dev@localhost:5111/postgres";

async fn admin_pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(10))
        .connect(ADMIN_DSN)
        .await
        .expect("admin connect")
}

async fn set_batch_window_us(admin: &PgPool, us: i64) {
    sqlx::query(&format!(
        "ALTER SYSTEM SET poc_v21.batch_window_us = {us}"
    ))
    .execute(admin)
    .await
    .expect("alter system set");
    sqlx::query("SELECT pg_reload_conf()")
        .execute(admin)
        .await
        .expect("pg_reload_conf");
    // Give the BGWorker latch cycle time to pick up the SIGHUP. Not
    // strictly required because the test drives router_tick() directly
    // from a backend (where the GUC is read at call time), but
    // belt-and-suspenders against any future caching layer.
    tokio::time::sleep(Duration::from_millis(100)).await;
}

async fn reset_batch_window(admin: &PgPool) {
    sqlx::query("ALTER SYSTEM RESET poc_v21.batch_window_us")
        .execute(admin)
        .await
        .ok();
    sqlx::query("SELECT pg_reload_conf()")
        .execute(admin)
        .await
        .ok();
}

async fn enqueue_one(pool: &PgPool, cid: Uuid, sku: i64, chrono: i64) {
    let payload = serde_json::json!({
        "sku_id": sku,
        "location_id": 1,
        "qty": 5,
        "unit_cost": 100,
        "business_date_jdate": 9999,
        "doc_chrono": chrono,
        "document_id": 7_300_000 + chrono,
    });
    let pool_keys = serde_json::json!({ "sku": [[sku, 1]], "wip": [] });
    sqlx::query("SELECT poc_v21_enqueue($1::uuid, $2, $3::jsonb, $4::jsonb, false)")
        .bind(cid)
        .bind("po_receipt")
        .bind(payload)
        .bind(pool_keys)
        .execute(pool)
        .await
        .expect("enqueue");
}

async fn router_tick(pool: &PgPool) -> bool {
    sqlx::query_scalar("SELECT poc_v21_test_router_tick()")
        .fetch_one(pool)
        .await
        .expect("test_router_tick")
}

async fn defers_total(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT poc_v21_router_window_defers_total()")
        .fetch_one(pool)
        .await
        .expect("router_window_defers_total")
}

async fn sb_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT poc_v21_router_superbatch_count()")
        .fetch_one(pool)
        .await
        .expect("router_superbatch_count")
}

async fn reset_router_stats(pool: &PgPool) {
    sqlx::query("SELECT poc_v21_router_stats_reset()")
        .execute(pool)
        .await
        .expect("router_stats_reset");
}

async fn seed_std(pool: &PgPool, sku: i64) {
    sqlx::query(
        "INSERT INTO poc_v21_sku_method_assignments (sku_id, method_id) \
         VALUES ($1, 'std') ON CONFLICT (sku_id) DO NOTHING",
    )
    .bind(sku)
    .execute(pool)
    .await
    .expect("seed method");
    sqlx::query(
        "INSERT INTO poc_v21_standard_costs (sku_id, location_id, unit_cost, effective_from) \
         VALUES ($1, 1, 100, now() - interval '1 day') \
         ON CONFLICT (sku_id, location_id, effective_from) DO NOTHING",
    )
    .bind(sku)
    .execute(pool)
    .await
    .expect("seed std cost");
    sqlx::query("SELECT poc_v21_invalidate_committer_caches()")
        .execute(pool)
        .await
        .expect("invalidate");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn acceptance_v21_batch_window_disabled_emits_immediately() {
    let pool = connect_pool().await;
    let admin = admin_pool().await;

    reset_state(&pool).await;
    seed_std(&pool, 7301).await;
    pause_bgworker(&pool).await;
    set_batch_window_us(&admin, 0).await;
    reset_router_stats(&pool).await;

    enqueue_one(&pool, Uuid::new_v4(), 7301, 1).await;

    let before_defers = defers_total(&pool).await;
    let before_sb = sb_count(&pool).await;

    let emitted = router_tick(&pool).await;
    assert!(
        emitted,
        "with batch_window_us=0, a single pending envelope must emit on the next router_tick"
    );

    let after_defers = defers_total(&pool).await;
    let after_sb = sb_count(&pool).await;
    assert_eq!(
        after_defers, before_defers,
        "window=0 must never increment router_window_defers_total ({} → {})",
        before_defers, after_defers
    );
    assert_eq!(
        after_sb,
        before_sb + 1,
        "exactly one SuperBatch should have been emitted"
    );

    resume_bgworker(&pool).await;
    reset_batch_window(&admin).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn acceptance_v21_batch_window_active_defers_then_emits() {
    let pool = connect_pool().await;
    let admin = admin_pool().await;

    reset_state(&pool).await;
    seed_std(&pool, 7302).await;
    pause_bgworker(&pool).await;

    // 2 second window — well past the BGWorker's 50ms tick cadence and
    // any test scheduling jitter, so timing is unambiguous.
    set_batch_window_us(&admin, 2_000_000).await;
    reset_router_stats(&pool).await;

    enqueue_one(&pool, Uuid::new_v4(), 7302, 1).await;

    // Immediate tick should defer: oldest age (~10ms) < window (2s).
    let emitted_first = router_tick(&pool).await;
    assert!(
        !emitted_first,
        "with batch_window_us=2s and an envelope enqueued <100ms ago, \
         the immediate router_tick must defer (return false)"
    );

    let defers_after_first = defers_total(&pool).await;
    assert_eq!(
        defers_after_first, 1,
        "router_window_defers_total should be 1 after the deferred tick, got {}",
        defers_after_first
    );
    assert_eq!(
        sb_count(&pool).await,
        0,
        "no SuperBatch should have emitted yet"
    );

    // Wait past the 2s window + margin.
    tokio::time::sleep(Duration::from_millis(2_200)).await;

    // Now the gate releases.
    let emitted_second = router_tick(&pool).await;
    assert!(
        emitted_second,
        "after waiting past the window, router_tick must emit"
    );
    assert_eq!(
        sb_count(&pool).await,
        1,
        "exactly one SuperBatch should have emitted post-window"
    );

    resume_bgworker(&pool).await;
    reset_batch_window(&admin).await;
}
