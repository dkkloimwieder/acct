//! R3 canonical (acct-shpc.4, spec §4.3 line 1675): interleaved
//! overlapping envelopes with `batch_size_max=2`.
//!
//! Spec scenario verbatim: inject [Env_A on SKU 5, Env_B on SKU 6,
//! Env_C on SKU 5] with batch_size_max=2, assert Env_A and Env_C land
//! in the same SuperBatch, Env_B in a different one.
//!
//! Loadbearing aspect: union-find groups by pool-key overlap, NOT by
//! enqueue order. Env_B sits chronologically between A and C; a naïve
//! "consecutive only" packer would put A+B together (size-2 burst).
//! Union-find correctly skips over B and merges A↔C.
//!
//! Run via:
//!   cargo test --release --test acceptance_v21_affinity_groups_overlapping_envelopes \
//!     --features pg18 --no-default-features -- --ignored --nocapture

#![cfg(test)]

mod common;

use common::{connect_pool, reset_state, wait_for_terminal};
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

async fn enqueue_one(pool: &PgPool, cid: Uuid, sku: i64, chrono: i64) -> Result<(), sqlx::Error> {
    let payload = serde_json::json!({
        "sku_id": sku,
        "location_id": 1,
        "qty": 5,
        "unit_cost": 100,
        "business_date_jdate": 9999,
        "doc_chrono": chrono,
        "document_id": 7_700_000 + chrono,
    });
    let pool_keys = serde_json::json!({ "sku": [[sku, 1]], "wip": [] });
    sqlx::query("SELECT poc_v21_enqueue($1::uuid, $2, $3::jsonb, $4::jsonb, false)")
        .bind(cid)
        .bind("po_receipt")
        .bind(payload)
        .bind(pool_keys)
        .execute(pool)
        .await
        .map(|_| ())
}

async fn superbatch_id_for(pool: &PgPool, cid: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT MIN(superbatch_id)::bigint \
           FROM poc_v21_posting_lines \
          WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(pool)
    .await
    .expect("superbatch_id lookup")
}

async fn set_batch_size_max(pool: &PgPool, n: i32) {
    sqlx::query(&format!("ALTER SYSTEM SET poc_v21.batch_size_max = {n}"))
        .execute(pool)
        .await
        .expect("alter system batch_size_max");
    sqlx::query("SELECT pg_reload_conf()")
        .execute(pool)
        .await
        .expect("pg_reload_conf");
    // Sighup propagation window — the router's next tick reads the new
    // value via batch_size_max_now().
    tokio::time::sleep(Duration::from_millis(150)).await;
}

async fn reset_batch_size_max(pool: &PgPool) {
    let _ = sqlx::query("ALTER SYSTEM RESET poc_v21.batch_size_max")
        .execute(pool)
        .await;
    let _ = sqlx::query("SELECT pg_reload_conf()").execute(pool).await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn acceptance_v21_affinity_groups_overlapping_envelopes() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    set_batch_size_max(&pool, 2).await;

    // chrono ordering matches spec: A=1 (SKU 5), B=2 (SKU 6), C=3 (SKU 5).
    let cid_a = Uuid::new_v4();
    let cid_b = Uuid::new_v4();
    let cid_c = Uuid::new_v4();
    let correlation_ids = vec![cid_a, cid_b, cid_c];

    let p = pool.clone();
    let a = tokio::spawn(async move { enqueue_one(&p, cid_a, 5, 1).await });
    let p = pool.clone();
    let b = tokio::spawn(async move { enqueue_one(&p, cid_b, 6, 2).await });
    let p = pool.clone();
    let c = tokio::spawn(async move { enqueue_one(&p, cid_c, 5, 3).await });
    a.await.unwrap().unwrap();
    b.await.unwrap().unwrap();
    c.await.unwrap().unwrap();

    let terminal = wait_for_terminal(&pool, &correlation_ids, Duration::from_secs(15)).await;
    assert_eq!(terminal, 3, "all 3 envelopes should reach terminal");

    let sb_a = superbatch_id_for(&pool, cid_a).await;
    let sb_b = superbatch_id_for(&pool, cid_b).await;
    let sb_c = superbatch_id_for(&pool, cid_c).await;

    println!("R3 sb mapping: A(SKU 5)={sb_a} B(SKU 6)={sb_b} C(SKU 5)={sb_c}");

    assert_eq!(
        sb_a, sb_c,
        "R3: A and C share SKU 5 — must land in same SB; got A={sb_a} C={sb_c}"
    );
    assert_ne!(
        sb_a, sb_b,
        "R3: A (SKU 5) and B (SKU 6) are disjoint — must land in different SBs; got A={sb_a} B={sb_b}"
    );

    reset_batch_size_max(&pool).await;
}
