//! E1 acceptance (acct-shpc.4): transitive union-find — three envelopes
//! A, B, C with pairwise overlap (A∩B≠∅, B∩C≠∅, A∩C=∅) pack into ONE
//! SuperBatch via union-find merge. A disjoint envelope D lands in its
//! own SuperBatch.
//!
//! Test shape: enqueue 4 envelopes burst-style:
//!   A: pool_keys [[10,1], [20,1]]  payload sku_id=10
//!   B: pool_keys [[20,1], [30,1]]  payload sku_id=20   (A∩B = SKU 20)
//!   C: pool_keys [[30,1], [40,1]]  payload sku_id=30   (B∩C = SKU 30)
//!   D: pool_keys [[50,1]]          payload sku_id=50   (disjoint)
//! Union-find: A↔B (via 20), B↔C (via 30) → component {A, B, C}.
//! D alone. Two SuperBatches expected; {A,B,C} share superbatch_id,
//! D's is distinct.
//!
//! Run via:
//!   cargo test --release --test acceptance_v21_router_groups_transitive \
//!     --features pg18 --no-default-features -- --ignored --nocapture

#![cfg(test)]

mod common;

use common::{connect_pool, reset_state, wait_for_terminal};
use sqlx::PgPool;
use std::collections::HashSet;
use std::time::Duration;
use uuid::Uuid;

async fn enqueue_multi_pool(
    pool: &PgPool,
    cid: Uuid,
    primary_sku: i64,
    extra_sku: Option<i64>,
    chrono: i64,
) -> Result<(), sqlx::Error> {
    let payload = serde_json::json!({
        "sku_id": primary_sku,
        "location_id": 1,
        "qty": 5,
        "unit_cost": 100,
        "business_date_jdate": 9999,
        "doc_chrono": chrono,
        "document_id": 7_500_000 + chrono,
    });
    let pool_keys = match extra_sku {
        Some(s) => serde_json::json!({ "sku": [[primary_sku, 1], [s, 1]], "wip": [] }),
        None => serde_json::json!({ "sku": [[primary_sku, 1]], "wip": [] }),
    };
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

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn acceptance_v21_router_groups_transitive_chain() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    let cid_a = Uuid::new_v4();
    let cid_b = Uuid::new_v4();
    let cid_c = Uuid::new_v4();
    let cid_d = Uuid::new_v4();
    let correlation_ids = vec![cid_a, cid_b, cid_c, cid_d];

    // Serial enqueue ensures all four envelopes land in staging before
    // the next router tick (50ms cadence; 4 SPI calls finish in <5ms).
    // Parallel tokio::spawn lets enqueues race across the connection
    // pool — the router can tick after envelope 1 lands and before
    // envelopes 2-4 arrive, splitting the union-find component across
    // multiple SBs.
    enqueue_multi_pool(&pool, cid_a, 10, Some(20), 1).await.unwrap();
    enqueue_multi_pool(&pool, cid_b, 20, Some(30), 2).await.unwrap();
    enqueue_multi_pool(&pool, cid_c, 30, Some(40), 3).await.unwrap();
    enqueue_multi_pool(&pool, cid_d, 50, None, 4).await.unwrap();

    let terminal = wait_for_terminal(&pool, &correlation_ids, Duration::from_secs(15)).await;
    assert_eq!(terminal, 4, "all four envelopes should reach terminal");

    let sb_a = superbatch_id_for(&pool, cid_a).await;
    let sb_b = superbatch_id_for(&pool, cid_b).await;
    let sb_c = superbatch_id_for(&pool, cid_c).await;
    let sb_d = superbatch_id_for(&pool, cid_d).await;

    println!("transitive sb mapping: A={sb_a} B={sb_b} C={sb_c} D={sb_d}");

    // {A, B, C} must share one superbatch_id (union-find merged the
    // chain A↔B↔C); D's must be distinct.
    assert_eq!(
        sb_a, sb_b,
        "A↔B link (via SKU 20) must put A and B in same SB; got A={sb_a} B={sb_b}"
    );
    assert_eq!(
        sb_b, sb_c,
        "B↔C link (via SKU 30) must put B and C in same SB; got B={sb_b} C={sb_c}"
    );
    assert_ne!(
        sb_d, sb_a,
        "D (disjoint SKU 50) must NOT join the {{A,B,C}} component; got D={sb_d} A={sb_a}"
    );

    let distinct: HashSet<i64> = [sb_a, sb_b, sb_c, sb_d].into_iter().collect();
    assert_eq!(
        distinct.len(),
        2,
        "exactly two SuperBatches expected (transitive {{A,B,C}} + disjoint {{D}}); got {} distinct ids: {:?}",
        distinct.len(),
        distinct
    );
}
