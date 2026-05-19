//! E1 acceptance (acct-shpc.4): multi-component split — three disjoint
//! clusters each form their own SuperBatch in one router tick.
//!
//! Test shape: enqueue 6 envelopes in three pairs, each pair sharing a
//! distinct SKU pool (no cross-pair overlap):
//!   E1, E2 → SKU 110           (component {E1, E2})
//!   E3, E4 → SKU 120           (component {E3, E4})
//!   E5, E6 → SKU 130           (component {E5, E6})
//! Union-find emits three components; router packs each into its own
//! SuperBatch. Expected: three distinct superbatch_ids; each one binds
//! exactly two envelopes' posting_lines.
//!
//! Run via:
//!   cargo test --release --test acceptance_v21_router_groups_split_disjoint \
//!     --features pg18 --no-default-features -- --ignored --nocapture

#![cfg(test)]

mod common;

use common::{connect_pool, reset_state, wait_for_terminal};
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
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
        "document_id": 7_600_000 + chrono,
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

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn acceptance_v21_router_groups_three_disjoint_clusters() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    // Three disjoint (sku, location) pairs.
    let clusters: [(i64, [Uuid; 2]); 3] = [
        (110, [Uuid::new_v4(), Uuid::new_v4()]),
        (120, [Uuid::new_v4(), Uuid::new_v4()]),
        (130, [Uuid::new_v4(), Uuid::new_v4()]),
    ];
    let mut correlation_ids: Vec<Uuid> = Vec::with_capacity(6);
    for (_sku, pair) in &clusters {
        correlation_ids.extend_from_slice(pair);
    }

    // Serial enqueue keeps all 6 envelopes inside the next router-tick
    // window (50ms cadence; 6 SPI calls finish in <10ms). Parallel
    // spawn risks the router emitting before the burst completes,
    // splitting clusters across ticks.
    let mut chrono = 0_i64;
    for (sku, pair) in clusters {
        for cid in pair {
            chrono += 1;
            enqueue_one(&pool, cid, sku, chrono).await.unwrap();
        }
    }

    let terminal = wait_for_terminal(&pool, &correlation_ids, Duration::from_secs(15)).await;
    assert_eq!(terminal, 6, "all 6 envelopes should reach terminal");

    // Build correlation_id → superbatch_id map.
    let mut sb_for: HashMap<Uuid, i64> = HashMap::new();
    for cid in &correlation_ids {
        sb_for.insert(*cid, superbatch_id_for(&pool, *cid).await);
    }
    println!("split_disjoint sb mapping: {:?}", sb_for);

    // Each pair must share its own superbatch_id.
    for (sku, pair) in clusters {
        let sb0 = sb_for[&pair[0]];
        let sb1 = sb_for[&pair[1]];
        assert_eq!(
            sb0, sb1,
            "cluster SKU {sku}: pair ({:?}, {:?}) must share superbatch_id; got sb0={sb0} sb1={sb1}",
            pair[0], pair[1]
        );
    }

    // The three pairs must produce three distinct superbatch_ids.
    let distinct: HashSet<i64> = sb_for.values().copied().collect();
    assert_eq!(
        distinct.len(),
        3,
        "expected 3 disjoint components → 3 distinct SuperBatches; got {} distinct ids: {:?}",
        distinct.len(),
        distinct
    );
}
