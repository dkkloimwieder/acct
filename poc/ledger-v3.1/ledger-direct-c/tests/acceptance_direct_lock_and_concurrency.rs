//! acct-2ttr.3 §9.2 acceptance: lock-hold structure, insufficient inventory, and
//! concurrent hot-pool correctness for ledger_submit_trx_c.
//!
//!   - Insufficient aggregate inventory → RAISE → caller's tx aborts, no rows land.
//!   - Deep pool (1000 stale layer rows present) depletion touches ONLY the
//!     aggregate — the structural form of the "lock-hold constant w.r.t. pool
//!     depth" property (§5.2). The actual timing sweep is harness work (P4/P5).
//!   - 50 concurrent depletions to one hot FIFO pool serialize correctly through
//!     pool_lock with no lost updates.

mod common;

use common::*;
use std::sync::Arc;

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn insufficient_inventory_raises_and_rolls_back() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;

    submit(&pool, "po_receipt", 1, vec![receipt(&f, 3, 100)]).await.expect("receipt");

    // Deplete 5 from a pool holding 3 → RAISE.
    let err = submit(&pool, "transfer_shipment", 2, vec![depletion(&f, 5)]).await;
    assert!(err.is_err(), "over-depletion must RAISE");
    assert!(format!("{}", err.unwrap_err()).contains("insufficient inventory"));

    // The aborted tx left nothing behind: aggregate unchanged, no trx for source 2.
    assert_eq!(aggregate(&pool, f.pool_id).await, Some((3, 100)));
    let trx_for_2: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM trx WHERE trx_type = 'transfer_shipment' AND source_id = 2",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(trx_for_2, 0, "failed submission writes no trx row");
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn deep_pool_depletion_touches_only_aggregate() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;

    // Seed a deep pool directly: aggregate plus 1000 stale layer rows. A strict
    // FIFO impl would iterate these on depletion; Path C must not — it reads and
    // writes only layer_id = 0.
    sqlx::query("INSERT INTO pool_state (pool_id, layer_id, qty, unit_cost, value_sum) VALUES ($1, 0, 5000, 100, 500000)")
        .bind(f.pool_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO pool_state (pool_id, layer_id, qty, unit_cost, value_sum) \
         SELECT $1, g, 5, 100, 500 FROM generate_series(1, 1000) AS g",
    )
    .bind(f.pool_id)
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(layer_count(&pool, f.pool_id).await, 1000);

    let dep = submit(&pool, "transfer_shipment", 1, vec![depletion(&f, 5)]).await.expect("dep");

    let lines = trx_lines(&pool, dep).await;
    assert_eq!(lines[0].1, 100, "priced off the aggregate running average");
    assert_eq!(lines[0].2, None, "no layer linked");
    assert_eq!(aggregate(&pool, f.pool_id).await, Some((4995, 100)));
    assert_eq!(
        layer_count(&pool, f.pool_id).await,
        1000,
        "Path C did not touch (iterate or delete) any layer row"
    );
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn concurrent_depletions_serialize_without_lost_updates() {
    let pool = Arc::new(connect_pool().await);
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    submit(&pool, "po_receipt", 1, vec![receipt(&f, 1000, 100)]).await.expect("receipt");

    // 50 concurrent depletions of 10 each, distinct source_ids. pool_lock must
    // serialize them; the final aggregate proves no update was lost.
    let mut handles = Vec::new();
    for i in 0..50i64 {
        let p = Arc::clone(&pool);
        let pool_id = f.pool_id;
        let inv = f.inv_acct;
        let ap = f.ap_acct;
        handles.push(tokio::spawn(async move {
            let line = serde_json::json!({
                "pool_id": pool_id, "line_type": "transfer_shipment_line",
                "qty": -10, "unit_cost": 0, "debit_account": ap, "credit_account": inv,
            });
            sqlx::query_scalar::<_, i64>("SELECT ledger_submit_trx_c($1,$2,$3,$4::jsonb)")
                .bind("transfer_shipment")
                .bind(100 + i)
                .bind(TS)
                .bind(serde_json::Value::Array(vec![line]))
                .fetch_one(&*p)
                .await
                .expect("concurrent depletion")
        }));
    }
    for h in handles {
        h.await.expect("join");
    }

    assert_eq!(
        aggregate(&pool, f.pool_id).await,
        Some((500, 100)),
        "1000 − 50×10 = 500; no lost updates"
    );
    let dep_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM trx WHERE trx_type = 'transfer_shipment'",
    )
    .fetch_one(&*pool)
    .await
    .unwrap();
    assert_eq!(dep_count, 50);
}
