//! Concurrency acceptance tests for the recalc claim protocol.
//!
//! The claim contract under test: a claim whose queue-row lock waited out a
//! sibling pass must observe that sibling's committed settlement state — the
//! generation, the frontier, and the floor are read AFTER the lock lands,
//! never through the locking statement's snapshot. Two consequences are
//! pinned deterministically here, with real multi-connection interleavings
//! (lock-wait polling on `pg_stat_activity` is the rendezvous — no sleeps as
//! synchronization):
//!
//! 1. `blocked_claim_reads_fresh_settlement_after_sibling_requeue` — an
//!    engine pass is parked inside `settle()` (a third session holds the
//!    `pool_settlement` row), a targeted `ledger_settle_pool` parks on its
//!    queue-row lock, and a fresh head receipt commits mid-park so the engine
//!    pass REQUEUES (re-stamps) the row rather than deleting it. When the
//!    parked claim is granted against the re-stamped tuple, its pass must see
//!    the committed generation: `pool_settlement.recalc_generation` ends at
//!    the sibling's value, never regressed to the pre-sibling value. A
//!    regressed generation strands committed generation-keyed rows
//!    (`cost_settlement`, `cost_layer_consumption`) ABOVE the recorded
//!    generation, and the next genuine re-cost collides with them on a
//!    primary key — the follow-up backdated receipt at the end of the test
//!    is exactly that re-cost and must drain cleanly. (In this fixture the
//!    re-cost draws a different layer, so `cost_settlement_pkey`
//!    `(depletion, generation)` is the constraint that fires; identical
//!    re-draws collide on `cost_layer_consumption_pkey` instead.)
//!
//!    Fixture choreography note: the backdated receipt is priced so the two
//!    re-costed depletions' GL deltas cancel (net zero). A net-zero pass
//!    skips the aggregate-row reconciliation, so the parked engine pass holds
//!    no `pool_state` lock and the mid-park head receipt can commit.
//!
//! 2. `blocked_claim_retries_when_queue_row_deleted` — a claim parked on the
//!    queue-row lock can wake to find the row deleted (the holder's pass
//!    drained the pool and consumed the mark). The blocking claim path must
//!    re-ensure the row and retry — settling the pool as a free no-op pass —
//!    not error out on a pool that plainly exists.

mod common;

use common::*;
use serde_json::{json, Value};
use sqlx::{Connection, PgConnection, PgPool};
use std::time::Duration;

/// A dedicated session connection + its backend pid (for lock-wait polling).
async fn session() -> (PgConnection, i32) {
    let mut c = PgConnection::connect(POC_DSN).await.expect("session connect");
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut c)
        .await
        .expect("pg_backend_pid");
    (c, pid)
}

/// Poll until the backend is provably parked on a heavyweight lock. Bounded:
/// a session that never parks means the choreography broke — fail, don't hang.
async fn wait_until_lock_blocked(pool: &PgPool, pid: i32) {
    for _ in 0..400 {
        let blocked: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
              WHERE pid = $1 AND wait_event_type = 'Lock')",
        )
        .bind(pid)
        .fetch_one(pool)
        .await
        .expect("pg_stat_activity");
        if blocked {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("backend {pid} never parked on a lock");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn blocked_claim_reads_fresh_settlement_after_sibling_requeue() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let consumer = reset_feed(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;

    // Layers R_a(100)@T10, R_b(150)@T11; D1@T12 draws R_a, D2@T13 draws R_b.
    let ra = receipt_at(&pool, f.pool_id, 101, T10, 10, 100).await;
    let rb = receipt_at(&pool, f.pool_id, 102, T11, 10, 150).await;
    let d1 = deplete_at(&pool, f.pool_id, 103, T12, 10).await;
    let d2 = deplete_at(&pool, f.pool_id, 104, T13, 10).await;
    ingest_all(&consumer).await;
    drain_recalc(&pool).await;
    let (g0, settled0, floor0) = settlement_of(&pool, f.pool_id).await.expect("settled");
    assert_eq!((settled0, floor0), (Some(d2), false), "clean settle before the choreography");

    // Backdated receipt priced at R_b's cost: the replay re-draws D1 from it
    // (100 → 150) and shifts D2 onto R_a (150 → 100) — two nonzero deltas
    // whose GL net is zero, so the engine pass skips the aggregate-row lock.
    let b1 = receipt_at(&pool, f.pool_id, 105, T09, 10, 150).await;
    ingest_all(&consumer).await;
    let (_, _, floor_set) = settlement_of(&pool, f.pool_id).await.expect("settlement row");
    assert!(floor_set, "backdated receipt sets the recost floor");
    assert_eq!(count(&pool, "SELECT count(*) FROM recalc_queue").await, 1);

    // s3 parks the settlement row so the engine pass blocks inside settle().
    let (mut c3, _) = session().await;
    sqlx::query("BEGIN").execute(&mut c3).await.unwrap();
    sqlx::query("SELECT pool_id FROM pool_settlement WHERE pool_id = $1 FOR UPDATE")
        .bind(f.pool_id)
        .execute(&mut c3)
        .await
        .unwrap();

    // s1: one engine pass in an open transaction — claims the queue row,
    // writes generation g0+1, parks at settle() on s3's row lock.
    let (mut c1, pid1) = session().await;
    let t1 = tokio::spawn(async move {
        sqlx::query("BEGIN").execute(&mut c1).await.unwrap();
        let report: Value = sqlx::query_scalar("SELECT ledger_recalc_step()")
            .fetch_one(&mut c1)
            .await
            .expect("ledger_recalc_step");
        (c1, report)
    });
    wait_until_lock_blocked(&pool, pid1).await;

    // s2: targeted settle. Its ensure-INSERT is a free no-op (the queue row
    // is live, merely lock-held) and its FOR UPDATE parks on s1's claim.
    let (mut c2, pid2) = session().await;
    let target = f.pool_id;
    let t2 = tokio::spawn(async move {
        let report: Value = sqlx::query_scalar("SELECT ledger_settle_pool($1)")
            .bind(target)
            .fetch_one(&mut c2)
            .await
            .expect("ledger_settle_pool");
        report
    });
    wait_until_lock_blocked(&pool, pid2).await;

    // A fresh head receipt commits while both sessions are parked: s1's
    // queue decision sees it past the new frontier (tail) and re-stamps the
    // claimed row instead of deleting it — the tuple update s2's parked lock
    // will be granted against.
    let e = receipt_at(&pool, f.pool_id, 106, T14, 5, 120).await;
    ingest_all(&consumer).await;

    // Release s3; s1's pass completes (g0+1, floor cleared, requeued) and
    // commits; s2's claim is then granted and runs its own pass.
    sqlx::query("COMMIT").execute(&mut c3).await.unwrap();
    let (mut c1, report1) = tokio::time::timeout(Duration::from_secs(30), t1)
        .await
        .expect("s1 pass finished")
        .expect("s1 join");
    assert_eq!(report1["full_replay"], json!(true), "floor forces full replay");
    assert_eq!(report1["generation"], json!(g0 + 1));
    assert_eq!(report1["settlements_written"], json!(2), "both depletions re-cost");
    assert_eq!(report1["requeued"], json!(true), "head receipt must keep the row queued");
    sqlx::query("COMMIT").execute(&mut c1).await.unwrap();

    let report2 = tokio::time::timeout(Duration::from_secs(30), t2)
        .await
        .expect("s2 settle finished")
        .expect("s2 join");
    assert_eq!(report2["settled"], json!(true));
    assert_eq!(
        report2["generation"],
        json!(g0 + 1),
        "the granted claim must report the sibling's committed generation"
    );
    assert_eq!(
        report2["settlements_written"],
        json!(0),
        "a fresh claim sees the cleared floor and settles the head receipt incrementally"
    );

    // THE pin: the settlement row holds the sibling's generation — a claim
    // that read stale state through its locking statement writes g0 back.
    let (g_now, settled_now, floor_now) = settlement_of(&pool, f.pool_id).await.expect("row");
    assert_eq!(g_now, g0 + 1, "recalc_generation must never regress");
    assert_eq!(settled_now, Some(e), "frontier advanced through the head receipt");
    assert!(!floor_now, "floor consumed by the sibling's pass");

    // Wedge probe: a second backdated receipt forces a genuine re-cost of D2
    // through a fresh generation. With a regressed generation this pass
    // recomputes g0+1 and collides with the committed generation-keyed rows
    // (here `cost_settlement_pkey` — D2 re-draws a different layer, so the
    // consumption key differs but (depletion, generation) duplicates); it
    // must instead drain cleanly.
    let b2 = receipt_at(&pool, f.pool_id, 107, T09, 10, 500).await;
    ingest_all(&consumer).await;
    let probe = settle_pool(&pool, f.pool_id).await;
    assert_eq!(probe["settled"], json!(true));
    assert_eq!(probe["generation"], json!(g0 + 2), "genuine re-cost bumps past the sibling");

    // Authoritative costs after both backdated receipts: business order is
    // B1(150), B2(500), R_a(100), R_b(150) — D1 draws B1, D2 draws B2.
    assert_eq!(authoritative_of(&pool, d1).await, Some(150));
    assert_eq!(authoritative_of(&pool, d2).await, Some(500));

    // Open layers: B1 and B2 fully drawn; R_a, R_b, and the head receipt E
    // remain.
    let mut layers: Vec<(i64, i64, i64)> = sqlx::query_as(
        "SELECT layer_id, qty, unit_cost FROM pool_state \
          WHERE pool_id = $1 AND layer_id > 0 ORDER BY layer_id",
    )
    .bind(f.pool_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    layers.sort();
    let mut expected = vec![(ra, 10, 100), (rb, 10, 150), (e, 5, 120)];
    expected.sort();
    assert_eq!(layers, expected, "open layers after the wedge probe");

    let (g_final, settled_final, floor_final) =
        settlement_of(&pool, f.pool_id).await.expect("row");
    assert_eq!((g_final, settled_final, floor_final), (g0 + 2, Some(e), false));
    assert_eq!(count(&pool, "SELECT count(*) FROM recalc_queue").await, 0);
    assert_eq!(b2 > b1, true, "id order breaks the T09 business-time tie");

    drop_feed_slot(&pool).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn blocked_claim_retries_when_queue_row_deleted() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;

    // Settle the pool clean via a direct dirty mark (no feed needed — nothing
    // is backdated here).
    receipt_at(&pool, f.pool_id, 201, T10, 10, 100).await;
    let d1 = deplete_at(&pool, f.pool_id, 202, T11, 4).await;
    mark_dirty(&pool, f.pool_id).await;
    drain_recalc(&pool).await;
    let (g0, settled0, _) = settlement_of(&pool, f.pool_id).await.expect("settled");
    assert_eq!(settled0, Some(d1));

    // Re-mark the clean pool, then lock the queue row in an open transaction.
    mark_dirty(&pool, f.pool_id).await;
    let (mut c1, _) = session().await;
    sqlx::query("BEGIN").execute(&mut c1).await.unwrap();
    sqlx::query("SELECT pool_id FROM recalc_queue WHERE pool_id = $1 FOR UPDATE")
        .bind(f.pool_id)
        .execute(&mut c1)
        .await
        .unwrap();

    // s2 parks on the locked queue row.
    let (mut c2, pid2) = session().await;
    let target = f.pool_id;
    let t2 = tokio::spawn(async move {
        sqlx::query_scalar::<_, Value>("SELECT ledger_settle_pool($1)")
            .bind(target)
            .fetch_one(&mut c2)
            .await
    });
    wait_until_lock_blocked(&pool, pid2).await;

    // The lock holder runs the engine pass itself (re-locking an owned row is
    // free): a clean pool drains as a no-op and the queue decision DELETES
    // the row. Committing hands s2 a granted lock on a vanished tuple.
    let report1: Value = sqlx::query_scalar("SELECT ledger_recalc_step()")
        .fetch_one(&mut c1)
        .await
        .expect("ledger_recalc_step");
    assert_eq!(report1["claimed"], json!(true));
    assert_eq!(report1["requeued"], json!(false), "the no-op pass must consume the mark");
    sqlx::query("COMMIT").execute(&mut c1).await.unwrap();

    // s2 must re-ensure the row and settle the pool as a free no-op — not
    // error on a pool that exists.
    let report2 = tokio::time::timeout(Duration::from_secs(30), t2)
        .await
        .expect("s2 settle finished")
        .expect("s2 join")
        .expect("settle_pool succeeds after the queue row vanishes");
    assert_eq!(report2["settled"], json!(true));
    assert_eq!(report2["settlements_written"], json!(0));
    assert_eq!(report2["generation"], json!(g0), "no-op passes do not move the generation");

    let (g_now, settled_now, floor_now) = settlement_of(&pool, f.pool_id).await.expect("row");
    assert_eq!((g_now, settled_now, floor_now), (g0, Some(d1), false));
    assert_eq!(count(&pool, "SELECT count(*) FROM recalc_queue").await, 0);
}
