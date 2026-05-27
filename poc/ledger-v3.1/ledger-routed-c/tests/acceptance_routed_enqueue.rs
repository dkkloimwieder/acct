//! acct-2ttr.4 §6.1 acceptance: ledger_enqueue_trx_c stages to shmem.
//!
//! P3.1 acceptance criteria (design-v3.1 §6.1, §6.2):
//!   - enqueue returns a monotonic submission_id;
//!   - the payload is staged to the spillover arena (outstanding allocs grow);
//!   - a staging slot transitions to `pending`;
//!   - NO DB row is written at enqueue (the trx row is the committer's job, P3.3);
//!   - the optional STD `variance_account` field is accepted;
//!   - empty / malformed submissions are rejected.
//!
//! Counts are measured as deltas because the staging queue + arena live in shmem
//! (not reset by TRUNCATE) and the P3.1 router/committer shells never drain them.

mod common;

use common::*;

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c preloaded"]
async fn enqueue_stages_to_shmem_without_db_write() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    let pending_before = staging_pending(&pool).await;
    let arena_before = arena_outstanding(&pool).await;

    // Pause the router + committer so the staged-but-unprocessed state is
    // observable. Otherwise the router's ~50ms tick races this read and can move
    // the slots pending→routed (and the committer can then drain their arena
    // blocks) before the assertions run.
    set_router_paused(&pool, true).await;
    set_committer_paused(&pool, true).await;
    // Let any in-progress tick finish so both workers are parked on their flags.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // One plain line, one carrying the optional variance account.
    let s1 = enqueue(&pool, "po_receipt", 1, vec![receipt_line(1, 10, 50)])
        .await
        .expect("enqueue 1");
    let s2 = enqueue(
        &pool,
        "po_receipt",
        2,
        vec![receipt_line_with_variance(2, 1, 1)],
    )
    .await
    .expect("enqueue 2 (with variance_account)");

    // Capture the staged-state readings while paused.
    let trx_after = trx_count(&pool).await;
    let pending_delta = staging_pending(&pool).await - pending_before;
    let arena_delta = arena_outstanding(&pool).await - arena_before;

    // Resume BEFORE asserting so a failed assert never leaves the workers parked
    // for the rest of this test binary.
    set_committer_paused(&pool, false).await;
    set_router_paused(&pool, false).await;

    // Monotonic submission ids (request_seq increments by 1).
    assert_eq!(s2, s1 + 1, "submission_id must be monotonic");

    // No DB write at enqueue — the trx row is created only at commit (P3.3).
    assert_eq!(trx_after, 0, "enqueue must not write a trx row");

    // Both submissions are staged pending and their payloads are in the arena.
    assert_eq!(pending_delta, 2, "two slots should be pending");
    // Each submission allocates 3 arena blocks: lines blob + submission blob +
    // pool-keys blob.
    assert_eq!(arena_delta, 6, "two submissions × 3 arena blocks each");
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c preloaded"]
async fn enqueue_submission_ids_are_strictly_monotonic() {
    let pool = connect_pool().await;
    let seq_before = request_seq_max(&pool).await;

    let mut ids = Vec::new();
    for i in 0..8i64 {
        ids.push(
            enqueue(&pool, "transfer_shipment", 100 + i, vec![receipt_line(i + 1, 3, 7)])
                .await
                .expect("enqueue"),
        );
    }
    // Each id is exactly one more than the last.
    for w in ids.windows(2) {
        assert_eq!(w[1], w[0] + 1, "ids must increment by 1: {ids:?}");
    }
    // request_seq_max advanced by the 8 successful enqueues.
    assert_eq!(request_seq_max(&pool).await - seq_before, 8);
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c preloaded"]
async fn enqueue_rejects_empty_lines() {
    let pool = connect_pool().await;
    let err = enqueue(&pool, "po_receipt", 9, vec![]).await;
    assert!(err.is_err(), "zero-line submission must be rejected");
    assert!(format!("{}", err.unwrap_err()).contains("non-empty"));
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c preloaded"]
async fn enqueue_rejects_unknown_line_type_via_decode() {
    // An object missing required fields fails JSONB decode into PocV3Line.
    let pool = connect_pool().await;
    let bad = serde_json::json!([{"pool_id": 1, "line_type": "po_receipt_line"}]);
    let err = sqlx::query_scalar::<_, i64>("SELECT ledger_enqueue_trx_c($1,$2,$3,$4::jsonb)")
        .bind("po_receipt")
        .bind(11i64)
        .bind(TS)
        .bind(bad)
        .fetch_one(&pool)
        .await;
    assert!(err.is_err(), "malformed line payload must be rejected");
    assert!(format!("{}", err.unwrap_err()).contains("decode failed"));
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c preloaded"]
async fn shmem_sizing_gucs_honored() {
    // The compile-time shmem sizes are surfaced via the hello banner and the
    // Postmaster-scope GUCs; assert they match the §6.2 defaults.
    let pool = connect_pool().await;
    let banner: String = sqlx::query_scalar("SELECT ledger_routed_c_hello()")
        .fetch_one(&pool)
        .await
        .expect("hello");
    assert!(banner.contains("staging=16384"), "banner: {banner}");
    assert!(banner.contains("committer=2048"), "banner: {banner}");
    assert!(banner.contains("arena_mb=128"), "banner: {banner}");

    let staging_guc: String = sqlx::query_scalar("SHOW ledger_routed_c.staging_queue_size")
        .fetch_one(&pool)
        .await
        .expect("show staging_queue_size");
    assert_eq!(staging_guc, "16384");
}
