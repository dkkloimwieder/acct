//! acct-2ttr.4 §6.1 acceptance: ledger_enqueue_trx_c stages to shmem.
//!
//! P3.1 acceptance criteria (design-v3.1 §6.1, §6.2):
//!   - enqueue returns a monotonic submission_id;
//!   - the payload is staged to the spillover arena (outstanding allocs grow);
//!   - a staging slot transitions to `pending`;
//!   - NO DB row is written at enqueue (the trx row is the committer's job, P3.3);
//!   - empty / malformed submissions are rejected.
//!
//! Counts are measured as deltas because the staging queue + arena live in shmem
//! (not reset by TRUNCATE) and the P3.1 router/committer shells never drain them.
//!
//! The batch entry point `ledger_enqueue_trx_batch_c` (acct-mvq4.36) is covered
//! here too: a stage-only round-trip pinning the alloc-all / push-all / return-
//! count contract, and a full commit round-trip through router + committer to trx
//! rows with arena release. The backpressure FAILURE arms (queue-full timeout,
//! arena-full deadline, deadline-partial-push remainder-free) are the dedicated
//! scope of acct-mvq4.37 and are NOT exercised here.

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

    // Two single-line submissions.
    let s1 = enqueue(&pool, "po_receipt", 1, vec![receipt_line(1, 10, 50)])
        .await
        .expect("enqueue 1");
    let s2 = enqueue(&pool, "po_receipt", 2, vec![receipt_line(2, 1, 1)])
        .await
        .expect("enqueue 2");

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
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn enqueue_batch_stages_all_envelopes_without_db_write() {
    // acct-mvq4.36: ledger_enqueue_trx_batch_c was suite-dark. Pin its stage-phase
    // contract — alloc-all + push-all + return the published count — with the
    // workers paused so the staged state is observable.
    let pool = connect_pool().await;
    reset_state(&pool).await;

    set_router_paused(&pool, true).await;
    set_committer_paused(&pool, true).await;
    // Let any in-progress tick finish so the queue is quiescent before baselines.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let pending_before = staging_pending(&pool).await;
    let arena_before = arena_outstanding(&pool).await;
    let seq_before = request_seq_max(&pool).await;

    // A four-envelope batch, distinct source_ids, one line each.
    let published = enqueue_batch(
        &pool,
        vec![
            ("po_receipt", 201, vec![receipt_line(1, 10, 50)]),
            ("po_receipt", 202, vec![receipt_line(2, 5, 20)]),
            ("po_receipt", 203, vec![receipt_line(3, 1, 1)]),
            ("po_receipt", 204, vec![receipt_line(4, 7, 30)]),
        ],
    )
    .await
    .expect("batch enqueue");

    let trx_after = trx_count(&pool).await;
    let pending_delta = staging_pending(&pool).await - pending_before;
    let arena_delta = arena_outstanding(&pool).await - arena_before;
    let seq_delta = request_seq_max(&pool).await - seq_before;

    // Resume before asserting so a failed assert never leaves the workers parked.
    set_committer_paused(&pool, false).await;
    set_router_paused(&pool, false).await;

    // Return contract: the count of submissions published.
    assert_eq!(published, 4, "batch must publish all four envelopes");
    // No DB write at enqueue — the trx rows are the committer's job.
    assert_eq!(trx_after, 0, "batch enqueue must not write a trx row");
    // Four slots pending; request_seq advanced once per published envelope.
    assert_eq!(pending_delta, 4, "four slots should be pending");
    assert_eq!(seq_delta, 4, "request_seq advances once per published envelope");
    // Each envelope allocates 3 arena blocks (lines + submission + pool-keys).
    assert_eq!(arena_delta, 12, "four envelopes × 3 arena blocks each");
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn enqueue_batch_round_trips_through_committer_to_trx_rows() {
    // acct-mvq4.36: a full round-trip — a batch stages, the router groups it, the
    // committer writes one trx per envelope and coalesces the aggregate, and the
    // batch's arena blocks are released on drain.
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;

    set_router_paused(&pool, true).await;
    set_committer_paused(&pool, true).await;
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let arena_before = arena_outstanding(&pool).await;
    let base_drains = committer_stat(&pool, "drains_total").await;

    // Five receipts of 10@100 to one pool → one commit_group, one aggregate.
    let published = enqueue_batch(
        &pool,
        (0..5i64)
            .map(|i| ("po_receipt", 301 + i, vec![receipt_line_for(&f, 10, 100)]))
            .collect(),
    )
    .await
    .expect("batch enqueue");

    set_committer_paused(&pool, false).await;
    set_router_paused(&pool, false).await;

    assert_eq!(published, 5, "batch must publish all five receipts");

    await_trx_count(&pool, 5).await;
    await_committer_drains(&pool, base_drains, 1).await;

    // One trx per envelope, keyed by source_id.
    for i in 0..5i64 {
        assert_eq!(
            trx_count_for_source(&pool, 301 + i).await,
            1,
            "each batched envelope commits exactly one trx (source {})",
            301 + i
        );
    }
    // Five receipts of 10@100 coalesce to one value-weighted aggregate.
    assert_eq!(
        aggregate(&pool, f.pool_id).await,
        Some((50, 100)),
        "batched receipts coalesce to (qty 50, unit_cost 100)"
    );

    // The whole batch's arena blocks are released once the committer drains it.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let outstanding = arena_outstanding(&pool).await;
        if outstanding <= arena_before {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("batch arena blocks not released: outstanding {outstanding} > baseline {arena_before}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
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
