//! acct-2ttr.6 §6.4 acceptance: the committer drains commit_groups —
//! pg_xact triage, pre-flight dedup, drop-and-continue apply, batch write with
//! the final-aggregate collapse.
//!
//! Determinism: each test PAUSES the router + committer, stages its whole batch,
//! then RESUMES — so the batch is collected in one router window (one commit_group
//! unless it exceeds batch_size_max) and processed in enqueue order. Tests then
//! poll until the committer has drained (trx rows appear / `committer_drains_total`
//! advances). Stat counters are cumulative since load, so tests measure deltas;
//! the cluster-per-binary runner restarts the container per binary.
//!
//! Requires the `test_hooks` build (the runner installs it).

mod common;

use common::*;
use sqlx::PgPool;
use std::time::Duration;

/// Pause router + committer, run `stage` (enqueues), then resume.
async fn paused<F, Fut>(pool: &PgPool, stage: F)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    set_router_paused(pool, true).await;
    set_committer_paused(pool, true).await;
    // Let any in-progress tick finish and both workers park on their flags.
    tokio::time::sleep(Duration::from_millis(150)).await;
    stage().await;
    set_committer_paused(pool, false).await;
    set_router_paused(pool, false).await;
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn two_lines_same_pool_one_submission_agrees_with_direct() {
    // acct-036x: the routed path must handle a single submission with two lines on
    // the same pool and land the same coalesced aggregate the (now-fixed) direct
    // path does — (20, 150) for 10@100 + 10@200. Routed reconstructs one aggregate
    // per touched pool from the post-pass snapshot, so it was never affected by the
    // direct duplicate-UPSERT crash; this locks the two paths' agreement.
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;

    let base_drains = committer_stat(&pool, "drains_total").await;
    paused(&pool, || async {
        enqueue(
            &pool,
            "po_receipt",
            1,
            vec![receipt_line_for(&f, 10, 100), receipt_line_for(&f, 10, 200)],
        )
        .await
        .expect("enqueue two-line same-pool submission");
    })
    .await;

    await_trx_count(&pool, 1).await;
    await_committer_drains(&pool, base_drains, 1).await;

    assert_eq!(
        aggregate(&pool, f.pool_id).await,
        Some((20, 150)),
        "routed aggregate matches the direct coalesced result (acct-036x)"
    );
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn hot_pool_collapses_to_one_commit_group_one_lock_one_aggregate_update() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    // One standard-basis FIFO pool; depletions cost at the standard (no
    // within-batch evolution — the cleanest collapse, §6.7).
    let f = seed_pool(&pool, 1, 1, 1, "fifo", "standard").await;
    seed_standard_cost(&pool, f.sku_id, f.loc_id, 500).await;
    seed_aggregate(&pool, f.pool_id, 100_000, 500).await;

    // Exactly batch_size_max submissions → one full chunk → one commit_group.
    let n = batch_size_max(&pool).await;

    let base_drains = committer_stat(&pool, "drains_total").await;
    let base_locks = committer_stat(&pool, "pool_lock_acquisitions_total").await;
    let base_aggs = committer_stat(&pool, "aggregate_upserts_total").await;
    let base_trx = committer_stat(&pool, "trx_committed_total").await;

    paused(&pool, || async {
        for i in 0..n {
            enqueue(&pool, "transfer_shipment", 1_000 + i, vec![depletion_line(&f, 1)])
                .await
                .expect("enqueue depletion");
        }
    })
    .await;

    await_trx_count(&pool, n).await;
    await_committer_drains(&pool, base_drains, 1).await;
    // Give a beat to ensure no extra commit_group sneaks in.
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        committer_stat(&pool, "drains_total").await - base_drains,
        1,
        "all {n} submissions collapse into exactly one commit_group"
    );
    assert_eq!(
        committer_stat(&pool, "pool_lock_acquisitions_total").await - base_locks,
        1,
        "one pool_lock acquisition for the whole batch (the §6.7 win)"
    );
    assert_eq!(
        committer_stat(&pool, "aggregate_upserts_total").await - base_aggs,
        1,
        "one aggregate UPDATE for the whole batch — not one per submission"
    );
    assert_eq!(
        committer_stat(&pool, "trx_committed_total").await - base_trx,
        n,
        "every submission produced a trx row"
    );
    assert_eq!(trx_count(&pool).await, n, "trx table holds exactly {n} rows");
    assert_eq!(
        aggregate(&pool, f.pool_id).await,
        Some((100_000 - n, 500)),
        "aggregate qty drawn down by the batch; standard cost unchanged"
    );
    assert_eq!(committer_stat(&pool, "tx_failures_total").await, 0);
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn scale_thousand_submissions_commit_with_per_group_aggregate_collapse() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    let f = seed_pool(&pool, 1, 1, 1, "fifo", "standard").await;
    seed_standard_cost(&pool, f.sku_id, f.loc_id, 500).await;
    seed_aggregate(&pool, f.pool_id, 1_000_000, 500).await;

    let base_drains = committer_stat(&pool, "drains_total").await;
    let base_aggs = committer_stat(&pool, "aggregate_upserts_total").await;

    // 1000 concurrent submissions to one hot pool. With the default
    // batch_size_max (50) the router splits the component into ~20 ordered
    // chunks; each chunk is one commit_group with one aggregate UPDATE. The win:
    // ~20 aggregate writes for 1000 submissions, not 1000.
    paused(&pool, || async {
        for i in 0..1000i64 {
            enqueue(&pool, "transfer_shipment", 10_000 + i, vec![depletion_line(&f, 1)])
                .await
                .expect("enqueue depletion");
        }
    })
    .await;

    await_trx_count(&pool, 1000).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(trx_count(&pool).await, 1000, "all 1000 submissions committed");
    let drains = committer_stat(&pool, "drains_total").await - base_drains;
    let aggs = committer_stat(&pool, "aggregate_upserts_total").await - base_aggs;
    assert_eq!(
        aggs, drains,
        "exactly one aggregate UPDATE per commit_group ({aggs} aggs / {drains} groups)"
    );
    assert!(
        aggs < 1000 / 2,
        "aggregate writes ({aggs}) are far below the submission count — the collapse"
    );
    assert_eq!(
        aggregate(&pool, f.pool_id).await,
        Some((1_000_000 - 1000, 500)),
        "final aggregate reflects all 1000 depletions"
    );
    assert_eq!(committer_stat(&pool, "tx_failures_total").await, 0);
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn failed_submission_excluded_via_drop_and_continue() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    // running_avg basis, starting qty 10 @ 100. Enqueue order [deplete 5,
    // deplete 8, deplete 3]: 5 ok (qty→5), 8 fails (8 > 5, dropped), 3 ok (qty→2).
    let f = seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;
    seed_aggregate(&pool, f.pool_id, 10, 100).await;

    let base_dropped = committer_stat(&pool, "dropped_submissions_total").await;

    paused(&pool, || async {
        enqueue(&pool, "transfer_shipment", 2_001, vec![depletion_line(&f, 5)])
            .await
            .expect("enqueue 5");
        enqueue(&pool, "transfer_shipment", 2_002, vec![depletion_line(&f, 8)])
            .await
            .expect("enqueue 8");
        enqueue(&pool, "transfer_shipment", 2_003, vec![depletion_line(&f, 3)])
            .await
            .expect("enqueue 3");
    })
    .await;

    // Two of three commit (the over-depletion is dropped).
    await_trx_count(&pool, 2).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(trx_count_for_source(&pool, 2_001).await, 1, "deplete 5 committed");
    assert_eq!(trx_count_for_source(&pool, 2_002).await, 0, "deplete 8 dropped");
    assert_eq!(trx_count_for_source(&pool, 2_003).await, 1, "deplete 3 committed");
    assert_eq!(
        committer_stat(&pool, "dropped_submissions_total").await - base_dropped,
        1,
        "exactly one submission dropped by drop-and-continue"
    );
    assert_eq!(
        aggregate(&pool, f.pool_id).await,
        Some((2, 100)),
        "final qty = 10 - 5 - 3 (the failed 8 contributed nothing)"
    );
    assert_eq!(
        committer_stat(&pool, "tx_failures_total").await,
        0,
        "a per-submission failure does NOT abort the tx (no replay)"
    );
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn duplicate_source_caught_by_preflight_dedup() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    let f = seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;
    seed_aggregate(&pool, f.pool_id, 0, 0).await;

    let base_skips = committer_stat(&pool, "dedup_skips_total").await;

    // Same (trx_type, source_id) twice. They share the pool → one commit_group;
    // within-batch dedup keeps the first and skips the second. No UNIQUE escape.
    paused(&pool, || async {
        enqueue(&pool, "po_receipt", 3_001, vec![receipt_line_for(&f, 10, 500)])
            .await
            .expect("enqueue dup #1");
        enqueue(&pool, "po_receipt", 3_001, vec![receipt_line_for(&f, 10, 500)])
            .await
            .expect("enqueue dup #2");
    })
    .await;

    await_trx_count(&pool, 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        trx_count_for_source(&pool, 3_001).await,
        1,
        "exactly one trx for the duplicated (trx_type, source_id)"
    );
    assert_eq!(
        committer_stat(&pool, "dedup_skips_total").await - base_skips,
        1,
        "the second submission was skipped by pre-flight dedup"
    );
    assert_eq!(
        committer_stat(&pool, "tx_failures_total").await,
        0,
        "dedup prevents the UNIQUE constraint from firing (no tx abort)"
    );
    assert_eq!(
        aggregate(&pool, f.pool_id).await,
        Some((10, 500)),
        "only one receipt applied to the aggregate"
    );
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn committed_group_reclaims_all_arena_blocks() {
    // Regression for the arena leak (acct-yojk.15): each submission allocates a
    // lines blob + submission blob + pool-keys block, and the router adds the
    // commit_group's two blocks. Cleanup must free ALL of them — the lines blob
    // was previously leaked (1 block/submission). After a full drain (committed
    // AND drop-and-continue paths), arena_outstanding must return to baseline.
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;
    seed_aggregate(&pool, f.pool_id, 1_000, 100).await;

    let base = arena_outstanding(&pool).await;

    paused(&pool, || async {
        for i in 0..5i64 {
            enqueue(&pool, "po_receipt", 5_000 + i, vec![receipt_line_for(&f, 10, 100)])
                .await
                .expect("enqueue receipt");
        }
        // An over-depletion the committer drops via drop-and-continue — its arena
        // blocks must be reclaimed too, not just the committed submissions'.
        enqueue(&pool, "transfer_shipment", 5_100, vec![depletion_line(&f, 999_999)])
            .await
            .expect("enqueue over-depletion");
    })
    .await;

    await_trx_count(&pool, 5).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(trx_count(&pool).await, 5, "5 receipts committed; the over-depletion dropped");
    assert_eq!(
        arena_outstanding(&pool).await,
        base,
        "every committed-and-dropped submission's arena blocks (incl. the lines blob) reclaimed"
    );
}
