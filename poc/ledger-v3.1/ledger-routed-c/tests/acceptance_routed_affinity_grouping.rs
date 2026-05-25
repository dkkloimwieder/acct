//! acct-2ttr.5 §6.3 acceptance: the router groups staged submissions into
//! commit_groups by pool overlap (union-find affinity), chunking oversized
//! components at `batch_size_max`.
//!
//! Determinism: the router ticks on a ~50 ms latch, so a batch enqueued without
//! coordination could be split across ticks. Each test therefore PAUSES the
//! router (`test_bgworker_paused`), stages its whole batch, then RESUMES — so
//! the batch is collected in one router window and the grouping is exact.
//!
//! The committer (P3.3) is still a shell that never drains, so every emitted
//! commit_group stays at `ready`; the tests read those ready groups directly
//! (`ledger_routed_c_ready_commit_groups`) rather than asserting on `trx` rows.
//! Each test uses a disjoint pool_id range so its assertions are isolated from
//! groups other tests leave in the (never-drained) shared queue.
//!
//! Requires the `test_hooks` build (the cluster-per-binary runner installs it).

mod common;

use common::*;
use sqlx::PgPool;

/// Pause the router, stage `batches` (each a list of pool_ids → one submission),
/// resume, and return the ready commit_groups attributed to `mine` once all
/// `batches.len()` submissions have routed.
async fn route_batch(
    pool: &PgPool,
    base_source: i64,
    mine: &[i64],
    batches: &[Vec<i64>],
) -> Vec<(i64, i64, Vec<i64>)> {
    // Keep the committer parked for the whole binary so emitted commit_groups
    // stay at `ready` for inspection (and so it never tries to drain these
    // pools, which aren't seeded in the DB). Idempotent across calls.
    set_committer_paused(pool, true).await;
    set_router_paused(pool, true).await;
    // Let any in-progress tick finish and the worker park on the flag.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    for (i, pools) in batches.iter().enumerate() {
        enqueue_pools(pool, base_source + i as i64, pools)
            .await
            .expect("enqueue");
    }

    set_router_paused(pool, false).await;
    await_routed(pool, mine, batches.len() as i64).await
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn overlapping_submissions_land_in_one_commit_group() {
    let pool = connect_pool().await;

    // Four submissions, all touching pool 5001 → one connected component.
    let mine = [5001];
    let groups = route_batch(
        &pool,
        5_000,
        &mine,
        &[vec![5001], vec![5001], vec![5001], vec![5001]],
    )
    .await;

    assert_eq!(groups.len(), 1, "all overlapping subs → one group: {groups:?}");
    assert_eq!(groups[0].1, 4, "the group holds all 4 submissions");
    assert_eq!(groups[0].2, vec![5001], "group owns exactly pool 5001");

    // The P3.2 committer shell must never advance a group past `ready`.
    assert_eq!(committer_queue_count(&pool, "in_flight").await, 0);
    assert_eq!(committer_queue_count(&pool, "done").await, 0);
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn disjoint_submissions_group_independently() {
    let pool = connect_pool().await;

    // Three submissions on three distinct pools → three singleton components.
    let mine = [5101, 5102, 5103];
    let groups = route_batch(
        &pool,
        5_100,
        &mine,
        &[vec![5101], vec![5102], vec![5103]],
    )
    .await;

    assert_eq!(groups.len(), 3, "disjoint pools → independent groups: {groups:?}");
    for g in &groups {
        assert_eq!(g.1, 1, "each disjoint group holds a single submission");
        assert_eq!(g.2.len(), 1, "each group owns exactly one pool");
    }
    let mut owned: Vec<i64> = groups.iter().flat_map(|g| g.2.clone()).collect();
    owned.sort();
    assert_eq!(owned, vec![5101, 5102, 5103]);
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn transitive_overlap_merges_into_one_component() {
    let pool = connect_pool().await;

    // A:[5201,5202], B:[5202,5203] share 5202 → merge; C:[5204] stands alone.
    let mine = [5201, 5202, 5203, 5204];
    let groups = route_batch(
        &pool,
        5_200,
        &mine,
        &[vec![5201, 5202], vec![5202, 5203], vec![5204]],
    )
    .await;

    assert_eq!(groups.len(), 2, "one merged component + one singleton: {groups:?}");
    let mut sizes: Vec<i64> = groups.iter().map(|g| g.1).collect();
    sizes.sort();
    assert_eq!(sizes, vec![1, 2], "merged group of 2, singleton of 1");

    // The 2-submission group owns the transitive pool closure {5201,5202,5203}.
    let merged = groups.iter().find(|g| g.1 == 2).expect("merged group");
    assert_eq!(merged.2, vec![5201, 5202, 5203]);
    let solo = groups.iter().find(|g| g.1 == 1).expect("solo group");
    assert_eq!(solo.2, vec![5204]);
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn oversized_component_splits_into_batch_size_max_chunks() {
    let pool = connect_pool().await;

    // One hot pool with batch_max + 1 submissions → exactly two chunks of
    // sizes [batch_max, 1]. (Within-chunk request_seq ordering is locked by
    // the affinity_group unit tests; here we assert the split shape.)
    let b = batch_size_max(&pool).await;
    let n = (b + 1) as usize;
    let mine = [5301];
    let batches: Vec<Vec<i64>> = (0..n).map(|_| vec![5301]).collect();
    let groups = route_batch(&pool, 5_400, &mine, &batches).await;

    assert_eq!(
        groups.len(),
        2,
        "oversized component splits into ceil((b+1)/b)=2 chunks: {groups:?}"
    );
    let total: i64 = groups.iter().map(|g| g.1).sum();
    assert_eq!(total, b + 1, "no submission lost across the chunk boundary");
    let max = groups.iter().map(|g| g.1).max().unwrap();
    let min = groups.iter().map(|g| g.1).min().unwrap();
    assert_eq!(max, b, "the full chunk is capped at batch_size_max");
    assert_eq!(min, 1, "the remainder chunk holds the overflow submission");
    for g in &groups {
        assert_eq!(g.2, vec![5301], "both chunks own the same hot pool");
    }
}
