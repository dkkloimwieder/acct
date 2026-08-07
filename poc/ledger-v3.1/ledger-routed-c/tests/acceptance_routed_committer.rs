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

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn racing_duplicate_redrives_group_minus_offender() {
    // acct-yojk.9: a 23505 that survives pre-flight dedup (a racer wrote one of the
    // group's (trx_type, source_id) keys AFTER our dedup ran but before our INSERT)
    // must NOT dead-letter the group's innocent siblings. The committer re-dedups
    // the now-visible racer out and re-drives the rest.
    //
    // Determinism: stall the committer between pool_lock acquire and the write —
    // a window past pre-flight dedup — then INSERT the racer trx on this connection.
    // When the stall ends, the committer's own INSERT for that key hits 23505; the
    // re-drive drops it and commits the innocent receipt.
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;
    seed_aggregate(&pool, f.pool_id, 0, 0).await;

    let base_redrives = committer_stat(&pool, "duplicate_redrives_total").await;
    let base_skips = committer_stat(&pool, "dedup_skips_total").await;
    let base_poisoned = committer_stat(&pool, "poisoned_total").await;
    let base_dropped = committer_stat(&pool, "dropped_submissions_total").await;
    let base_committed = committer_stat(&pool, "trx_committed_total").await;
    let base_stall = committer_stall_hits(&pool).await;

    // Long stall so the racer-insert lands comfortably inside the window.
    set_committer_stall_us(&pool, 3_000_000).await;

    // Innocent receipt 7001 + receipt 7002 (the one a racer will steal), one group.
    paused(&pool, || async {
        enqueue(&pool, "po_receipt", 7_001, vec![receipt_line_for(&f, 10, 100)])
            .await
            .expect("enqueue innocent receipt");
        enqueue(&pool, "po_receipt", 7_002, vec![receipt_line_for(&f, 20, 100)])
            .await
            .expect("enqueue soon-to-race receipt");
    })
    .await;

    // The committer has claimed the group, passed pre-flight dedup + locks, and is
    // now parked in the stall. Clear the stall so the re-drive attempt won't stall
    // again, then commit the racer's trx for 7002 on this connection.
    await_stall_hit(&pool, base_stall).await;
    set_committer_stall_us(&pool, 0).await;
    sqlx::query("INSERT INTO trx (trx_type, source_id, posted_at) VALUES ('po_receipt'::trx_type, 7002, now())")
        .execute(&pool)
        .await
        .expect("insert racing trx for 7002");

    // After the stall ends: insert_trx(7002) → 23505 → re-drive drops 7002 →
    // insert_trx(7001) commits. trx ends with 7001 (committer) + 7002 (racer) = 2.
    await_trx_count(&pool, 2).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(
        trx_count_for_source(&pool, 7_001).await,
        1,
        "the innocent sibling committed — it was NOT dead-lettered with the offender"
    );
    assert_eq!(
        trx_count_for_source(&pool, 7_002).await,
        1,
        "exactly one trx for the raced key (the racer's; the committer dropped its own)"
    );
    assert_eq!(
        committer_stat(&pool, "trx_committed_total").await - base_committed,
        1,
        "the committer wrote exactly one trx (the innocent 7001), not the duplicate"
    );
    assert_eq!(
        committer_stat(&pool, "duplicate_redrives_total").await - base_redrives,
        1,
        "exactly one re-drive for the surviving-UNIQUE 7002"
    );
    assert_eq!(
        committer_stat(&pool, "dedup_skips_total").await - base_skips,
        1,
        "the raced 7002 was re-dedup'd out"
    );
    assert_eq!(
        committer_stat(&pool, "poisoned_total").await - base_poisoned,
        0,
        "the group was NOT poisoned — only the offender dropped"
    );
    assert_eq!(
        committer_stat(&pool, "dropped_submissions_total").await - base_dropped,
        0,
        "the offender is a dedup skip, not a drop-and-continue drop"
    );
    assert_eq!(
        aggregate(&pool, f.pool_id).await,
        Some((10, 100)),
        "aggregate reflects only the innocent receipt (the raced 7002 wrote no lines)"
    );
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn irresolvable_unique_poisons_group() {
    // acct-yojk.9 safety valve: a 23505 with NO resolvable duplicate in trx (e.g. a
    // UNIQUE other than (trx_type, source_id), modeled here by a synthetic injected
    // 23505) can't make progress by re-driving — it must poison, not loop.
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;
    seed_aggregate(&pool, f.pool_id, 0, 0).await;

    let base_poisoned = committer_stat(&pool, "poisoned_total").await;
    let base_redrives = committer_stat(&pool, "duplicate_redrives_total").await;

    set_inject_unique(&pool, true).await;
    paused(&pool, || async {
        enqueue(&pool, "po_receipt", 8_001, vec![receipt_line_for(&f, 10, 100)])
            .await
            .expect("enqueue receipt");
    })
    .await;

    // No trx will ever appear; poll the poison counter instead.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if committer_stat(&pool, "poisoned_total").await - base_poisoned >= 1 {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("timed out waiting for the irresolvable 23505 to poison the group");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        committer_stat(&pool, "duplicate_redrives_total").await - base_redrives,
        1,
        "the 23505 entered the re-drive arm once before poisoning"
    );
    assert_eq!(
        committer_stat(&pool, "poisoned_total").await - base_poisoned,
        1,
        "an irresolvable 23505 poisons (does not loop)"
    );
    assert_eq!(trx_count_for_source(&pool, 8_001).await, 0, "no trx written for the poisoned group");
    assert_eq!(
        aggregate(&pool, f.pool_id).await,
        Some((0, 0)),
        "aggregate untouched by the poisoned group"
    );
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn deadlock_retry_then_racing_duplicate_drops_offender_survivor_commits() {
    // acct-mvq4.38 (Pass-3 C3): the §6.8 retry shape re-attempts a deadlocked
    // write WITHOUT re-triage or re-dedup; its safety rests on the UNIQUE backstop
    // turning a duplicate that arrives during the retry into a 23505 -> re-drive.
    // deadlock_during_write_retries_then_commits proves retry-then-commit alone;
    // racing_duplicate_redrives_group_minus_offender proves the re-drive alone. The
    // specific interleaving — a duplicate lands between attempt N's rollback and
    // attempt N+1's write — had never executed. This drives exactly that.
    //
    // Determinism rests on the injection order inside attempt_commit_phase:
    // maybe_inject_deadlock() raises BEFORE maybe_stall(), so
    //   attempt 1: pool_lock -> injected 40P01 -> rollback + backoff (no stall), and
    //   attempt 2: pool_lock -> (deadlock budget spent) -> STALL.
    // The stall is the window strictly AFTER attempt 1's rollback. We insert the
    // racer there; attempt 2's own INSERT then hits a real 23505, the re-drive drops
    // the offender, and attempt 3 commits the innocent survivor.
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;
    seed_aggregate(&pool, f.pool_id, 0, 0).await;

    let base_retries = committer_stat(&pool, "deadlock_retries_total").await;
    let base_redrives = committer_stat(&pool, "duplicate_redrives_total").await;
    let base_skips = committer_stat(&pool, "dedup_skips_total").await;
    let base_poisoned = committer_stat(&pool, "poisoned_total").await;
    let base_committed = committer_stat(&pool, "trx_committed_total").await;
    let base_stall = committer_stall_hits(&pool).await;

    // Innocent survivor 7401 + soon-to-race offender 7402, one commit_group (same
    // pool -> same affinity component). Route with the committer parked so the
    // injections land on exactly this group's write phase.
    route_with_committer_paused(&pool, || async {
        enqueue(&pool, "po_receipt", 7_401, vec![receipt_line_for(&f, 10, 100)])
            .await
            .expect("enqueue innocent survivor");
        enqueue(&pool, "po_receipt", 7_402, vec![receipt_line_for(&f, 20, 100)])
            .await
            .expect("enqueue soon-to-race offender");
    })
    .await;

    // One deadlock (consumed by attempt 1) and a long stall (reached only on
    // attempt 2, once the deadlock budget is spent).
    set_inject_deadlock_count(&pool, 1).await;
    set_committer_stall_us(&pool, 3_000_000).await;
    set_committer_paused(&pool, false).await;

    // Attempt 1 has already deadlocked + rolled back; the committer is now parked in
    // attempt 2's stall — strictly between the rollback and attempt 2's write. Clear
    // the stall so the re-drive attempt (3) won't stall, then commit the racer's trx
    // for 7402 on this connection.
    await_stall_hit(&pool, base_stall).await;
    set_committer_stall_us(&pool, 0).await;
    sqlx::query("INSERT INTO trx (trx_type, source_id, posted_at) VALUES ('po_receipt'::trx_type, 7402, now())")
        .execute(&pool)
        .await
        .expect("insert racing trx for 7402");

    // Final trx set: 7401 (committer) + 7402 (racer) = 2.
    await_trx_count(&pool, 2).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(
        trx_count_for_source(&pool, 7_401).await,
        1,
        "the innocent survivor commits exactly once — not dead-lettered with the offender"
    );
    assert_eq!(
        trx_count_for_source(&pool, 7_402).await,
        1,
        "exactly one trx for the offender key (the racer's; the committer dropped its own)"
    );
    assert_eq!(
        committer_stat(&pool, "deadlock_retries_total").await - base_retries,
        1,
        "the injected deadlock forced exactly one retry"
    );
    assert_eq!(
        committer_stat(&pool, "duplicate_redrives_total").await - base_redrives,
        1,
        "the duplicate that arrived across the retry boundary forced exactly one re-drive"
    );
    assert_eq!(
        committer_stat(&pool, "dedup_skips_total").await - base_skips,
        1,
        "the offender was re-dedup'd out on the re-drive"
    );
    assert_eq!(
        committer_stat(&pool, "trx_committed_total").await - base_committed,
        1,
        "the committer wrote exactly one trx (the survivor 7401), not the duplicate"
    );
    assert_eq!(
        committer_stat(&pool, "poisoned_total").await - base_poisoned,
        0,
        "deadlock-retry composed with duplicate-re-drive does NOT poison the group"
    );
    assert_eq!(
        aggregate(&pool, f.pool_id).await,
        Some((10, 100)),
        "aggregate reflects only the survivor (the raced 7402 wrote no lines)"
    );
}
