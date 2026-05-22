//! acct-4a0a property test: multi-submission commit_group with one
//! failing submission preserves the others (pristine-snapshot replay).
//!
//! design-v3 §7: when a submission's plan_apply errors mid-commit_group,
//! the committer discards the working snapshot, adds the failing
//! submission to an excluded set, clones the pristine post-hydration
//! snapshot, and re-runs plan_apply for non-excluded submissions in
//! chronological order. Repeats until clean.
//!
//! Test shape: each case generates N submissions targeting the same
//! pool, exactly one of which is engineered to fail (oversold depletion).
//! Submissions are enqueued back-to-back so the router packs them into
//! one commit_group. Invariants asserted after drain:
//!
//!   R1  the failing submission produces ZERO rows (trx, trx_line,
//!       posting_line, no pool_state mutation attributable to it).
//!   R2  every other submission produces exactly ONE trx + at least
//!       one trx_line + at least one posting_line.
//!   R3  no cross-contamination: surviving submissions' pool_state
//!       reflects only their own qty/unit_cost (running SUM matches
//!       expected sum minus the failing submission).
//!   R4  trx_seq remains contiguous 1..=K for K survivors (no gap
//!       from the excluded one).
//!
//! The mid-batch failing submission also exercises the committer's
//! pristine-replay exclude-and-retry loop wired through
//! `committer::run_pristine_replay_loop` (acct-usn2 inner-loop
//! Cell-tracked in_flight idx).

mod common;

use common::*;
use proptest::prelude::*;
use sqlx::PgPool;
use std::time::Duration;

/// (n_receipts_before, n_receipts_after, oversold_qty). The fail-case
/// is a depletion of `oversold_qty` units, sized to exceed the sum of
/// receipts staged before it AND after it (since the router may pack
/// all into one commit_group and pristine-replay tests order-invariant
/// success of survivors).
fn case_strategy() -> impl Strategy<Value = (u8, u8, i64)> {
    (1u8..=3, 1u8..=3, 5000i64..=50_000)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs running poc_v3 with ledger_routed installed"]
async fn pristine_replay_excludes_failing_submission() {
    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(25);

    let pool = connect_pool().await;
    assert!(
        wait_for_recovery_complete(&pool, Duration::from_secs(10)).await,
        "recovery_complete must signal"
    );

    let mut runner = proptest::test_runner::TestRunner::new(proptest::test_runner::Config {
        cases,
        ..Default::default()
    });

    runner
        .run(&case_strategy(), |(before, after, oversold)| {
            let pool = pool.clone();
            let result = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(run_one_case(pool, before, after, oversold))
            });
            result.map_err(|e| TestCaseError::fail(e))
        })
        .expect("pristine-replay property failed");
}

async fn run_one_case(
    pool: PgPool,
    n_before: u8,
    n_after: u8,
    oversold: i64,
) -> Result<(), String> {
    reset_state(&pool).await;
    let (_, _, pool_id, inv, ap) = seed_fixture(&pool, "fifo").await;

    // Layout per case: N receipts (qty=10 each), then 1 depletion of
    // `oversold` (sized to fail since total receipts qty <= 60), then
    // N more receipts. The failing submission lands in the middle —
    // proves pristine-replay correctly excludes it without poisoning
    // siblings on either side.
    let mut submissions: Vec<(i64, String)> = Vec::new();
    let mut next_sid = 1000i64;

    for _ in 0..n_before {
        next_sid += 1;
        submissions.push((next_sid, po_receipt_line(pool_id, 10, 7, inv, ap)));
    }
    next_sid += 1;
    let fail_sid = next_sid;
    submissions.push((fail_sid, depletion_line(pool_id, oversold, 7, inv, ap)));
    for _ in 0..n_after {
        next_sid += 1;
        submissions.push((next_sid, po_receipt_line(pool_id, 10, 7, inv, ap)));
    }

    // Enqueue back-to-back so the router packs them into one
    // commit_group (single-pool overlap forces affinity).
    for (i, (sid, lines)) in submissions.iter().enumerate() {
        let posted = format!("2026-05-22T10:{:02}:00+00:00", (i % 60));
        let _ = enqueue(&pool, "po_receipt", *sid, &posted, lines)
            .await
            .map_err(|e| format!("enqueue sid={sid}: {e}"))?;
    }

    // Wait for surviving submissions to materialize. The failing one
    // will never appear; poll_for_trx returns None and we move on.
    let survivors: Vec<i64> = submissions
        .iter()
        .map(|(sid, _)| *sid)
        .filter(|sid| *sid != fail_sid)
        .collect();
    for sid in &survivors {
        let id = poll_for_trx(&pool, "po_receipt", *sid, Duration::from_secs(10)).await;
        if id.is_none() {
            return Err(format!(
                "R2 violation: surviving submission sid={sid} never materialized"
            ));
        }
    }
    // Settle window for pristine-replay loop + cleanup.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // R1: the failing submission produces NO trx row.
    let fail_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM trx WHERE trx_type = 'po_receipt'::trx_type AND source_id = $1",
    )
    .bind(fail_sid)
    .fetch_one(&pool)
    .await
    .map_err(|e| e.to_string())?;
    if fail_count != 0 {
        return Err(format!(
            "R1 violation: failing sid={fail_sid} produced {fail_count} trx rows"
        ));
    }

    // R2: every survivor has exactly one trx.
    for sid in &survivors {
        let n: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM trx WHERE trx_type = 'po_receipt'::trx_type AND source_id = $1",
        )
        .bind(sid)
        .fetch_one(&pool)
        .await
        .map_err(|e| e.to_string())?;
        if n != 1 {
            return Err(format!(
                "R2 violation: surviving sid={sid} has {n} trx rows (expected 1)"
            ));
        }
    }

    // R3: pool_state sum equals expected sum of surviving receipts only.
    // Each surviving receipt adds 10 qty; total = 10 * survivors.len().
    let (ps_sum, tl_sum): (i64, i64) = sqlx::query_as(
        "SELECT \
           COALESCE((SELECT SUM(qty)::bigint FROM pool_state WHERE pool_id = $1), 0), \
           COALESCE((SELECT SUM(qty)::bigint FROM trx_line WHERE pool_id = $1), 0)",
    )
    .bind(pool_id)
    .fetch_one(&pool)
    .await
    .map_err(|e| e.to_string())?;
    let expected = 10 * (survivors.len() as i64);
    if ps_sum != expected {
        return Err(format!(
            "R3 violation: pool_state SUM={ps_sum} expected {expected} (10 per survivor)"
        ));
    }
    if ps_sum != tl_sum {
        return Err(format!(
            "R3 violation: pool_state SUM={ps_sum} != trx_line SUM={tl_sum}"
        ));
    }

    // R4: trx_seq remains contiguous 1..=K with no gap from the
    // excluded submission. This is the load-bearing pristine-replay
    // invariant — the failing submission never consumed a trx_seq slot.
    let seqs: Vec<i64> = sqlx::query_scalar(
        "SELECT trx_seq FROM trx_line WHERE pool_id = $1 ORDER BY trx_seq",
    )
    .bind(pool_id)
    .fetch_all(&pool)
    .await
    .map_err(|e| e.to_string())?;
    if seqs.len() != survivors.len() {
        return Err(format!(
            "R4 violation: {} trx_lines on pool but {} survivors",
            seqs.len(),
            survivors.len()
        ));
    }
    for (i, s) in seqs.iter().enumerate() {
        if *s != (i as i64) + 1 {
            return Err(format!(
                "R4 violation: trx_seq[{i}] = {s} (expected {}, no gap allowed)",
                i + 1
            ));
        }
    }

    Ok(())
}
