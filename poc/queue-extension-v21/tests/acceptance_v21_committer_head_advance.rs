//! E1 (acct-gx1z.1.1) acceptance: claim_next_committer_entry advances
//! head PAST the winning slot, not just past the original head.
//!
//! Pre-fix bug (committer.rs:281): `queue.head.store((head + 1) %
//! capacity, Relaxed)` used the loop's original `head` value, not the
//! index that won. If head=0 and valid==1 at idx=5, head would advance
//! to 1 — and the next tick would re-scan idx 1..4 (all valid==0)
//! before reaching the next valid==1 entry.
//!
//! Post-fix: head advances to `(head + i + 1) % capacity` where `i`
//! is the loop offset that won. Same example: head advances to 6.
//!
//! Run via:
//!   cargo test --release --test acceptance_v21_committer_head_advance \
//!     --features pg18 --no-default-features -- --ignored --nocapture

#![cfg(test)]

mod common;

use common::{connect_pool, reset_state};
use sqlx::Row;

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn head_advances_past_winning_slot_not_just_head() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    // Pin head at 0.
    sqlx::query("SELECT poc_v21_test_committer_head_set(0)")
        .execute(&pool)
        .await
        .expect("head_set");

    // Inject a claimable entry at slot 5 (slots 0..4 stay valid==0).
    let injected: bool =
        sqlx::query("SELECT poc_v21_test_inject_claimable_entry(5)")
            .fetch_one(&pool)
            .await
            .expect("inject")
            .get(0);
    assert!(injected, "should successfully inject valid==1 at slot 5");

    // Claim — scan walks head..head+capacity, finds slot 5 (i=5),
    // CAS-claims, advances head.
    let claimed: i64 = sqlx::query("SELECT poc_v21_test_claim_committer_entry()")
        .fetch_one(&pool)
        .await
        .expect("claim")
        .get(0);
    assert_eq!(claimed, 5, "should claim slot 5");

    // Post-fix expected head: 6. Pre-fix bug: head would be 1.
    let head: i64 = sqlx::query("SELECT poc_v21_test_committer_head_get()")
        .fetch_one(&pool)
        .await
        .expect("head_get")
        .get(0);
    assert_eq!(
        head, 6,
        "head must advance past the winning slot (idx=5 + 1), not just past the original head (0 + 1). got head={head}"
    );

    // Cleanup: slot 5 is now valid==2 + committer_pid=MyProcPid; reset
    // so subsequent tests / live workers don't see a phantom in-flight.
    sqlx::query("SELECT poc_v21_test_force_reset_slot(5)")
        .execute(&pool)
        .await
        .expect("force_reset_slot");
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn head_advances_correctly_when_scanning_from_nonzero_head() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    // Pin head at 100. Inject claimable at 107 (offset i=7).
    sqlx::query("SELECT poc_v21_test_committer_head_set(100)")
        .execute(&pool)
        .await
        .expect("head_set");

    let injected: bool =
        sqlx::query("SELECT poc_v21_test_inject_claimable_entry(107)")
            .fetch_one(&pool)
            .await
            .expect("inject")
            .get(0);
    assert!(injected);

    let claimed: i64 = sqlx::query("SELECT poc_v21_test_claim_committer_entry()")
        .fetch_one(&pool)
        .await
        .expect("claim")
        .get(0);
    assert_eq!(claimed, 107);

    let head: i64 = sqlx::query("SELECT poc_v21_test_committer_head_get()")
        .fetch_one(&pool)
        .await
        .expect("head_get")
        .get(0);
    assert_eq!(
        head, 108,
        "head should advance to claim_idx + 1 = 108, not original_head + 1 = 101. got head={head}"
    );

    sqlx::query("SELECT poc_v21_test_force_reset_slot(107)")
        .execute(&pool)
        .await
        .expect("force_reset_slot");
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn head_wraps_at_capacity_boundary() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    // POC_V21_COMMITTER_QUEUE_SIZE = 2048 (lib.rs:80).
    // Pin head at 2045; inject claimable at 2047 (i=2). Post-claim
    // head = (2045 + 2 + 1) % 2048 = 0.
    sqlx::query("SELECT poc_v21_test_committer_head_set(2045)")
        .execute(&pool)
        .await
        .expect("head_set");

    let injected: bool =
        sqlx::query("SELECT poc_v21_test_inject_claimable_entry(2047)")
            .fetch_one(&pool)
            .await
            .expect("inject")
            .get(0);
    assert!(injected);

    let claimed: i64 = sqlx::query("SELECT poc_v21_test_claim_committer_entry()")
        .fetch_one(&pool)
        .await
        .expect("claim")
        .get(0);
    assert_eq!(claimed, 2047);

    let head: i64 = sqlx::query("SELECT poc_v21_test_committer_head_get()")
        .fetch_one(&pool)
        .await
        .expect("head_get")
        .get(0);
    assert_eq!(
        head, 0,
        "head should wrap: (2045 + 2 + 1) % 2048 = 0. got head={head}"
    );

    sqlx::query("SELECT poc_v21_test_force_reset_slot(2047)")
        .execute(&pool)
        .await
        .expect("force_reset_slot");
}
