//! acct-mvq4.37 (Pass-3 C2): cover enqueue's two backpressure ERROR exits.
//!
//! The wait-and-recover arm of enqueue backpressure is heavily exercised (full-
//! blast benches engage the CV-wait with zero drops), but the two ERROR exits
//! had never run in any test or bench:
//!
//!   - staging-queue full past `queue_full_timeout_ms` -> ERRCODE_INSUFFICIENT_
//!     RESOURCES (SQLSTATE 53000), and
//!   - spillover-arena exhausted (single-push deadline / batch mid-alloc).
//!
//! Filling the 16384-slot ring or the 128 MB arena for real, or SET-tweaking the
//! 5 s Sighup `queue_full_timeout_ms`, is impractical. Instead the `test_hooks`
//! build exposes force-fail flags (`set_force_queue_full` / `set_force_arena_full`
//! / `set_queue_full_after`) plus a `set_deadline_expired` override so the wait
//! loop raises immediately. Each case asserts the SQLSTATE and — the real point —
//! that the error path leaves the arena clean (partial allocs freed), so a
//! backpressure rejection cannot leak spillover blocks.
//!
//! Workers are paused throughout so the forced state is observable and nothing
//! drains the arena mid-measurement. Every case disarms its flags and resumes the
//! workers BEFORE asserting, so a failed assert never strands the binary.

mod common;

use common::*;

const SQLSTATE_INSUFFICIENT_RESOURCES: &str = "53000";

/// Pause both workers and let any in-progress tick settle, so the forced
/// backpressure state is observable and the arena is quiescent for a baseline.
async fn quiesce(pool: &sqlx::PgPool) {
    set_router_paused(pool, true).await;
    set_committer_paused(pool, true).await;
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
}

/// Clear all backpressure force-fail flags and resume both workers. Called before
/// assertions so a panic never leaves the flags armed for the rest of the binary.
async fn disarm_and_resume(pool: &sqlx::PgPool) {
    set_force_queue_full(pool, false).await;
    set_force_arena_full(pool, false).await;
    set_deadline_expired(pool, false).await;
    set_queue_full_after(pool, -1).await;
    set_committer_paused(pool, false).await;
    set_router_paused(pool, false).await;
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn single_push_queue_full_timeout_raises_53000_and_frees_arena() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    quiesce(&pool).await;

    let arena_before = arena_outstanding(&pool).await;

    // Ring reports full on every push; the deadline reads elapsed so the wait
    // loop raises on the first pass instead of sleeping 5 s.
    set_force_queue_full(&pool, true).await;
    set_deadline_expired(&pool, true).await;

    let res = enqueue(&pool, "po_receipt", 1, vec![receipt_line(1, 10, 50)]).await;

    // Measure while still paused: the alloc that the failed push frees must be
    // back before anything else can touch the arena.
    let arena_after = arena_outstanding(&pool).await;
    disarm_and_resume(&pool).await;

    let err = res.expect_err("queue-full past deadline must raise");
    assert_eq!(
        pgcode(&err),
        SQLSTATE_INSUFFICIENT_RESOURCES,
        "queue-full timeout must raise SQLSTATE 53000: {err}"
    );
    assert!(
        format!("{err}").contains("staging queue full"),
        "message should name the staging-queue-full cause: {err}"
    );
    // The submission allocated 3 arena blocks, then freed them when the push
    // failed — a rejected enqueue must not leak spillover.
    assert_eq!(
        arena_after, arena_before,
        "queue-full raise must free its partial arena allocation"
    );
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn single_push_arena_full_timeout_raises_53000() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    quiesce(&pool).await;

    let arena_before = arena_outstanding(&pool).await;

    // Arena reports exhausted before allocating; the deadline reads elapsed so the
    // ArenaFull arm raises instead of CV-waiting.
    set_force_arena_full(&pool, true).await;
    set_deadline_expired(&pool, true).await;

    let res = enqueue(&pool, "po_receipt", 1, vec![receipt_line(1, 10, 50)]).await;

    let arena_after = arena_outstanding(&pool).await;
    disarm_and_resume(&pool).await;

    let err = res.expect_err("arena-full past deadline must raise");
    assert_eq!(
        pgcode(&err),
        SQLSTATE_INSUFFICIENT_RESOURCES,
        "arena-full timeout must raise SQLSTATE 53000: {err}"
    );
    assert!(
        format!("{err}").contains("arena exhausted"),
        "message should name the arena-exhausted cause: {err}"
    );
    // No block was ever allocated on this path.
    assert_eq!(
        arena_after, arena_before,
        "arena-full raise must not have allocated anything"
    );
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn batch_queue_full_full_fail_raises_53000_and_frees_all() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    quiesce(&pool).await;

    let arena_before = arena_outstanding(&pool).await;

    // Batch allocs all five up front, then the push phase reports the ring full
    // for every entry -> 0/5 pushed -> the whole batch's arena is freed at raise.
    set_force_queue_full(&pool, true).await;
    set_deadline_expired(&pool, true).await;

    let res = enqueue_batch(
        &pool,
        vec![
            ("po_receipt", 501, vec![receipt_line(1, 10, 50)]),
            ("po_receipt", 502, vec![receipt_line(2, 10, 50)]),
            ("po_receipt", 503, vec![receipt_line(3, 10, 50)]),
            ("po_receipt", 504, vec![receipt_line(4, 10, 50)]),
            ("po_receipt", 505, vec![receipt_line(5, 10, 50)]),
        ],
    )
    .await;

    let arena_after = arena_outstanding(&pool).await;
    disarm_and_resume(&pool).await;

    let err = res.expect_err("batch queue-full full-fail must raise");
    assert_eq!(
        pgcode(&err),
        SQLSTATE_INSUFFICIENT_RESOURCES,
        "batch queue-full must raise SQLSTATE 53000: {err}"
    );
    assert!(
        format!("{err}").contains("at 0/5 of batch"),
        "message should report 0/5 pushed: {err}"
    );
    // Alloc-all then free-all: no submission published, arena back to baseline.
    assert_eq!(
        arena_after, arena_before,
        "full-fail batch must free every allocated block"
    );
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn batch_arena_full_midalloc_raises_53000_and_frees_partial() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    quiesce(&pool).await;

    let arena_before = arena_outstanding(&pool).await;

    // The batch alloc loop is all-or-nothing: the first ArenaFull frees whatever
    // it already alloced and raises immediately (no deadline gate on this arm).
    set_force_arena_full(&pool, true).await;

    let res = enqueue_batch(
        &pool,
        vec![
            ("po_receipt", 511, vec![receipt_line(1, 10, 50)]),
            ("po_receipt", 512, vec![receipt_line(2, 10, 50)]),
            ("po_receipt", 513, vec![receipt_line(3, 10, 50)]),
            ("po_receipt", 514, vec![receipt_line(4, 10, 50)]),
            ("po_receipt", 515, vec![receipt_line(5, 10, 50)]),
        ],
    )
    .await;

    let arena_after = arena_outstanding(&pool).await;
    disarm_and_resume(&pool).await;

    let err = res.expect_err("batch arena-full must raise");
    assert_eq!(
        pgcode(&err),
        SQLSTATE_INSUFFICIENT_RESOURCES,
        "batch arena-full must raise SQLSTATE 53000: {err}"
    );
    assert!(
        format!("{err}").contains("arena exhausted"),
        "message should name the arena-exhausted cause: {err}"
    );
    // The first alloc was forced full before any block was taken, so free_alloced
    // ran over an empty set and the arena is untouched.
    assert_eq!(
        arena_after, arena_before,
        "arena-full mid-alloc must free its partial allocation"
    );
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn batch_partial_push_frees_remainder_and_raises_53000() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    quiesce(&pool).await;

    let arena_before = arena_outstanding(&pool).await;

    // Allow exactly two pushes, then the ring reports full; the deadline reads
    // elapsed so the remainder is freed and the call raises rather than waiting.
    set_queue_full_after(&pool, 2).await;
    set_deadline_expired(&pool, true).await;

    let res = enqueue_batch(
        &pool,
        vec![
            ("po_receipt", 521, vec![receipt_line(1, 10, 50)]),
            ("po_receipt", 522, vec![receipt_line(2, 10, 50)]),
            ("po_receipt", 523, vec![receipt_line(3, 10, 50)]),
            ("po_receipt", 524, vec![receipt_line(4, 10, 50)]),
            ("po_receipt", 525, vec![receipt_line(5, 10, 50)]),
        ],
    )
    .await;

    // Two envelopes were published (their 6 blocks ride the now-aborted user-tx);
    // the three-envelope remainder (9 blocks) is freed at raise.
    let arena_after = arena_outstanding(&pool).await;
    disarm_and_resume(&pool).await;

    let err = res.expect_err("batch partial-push must raise");
    assert_eq!(
        pgcode(&err),
        SQLSTATE_INSUFFICIENT_RESOURCES,
        "batch partial-push must raise SQLSTATE 53000: {err}"
    );
    assert!(
        format!("{err}").contains("at 2/5 of batch"),
        "message should report 2/5 pushed: {err}"
    );
    // Remainder freed; only the two published submissions' blocks remain
    // outstanding (3 arena blocks each).
    assert_eq!(
        arena_after - arena_before,
        6,
        "partial-push must free the unpushed remainder, leaving only the 2 pushed"
    );
}
