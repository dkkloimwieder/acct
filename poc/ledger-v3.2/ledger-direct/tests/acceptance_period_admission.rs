//! Acceptance tests for period admission structure (acct-1vur.3; 0021 + 0022,
//! design-v3.2 §7).
//!
//! Two independent defences against the permanent-wedge class:
//!
//!   * **Structure** (0021) — `accounting_period` may not carry inverted
//!     bounds (a period covering nothing silently disarms the 0017 guards) or
//!     overlap another period (which would let one date be simultaneously
//!     frozen and appendable). Gaps between periods stay legal by design.
//!   * **Frontier** (0022) — once any period is closed, no physical event may
//!     land at or before its `end_date`, covered or not. Coverage alone left
//!     two holes — dates before the first period, and inter-period gaps —
//!     through which a backdate is admitted, lowers a recost floor below the
//!     closed boundary, and wedges the engine against the 0017 settlement
//!     guard on every subsequent pass.
//!
//! The gap fixtures here are constructible precisely because 0021 does not
//! mandate abutment.

mod common;

use common::*;
use serde_json::{json, Value};
use sqlx::{Connection, PgConnection, PgPool};

/// Insert a period directly, returning the error rather than panicking, so
/// structural rejections can be asserted.
async fn try_create_period(
    pool: &PgPool,
    id: i64,
    start: &str,
    end: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO accounting_period (id, start_date, end_date, state) \
         VALUES ($1, $2::date, $3::date, 'open')",
    )
    .bind(id)
    .bind(start)
    .bind(end)
    .execute(pool)
    .await
    .map(|_| ())
}

/// Submit a physical receipt, returning the error rather than panicking.
async fn try_receipt_at(
    pool: &PgPool,
    pool_id: i64,
    source_id: i64,
    posted_at: &str,
    qty: i64,
    unit_cost: i64,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar::<_, i64>("SELECT ledger_submit_trx($1, $2, $3, $4::jsonb)")
        .bind("po_receipt")
        .bind(source_id)
        .bind(posted_at)
        .bind(json!([line(pool_id, "po_receipt_line", qty, unit_cost)]))
        .fetch_one(pool)
        .await
}

/// 0021: an inverted period covers no dates at all, which would silently
/// disarm both 0017 guards for that range.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn inverted_period_bounds_are_rejected() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    let err = try_create_period(&pool, 1, "2026-07-31", "2026-07-01").await.unwrap_err();
    assert_eq!(sqlstate(&err), "23514", "CHECK violation, not a silent insert");
    // The degenerate single-day period is legal (start == end).
    try_create_period(&pool, 2, "2026-07-01", "2026-07-01").await.expect("single-day period");
}

/// 0021: overlapping periods would let one date be frozen and appendable at
/// once. Abutting and gapped neighbours both stay legal.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn overlapping_periods_are_rejected_but_gaps_are_allowed() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    create_period(&pool, 1, "2026-07-01", "2026-07-15").await;

    for (id, start, end, what) in [
        (2, "2026-07-10", "2026-07-20", "straddling the tail"),
        (3, "2026-07-05", "2026-07-06", "contained"),
        (4, "2026-06-01", "2026-08-01", "containing"),
        (5, "2026-07-15", "2026-07-16", "sharing the inclusive end date"),
    ] {
        let err = try_create_period(&pool, id, start, end).await.unwrap_err();
        assert_eq!(sqlstate(&err), "23P01", "exclusion violation for {what}");
    }

    // Abutting is legal...
    try_create_period(&pool, 6, "2026-07-16", "2026-07-31").await.expect("abutting period");
    // ...and so is a gap: 0021 deliberately does not mandate contiguity, so
    // the uncovered-range fixtures below are constructible.
    try_create_period(&pool, 7, "2026-08-05", "2026-08-10").await.expect("gapped period");
}

/// The frontier's headline case: after a close, a backdate into a date range
/// NO period covers — before the first period — must reject at insert time.
/// Coverage alone admitted it; the wedge followed at the next engine pass.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn backdate_before_the_first_period_is_rejected_after_a_close() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    set_feed_required(&pool, false).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;
    receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;
    deplete_at(&pool, f.pool_id, 2, T10, 4).await;

    // Pre-close, an uncovered pre-period date is legitimately appendable:
    // gate_pools has no lower bound, so pre-period activity is in scope.
    try_receipt_at(&pool, f.pool_id, 3, "2026-06-15T09:00:00+00:00", 5, 100)
        .await
        .expect("pre-period backdate is legal while nothing is closed");

    assert_eq!(close_period(&pool, 1, "closer", true).await["closed"], json!(true));

    // Post-close the same date is at or before the frontier, though no period
    // covers it. This is the side door.
    let err = try_receipt_at(&pool, f.pool_id, 4, "2026-06-15T10:00:00+00:00", 5, 100)
        .await
        .unwrap_err();
    assert_eq!(sqlstate(&err), "55000");
    assert!(format!("{err}").contains("frontier"), "names the frontier: {err}");
}

/// The same hole through an inter-period gap, and the proof that the frontier
/// is what closes it: the gap sits inside no period at all, yet is refused —
/// while a date beyond the frontier still flows.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn backdate_into_an_inter_period_gap_is_rejected() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    set_feed_required(&pool, false).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;

    // Period 1 covers the T-grid day; period 2 starts three days later, so
    // 2026-07-13 .. 2026-07-14 is covered by nothing.
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;
    create_period(&pool, 2, "2026-07-15", "2026-07-20").await;
    receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;
    deplete_at(&pool, f.pool_id, 2, T10, 4).await;

    let gap_ts = "2026-07-13T09:00:00+00:00";
    // Legal while nothing is closed...
    try_receipt_at(&pool, f.pool_id, 3, gap_ts, 5, 100).await.expect("gap date open");

    assert_eq!(close_period(&pool, 1, "closer", true).await["closed"], json!(true));

    // Closing period 1 (ends 2026-07-11) does NOT lift the frontier over the
    // gap date, so the gap stays open — the frontier is monotonic, not a
    // blanket freeze.
    try_receipt_at(&pool, f.pool_id, 4, gap_ts, 5, 100)
        .await
        .expect("gap above the frontier stays appendable");

    // Closing period 2 moves the frontier past the gap; now it is refused.
    assert_eq!(close_period(&pool, 2, "closer", true).await["closed"], json!(true));
    let err = try_receipt_at(&pool, f.pool_id, 5, gap_ts, 5, 100).await.unwrap_err();
    assert_eq!(sqlstate(&err), "55000");
    assert!(format!("{err}").contains("frontier"), "names the frontier: {err}");

    // Beyond the frontier still flows.
    try_receipt_at(&pool, f.pool_id, 6, "2026-07-21T09:00:00+00:00", 5, 100)
        .await
        .expect("above the frontier");
}

/// `closing` is draining, not frozen: a gate-failed close persists `closing`
/// precisely so the tail can still be appended and drained, so it must NOT
/// raise the frontier.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn closing_does_not_raise_the_frontier() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    set_feed_required(&pool, false).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;
    receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;
    deplete_at(&pool, f.pool_id, 2, T11, 4).await;

    // Unforced close with an unsettled tail: gate fails, period goes 'closing'.
    let report = close_period(&pool, 1, "closer", false).await;
    assert_eq!(report["closed"], json!(false));
    assert_eq!(period_state(&pool, 1).await.0, "closing");

    // Both a covered date and an uncovered pre-period date stay appendable.
    try_receipt_at(&pool, f.pool_id, 3, T12, 5, 100).await.expect("covered, still draining");
    try_receipt_at(&pool, f.pool_id, 4, "2026-06-15T09:00:00+00:00", 5, 100)
        .await
        .expect("uncovered, still draining");
}

/// The frontier is monotonic across several closed periods: it tracks the
/// greatest closed `end_date`, not the most recently closed period.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn frontier_tracks_the_greatest_closed_end_date() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    set_feed_required(&pool, false).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;
    create_period(&pool, 2, "2026-07-12", "2026-07-20").await;
    receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;

    assert_eq!(close_period(&pool, 1, "closer", true).await["closed"], json!(true));
    assert_eq!(close_period(&pool, 2, "closer", true).await["closed"], json!(true));

    // Everything at or before 2026-07-20 is refused, including the gap-free
    // interior of period 2 and the uncovered range below period 1.
    for ts in ["2026-06-15T09:00:00+00:00", "2026-07-18T09:00:00+00:00"] {
        let err = try_receipt_at(&pool, f.pool_id, 9, ts, 5, 100).await.unwrap_err();
        assert_eq!(sqlstate(&err), "55000", "at {ts}");
    }
    try_receipt_at(&pool, f.pool_id, 10, "2026-07-21T00:00:00+00:00", 5, 100)
        .await
        .expect("first instant above the frontier");
}

/// The engine's own `cost_adjustment_line` output stays exempt: it is stamped
/// `posted_at = now()` and excluded from replay, and the close's residue
/// sweep must be able to post it while the period is being finalized.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn adjustment_lines_stay_exempt_from_the_frontier() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    set_feed_required(&pool, false).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;
    // Two lots at different costs so the FIFO authoritative cost (167) differs
    // from the running-average observed cost (200) — otherwise the delta is
    // zero and the close posts no adjustment line at all, proving nothing.
    receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;
    receipt_at(&pool, f.pool_id, 2, T10, 10, 300).await;
    deplete_at(&pool, f.pool_id, 3, T11, 15).await;

    // The close drains and sweeps, both of which insert adjustment lines while
    // the frontier is being established.
    let report = close_period(&pool, 1, "closer", true).await;
    assert_eq!(report["closed"], json!(true));
    assert!(
        count(&pool, "SELECT count(*) FROM trx_line WHERE line_type = 'cost_adjustment_line'")
            .await
            > 0,
        "the close posted adjustment lines under the new frontier"
    );

    // A post-close settle on the same pool still posts its own adjustments.
    settle_pool(&pool, f.pool_id).await;
}

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
        .expect("poll pg_stat_activity");
        if blocked {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("session {pid} never parked on a lock");
}

/// The interlock the sentinel exists for: an insert at a date NO period covers
/// races a close that is about to raise the frontier past it.
///
/// Fencing the frontier on the period that already supplies it would not work
/// — the exclusive side of a per-period key is only ever held for the period
/// being closed, which is not yet 'closed', so the two sides would never meet
/// and an uncovered-date insert (whose coverage loop iterates zero times)
/// would take no lock at all. The insert must therefore park on the shared
/// side of `(32022, 0)` while the close holds it, and reject on the fresh
/// frontier once the close commits.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn uncovered_backdate_interlocks_with_a_concurrent_close() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    set_feed_required(&pool, false).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;
    receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;

    // The inserter targets a SECOND pool with no history. `gate_pools`
    // requires in-period activity, so pool 2 is out of gate scope and the
    // close never locks its aggregate row — which keeps this test measuring
    // the sentinel and nothing else. Targeting pool 1 would instead deadlock:
    // the hot-path CTE upserts the aggregate before the trigger runs, so the
    // inserter would hold the row the closer's sweep wants while waiting on
    // the sentinel the closer holds (see the close.rs deadlock note).
    seed_pool(&pool, 2, 2, 2, "fifo", "running_avg").await;

    // Nothing is closed yet, so this date is legally appendable right now —
    // and it is covered by no period, so only the sentinel can interlock it.
    let gap_ts = "2026-06-15T09:00:00+00:00";
    assert_eq!(count(&pool, "SELECT count(*) FROM accounting_period WHERE state = 'closed'").await, 0);

    // Closer session: hold the sentinel, then close inside the same
    // transaction. Holding it first is what the closer's own fence does.
    let (mut closer, _) = session().await;
    let mut closer_tx = closer.begin().await.expect("closer begin");
    sqlx::query("SELECT pg_advisory_xact_lock(32022, 0)")
        .execute(&mut *closer_tx)
        .await
        .expect("closer takes the sentinel");

    // Inserter session: must park on the sentinel rather than sail through.
    let (mut inserter, inserter_pid) = session().await;
    let insert = tokio::spawn(async move {
        sqlx::query_scalar::<_, i64>("SELECT ledger_submit_trx($1, $2, $3, $4::jsonb)")
            .bind("po_receipt")
            .bind(99i64)
            .bind(gap_ts)
            .bind(json!([line(2, "po_receipt_line", 5, 100)]))
            .fetch_one(&mut inserter)
            .await
    });
    wait_until_lock_blocked(&pool, inserter_pid).await;

    // The close commits, raising the frontier over the parked insert's date.
    let report: Value = sqlx::query_scalar("SELECT ledger_close_period(1, 'closer', true)")
        .fetch_one(&mut *closer_tx)
        .await
        .expect("close under the held sentinel");
    assert_eq!(report["closed"], json!(true));
    closer_tx.commit().await.expect("closer commit");

    // The insert wakes on a fresh snapshot and must reject — not land below a
    // now-closed frontier.
    let err = insert.await.expect("inserter task").unwrap_err();
    assert_eq!(sqlstate(&err), "55000");
    assert!(format!("{err}").contains("frontier"), "rejected on the frontier: {err}");
    assert_eq!(
        count(&pool, "SELECT count(*) FROM trx_line WHERE posted_at = '2026-06-15T09:00:00+00:00'")
            .await,
        0,
        "nothing landed below the frontier"
    );
}
