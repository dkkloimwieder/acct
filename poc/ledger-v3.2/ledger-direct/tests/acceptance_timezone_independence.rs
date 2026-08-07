//! Acceptance tests for session-timezone independence (acct-1vur.5; 0024,
//! design-v3.2 §7).
//!
//! Every period-coverage predicate compares a TIMESTAMPTZ against a DATE
//! boundary. Cast naively with `::timestamptz`, that boundary resolves in the
//! SESSION's TimeZone, so a client that sends its own zone (many drivers do)
//! evaluates a different period window than the UTC writers did — and a
//! boundary-adjacent event admitted by an eastern writer is then counted
//! in-period by the UTC closer and rejected by the UTC settlement guard,
//! reaching the acct-1vur.3 permanent wedge without any backdating at all.
//!
//! The T-grid is accidentally robust to this (its timestamps sit far from
//! both boundaries), so these cases use deliberately boundary-adjacent
//! instants where a UTC and an America/New_York session disagree:
//!
//!   * `2026-07-12T02:00Z` — OUTSIDE the period in UTC (period ends
//!     2026-07-11, so the boundary is 2026-07-12T00:00Z), but INSIDE it under
//!     a naive New_York cast (whose boundary is 2026-07-12T04:00Z).
//!   * `2026-07-01T02:00Z` — INSIDE the period in UTC (which starts at
//!     2026-07-01T00:00Z), but OUTSIDE it under a naive New_York cast (whose
//!     start is 2026-07-01T04:00Z).
//!
//! Both must be classified identically from either session.

mod common;

use common::*;
use serde_json::json;
use sqlx::{Connection, PgConnection};

/// After the period ends in UTC; before it ends under a naive New_York cast.
const AFTER_END_UTC: &str = "2026-07-12T02:00:00+00:00";
/// Inside the period in UTC; before it starts under a naive New_York cast.
const AFTER_START_UTC: &str = "2026-07-01T02:00:00+00:00";

const EAST: &str = "America/New_York";

/// A connection pinned to a session TimeZone, mimicking a driver that sends
/// its own on connect.
async fn session_in(tz: &str) -> PgConnection {
    let mut c = PgConnection::connect(POC_DSN).await.expect("session connect");
    sqlx::query(&format!("SET TIME ZONE '{tz}'")).execute(&mut c).await.expect("set tz");
    c
}

async fn submit_on(
    conn: &mut PgConnection,
    pool_id: i64,
    source_id: i64,
    posted_at: &str,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar::<_, i64>("SELECT ledger_submit_trx($1, $2, $3, $4::jsonb)")
        .bind("po_receipt")
        .bind(source_id)
        .bind(posted_at)
        .bind(json!([line(pool_id, "po_receipt_line", 5, 100)]))
        .fetch_one(conn)
        .await
}

/// Pins the CAST SEMANTICS the whole fix rests on, so a later "simplification"
/// back to a naive cast fails here first. This asserts PostgreSQL's behaviour
/// rather than this repository's, so it does not discriminate the fix on its
/// own — the guard/close cases below do that.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn the_pinned_boundary_is_session_independent() {
    let mut utc = session_in("UTC").await;
    let mut east = session_in(EAST).await;

    let pinned_utc: String = sqlx::query_scalar(
        "SELECT (('2026-07-11'::date + 1)::timestamp AT TIME ZONE 'UTC')::text",
    )
    .fetch_one(&mut utc)
    .await
    .unwrap();
    let pinned_east: String = sqlx::query_scalar(
        "SELECT (('2026-07-11'::date + 1)::timestamp AT TIME ZONE 'UTC')::text",
    )
    .fetch_one(&mut east)
    .await
    .unwrap();
    // Rendered in each session's zone, but the same instant.
    let same_instant: bool = sqlx::query_scalar(
        "SELECT (('2026-07-11'::date + 1)::timestamp AT TIME ZONE 'UTC') \
              = '2026-07-12T00:00:00+00:00'::timestamptz",
    )
    .fetch_one(&mut east)
    .await
    .unwrap();
    assert!(same_instant, "pinned boundary is UTC midnight from the east session");

    // The naive cast, by contrast, moves with the session.
    let naive_east: bool = sqlx::query_scalar(
        "SELECT ('2026-07-11'::date + 1)::timestamptz \
              = '2026-07-12T00:00:00+00:00'::timestamptz",
    )
    .fetch_one(&mut east)
    .await
    .unwrap();
    assert!(!naive_east, "the naive cast is exactly what 0024 removes");
    let _ = (pinned_utc, pinned_east);
}

/// The admission guards classify a boundary-adjacent event identically from
/// either session. Under the naive cast the eastern writer would consider
/// 2026-07-12T02:00Z still inside the closed period and reject it, while UTC
/// considers it the next period's first work and admits it.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn admission_agrees_across_sessions_after_the_period_end() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    set_feed_required(&pool, false).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    seed_pool(&pool, 2, 2, 2, "fifo", "running_avg").await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;
    receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;
    assert_eq!(close_period(&pool, 1, "closer", true).await["closed"], json!(true));

    // 2026-07-12T02:00Z is above the frontier (2026-07-12T00:00Z) in UTC, so
    // both sessions must ADMIT it.
    let mut utc = session_in("UTC").await;
    submit_on(&mut utc, f.pool_id, 10, AFTER_END_UTC).await.expect("UTC admits above frontier");

    let mut east = session_in(EAST).await;
    submit_on(&mut east, 2, 11, AFTER_END_UTC)
        .await
        .expect("east session must agree: above the frontier");
}

/// The mirror case: an instant inside the period in UTC but before its start
/// under a naive eastern cast. Both sessions must REFUSE it once the period
/// is closed.
///
/// Note this case does NOT discriminate the fix by itself: 0022's frontier arm
/// strictly subsumes the coverage arm as a rejection rule (anything inside a
/// closed period is also at or below the greatest closed end_date), so the
/// verdict is the same either way and only the message differs. It is a
/// regression lock on the start-boundary pins, which no admission-outcome test
/// can discriminate.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn admission_agrees_across_sessions_at_the_period_start() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    set_feed_required(&pool, false).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;
    receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;

    // Open: both sessions admit it (it is inside the open period in UTC, and
    // pre-period-but-above-no-frontier in the east).
    let mut utc = session_in("UTC").await;
    submit_on(&mut utc, f.pool_id, 20, AFTER_START_UTC).await.expect("UTC admits while open");
    let mut east = session_in(EAST).await;
    submit_on(&mut east, f.pool_id, 21, AFTER_START_UTC).await.expect("east admits while open");

    assert_eq!(close_period(&pool, 1, "closer", true).await["closed"], json!(true));

    // Closed: both must refuse, with the same SQLSTATE.
    let utc_err = submit_on(&mut utc, f.pool_id, 22, AFTER_START_UTC).await.unwrap_err();
    assert_eq!(sqlstate(&utc_err), "55000");
    let east_err = submit_on(&mut east, f.pool_id, 23, AFTER_START_UTC).await.unwrap_err();
    assert_eq!(sqlstate(&east_err), "55000", "east must not disagree: {east_err}");
}

/// The close itself — gate scoping and the in-period recount — must classify
/// the same events from a non-UTC session, and the resulting period must be
/// identical.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn close_scoping_is_session_independent() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    set_feed_required(&pool, false).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;

    // One event just inside the period's start, one just outside its end —
    // exactly the pair a naive cast would mis-sort.
    receipt_at(&pool, f.pool_id, 1, AFTER_START_UTC, 10, 100).await;
    deplete_at(&pool, f.pool_id, 2, T11, 4).await;
    receipt_at(&pool, f.pool_id, 3, AFTER_END_UTC, 10, 300).await;

    // Close from an eastern session. Under the naive cast its gate scope
    // would have swept the next-period receipt into this period.
    let mut east = session_in(EAST).await;
    let report: serde_json::Value =
        sqlx::query_scalar("SELECT ledger_close_period(1, 'east-closer', true)")
            .fetch_one(&mut east)
            .await
            .expect("close from a non-UTC session");
    assert_eq!(report["closed"], json!(true), "{report}");

    // The out-of-period receipt is untouched by the close: still unsettled,
    // and still appendable territory above the frontier.
    let mut utc = session_in("UTC").await;
    submit_on(&mut utc, f.pool_id, 4, AFTER_END_UTC)
        .await
        .expect("above the frontier from UTC after an eastern close");
    let mut east2 = session_in(EAST).await;
    submit_on(&mut east2, f.pool_id, 5, AFTER_END_UTC)
        .await
        .expect("and from the east — the frontier is the same instant either way");
}

/// The settlement guard runs in the recalc worker's session, which need not
/// be the writer's. This is the discriminating case for that guard: a
/// depletion at 2026-07-12T02:00Z sits ABOVE the frontier in UTC (so it is
/// legitimately admitted and must be settleable), but a naive eastern cast
/// places it INSIDE the just-closed period — so an unpinned eastern worker
/// rejects its settlement with PeriodClosed and wedges the pool, while UTC
/// settles it happily.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn settlement_guard_agrees_across_sessions() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    set_feed_required(&pool, false).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;

    receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;
    receipt_at(&pool, f.pool_id, 2, T10, 10, 300).await;
    assert_eq!(close_period(&pool, 1, "closer", true).await["closed"], json!(true));

    // Next-period work, above the UTC frontier — but inside period 1 under a
    // naive eastern cast.
    let d = deplete_at(&pool, f.pool_id, 3, AFTER_END_UTC, 15).await;

    // The eastern worker must settle it exactly as a UTC worker would.
    let mut east = session_in(EAST).await;
    mark_dirty(&pool, f.pool_id).await;
    let report: serde_json::Value = sqlx::query_scalar("SELECT ledger_recalc_step()")
        .fetch_one(&mut east)
        .await
        .expect("eastern worker must not reject a post-frontier depletion");
    assert_eq!(report["claimed"], json!(true), "{report}");
    assert_eq!(
        authoritative_of(&pool, d).await,
        Some(167),
        "settled at the FIFO authoritative cost from a non-UTC session"
    );
}

/// The discriminating case for close.rs's four pins, in the direction that
/// actually wedges: a session AHEAD of UTC.
///
/// Every other case here uses America/New_York, whose negative offset pushes
/// the naive bound LATER, so its failure mode is over-inclusion. A positive
/// offset is strictly worse. From Asia/Tokyo the naive bound for a period
/// ending 2026-07-11 is 2026-07-11T15:00Z, so a pool whose only activity sits
/// in the late UTC day fails `gate_pools`' EXISTS entirely: the pool is never
/// gate-scoped, never claimed, never drained, never swept, and the period
/// stamps closed with in-period depletions unsettled. The feed then lowers a
/// recost floor into the closed range and every engine pass is rejected — the
/// acct-1vur.3 permanent wedge, reached with no backdating at all.
///
/// Unpinned, this close reports `pools_in_scope = 0` and closes cleanly.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn gate_scope_is_session_independent_from_a_zone_ahead_of_utc() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    set_feed_required(&pool, false).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;

    // Unambiguously inside period 1 in UTC, but above a naive Tokyo bound.
    receipt_at(&pool, f.pool_id, 1, "2026-07-11T20:00:00+00:00", 10, 100).await;
    deplete_at(&pool, f.pool_id, 2, "2026-07-11T21:00:00+00:00", 4).await;

    let mut tokyo = session_in("Asia/Tokyo").await;
    let report: serde_json::Value =
        sqlx::query_scalar("SELECT ledger_close_period(1, 'tokyo-closer', false)")
            .fetch_one(&mut tokyo)
            .await
            .expect("unforced close from Asia/Tokyo");

    // The pool must be visible to the gate, and its unsettled tail must block
    // the close. Unpinned, the scope is empty and the gate waves it through.
    assert_eq!(report["gate"]["pools_in_scope"], json!(1), "pool must be in gate scope: {report}");
    assert_eq!(report["closed"], json!(false), "unsettled tail must block: {report}");
    assert_eq!(period_state(&pool, 1).await.0, "closing");

    // Once drained, the same eastern-of-UTC closer completes it.
    mark_dirty(&pool, f.pool_id).await;
    drain_recalc(&pool).await;
    let done: serde_json::Value =
        sqlx::query_scalar("SELECT ledger_close_period(1, 'tokyo-closer', false)")
            .fetch_one(&mut tokyo)
            .await
            .expect("second close");
    assert_eq!(done["closed"], json!(true), "{done}");
}
