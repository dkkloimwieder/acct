//! Acceptance tests for slot-loss detection and recovery (acct-1vur.2; 0026,
//! design-v3.2 §6/§7).
//!
//! `max_slot_wal_keep_size` is deliberately finite, so the cluster may
//! invalidate the feed slot rather than let an abandoned consumer pin WAL
//! without bound. `ensure_slot` then recreates it at the CURRENT WAL
//! position, silently skipping every event decodable in the gap: those events
//! never lower a recost floor and never mark their pool dirty, so they are
//! never re-folded. The alarm the design relies on only fires if the event is
//! eventually delivered — so this is the one genuinely silent path left.
//!
//! Recovery ships under the decided posture (acct-1vur.2b): when a re-fold
//! discovers a CLOSED period was valued over events the feed never delivered,
//! correctness wins. The pool halts into a durable, named escalation state
//! whose only exit is the reopen workflow (acct-1vur.9). So there are two
//! outcomes to pin, and both are here: recovery CONVERGES when frozen history
//! is not contradicted, and it ESCALATES LOUDLY when it is.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

/// Independent strict-FIFO walk over the pool's physical stream in R-1 order,
/// sharing no code with the engine: returns the authoritative unit cost per
/// depletion, banker-rounded like `banker_div`.
async fn oracle_costs(pool: &PgPool, pool_id: i64) -> Vec<(i64, i64)> {
    let rows = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT id, qty, unit_cost FROM trx_line \
          WHERE pool_id = $1 AND line_type <> 'cost_adjustment_line' \
          ORDER BY posted_at, id",
    )
    .bind(pool_id)
    .fetch_all(pool)
    .await
    .expect("read physical stream");

    // Open layers as (qty, unit_cost), consumed front-first.
    let mut layers: Vec<(i64, i64)> = Vec::new();
    let mut out = Vec::new();
    for (id, qty, unit_cost) in rows {
        if qty > 0 {
            layers.push((qty, unit_cost));
            continue;
        }
        let mut want = -qty;
        let mut value: i128 = 0;
        let mut drawn: i64 = 0;
        while want > 0 && !layers.is_empty() {
            let (lq, lc) = layers[0];
            let take = want.min(lq);
            value += take as i128 * lc as i128;
            drawn += take;
            want -= take;
            if take == lq {
                layers.remove(0);
            } else {
                layers[0].0 -= take;
            }
        }
        // The uncovered remainder is costed at the depletion's own observed
        // cost (recalc-a); with a covered fixture this branch stays unused.
        if want > 0 {
            value += want as i128 * unit_cost as i128;
            drawn += want;
        }
        out.push((id, banker_div(value, drawn as i128)));
    }
    out
}

/// Banker's rounding, mirroring the SQL `banker_div` the engine uses.
fn banker_div(value: i128, qty: i128) -> i64 {
    if qty == 0 {
        return 0;
    }
    let (q, r) = (value / qty, value % qty);
    let twice = r.abs() * 2;
    let round_away = twice > qty.abs() || (twice == qty.abs() && q % 2 != 0);
    let adj = if round_away {
        if (value < 0) != (qty < 0) {
            -1
        } else {
            1
        }
    } else {
        0
    };
    (q + adj) as i64
}

async fn engine_costs(pool: &PgPool, pool_id: i64) -> Vec<(i64, i64)> {
    sqlx::query_as::<_, (i64, i64)>(
        "SELECT cs.depletion_trx_line_id, cs.authoritative_unit_cost \
           FROM cost_settlement cs \
           JOIN trx_line t ON t.id = cs.depletion_trx_line_id \
          WHERE t.pool_id = $1 \
            AND cs.recalc_generation = ( \
                SELECT max(recalc_generation) FROM cost_settlement \
                 WHERE depletion_trx_line_id = cs.depletion_trx_line_id) \
          ORDER BY cs.depletion_trx_line_id",
    )
    .bind(pool_id)
    .fetch_all(pool)
    .await
    .expect("read engine settlements")
}

/// The gauge must report a MISSING slot as a value, not as an empty result —
/// absence is the state an operator most needs to see, and 0014's `feed_lag`
/// returns zero rows for it.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn health_gauge_reports_absence_as_a_row() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    drop_feed_slot(&pool).await;

    let (present, unhealthy): (bool, bool) =
        sqlx::query_as("SELECT present, unhealthy FROM feed_slot_health")
            .fetch_one(&pool)
            .await
            .expect("gauge emits a row with no slot");
    assert!(!present, "absence is reported");
    assert!(unhealthy, "and flagged unhealthy");
    // 0014's gauge, by contrast, is empty — which is the bug this fixes.
    assert_eq!(count(&pool, "SELECT count(*) FROM feed_lag").await, 0);

    let consumer = reset_feed(&pool).await;
    let (present, unhealthy, lag): (bool, bool, Option<i64>) =
        sqlx::query_as("SELECT present, unhealthy, lag_bytes FROM feed_slot_health")
            .fetch_one(&pool)
            .await
            .expect("gauge with a slot");
    assert!(present && !unhealthy, "a live slot reads healthy");
    assert!(lag.is_some(), "and carries a real lag reading");
    let _ = consumer;
    drop_feed_slot(&pool).await;
}

/// An invalidated slot is terminal, not transient: retrying the peek can only
/// spin, because the WAL it never delivered has been discarded.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn an_absent_slot_fails_loud_rather_than_retrying() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    drop_feed_slot(&pool).await;

    let consumer = ledger_feed::FeedConsumer::new(pool.clone(), SLOT, PUBLICATION);
    let err = consumer.ingest_once(10_000).await.expect_err("absent slot must fail loud");
    assert!(
        matches!(err, ledger_feed::FeedError::SlotLost { .. }),
        "distinguishable from a transient error: {err}"
    );
    assert!(format!("{err}").contains("cannot recover"), "says why retrying is futile: {err}");
}
/// The headline case: the slot is lost mid-run, events commit into the gap,
/// and a naive recreate would skip them forever. Recovery must converge on
/// the oracle.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn slot_loss_mid_run_recovers_to_oracle_equivalence() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let consumer = reset_feed(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;

    // Settled steady state.
    receipt_at(&pool, f.pool_id, 1, T10, 100, 10).await;
    let d1 = deplete_at(&pool, f.pool_id, 2, T11, 40).await;
    ingest_all(&consumer).await;
    drain_recalc(&pool).await;
    assert_eq!(engine_costs(&pool, f.pool_id).await, oracle_costs(&pool, f.pool_id).await);

    // THE GAP: the slot disappears, and work continues — including a backdate
    // BELOW the settled frontier, which re-prices the already-settled T11
    // depletion. Nothing marks the pool dirty.
    drop_feed_slot(&pool).await;
    receipt_at(&pool, f.pool_id, 3, T09, 100, 5).await;
    let d2 = deplete_at(&pool, f.pool_id, 4, T12, 30).await;
    assert_eq!(
        count(&pool, "SELECT count(*) FROM recalc_queue").await,
        0,
        "no slot ⇒ nothing marked dirty: the silent hole"
    );

    // A bare recreate + drain leaves the gap events uncosted — the engine has
    // no reason to look at the pool at all.
    let consumer = ledger_feed::FeedConsumer::new(pool.clone(), SLOT, PUBLICATION);
    assert!(consumer.ensure_slot().await.expect("recreate slot"));
    ingest_all(&consumer).await;
    drain_recalc(&pool).await;
    assert!(
        engine_costs(&pool, f.pool_id).await != oracle_costs(&pool, f.pool_id).await,
        "bare recreate must NOT converge — that is the defect being fixed"
    );
    assert_eq!(authoritative_of(&pool, d1).await, Some(10), "stale: backdated lot unseen");

    // RECOVERY: reseed rebuilds the dirty set from durable state.
    let reseeded: i64 = sqlx::query_scalar("SELECT ledger_feed_reseed()")
        .fetch_one(&pool)
        .await
        .expect("reseed");
    assert_eq!(reseeded, 1, "the one strict pool is enqueued");
    drain_recalc(&pool).await;

    // R1: engine == oracle, and the backdated lot is now priced in.
    assert_eq!(
        engine_costs(&pool, f.pool_id).await,
        oracle_costs(&pool, f.pool_id).await,
        "converged on the independent FIFO walk"
    );
    assert_eq!(authoritative_of(&pool, d1).await, Some(5), "re-costed against the backdated lot");
    assert!(authoritative_of(&pool, d2).await.is_some());
    assert_eq!(count(&pool, "SELECT count(*) FROM recalc_queue").await, 0, "engine at rest");

    drop_feed_slot(&pool).await;
}

/// `ensure_slot_with_recovery` is the startup path: creating a slot over a
/// database that already holds events reseeds automatically, so an operator
/// who simply restarts the feed gets the recovery without knowing to ask.
/// This is also the first-startup bootstrap — events that predate the feed.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn ensure_slot_with_recovery_bootstraps_pre_feed_events() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    drop_feed_slot(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;

    // Events committed before any feed ever ran.
    receipt_at(&pool, f.pool_id, 1, T09, 100, 10).await;
    receipt_at(&pool, f.pool_id, 2, T10, 100, 30).await;
    deplete_at(&pool, f.pool_id, 3, T11, 150).await;

    let consumer = ledger_feed::FeedConsumer::new(pool.clone(), SLOT, PUBLICATION);
    let (created, reseeded) =
        consumer.ensure_slot_with_recovery().await.expect("startup recovery");
    assert!(created);
    assert_eq!(reseeded, Some(1), "history present ⇒ reseeded");

    drain_recalc(&pool).await;
    assert_eq!(
        engine_costs(&pool, f.pool_id).await,
        oracle_costs(&pool, f.pool_id).await,
        "bootstrapped pool converges on the oracle"
    );

    // A virgin database needs no reseed — the common case stays free.
    reset_state(&pool).await;
    drop_feed_slot(&pool).await;
    let consumer = ledger_feed::FeedConsumer::new(pool.clone(), SLOT, PUBLICATION);
    assert_eq!(
        consumer.ensure_slot_with_recovery().await.expect("virgin startup"),
        (true, None)
    );
    drop_feed_slot(&pool).await;
}


/// The other outcome, and the one the decision is about: the re-fold
/// CONTRADICTS a closed period.
///
/// The gap event lands inside a period that is then closed — reachable
/// because a re-created slot reads current, so the feed-currency gate passes
/// over an undelivered event (an accepted gap, per close.rs). Recovery must
/// not paper over it and must not spin: the pool halts into a durable, named
/// escalation, stays there across further passes and worker restarts, and the
/// next period's close fails naming the wedged pool rather than dying of a
/// mystery.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn closed_period_divergence_halts_loudly_and_stays_halted() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    set_feed_required(&pool, false).await;
    let consumer = reset_feed(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;

    // Settled in-period state.
    receipt_at(&pool, f.pool_id, 1, T10, 100, 10).await;
    let d = deplete_at(&pool, f.pool_id, 2, T11, 40).await;
    ingest_all(&consumer).await;
    drain_recalc(&pool).await;
    assert_eq!(authoritative_of(&pool, d).await, Some(10));

    // THE GAP: slot lost; a cheaper backdated lot commits IN-period, below the
    // settled frontier, and is never delivered. It re-prices the T11 depletion.
    drop_feed_slot(&pool).await;
    receipt_at(&pool, f.pool_id, 3, T09, 100, 5).await;

    // The period closes clean: the gate cannot see the straggler (below the
    // frontier ⇒ no lag) and no slot ⇒ the currency leg is waived here.
    assert_eq!(close_period(&pool, 1, "closer", true).await["closed"], json!(true));

    // Recovery runs. The re-fold now contradicts frozen history.
    let consumer = ledger_feed::FeedConsumer::new(pool.clone(), SLOT, PUBLICATION);
    assert!(consumer.ensure_slot().await.expect("recreate slot"));
    let reseeded: i64 =
        sqlx::query_scalar("SELECT ledger_feed_reseed()").fetch_one(&pool).await.expect("reseed");
    assert_eq!(reseeded, 1);

    let report = recalc_step(&pool).await;
    assert_eq!(report["claimed"], json!(true));
    assert_eq!(report["escalated"]["closed_period_id"], json!(1), "names the period: {report}");
    assert_eq!(report["escalated"]["pool_id"], json!(f.pool_id));
    assert!(
        report["escalated"]["remedy"].as_str().unwrap().contains("acct-1vur.9"),
        "the pass report points at the exit: {report}"
    );
    assert_eq!(report["settlements_written"], json!(0), "nothing half-applied");

    // Durable and legible: pool, period, depletion, both costs, and the remedy.
    let (pool_id, period_id, frozen, recomputed, msg): (i64, i64, i64, i64, String) =
        sqlx::query_as(
            "SELECT pool_id, closed_period_id, exemplar_frozen_unit_cost, \
                    exemplar_recomputed_unit_cost, operator_message FROM recalc_escalations",
        )
        .fetch_one(&pool)
        .await
        .expect("escalation is visible on the operator surface");
    assert_eq!((pool_id, period_id, frozen, recomputed), (f.pool_id, 1, 10, 5));
    assert!(msg.contains("HALTED") && msg.contains("acct-1vur.9"), "names the remedy: {msg}");

    // The registered gauges must NOT read green: a halted pool keeps its queue
    // row precisely so queue depth and per-pool lag stay honest, and both
    // carry the halt explicitly.
    let (dirty, halted_pools): (i64, i64) =
        sqlx::query_as("SELECT dirty_pools, halted_pools FROM recalc_queue_depth")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((dirty, halted_pools), (1, 1), "gauges name the halt");
    let lagged: Option<i64> = sqlx::query_scalar(
        "SELECT halted_by_closed_period FROM recalc_pool_lag WHERE pool_id = $1",
    )
    .bind(f.pool_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(lagged, Some(1), "per-pool gauge names the closed period");

    // The on-demand shape must not answer "settled" for a pool that halted.
    let settle_err = sqlx::query_scalar::<_, serde_json::Value>("SELECT ledger_settle_pool($1)")
        .bind(f.pool_id)
        .fetch_one(&pool)
        .await
        .unwrap_err();
    assert_eq!(sqlstate(&settle_err), "55000");
    assert!(format!("{settle_err}").contains("RecalcHalted"), "{settle_err}");

    // It STAYS halted: no spin, no silent recovery, across further ticks and
    // a fresh worker session (the state is in the database, not in a process).
    for _ in 0..3 {
        assert_eq!(recalc_step(&pool).await["claimed"], json!(false), "halted pool is not claimed");
    }
    let other: PgPool = connect_pool().await;
    assert_eq!(recalc_step(&other).await["claimed"], json!(false), "survives a worker restart");
    assert_eq!(authoritative_of(&pool, d).await, Some(10), "frozen valuation untouched");
    assert_eq!(count(&pool, "SELECT count(*) FROM recalc_escalations").await, 1);

    // And the NEXT period cannot close silently: the gate names the wedged
    // pool and the remedy, forced or not.
    create_period(&pool, 2, "2026-07-12", "2026-07-31").await;
    for force in [false, true] {
        let next = close_period(&pool, 2, "closer", force).await;
        assert_eq!(next["closed"], json!(false), "force={force}: {next}");
        assert_eq!(next["gate"]["halted"][0]["pool_id"], json!(f.pool_id), "{next}");
        assert_eq!(next["gate"]["halted"][0]["closed_period_id"], json!(1));
        assert!(
            next["gate"]["halted"][0]["remedy"].as_str().unwrap().contains("acct-1vur.9"),
            "the gate points at the exit: {next}"
        );
    }

    drop_feed_slot(&pool).await;
}

/// The close's OWN drain can be the thing that discovers the divergence — the
/// gate-time halted check cannot see it, because no worker has run yet. That
/// path must abort by NAME rather than sweeping a contradicted pool (which
/// would absorb the divergence into variance) or spinning the re-check loop to
/// its cap and dying anonymous.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn a_close_that_discovers_the_divergence_itself_aborts_by_name() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    set_feed_required(&pool, false).await;
    let consumer = reset_feed(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;

    receipt_at(&pool, f.pool_id, 1, T10, 100, 10).await;
    deplete_at(&pool, f.pool_id, 2, T11, 40).await;
    ingest_all(&consumer).await;
    drain_recalc(&pool).await;

    drop_feed_slot(&pool).await;
    receipt_at(&pool, f.pool_id, 3, T09, 100, 5).await;
    assert_eq!(close_period(&pool, 1, "closer", true).await["closed"], json!(true));

    // Reseed, then close the NEXT period with NO worker tick in between, so
    // the divergence surfaces inside the close's own forced drain.
    let consumer = ledger_feed::FeedConsumer::new(pool.clone(), SLOT, PUBLICATION);
    assert!(consumer.ensure_slot().await.expect("recreate slot"));
    sqlx::query_scalar::<_, i64>("SELECT ledger_feed_reseed()")
        .fetch_one(&pool)
        .await
        .expect("reseed");
    assert_eq!(count(&pool, "SELECT count(*) FROM recalc_escalations").await, 0);

    create_period(&pool, 2, "2026-07-12", "2026-07-31").await;
    let err = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT ledger_close_period(2, 'closer', true)",
    )
    .fetch_one(&pool)
    .await
    .unwrap_err();
    assert_eq!(sqlstate(&err), "55000", "not an anonymous internal error: {err}");
    let text = format!("{err}");
    assert!(text.contains("RecalcHalted"), "{text}");
    assert!(text.contains(&format!("pool {}", f.pool_id)), "names the pool: {text}");
    assert!(text.contains("acct-1vur.9"), "names the exit: {text}");
    // The abort is atomic: the close raised, so nothing it did survives —
    // not the stamp, not `closing`, not the sweep's GL. The escalation row
    // rolls back with it too, which is why the ERROR has to carry the facts.
    assert_eq!(period_state(&pool, 2).await.0, "open", "no partial close");
    assert_eq!(count(&pool, "SELECT count(*) FROM recalc_escalations").await, 0);

    // A worker tick records it durably, and only then does the gate report it
    // by name instead of the close dying mid-drain.
    recalc_step(&pool).await;
    assert_eq!(count(&pool, "SELECT count(*) FROM recalc_escalations").await, 1);
    let next = close_period(&pool, 2, "closer", true).await;
    assert_eq!(next["closed"], json!(false));
    assert_eq!(next["gate"]["halted"][0]["pool_id"], json!(f.pool_id), "{next}");

    drop_feed_slot(&pool).await;
}
