//! Acceptance tests for append-only enforcement (acct-1vur.4; 0023,
//! design-v3.2 §7).
//!
//! The engine's closed-period correctness argument is that an unchanged
//! physical prefix means it can never recompute a different authoritative
//! cost. 0017 guarded only INSERTs, so a stray UPDATE to a historical
//! `trx_line` broke replay determinism with no fail-loud. These cases pin the
//! guards on the four prefix tables, the narrower transition guard on
//! `accounting_period` (whose live `state` read is what 0022's monotonic
//! frontier rests on), and — as negative controls — that the derived tables
//! the engine must keep rewriting are NOT frozen.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

/// Every guarded table, with a mutation that would otherwise succeed.
const GUARDED: &[(&str, &str, &str)] = &[
    ("trx_line", "UPDATE trx_line SET unit_cost = unit_cost + 1", "DELETE FROM trx_line"),
    ("posting_line", "UPDATE posting_line SET amount = amount + 1", "DELETE FROM posting_line"),
    (
        "cost_settlement",
        "UPDATE cost_settlement SET authoritative_unit_cost = authoritative_unit_cost + 1",
        "DELETE FROM cost_settlement",
    ),
    (
        "cost_layer_consumption",
        "UPDATE cost_layer_consumption SET qty = qty + 1",
        "DELETE FROM cost_layer_consumption",
    ),
];

async fn expect_append_only(pool: &PgPool, sql: &str) {
    let err = sqlx::query(sql).execute(pool).await.unwrap_err();
    assert_eq!(sqlstate(&err), "55006", "{sql}");
    assert!(format!("{err}").contains("AppendOnly"), "{sql}: {err}");
}

/// Build a fixture with rows in all four guarded tables: two lots at
/// different costs so the FIFO authoritative cost differs from the observed
/// running average, which forces settlements, consumption draws and a GL
/// adjustment to exist.
async fn seeded(pool: &PgPool) -> Fixture {
    reset_state(pool).await;
    set_feed_required(pool, false).await;
    let f = seed_fixture(pool, "fifo", "running_avg").await;
    receipt_at(pool, f.pool_id, 1, T09, 10, 100).await;
    receipt_at(pool, f.pool_id, 2, T10, 10, 300).await;
    deplete_at(pool, f.pool_id, 3, T11, 15).await;
    mark_dirty(pool, f.pool_id).await;
    drain_recalc(pool).await;
    for (table, _, _) in GUARDED {
        assert!(
            count(pool, &format!("SELECT count(*) FROM {table}")).await > 0,
            "{table} must have rows for the guard test to mean anything"
        );
    }
    f
}

/// UPDATE and DELETE are both refused on every table carrying the replay
/// prefix or the audit trail derived from it.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn mutations_on_the_replay_prefix_are_refused() {
    let pool = connect_pool().await;
    seeded(&pool).await;

    for (_, update, delete) in GUARDED {
        expect_append_only(&pool, update).await;
        expect_append_only(&pool, delete).await;
    }
}

/// Negative controls: the derived tables the engine rewrites on every pass
/// must NOT be frozen, or the engine could not run at all.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn derived_state_stays_mutable() {
    let pool = connect_pool().await;
    let f = seeded(&pool).await;

    // pool_settlement is the moving frontier, upserted every pass.
    sqlx::query("UPDATE pool_settlement SET recalc_generation = recalc_generation")
        .execute(&pool)
        .await
        .expect("pool_settlement stays mutable");
    // pool_state layers are rebuilt wholesale by each replay.
    sqlx::query("UPDATE pool_state SET value_sum = value_sum WHERE pool_id = $1")
        .bind(f.pool_id)
        .execute(&pool)
        .await
        .expect("pool_state stays mutable");
    sqlx::query("DELETE FROM pool_state WHERE pool_id = $1 AND layer_id > 0")
        .bind(f.pool_id)
        .execute(&pool)
        .await
        .expect("layer rows stay deletable");
}

/// The guards are inert on every path that exists: a full workload — submit,
/// ingest, recalc, close (which drains, sweeps and stamps) — runs untouched.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn happy_path_is_unaffected() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let consumer = reset_feed(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;

    receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;
    receipt_at(&pool, f.pool_id, 2, T10, 10, 300).await;
    let d = deplete_at(&pool, f.pool_id, 3, T11, 15).await;
    ingest_until_current(&pool, &consumer).await;
    drain_recalc(&pool).await;
    assert_eq!(authoritative_of(&pool, d).await, Some(167));
    // The recalc pass posted its adjustment GL, so the slot is behind again;
    // the close gate requires a current feed (0020).
    ingest_until_current(&pool, &consumer).await;

    // The close re-costs, sweeps residue and stamps the period — the busiest
    // path across all five guarded tables.
    let report = close_period(&pool, 1, "closer", false).await;
    assert_eq!(report["closed"], json!(true), "{report}");
    assert_eq!(period_state(&pool, 1).await.0, "closed");

    // The frontier still admits next-period work.
    receipt_at(&pool, f.pool_id, 4, N09, 5, 100).await;
    drop_feed_slot(&pool).await;
}

/// `accounting_period` is not append-only — a close legitimately moves state
/// — but only along the three transitions close.rs performs.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn only_the_close_transitions_are_legal() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;

    // Illegal from 'open': self-transition and any non-close target.
    for target in ["open", "bogus"] {
        let err = sqlx::query("UPDATE accounting_period SET state = $1 WHERE id = 1")
            .bind(target)
            .execute(&pool)
            .await
            .unwrap_err();
        // 'bogus' trips the 0002 CHECK first; 'open' trips the transition
        // guard. Either way it does not silently apply.
        assert!(
            ["55000", "23514"].contains(&sqlstate(&err).as_str()),
            "open -> {target}: {err}"
        );
    }

    // open -> closing -> closed, the drain path.
    sqlx::query("UPDATE accounting_period SET state = 'closing' WHERE id = 1")
        .execute(&pool)
        .await
        .expect("open -> closing");
    sqlx::query("UPDATE accounting_period SET state = 'closed' WHERE id = 1")
        .execute(&pool)
        .await
        .expect("closing -> closed");

    // open -> closed directly, the gate-passed-first-time path.
    create_period(&pool, 2, "2026-08-01", "2026-08-31").await;
    sqlx::query("UPDATE accounting_period SET state = 'closed' WHERE id = 2")
        .execute(&pool)
        .await
        .expect("open -> closed");
}

/// A closed period is frozen outright: no unfreeze, no date move, no audit
/// touch-up, no delete. This is what makes 0022's monotonic frontier actually
/// monotonic — the guards re-read `state` live on every insert.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn a_closed_period_is_frozen_outright() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    set_feed_required(&pool, false).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;
    receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;
    assert_eq!(close_period(&pool, 1, "closer", true).await["closed"], json!(true));

    for (what, sql) in [
        ("unfreeze", "UPDATE accounting_period SET state = 'open' WHERE id = 1"),
        ("back to closing", "UPDATE accounting_period SET state = 'closing' WHERE id = 1"),
        // Extending end_date would swallow events legally admitted above the
        // frontier and still unsettled.
        ("extend the range", "UPDATE accounting_period SET end_date = '2026-07-14' WHERE id = 1"),
        // Even a well-meaning audit correction: the stamp is the record.
        ("correct the actor", "UPDATE accounting_period SET closed_by = 'someone' WHERE id = 1"),
        ("delete it", "DELETE FROM accounting_period WHERE id = 1"),
    ] {
        let err = sqlx::query(sql).execute(&pool).await.unwrap_err();
        assert_eq!(sqlstate(&err), "55000", "{what}: {err}");
        assert!(format!("{err}").contains("PeriodClosed"), "{what}: {err}");
    }

    // ...and the frontier it anchors still holds.
    let err = sqlx::query_scalar::<_, i64>("SELECT ledger_submit_trx($1, $2, $3, $4::jsonb)")
        .bind("po_receipt")
        .bind(99i64)
        .bind("2026-06-15T09:00:00+00:00")
        .bind(json!([line(f.pool_id, "po_receipt_line", 5, 100)]))
        .fetch_one(&pool)
        .await
        .unwrap_err();
    assert_eq!(sqlstate(&err), "55000");
}

/// Because the legal set is exactly the three close transitions, a
/// non-state-changing UPDATE (open -> open) is refused too — so an open
/// period's DATES are immutable as well. That is deliberate: moving
/// `end_date` under an open period silently changes what a pending close will
/// sweep. An open period may still be deleted outright, which fixtures need
/// and which is not a silent mutation.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn an_open_period_has_immutable_dates_but_stays_deletable() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;

    let err = sqlx::query("UPDATE accounting_period SET end_date = '2026-07-12' WHERE id = 1")
        .execute(&pool)
        .await
        .unwrap_err();
    assert_eq!(sqlstate(&err), "55000");
    assert!(format!("{err}").contains("open -> open"), "{err}");

    sqlx::query("DELETE FROM accounting_period WHERE id = 1")
        .execute(&pool)
        .await
        .expect("open period stays deletable");
    assert_eq!(count(&pool, "SELECT count(*) FROM accounting_period").await, 0);
}
