//! acct-3ovt / M10.C2 — bgworker SPI error escalation.
//!
//! # Background
//!
//! `do_drain_tick` runs every `drain_interval_ms` (default 100ms),
//! reads dirty cells, and UPSERTs them into `account_balances_rollup`
//! via SPI. If the rollup table is missing or has schema drift, every
//! upsert fails — pre-acct-3ovt this was log-and-swallowed forever
//! with no escalation, so silent outages were invisible.
//!
//! Post-acct-3ovt:
//! - `LEDGER_DRAIN_CONSECUTIVE_FAILS` increments by 1 per tick with
//!   any failed upsert; resets to 0 on the first clean tick.
//! - `LEDGER_DRAIN_TOTAL_FAILURES` counts every failed upsert
//!   cumulatively until `ledger_shmem_reset`.
//! - When consecutive reaches `LEDGER_DRAIN_WARN_AFTER` (5 ticks =
//!   500ms at default cadence), a single `warning!` is emitted with
//!   the last SPI error message. Threshold-crossing only — bounded
//!   log volume under sustained outages.
//! - SQL accessors: `ledger_drain_consecutive_fails()`,
//!   `ledger_drain_total_failures()`.
//!
//! # Tests
//!
//! - **T1** sustained-outage probe: rename `account_balances_rollup`
//!   so the bgworker's UPSERT fails. Apply a delta. Wait ~1s.
//!   Verify counters increment. Restore rollup. Apply another delta.
//!   Verify counters reset.
//! - **T2** zero-failure baseline: after reset, counters are 0 and
//!   stay 0 during a clean apply + drain cycle.
//!
//! # Location
//!
//! Sibling of other M10 t1 binaries.

use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use uuid::Uuid;

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

fn db_url() -> String {
    std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string())
}

fn unique_offset() -> i64 {
    let u = Uuid::new_v4();
    let bytes = u.as_bytes();
    let h = ((bytes[0] as i64) << 24)
        | ((bytes[1] as i64) << 16)
        | ((bytes[2] as i64) << 8)
        | (bytes[3] as i64);
    1_100_000_000_000_i64 + (h.abs() % 1_000_000_000)
}

async fn pool() -> sqlx::PgPool {
    let p = PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url())
        .await
        .expect("connect");
    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&p)
        .await
        .expect("ext");
    // If a prior test left the rollup table renamed, restore it.
    let _ = sqlx::query(
        "ALTER TABLE IF EXISTS account_balances_rollup_t1_hidden RENAME TO account_balances_rollup"
    )
    .execute(&p)
    .await;
    p
}

async fn consec(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT ledger_drain_consecutive_fails()")
        .fetch_one(pool)
        .await
        .expect("consec")
}

async fn total_fail(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT ledger_drain_total_failures()")
        .fetch_one(pool)
        .await
        .expect("total_fail")
}

async fn apply(pool: &sqlx::PgPool, acct: i64, amt: i64) {
    sqlx::query(
        "SELECT ledger_apply_balance_delta($1::bigint, 1::int, 1::smallint, 1::smallint, $2, 0)",
    )
    .bind(acct)
    .bind(amt)
    .execute(pool)
    .await
    .expect("apply");
}

/// T1 — sustained outage. Rename the rollup table so the bgworker's
/// UPSERT fails; apply a delta; observe counters increment; restore;
/// apply again; observe counters reset.
#[tokio::test]
async fn t1_sustained_outage_escalates_then_recovers() {
    let p = pool().await;

    // Baseline: reset shmem + counters; verify clean state.
    sqlx::query("SELECT ledger_shmem_reset()")
        .execute(&p)
        .await
        .expect("reset");
    assert_eq!(consec(&p).await, 0, "T1 baseline: consec should be 0");

    let acct = unique_offset();

    // Break the rollup table by renaming it. Postgres invalidates
    // pg_class caches across backends on schema change, so the
    // bgworker's next SPI plan will fail to resolve the table.
    sqlx::query("ALTER TABLE account_balances_rollup RENAME TO account_balances_rollup_t1_hidden")
        .execute(&p)
        .await
        .expect("rename out");

    // Apply a delta — cell becomes dirty.
    apply(&p, acct, 100).await;

    // Wait for ~10 ticks at default 100ms cadence. Each failed tick
    // increments consecutive; after 5 the warning fires; counter
    // keeps climbing.
    tokio::time::sleep(Duration::from_millis(1200)).await;

    let c_outage = consec(&p).await;
    let t_outage = total_fail(&p).await;
    eprintln!("T1 mid-outage: consecutive={c_outage} total_failures={t_outage}");
    assert!(
        c_outage >= 5,
        "T1: consecutive_fails should reach at least 5 (warning threshold) \
         during 1.2s outage; got {c_outage}"
    );
    assert!(
        t_outage >= c_outage,
        "T1: total_failures ({t_outage}) should be >= consecutive_fails ({c_outage}) \
         since each failing tick failed at least one upsert"
    );

    // Restore the rollup table.
    sqlx::query("ALTER TABLE account_balances_rollup_t1_hidden RENAME TO account_balances_rollup")
        .execute(&p)
        .await
        .expect("rename back");

    // Apply another delta so the next tick has work.
    let acct2 = unique_offset();
    apply(&p, acct2, 200).await;

    // Wait for at least one successful drain tick.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let c_recover = consec(&p).await;
    let t_recover = total_fail(&p).await;
    eprintln!("T1 post-recovery: consecutive={c_recover} total_failures={t_recover}");
    assert_eq!(
        c_recover, 0,
        "T1: consecutive_fails should reset to 0 after a clean tick; got {c_recover}"
    );
    // total_failures is cumulative — stays at its outage value.
    assert!(
        t_recover >= t_outage,
        "T1: total_failures is cumulative; should be >= mid-outage value ({t_outage}); got {t_recover}"
    );

    // Verify the post-recovery cell was actually drained.
    let row = sqlx::query(
        "SELECT balance FROM account_balances_rollup \
         WHERE account_id = $1 AND period_id = 1 \
           AND currency_id = 1 AND ledger_kind = 1",
    )
    .bind(acct2)
    .fetch_optional(&p)
    .await
    .expect("rollup lookup");
    assert!(
        row.is_some(),
        "T1: after recovery the second cell should land in rollup; not found for acct={acct2}"
    );
    let bal: i64 = row.unwrap().get("balance");
    assert_eq!(
        bal, 200,
        "T1: post-recovery cell balance should be 200; got {bal}"
    );
}

/// T2 — clean baseline. After reset, counters are 0; a normal apply
/// + drain cycle does not change them.
#[tokio::test]
async fn t2_clean_baseline_keeps_counters_zero() {
    let p = pool().await;
    sqlx::query("SELECT ledger_shmem_reset()")
        .execute(&p)
        .await
        .expect("reset");

    let acct = unique_offset();
    apply(&p, acct, 50).await;

    // Wait for several drain ticks.
    tokio::time::sleep(Duration::from_millis(600)).await;

    assert_eq!(
        consec(&p).await,
        0,
        "T2: consecutive_fails must be 0 across clean ticks"
    );
    assert_eq!(
        total_fail(&p).await,
        0,
        "T2: total_failures must be 0 across clean ticks"
    );

    // And the cell drained.
    let bal: Option<i64> = sqlx::query_scalar(
        "SELECT balance FROM account_balances_rollup \
         WHERE account_id = $1 AND period_id = 1 \
           AND currency_id = 1 AND ledger_kind = 1",
    )
    .bind(acct)
    .fetch_optional(&p)
    .await
    .expect("rollup");
    assert_eq!(bal, Some(50), "T2: clean drain should land balance=50");
}
