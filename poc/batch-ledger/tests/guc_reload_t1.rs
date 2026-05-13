//! acct-vd74 / M10.C4 — GUC SIGHUP reload smoke test.
//!
//! Two GUCs registered by the extension (`_PG_init` in lib.rs):
//!
//! - `ledger.drain_interval_ms` — `GucContext::Sighup`. Bgworker reads
//!   `DRAIN_INTERVAL_MS.get()` at each tick (lib.rs `wait_latch` arg),
//!   so a SIGHUP-triggered reload propagates within 1–2 ticks.
//! - `ledger.drain_database` — `GucContext::Postmaster`. Only set at
//!   postmaster start. `ALTER SYSTEM SET` succeeds and persists to
//!   `postgresql.auto.conf`, but the running postmaster does NOT pick
//!   up the new value until a full PG restart.
//!
//! # Tests
//!
//! - **T1** verifies a `drain_interval_ms` hot-change. Two
//!   reload-and-observe phases bracket an `ALTER SYSTEM SET`:
//!   measure drain cadence under the original and new intervals via
//!   the rollup's `drained_at` column.
//! - **T2** verifies `drain_database` is restart-only. Setting it via
//!   `ALTER SYSTEM SET` + `pg_reload_conf()` does not change the
//!   bgworker's current connection.
//!
//! Both tests reset the GUC to default at the end via `ALTER SYSTEM RESET`.
//!
//! # Location
//!
//! Sibling of other M10 t1 binaries. Drives the extension via SQL.

use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use std::time::{Duration, Instant};
use uuid::Uuid;

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

fn db_url() -> String {
    std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string())
}

async fn ensure_account(pool: &sqlx::PgPool) -> i64 {
    let code = format!("guc_reload_{}", Uuid::new_v4().simple());
    let row = sqlx::query(
        "INSERT INTO accounts (code, currency, kind) VALUES ($1, 'USD', 'debit_normal') RETURNING id",
    )
    .bind(&code)
    .fetch_one(pool)
    .await
    .expect("insert acct");
    row.get::<i64, _>("id")
}

/// Apply a delta and then wait long enough for at least one drain
/// tick at the current interval to fire and write the rollup row.
/// Returns the `drained_at` timestamp from rollup.
async fn apply_and_capture_drained_at(
    pool: &sqlx::PgPool,
    acct: i64,
    amount: i64,
    wait_ms: u64,
) -> Option<chrono::DateTime<chrono::Utc>> {
    sqlx::query(
        "SELECT ledger_apply_balance_delta($1::bigint, 1::int, 1::smallint, 1::smallint, $2, 0)",
    )
    .bind(acct)
    .bind(amount)
    .execute(pool)
    .await
    .expect("apply");

    tokio::time::sleep(Duration::from_millis(wait_ms)).await;

    let row = sqlx::query(
        "SELECT drained_at FROM account_balances_rollup \
          WHERE account_id = $1 AND period_id = 1 \
            AND currency_id = 1 AND ledger_kind = 1",
    )
    .bind(acct)
    .fetch_optional(pool)
    .await
    .expect("rollup lookup");
    row.map(|r| r.get::<chrono::DateTime<chrono::Utc>, _>("drained_at"))
}

/// T1 — `drain_interval_ms` SIGHUP reload. Pre-reload at default
/// 100ms (drain cadence fast); post-reload at 1500ms (drain cadence
/// slow). Measure observed cadence on both sides.
#[tokio::test]
async fn t1_drain_interval_ms_sighup_reload() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url())
        .await
        .expect("connect");
    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("ext");

    // Best-effort reset to default 100ms in case a prior test left it
    // somewhere else. RESET requires superuser; if we don't have it,
    // SET at the system level via ALTER SYSTEM works.
    let _ = sqlx::query("ALTER SYSTEM SET ledger.drain_interval_ms = 100")
        .execute(&pool)
        .await;
    let _ = sqlx::query("SELECT pg_reload_conf()")
        .execute(&pool)
        .await;
    // Let the next bgworker tick observe the reset.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let cur: i32 = sqlx::query_scalar("SELECT current_setting('ledger.drain_interval_ms')::int")
        .fetch_one(&pool)
        .await
        .expect("show");
    assert_eq!(cur, 100, "T1: precondition — drain_interval_ms should be 100");

    let acct = ensure_account(&pool).await;

    // Phase 1 — at 100ms cadence. Apply delta; observed drained_at
    // should land within ~300ms (one tick + tick work).
    let t0 = Instant::now();
    let drained_a = apply_and_capture_drained_at(&pool, acct, 100, 400).await;
    let dur_a = t0.elapsed();
    assert!(
        drained_a.is_some(),
        "T1 Phase 1: rollup row must exist within 400ms wait at 100ms interval (took {dur_a:?})"
    );

    // Phase 2 — raise interval to 1500ms via SIGHUP reload.
    sqlx::query("ALTER SYSTEM SET ledger.drain_interval_ms = 1500")
        .execute(&pool)
        .await
        .expect("alter system");
    sqlx::query("SELECT pg_reload_conf()")
        .execute(&pool)
        .await
        .expect("reload");

    // Wait long enough that the bgworker's next wait_latch returns
    // and reads the new GUC (within 1 tick at the old 100ms cadence).
    tokio::time::sleep(Duration::from_millis(300)).await;

    let cur_b: i32 = sqlx::query_scalar(
        "SELECT current_setting('ledger.drain_interval_ms')::int",
    )
    .fetch_one(&pool)
    .await
    .expect("show post-reload");
    assert_eq!(
        cur_b, 1500,
        "T1: post-reload current_setting should reflect 1500"
    );

    // Apply a NEW delta on a different account so the next drain tick
    // has work to do; capture the moment immediately before the apply
    // for cadence measurement.
    let acct2 = ensure_account(&pool).await;
    sqlx::query(
        "SELECT ledger_apply_balance_delta($1::bigint, 1::int, 1::smallint, 1::smallint, 200, 0)",
    )
    .bind(acct2)
    .execute(&pool)
    .await
    .expect("apply phase 2");

    // Probe at 400ms: rollup row should NOT exist yet (next tick is ~1.5s away).
    tokio::time::sleep(Duration::from_millis(400)).await;
    let early: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
        "SELECT drained_at FROM account_balances_rollup \
          WHERE account_id = $1 AND period_id = 1 \
            AND currency_id = 1 AND ledger_kind = 1",
    )
    .bind(acct2)
    .fetch_optional(&pool)
    .await
    .expect("rollup early");
    assert!(
        early.is_none(),
        "T1 Phase 2: at 400ms post-apply the 1500ms-interval bgworker should NOT \
         have drained yet; got drained_at={early:?}. SIGHUP reload didn't take effect."
    );

    // Probe at total ~2.2s post-apply: rollup row SHOULD now exist.
    tokio::time::sleep(Duration::from_millis(1800)).await;
    let late: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
        "SELECT drained_at FROM account_balances_rollup \
          WHERE account_id = $1 AND period_id = 1 \
            AND currency_id = 1 AND ledger_kind = 1",
    )
    .bind(acct2)
    .fetch_optional(&pool)
    .await
    .expect("rollup late");
    assert!(
        late.is_some(),
        "T1 Phase 2: at 2.2s post-apply the 1500ms-interval bgworker should \
         have drained at least once; rollup still empty for acct={acct2}"
    );

    // Restore default cadence for downstream tests.
    sqlx::query("ALTER SYSTEM SET ledger.drain_interval_ms = 100")
        .execute(&pool)
        .await
        .expect("restore");
    sqlx::query("SELECT pg_reload_conf()")
        .execute(&pool)
        .await
        .expect("reload restore");

    eprintln!(
        "T1: drain_interval_ms cadence change confirmed via SIGHUP. \
         Phase 1 wait=400ms → drained; Phase 2 wait=400ms → NOT drained; \
         Phase 2 wait=2200ms → drained."
    );
}

/// T2 — `drain_database` is `GucContext::Postmaster`. `ALTER SYSTEM SET`
/// succeeds and persists, but the running postmaster doesn't pick up
/// the new value until a full restart. `pending_restart` should flip
/// to `true` after the reload.
#[tokio::test]
async fn t2_drain_database_is_restart_only() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url())
        .await
        .expect("connect");
    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("ext");

    // Capture current effective value (Postmaster-scope: this is what
    // the running bgworker is connected to).
    let pre: String = sqlx::query_scalar("SELECT current_setting('ledger.drain_database')")
        .fetch_one(&pool)
        .await
        .expect("show pre");

    // ALTER SYSTEM SET to a different database name. This should
    // succeed (writes to postgresql.auto.conf) but NOT change the
    // running setting.
    let target = "guc_reload_t2_target_db_should_not_take_effect";
    sqlx::query(&format!(
        "ALTER SYSTEM SET ledger.drain_database = '{target}'"
    ))
    .execute(&pool)
    .await
    .expect("alter system");
    sqlx::query("SELECT pg_reload_conf()")
        .execute(&pool)
        .await
        .expect("reload");

    tokio::time::sleep(Duration::from_millis(300)).await;

    // current_setting() returns the EFFECTIVE running value (NOT what
    // ALTER SYSTEM wrote to .auto.conf — that's what
    // pg_settings.reset_val or boot_val reflect).
    let post: String =
        sqlx::query_scalar("SELECT current_setting('ledger.drain_database')")
            .fetch_one(&pool)
            .await
            .expect("show post");
    assert_eq!(
        post, pre,
        "T2: current_setting('ledger.drain_database') changed without restart: \
         pre={pre} post={post}. GucContext::Postmaster should make this restart-only."
    );

    // pending_restart should be TRUE for this row in pg_settings.
    let pending: bool = sqlx::query_scalar(
        "SELECT pending_restart FROM pg_settings WHERE name = 'ledger.drain_database'",
    )
    .fetch_one(&pool)
    .await
    .expect("pending_restart");
    assert!(
        pending,
        "T2: pg_settings.pending_restart should be TRUE after ALTER SYSTEM SET on a \
         postmaster-scope GUC; got false. Verify GucContext::Postmaster is registered \
         correctly."
    );

    // Reset auto.conf so the next PG restart doesn't try to connect
    // to a non-existent database.
    sqlx::query("ALTER SYSTEM RESET ledger.drain_database")
        .execute(&pool)
        .await
        .expect("reset");
    sqlx::query("SELECT pg_reload_conf()")
        .execute(&pool)
        .await
        .expect("reload reset");

    eprintln!(
        "T2: ALTER SYSTEM SET ledger.drain_database succeeded; \
         current_setting unchanged (Postmaster-scope); \
         pg_settings.pending_restart = true. Reset for cleanup."
    );
}
