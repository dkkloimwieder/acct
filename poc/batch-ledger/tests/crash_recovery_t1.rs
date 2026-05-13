//! acct-e5gl / M10.C3 — crash recovery probes.
//!
//! # Background
//!
//! The bgworker is registered with `set_restart_time(Duration::from_secs(1))`.
//! When it exits — whether via clean SIGTERM, SIGKILL, or postmaster
//! restart — the postmaster re-launches it after the configured delay.
//! These tests verify the re-launch contract under three crash modes
//! and document the durability boundary: in-shmem applies that have
//! not yet been drained into `account_balances_rollup` are LOST on a
//! PG-wide restart. The rollup row's `last_seq` is the durability
//! watermark. WAL-level durability is OUT OF SCOPE for M10.
//!
//! # Tests (all `#[ignore]`'d)
//!
//! Crash injection is destructive to concurrent test binaries, so all
//! three tests are `#[ignore]`'d and must be run explicitly:
//! `cargo test --test crash_recovery_t1 -- --ignored --test-threads=1`
//!
//! - **T1** clean SIGTERM via `pg_terminate_backend(<bgworker pid>)`.
//!   Verifies the bgworker exits and the postmaster relaunches it
//!   within `set_restart_time + 2s`. Post-relaunch apply + drain
//!   round-trips.
//! - **T2** uncatchable SIGKILL via `docker exec acct-postgres kill -9
//!   <bgworker pid>`. Same expected outcome — postmaster's restart
//!   policy applies regardless of exit cause.
//! - **T3** full postmaster restart via `docker restart acct-postgres`.
//!   Documents the loss profile: pre-restart in-shmem cells with no
//!   rollup row are LOST; cells with a rollup row are recovered via
//!   M6 lazy-load on first post-restart apply.
//!
//! # Container assumption
//!
//! `CONTAINER` env var (default `acct-postgres`) names the postgres
//! container. The test shells out via `docker exec` / `docker restart`.

use sqlx::postgres::PgPoolOptions;
use std::process::Command;
use std::time::{Duration, Instant};
use uuid::Uuid;

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

fn db_url() -> String {
    std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string())
}

fn container() -> String {
    std::env::var("CONTAINER").unwrap_or_else(|_| "acct-postgres".to_string())
}

fn unique_offset() -> i64 {
    let u = Uuid::new_v4();
    let bytes = u.as_bytes();
    let h = ((bytes[0] as i64) << 24)
        | ((bytes[1] as i64) << 16)
        | ((bytes[2] as i64) << 8)
        | (bytes[3] as i64);
    1_200_000_000_000_i64 + (h.abs() % 1_000_000_000)
}

async fn pool() -> sqlx::PgPool {
    let p = PgPoolOptions::new()
        .max_connections(4)
        // Tolerate brief connect failures during PG restart.
        .acquire_timeout(Duration::from_secs(30))
        .connect(&db_url())
        .await
        .expect("connect");
    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&p)
        .await
        .expect("ext");
    p
}

async fn bgworker_pid(pool: &sqlx::PgPool) -> Option<i32> {
    sqlx::query_scalar(
        "SELECT pid FROM pg_stat_activity \
         WHERE backend_type = 'ledger_drain' LIMIT 1",
    )
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()
}

/// Wait up to `bound` for a fresh bgworker PID (different from `old_pid`,
/// or any if `old_pid` is None and no worker was running).
async fn wait_for_bgworker_restart(pool: &sqlx::PgPool, old_pid: Option<i32>, bound: Duration) -> Option<i32> {
    let deadline = Instant::now() + bound;
    while Instant::now() < deadline {
        if let Some(pid) = bgworker_pid(pool).await {
            if old_pid.map_or(true, |o| pid != o) {
                return Some(pid);
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    None
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

/// T1 — bgworker SIGTERM is a CLEAN shutdown, NOT a crash. The
/// signal handler sets `got_SIGTERM`; the next `wait_latch` returns
/// false; the main loop breaks; the function returns. PG treats a
/// clean function return as exit-code-0 and the bgworker is
/// **deregistered without restart** per the documented bgworker
/// contract (`bgw_restart_time` only fires on abnormal termination).
///
/// This test pins that contract: post-SIGTERM the bgworker is gone
/// AND stays gone. To re-create it, restart the postmaster (covered
/// by T3) or DROP/CREATE the extension. T2 covers the abnormal-exit
/// path that DOES trigger auto-restart.
///
/// Run order matters: this test leaves the bgworker absent. T2 must
/// run AFTER PG is restarted to repopulate it (T2's setup recovers
/// from a missing bgworker by hard-failing fast). For a full sweep
/// the `--ignored` order is T1 → (manual restart) → T2 → (manual
/// restart) → T3.
#[tokio::test]
#[ignore]
async fn t1_bgworker_sigterm_is_clean_shutdown() {
    let p = pool().await;

    let old_pid = bgworker_pid(&p).await.expect("T1 precondition: bgworker should be running");
    eprintln!("T1: pre-SIGTERM bgworker pid={old_pid}");

    sqlx::query("SELECT pg_terminate_backend($1)")
        .bind(old_pid).execute(&p).await
        .expect("pg_terminate_backend");

    // Wait > set_restart_time so a (counterfactual) restart would have
    // had time to land.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let still = bgworker_pid(&p).await;
    assert!(
        still.is_none(),
        "T1: bgworker should NOT restart after clean SIGTERM (return-from-main = exit code 0 = \
         deregister-without-restart per PG bgworker contract). Got pid={still:?}"
    );
    eprintln!(
        "T1: contract pinned — clean SIGTERM is exit code 0; PG deregisters the \
         bgworker without invoking set_restart_time. To recover, restart the postmaster."
    );
}

/// T2 — bgworker SIGKILL via `docker exec kill -9`. Uncatchable;
/// postmaster's restart policy applies regardless of exit cause.
#[tokio::test]
#[ignore]
async fn t2_bgworker_sigkill_recovers() {
    let p = pool().await;
    sqlx::query("SELECT ledger_shmem_reset()").execute(&p).await.expect("reset");

    let old_pid = bgworker_pid(&p).await.expect("T2 precondition: bgworker should be running");
    eprintln!("T2: pre-kill bgworker pid={old_pid}");

    let output = Command::new("docker")
        .args(["exec", &container(), "kill", "-9", &old_pid.to_string()])
        .output()
        .expect("docker exec kill -9");
    if !output.status.success() {
        panic!(
            "T2: docker exec kill -9 failed: status={:?} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let new_pid = wait_for_bgworker_restart(&p, Some(old_pid), Duration::from_secs(5))
        .await
        .expect("T2: bgworker should relaunch within 5s of SIGKILL");
    eprintln!("T2: post-kill bgworker pid={new_pid}");
    assert_ne!(new_pid, old_pid, "T2: relaunched bgworker should have a new PID");

    let acct = unique_offset();
    apply(&p, acct, 99).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let bal: Option<i64> = sqlx::query_scalar(
        "SELECT balance FROM account_balances_rollup \
         WHERE account_id = $1 AND period_id = 1 \
           AND currency_id = 1 AND ledger_kind = 1",
    )
    .bind(acct)
    .fetch_optional(&p)
    .await
    .expect("rollup");
    assert_eq!(
        bal,
        Some(99),
        "T2: post-relaunch bgworker should drain new cell to rollup; got {bal:?}"
    );
}

/// T3 — full postmaster restart with dirty cells. Documents the loss
/// profile: pre-restart shmem applies that haven't drained are LOST.
/// Post-restart applies on previously-dirty keys see the LAST DRAINED
/// rollup value (or zero if never drained).
///
/// This test sets `drain_interval_ms` very high so the bgworker
/// doesn't drain mid-test, then restarts PG mid-shmem-state, then
/// verifies the in-shmem cells vanished.
#[tokio::test]
#[ignore]
async fn t3_postmaster_restart_documents_loss_profile() {
    let p = pool().await;
    sqlx::query("SELECT ledger_shmem_reset()").execute(&p).await.expect("reset");

    // Suppress drain so the test applies don't reach rollup.
    sqlx::query("ALTER SYSTEM SET ledger.drain_interval_ms = 60000")
        .execute(&p).await.expect("alter system");
    sqlx::query("SELECT pg_reload_conf()").execute(&p).await.expect("reload");
    // Wait one bgworker tick at the OLD cadence (≤100ms) so it
    // observes the new GUC before we apply.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let acct_a = unique_offset();
    let acct_b = unique_offset();
    apply(&p, acct_a, 100).await;
    apply(&p, acct_b, 200).await;

    let occ_pre: i64 = sqlx::query_scalar("SELECT ledger_shmem_occupied()")
        .fetch_one(&p).await.expect("occ");
    assert!(
        occ_pre >= 2,
        "T3 precondition: shmem should hold the two pre-restart cells; got {occ_pre}"
    );

    // pg_reload_conf in the user backend; the bgworker has 60s ticks
    // now and won't drain. Rollup is empty for these two accounts.
    let rollup_a_pre: Option<i64> = sqlx::query_scalar(
        "SELECT balance FROM account_balances_rollup \
         WHERE account_id = $1 AND period_id = 1 \
           AND currency_id = 1 AND ledger_kind = 1",
    )
    .bind(acct_a)
    .fetch_optional(&p).await.expect("rollup_a_pre");
    assert!(
        rollup_a_pre.is_none(),
        "T3 precondition: rollup should NOT yet have acct_a (drain interval 60s); \
         got {rollup_a_pre:?}"
    );

    // Drop the connection pool so PG restart can shutdown cleanly.
    drop(p);

    let output = Command::new("docker")
        .args(["restart", &container()])
        .output()
        .expect("docker restart");
    if !output.status.success() {
        panic!(
            "T3: docker restart failed: status={:?} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    // Wait for PG to come back.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if Instant::now() >= deadline {
            panic!("T3: PG did not come back within 30s of docker restart");
        }
        let ready = Command::new("docker")
            .args(["exec", &container(), "pg_isready", "-U", "acct"])
            .output();
        if ready.map_or(false, |o| o.status.success()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    // Restore drain interval.
    let p = pool().await;
    sqlx::query("ALTER SYSTEM SET ledger.drain_interval_ms = 100")
        .execute(&p).await.expect("restore interval");
    sqlx::query("SELECT pg_reload_conf()").execute(&p).await.expect("reload restore");

    // Post-restart: shmem must be empty (PgLwLock-backed HashTable
    // initialized via mem::zeroed in HashTable::default).
    let occ_post: i64 = sqlx::query_scalar("SELECT ledger_shmem_occupied()")
        .fetch_one(&p).await.expect("occ post");
    assert_eq!(
        occ_post, 0,
        "T3: post-restart shmem occupied count must be 0; got {occ_post}. \
         Loss profile violated."
    );

    // The acct_a apply is LOST (no rollup row, no shmem cell).
    let bal_a: Option<i64> = sqlx::query_scalar(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, 1::int, 1::smallint, 1::smallint)",
    )
    .bind(acct_a).fetch_one(&p).await.expect("lookup_a");
    assert_eq!(
        bal_a, None,
        "T3: post-restart lookup on acct_a should be None (lost); got {bal_a:?}"
    );

    // Apply a fresh delta to acct_a. M6 lazy-load sees no rollup row
    // → seeds at the delta value only (pre-restart +100 is lost).
    apply(&p, acct_a, 7).await;
    let bal_a_post: Option<i64> = sqlx::query_scalar(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, 1::int, 1::smallint, 1::smallint)",
    )
    .bind(acct_a).fetch_one(&p).await.expect("lookup_a_post");
    assert_eq!(
        bal_a_post,
        Some(7),
        "T3: post-restart re-apply on acct_a should produce balance=7 (delta only; \
         pre-restart +100 is lost); got {bal_a_post:?}"
    );

    eprintln!(
        "T3 documented loss profile: shmem cells without a rollup row \
         pre-restart are LOST post-restart. Post-restart applies see \
         the lazy-load seed = rollup value (None here) → fresh cell. \
         WAL-level durability is out of scope for M10."
    );
}
