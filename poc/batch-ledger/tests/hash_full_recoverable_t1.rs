//! acct-3ee2 / M10.C1 — hash-table-full as a recoverable contract.
//!
//! # Background
//!
//! Pre-acct-3ee2, `insert_new_seeded` raised `error!()` when no empty
//! slot was found within `N_BUCKETS` probes. That was load-bearing
//! for the pre-A2 synchronous-apply path (caller's transaction would
//! abort cleanly). Post-A2 the apply happens inside `xact_commit` —
//! the user transaction has already committed in PG's durable sense,
//! so an error there cannot abort the tx.
//!
//! Post-acct-3ee2:
//! - `insert_new_seeded` returns `Option<u64>` (None = no slot).
//! - `xact_pre_commit` is the load-bearing capacity gate. It projects
//!   `current_occupied + new_keys`; if > `N_BUCKETS`, raises `error!`.
//!   PG sees the PRE_COMMIT error and aborts cleanly.
//! - `xact_commit` handles `None` from `insert_new_seeded` (only
//!   reachable via a post-pre_commit race) by emitting a WARNING
//!   and bumping `LEDGER_SHMEM_INSERT_FAILURES`.
//!
//! # Tests
//!
//! - **T1** fills the hash to `N_BUCKETS` cells in one tx — commits
//!   cleanly. occupied = N_BUCKETS afterwards.
//! - **T2** with hash full, an apply on a NEW (N+1th distinct) key
//!   triggers `xact_pre_commit` error at COMMIT. The user transaction
//!   aborts and the SQLSTATE error message includes "would overflow".
//! - **T3** with hash full, an apply on an EXISTING key still succeeds
//!   (no new slot needed; just a CAS on the packed atomic).
//! - **T4** insert-failure counter starts at 0 (production invariant).
//!
//! These tests share state (they fill and depend on a full hash).
//! They use `ledger_shmem_reset()` between phases and at the end so
//! downstream test binaries see a clean state. Run with
//! `--test-threads=1` for determinism.
//!
//! # Location
//!
//! Sibling of other M10 t1 binaries.

use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

/// Per-invocation unique start offset for synthetic account_ids.
/// Avoids M6 lazy-load picking up stale `account_balances_rollup`
/// rows from prior test invocations (the bug rollback_correctness_t1
/// V3 hit; same pattern.) `1e12` base keeps us clear of all other
/// tests' synthetic ranges and of the BIGSERIAL accounts.id space.
fn unique_offset() -> i64 {
    let u = Uuid::new_v4();
    let bytes = u.as_bytes();
    // Use 4 bytes for ~4e9 distinct offsets, well above N_BUCKETS.
    let h = ((bytes[0] as i64) << 24)
        | ((bytes[1] as i64) << 16)
        | ((bytes[2] as i64) << 8)
        | (bytes[3] as i64);
    1_000_000_000_000_i64 + (h.abs() % 1_000_000_000)
}

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

fn db_url() -> String {
    std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string())
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
    p
}

async fn reset(pool: &sqlx::PgPool) {
    sqlx::query("SELECT ledger_shmem_reset()")
        .execute(pool)
        .await
        .expect("reset");
}

async fn capacity(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT ledger_shmem_capacity()")
        .fetch_one(pool)
        .await
        .expect("capacity")
}

async fn occupied(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT ledger_shmem_occupied()")
        .fetch_one(pool)
        .await
        .expect("occupied")
}

async fn insert_failures(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT ledger_shmem_insert_failure_count()")
        .fetch_one(pool)
        .await
        .expect("insert_failures")
}

/// Build a one-shot SQL command that stages `count` distinct keys in
/// a single implicit transaction. Account-ids start at `start_acct`.
/// Returns the count of staged keys (matching `count`).
async fn fill_n_cells_in_one_tx(
    pool: &sqlx::PgPool,
    start_acct: i64,
    count: usize,
) -> sqlx::Result<()> {
    let mut conn = pool.acquire().await?;
    sqlx::query("BEGIN").execute(&mut *conn).await?;
    // SQL: SELECT a single statement that invokes ledger_apply_balance_delta
    // for each of count keys via generate_series. Distinct account_ids
    // via the series offset.
    sqlx::query(
        "SELECT ledger_apply_balance_delta(\
              ($1::bigint + g.s), 1::int, 1::smallint, 1::smallint, 1, 0\
          ) FROM generate_series(0, $2::int - 1) AS g(s)",
    )
    .bind(start_acct)
    .bind(count as i32)
    .execute(&mut *conn)
    .await?;
    sqlx::query("COMMIT").execute(&mut *conn).await?;
    Ok(())
}

/// T1 — fill the hash table to exactly `N_BUCKETS` cells in one tx.
/// Pre-commit projection passes (0 + N = N, not > N). Commit succeeds.
#[tokio::test]
async fn t1_fill_to_capacity_in_one_tx() {
    let p = pool().await;
    reset(&p).await;
    let cap = capacity(&p).await as usize;

    let start = unique_offset();
    fill_n_cells_in_one_tx(&p, start, cap)
        .await
        .expect("fill to capacity");

    let occ = occupied(&p).await;
    assert_eq!(
        occ as usize, cap,
        "T1: occupied count should equal capacity ({cap}); got {occ}"
    );
    let fail = insert_failures(&p).await;
    assert_eq!(
        fail, 0,
        "T1: insert_failures should be 0 when fill stays within capacity; got {fail}"
    );
}

/// T2 — with hash full, adding 1 new key triggers pre_commit error.
/// The error message must mention "overflow" and the user transaction
/// must abort cleanly.
#[tokio::test]
async fn t2_overflow_rejected_at_pre_commit() {
    let p = pool().await;
    reset(&p).await;
    let cap = capacity(&p).await as usize;

    // Fill first.
    let start = unique_offset();
    fill_n_cells_in_one_tx(&p, start, cap)
        .await
        .expect("fill to capacity");

    let occ_pre = occupied(&p).await;
    assert_eq!(occ_pre as usize, cap, "T2 precondition: hash must be full");

    // Try one new key in its own tx — pre_commit must abort.
    let new_key = start + cap as i64 + 1; // outside the filled range
    let res = sqlx::query(
        "SELECT ledger_apply_balance_delta($1::bigint, 1::int, 1::smallint, 1::smallint, 99, 0)",
    )
    .bind(new_key)
    .execute(&p)
    .await;

    let err = res.expect_err("T2: overflow apply must abort via pre_commit error");
    let msg = format!("{err}");
    assert!(
        msg.contains("overflow") || msg.contains("hash table"),
        "T2: pre_commit error message should mention overflow / hash table; got: {msg}"
    );

    // Occupied count unchanged — the staged delta was never applied.
    let occ_post = occupied(&p).await;
    assert_eq!(
        occ_post, occ_pre,
        "T2: occupied count should be unchanged after rejected commit ({occ_pre}); got {occ_post}"
    );

    // The post-pre_commit failure counter stays at 0 — pre_commit
    // caught it before xact_commit ran.
    let fail = insert_failures(&p).await;
    assert_eq!(
        fail, 0,
        "T2: pre_commit gate catches the overflow before xact_commit runs; \
         post-pre_commit insert_failures should remain 0. Got {fail}"
    );
}

/// T3 — with hash full, applying to an EXISTING key still succeeds
/// (no new slot needed). The CAS path on `Bucket::balance_qty` is
/// independent of slot availability.
#[tokio::test]
async fn t3_existing_key_still_applies_at_full_capacity() {
    let p = pool().await;
    reset(&p).await;
    let cap = capacity(&p).await as usize;

    // Fill, then mutate one existing cell.
    let start = unique_offset();
    fill_n_cells_in_one_tx(&p, start, cap).await.expect("fill");
    let existing_acct: i64 = start + 5; // any cell within range

    // Capture pre-state via lookup: the M6 lazy-load may have seeded
    // this cell from a stale `account_balances_rollup` row written by
    // a previous run of this test against a colliding offset. Assert
    // delta-relative rather than absolute.
    let row_pre = sqlx::query(
        "SELECT balance, qty FROM ledger_balance_lookup($1::bigint, 1::int, 1::smallint, 1::smallint)",
    )
    .bind(existing_acct)
    .fetch_one(&p)
    .await
    .expect("pre lookup");
    let pre_bal: i64 = row_pre.get::<Option<i64>, _>("balance").unwrap_or(0);

    // Apply +1000 to the existing cell; pre_commit projects 0 new keys → OK.
    sqlx::query(
        "SELECT ledger_apply_balance_delta($1::bigint, 1::int, 1::smallint, 1::smallint, 1000, 0)",
    )
    .bind(existing_acct)
    .execute(&p)
    .await
    .expect("T3: apply to existing key at full capacity");

    let row_post = sqlx::query(
        "SELECT balance, qty FROM ledger_balance_lookup($1::bigint, 1::int, 1::smallint, 1::smallint)",
    )
    .bind(existing_acct)
    .fetch_one(&p)
    .await
    .expect("post lookup");
    let post_bal: Option<i64> = row_post.get("balance");
    assert_eq!(
        post_bal,
        Some(pre_bal + 1000),
        "T3: existing cell should advance by +1000 (pre={pre_bal}); got {post_bal:?}"
    );

    let occ_post = occupied(&p).await;
    assert_eq!(
        occ_post as usize, cap,
        "T3: occupied count should still be full ({cap}); got {occ_post}"
    );
}

/// T4 — production invariant: `LEDGER_SHMEM_INSERT_FAILURES` is 0 on
/// a fresh extension load. Catches a future regression where the
/// counter starts un-zero or doesn't reset.
#[tokio::test]
async fn t4_insert_failure_counter_starts_clean() {
    let p = pool().await;
    reset(&p).await;
    let fail = insert_failures(&p).await;
    assert_eq!(
        fail, 0,
        "T4: after reset, ledger_shmem_insert_failure_count must be 0; got {fail}"
    );
}
