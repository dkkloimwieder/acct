//! acct-plle / M10.C6 — panic cleanup path.
//!
//! pgrx wraps every `#[pg_extern]` in a `#[pg_guard]` that:
//! 1. Converts Rust panics into PostgreSQL ERRORs (no backend crash).
//! 2. Runs Drop on stack-allocated guards on the unwind path —
//!    including `PgLwLock` guards — so an in-flight LWLock is released
//!    before the SQL ERROR is reported.
//!
//! This is a defense-in-depth test: it does not fix a known bug, it
//! pins the existing FFI guarantee. If pgrx ever regressed the
//! panic-catcher / Drop-on-unwind contract, the next normal apply
//! would deadlock against the leaked LWLock. The test catches that
//! deadlock in bounded time (configurable timeout per probe).
//!
//! # Variants
//!
//! - **P1 panic AFTER mutation under SHARED lock** — verifies the
//!   shared-mode guard releases. Cell mutation persists (the
//!   AtomicU128 write already landed); next apply succeeds in
//!   bounded time.
//! - **P2 panic BEFORE any mutation under SHARED lock** — same
//!   shared-guard-release semantics, no mutation observed.
//! - **P3 panic while holding EXCLUSIVE lock** — load-bearing: if
//!   the exclusive guard didn't release, ALL subsequent
//!   shared-mode applies would deadlock against it. Next apply
//!   must succeed in bounded time.
//! - **P4 sanity** — backend health probe after panics
//!   (a fresh apply + balance lookup round-trips cleanly).
//!
//! # Location
//!
//! Sibling of `seqlock_torn_read_t1.rs` / `recon_under_load_t1.rs`.
//! Uses three test helpers added in `poc/ledger-extension/src/lib.rs`:
//! `ledger_test_panic_after_fetch_add`,
//! `ledger_test_panic_before_fetch_add`,
//! `ledger_test_panic_in_exclusive`.

use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use std::time::Instant;
use uuid::Uuid;

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

fn db_url() -> String {
    std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string())
}

fn synthetic_offset() -> i64 {
    let u = Uuid::new_v4();
    let bytes = u.as_bytes();
    let hash = ((bytes[0] as i64) << 24)
        | ((bytes[1] as i64) << 16)
        | ((bytes[2] as i64) << 8)
        | (bytes[3] as i64);
    910_000_000_i64 + (hash.abs() % 10_000_000)
}

/// Run a SQL apply call and assert it returns within `bound_ms` ms.
/// If the LWLock leaked from a prior panic, this hangs until the
/// statement_timeout (or test timeout) kicks in — the elapsed-time
/// check catches that.
async fn time_bounded_apply(
    pool: &sqlx::PgPool,
    acct: i64,
    period: i32,
    amount_delta: i64,
    qty_delta: i64,
    bound_ms: u128,
    label: &str,
) {
    let t0 = Instant::now();
    sqlx::query(
        "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, $3, $4)",
    )
    .bind(acct)
    .bind(period)
    .bind(amount_delta)
    .bind(qty_delta)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("{label}: apply failed: {e}"));
    let elapsed = t0.elapsed().as_millis();
    assert!(
        elapsed < bound_ms,
        "{label}: apply took {elapsed}ms (bound {bound_ms}ms); \
         likely a leaked LWLock from prior panic"
    );
}

/// P1 — panic AFTER mutation under SHARED. The mutation should persist
/// (AtomicU128 write already landed), the SQL ERROR should propagate
/// cleanly with the panic message, and the next apply must proceed.
#[tokio::test]
async fn p1_panic_after_mutation_under_shared() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url())
        .await
        .expect("connect");
    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("ext");

    let acct = synthetic_offset();
    let period: i32 = 1;

    // Capture pre-state via delta semantics.
    let pre_bal: i64 = sqlx::query_scalar(
        "SELECT COALESCE(balance, 0) FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct).bind(period)
    .fetch_one(&pool).await.expect("pre");

    // Prime so the panic-helper takes the SHARED fast path (cell exists).
    sqlx::query(
        "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 100, 0)",
    )
    .bind(acct).bind(period).execute(&pool).await.expect("prime");

    let res = sqlx::query(
        "SELECT ledger_test_panic_after_fetch_add($1::bigint, $2::int, 1::smallint, 1::smallint, 555, 0)",
    )
    .bind(acct).bind(period).execute(&pool).await;

    let err = res.expect_err("P1: panic helper must return a SQL ERROR");
    let msg = format!("{err}");
    assert!(
        msg.contains("deliberate panic after mutation")
            || msg.contains("ledger_test_panic_after_fetch_add"),
        "P1: SQL error should carry the panic message; got: {msg}"
    );

    // Mutation persists: balance = pre + 100 (prime) + 555 (panic-helper mutation).
    // The test helper takes SHARED + try_update_existing, which uses the
    // atomic CAS; the write lands BEFORE the panic.
    let post_bal: i64 = sqlx::query_scalar(
        "SELECT COALESCE(balance, 0) FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct).bind(period)
    .fetch_one(&pool).await.expect("post");
    assert_eq!(
        post_bal,
        pre_bal + 100 + 555,
        "P1: expected post={} (pre+100+555); got {post_bal}",
        pre_bal + 100 + 555
    );

    // Next apply must proceed in bounded time. 5s is generous; a leaked
    // SHARED guard would deadlock here (a future EXCLUSIVE-needing
    // insert path would block, and SHARED-vs-SHARED doesn't deadlock,
    // so this probe really catches the EXCLUSIVE-insert variant —
    // covered explicitly in P3).
    time_bounded_apply(&pool, acct, period, 1, 0, 5000, "P1 post-panic").await;
}

/// P2 — panic BEFORE any mutation under SHARED. No mutation observed;
/// next apply proceeds.
#[tokio::test]
async fn p2_panic_before_mutation_under_shared() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url())
        .await
        .expect("connect");
    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("ext");

    let acct = synthetic_offset();
    let period: i32 = 1;
    let pre_bal: i64 = sqlx::query_scalar(
        "SELECT COALESCE(balance, 0) FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct).bind(period).fetch_one(&pool).await.expect("pre");

    let res = sqlx::query(
        "SELECT ledger_test_panic_before_fetch_add($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct).bind(period).execute(&pool).await;
    res.expect_err("P2: panic helper must return a SQL ERROR");

    // No mutation observed.
    let post_bal: i64 = sqlx::query_scalar(
        "SELECT COALESCE(balance, 0) FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct).bind(period).fetch_one(&pool).await.expect("post");
    assert_eq!(
        post_bal, pre_bal,
        "P2: panic before mutation should leave balance unchanged"
    );

    // Next apply proceeds.
    time_bounded_apply(&pool, acct, period, 1, 0, 5000, "P2 post-panic").await;
}

/// P3 — panic while holding EXCLUSIVE. Load-bearing: a leaked
/// EXCLUSIVE would deadlock the next SHARED apply. Next apply must
/// proceed in bounded time.
#[tokio::test]
async fn p3_panic_in_exclusive_lock() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url())
        .await
        .expect("connect");
    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("ext");

    let acct = synthetic_offset();
    let period: i32 = 1;

    let res = sqlx::query(
        "SELECT ledger_test_panic_in_exclusive($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct).bind(period).execute(&pool).await;
    res.expect_err("P3: panic helper must return a SQL ERROR");

    // Next apply proceeds — this is the critical assertion. If
    // EXCLUSIVE was leaked, this hangs.
    time_bounded_apply(&pool, acct, period, 42, 1, 5000, "P3 post-exclusive-panic").await;

    // And the mutation landed.
    let row = sqlx::query(
        "SELECT balance, qty FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct).bind(period).fetch_one(&pool).await.expect("lookup");
    let bal: Option<i64> = row.get("balance");
    let qty: Option<i64> = row.get("qty");
    assert_eq!(bal, Some(42), "P3: post-panic apply should land balance=42; got {bal:?}");
    assert_eq!(qty, Some(1), "P3: post-panic apply should land qty=1; got {qty:?}");
}

/// P4 — backend health sanity. After a sequence of mixed panic
/// variants on the same connection, the backend remains healthy:
/// a normal apply + lookup roundtrips cleanly.
#[tokio::test]
async fn p4_backend_health_after_mixed_panics() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url())
        .await
        .expect("connect");
    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("ext");

    let acct = synthetic_offset();
    let period: i32 = 1;

    // Run all three panic helpers in sequence on the same pool.
    for helper in [
        "SELECT ledger_test_panic_after_fetch_add($1::bigint, $2::int, 1::smallint, 1::smallint, 7, 0)",
        "SELECT ledger_test_panic_before_fetch_add($1::bigint, $2::int, 1::smallint, 1::smallint)",
        "SELECT ledger_test_panic_in_exclusive($1::bigint, $2::int, 1::smallint, 1::smallint)",
    ] {
        let _ = sqlx::query(helper).bind(acct).bind(period).execute(&pool).await;
    }

    // Pool may have evicted the connection that hit the panic — refresh.
    // sqlx releases broken conns automatically; we just need to issue
    // a fresh statement.
    time_bounded_apply(&pool, acct, period, 13, 0, 5000, "P4 health probe").await;

    let bal: Option<i64> = sqlx::query_scalar(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct).bind(period).fetch_one(&pool).await.expect("post");
    assert!(
        bal.is_some(),
        "P4: post-panics health probe should observe a balance for acct={acct}; got None"
    );
}
