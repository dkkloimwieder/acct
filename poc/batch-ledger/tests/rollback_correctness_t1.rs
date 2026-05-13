//! acct-2733 / M10.A1 + acct-4e91 / M10.A2 — rollback correctness
//! regression net.
//!
//! ## History
//!
//! - **A1 (acct-2733)**: confirmed the bug. Pre-A2, the M9 shmem
//!   extension mutated `Bucket.balance` / `Bucket.qty` synchronously
//!   on the apply call. PG transaction rollback unwound heap rows
//!   (MVCC) but had no awareness of the shmem mutations, so
//!   `BEGIN; apply; ROLLBACK;` left the delta in shmem. Confirmed
//!   via V1 (minimal) + V2 (recon-coupled).
//!
//! - **A2 (acct-4e91)**: the fix. `ledger_apply_balance_delta` now
//!   STAGES the delta into a per-backend `PENDING_STACK`; an
//!   XactCallback applies on COMMIT, discards on ABORT. SubXact
//!   callbacks support SAVEPOINT / ROLLBACK TO. This file's
//!   assertions FLIPPED in the same commit that landed A2:
//!
//!   - V1 post-rollback shmem state: **absent** (was: `Some(1000)`).
//!   - V2 post-rollback recon: drift=0 (was: drift=+1000).
//!
//! Together V1 + V2 form the canonical regression net for "rollback
//! unwinds shmem correctly." A future refactor that reverts A2's
//! staging would flip these to FAILED.

use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

fn db_url() -> String {
    std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string())
}

/// V1 — Minimal regression net. Asserts the M10.A2 fix: after
/// `BEGIN; apply; ROLLBACK;`, the shmem cell is absent.
#[tokio::test]
async fn v1_rollback_discards_shmem_delta() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url())
        .await
        .expect("connect");

    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("create ext");

    // Synthetic key well above any seeded fixture or concurrent test.
    let acct: i64 = 999_001;
    let period: i32 = 9_001;

    let pre: Option<i64> = sqlx::query_scalar(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("pre lookup");

    assert_eq!(
        pre, None,
        "test isolation violated — cell at ({acct}, {period}) already exists. \
         Run with RUST_TEST_THREADS=1 or pick a different synthetic key."
    );

    // Apply inside a transaction, then ROLLBACK.
    let mut tx = pool.begin().await.expect("begin tx");
    sqlx::query(
        "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 1000, 0)",
    )
    .bind(acct)
    .bind(period)
    .execute(&mut *tx)
    .await
    .expect("apply");
    tx.rollback().await.expect("rollback");

    // Probe: shmem must NOT have retained the delta.
    let post: Option<i64> = sqlx::query_scalar(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("post lookup");

    assert_eq!(
        post, None,
        "M10.A2 REGRESSION: shmem retained applied delta after ROLLBACK. \
         pre={pre:?}, post={post:?}. \
         The XactCallback abort path failed to discard the pending entry. \
         Check PENDING_STACK handling in lib.rs and pg_sys::RegisterXactCallback \
         registration in ensure_callbacks_registered()."
    );
}

/// V2 — Realistic regression net coupling shmem to posting_lines.
///
/// Same scenario as A1's V2: a transaction INSERTs a posting_line AND
/// calls ledger_apply_balance_delta for both legs; rollback unwinds
/// the posting_lines row. Post-A2, shmem also unwinds → drift=0.
#[tokio::test]
async fn v2_rollback_keeps_recon_clean() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url())
        .await
        .expect("connect");

    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("create ext");

    let suffix = Uuid::new_v4().to_string()[..8].to_string();
    let dr_code = format!("V2-DR-{suffix}");
    let cr_code = format!("V2-CR-{suffix}");

    let dr: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (code, currency, kind) \
         VALUES ($1, 'USD', 'debit_normal'::account_kind) RETURNING id",
    )
    .bind(&dr_code)
    .fetch_one(&pool)
    .await
    .expect("dr account");
    let cr: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (code, currency, kind) \
         VALUES ($1, 'USD', 'credit_normal'::account_kind) RETURNING id",
    )
    .bind(&cr_code)
    .fetch_one(&pool)
    .await
    .expect("cr account");

    let dr_pre: Option<i64> = sqlx::query_scalar(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, 1::int, 1::smallint, 1::smallint)",
    )
    .bind(dr)
    .fetch_one(&pool)
    .await
    .expect("dr pre");
    assert_eq!(dr_pre, None, "fresh DR account should have no shmem cell");

    let mut tx = pool.begin().await.expect("begin tx");
    let idem = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO posting_lines \
            (debit_account_id, credit_account_id, amount, currency, idempotency_key, business_date) \
         VALUES ($1, $2, $3, 'USD', $4, '2026-05-12'::date)",
    )
    .bind(dr)
    .bind(cr)
    .bind(1000i64)
    .bind(idem)
    .execute(&mut *tx)
    .await
    .expect("insert posting_line");

    sqlx::query(
        "SELECT ledger_apply_balance_delta($1::bigint, 1::int, 1::smallint, 1::smallint, 1000, 0)",
    )
    .bind(dr)
    .execute(&mut *tx)
    .await
    .expect("apply dr");
    sqlx::query(
        "SELECT ledger_apply_balance_delta($1::bigint, 1::int, 1::smallint, 1::smallint, -1000, 0)",
    )
    .bind(cr)
    .execute(&mut *tx)
    .await
    .expect("apply cr");

    tx.rollback().await.expect("rollback");

    let posting_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM posting_lines WHERE debit_account_id = $1 OR credit_account_id = $1",
    )
    .bind(dr)
    .fetch_one(&pool)
    .await
    .expect("count posting_lines");
    assert_eq!(posting_rows, 0, "posting_lines must be empty after rollback");

    // Post-A2: no shmem cell should exist for these accounts, so recon
    // returns no row. The clean-ledger assertion is "no row OR drift=0
    // OR cell exists with shmem_balance=0".
    let row = sqlx::query(
        "SELECT shmem_balance, ledger_balance, drift FROM ledger_shmem_recon() WHERE account_id = $1",
    )
    .bind(dr)
    .fetch_optional(&pool)
    .await
    .expect("recon dr");

    match row {
        None => {
            // Best outcome: no shmem cell at all.
        }
        Some(r) => {
            let shmem_bal: i64 = r.get(0);
            let drift: Option<i64> = r.get(2);
            assert_eq!(
                drift,
                Some(0),
                "M10.A2 REGRESSION: shmem retained delta after rollback. \
                 shmem_balance={shmem_bal}, drift={drift:?}. \
                 Expected no recon row (clean unwind) or drift=0."
            );
        }
    }

    // Cleanup.
    let _ = sqlx::query("DELETE FROM accounts WHERE id IN ($1, $2)")
        .bind(dr)
        .bind(cr)
        .execute(&pool)
        .await;
}

/// V3 — A2-specific: COMMIT applies the staged delta.
///
/// Mirror of V1 but with COMMIT instead of ROLLBACK. Confirms the
/// XactCallback Commit path actually mutates shmem (the other half of
/// A2's contract).
#[tokio::test]
async fn v3_commit_applies_staged_delta() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url())
        .await
        .expect("connect");

    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("create ext");

    let acct: i64 = 999_003;
    let period: i32 = 9_003;

    // Ensure clean baseline (cell may persist across runs since we
    // commit; assert via delta semantics).
    let pre: Option<i64> = sqlx::query_scalar(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("pre");
    let pre_bal = pre.unwrap_or(0);

    let mut tx = pool.begin().await.expect("begin");
    sqlx::query(
        "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 250, 0)",
    )
    .bind(acct)
    .bind(period)
    .execute(&mut *tx)
    .await
    .expect("apply");
    tx.commit().await.expect("commit");

    let post: Option<i64> = sqlx::query_scalar(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("post");

    assert_eq!(
        post,
        Some(pre_bal + 250),
        "V3 COMMIT path: expected balance to advance by 250 (pre={pre_bal}); got {post:?}"
    );
}
