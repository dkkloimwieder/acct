//! acct-2733 / M10.A1 — confirm the rollback correctness bug exists.
//!
//! GATE TASK for M10 Track A. The M9 shmem extension mutates `Bucket.balance`
//! and `Bucket.qty` via atomic `fetch_add` on the apply call. PG transaction
//! rollback unwinds heap rows (MVCC) but has no awareness of shmem mutations.
//!
//! Hypothesis: after `BEGIN; ledger_apply_balance_delta(...); ROLLBACK;` the
//! shmem cell retains the delta. The ledger and shmem diverge.
//!
//! If this hypothesis is CONFIRMED:
//!   - M10 Track A (XactCallback + SubXactCallback + PENDING_STACK) is
//!     correctly scoped at ~1 day and proceeds.
//!   - This test's assertion FLIPS after M10.A2 lands — at that point the
//!     same scenario must produce drift=0. The file becomes the canonical
//!     regression net for "rollback unwinds shmem correctly."
//!
//! If this hypothesis is FALSIFIED:
//!   - Investigate the mechanism that's unwinding the apply (pgrx
//!     machinery, default callback, something I missed) before scoping A.2.
//!   - M10 Track A may collapse entirely or refocus.

use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

fn db_url() -> String {
    std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string())
}

/// V1 — Minimal probe. The actual gate.
///
/// Does not touch posting_lines or accounts. Pure probe of shmem
/// behavior under transaction rollback. Account-id is fully synthetic;
/// no `accounts` row needed because `ledger_apply_balance_delta` is
/// accounts-existence-agnostic by design.
#[tokio::test]
async fn v1_rollback_retains_shmem_delta() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url())
        .await
        .expect("connect");

    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("create ext");

    // High synthetic key — unlikely to collide with any seeded test/bench
    // workload. (period_id=9001 is well above any realistic period seed.)
    let acct: i64 = 999_001;
    let period: i32 = 9_001;

    // Pre-state: cell should be absent. If not, test isolation violated
    // by a concurrent test — fail loudly rather than silently mis-asserting.
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

    // Probe: did shmem retain the delta?
    let post: Option<i64> = sqlx::query_scalar(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("post lookup");

    // Capture the outcome explicitly so the assertion message names the
    // hypothesis branch.
    match post {
        Some(1000) => {
            eprintln!(
                "\n=== M10.A1 V1: ROLLBACK BUG CONFIRMED ===\n\
                 shmem retained applied delta after ROLLBACK.\n\
                 Pre-rollback shmem state: absent (None).\n\
                 Applied: +1000.\n\
                 Post-rollback shmem state: 1000.\n\
                 Drift: +1000 (relative to never-applied baseline).\n\
                 \n\
                 M10 Track A (acct-4e91 XactCallback + SubXactCallback) is\n\
                 correctly scoped. Proceed.\n"
            );
        }
        None => panic!(
            "M10.A1 V1: HYPOTHESIS FALSIFIED — shmem cell absent post-rollback. \
             Something is unwinding the apply. Investigate before committing M10 Track A. \
             pre={pre:?}, post={post:?}"
        ),
        Some(other) => panic!(
            "M10.A1 V1: UNEXPECTED — shmem balance is {other}, expected either Some(1000) (bug) or None (falsified). \
             Test state may be polluted. pre={pre:?}, post={post:?}"
        ),
    }

    // Cleanup: leaves the cell at +1000. Subsequent runs of V1 would see
    // pre=Some(N*1000) and the isolation assertion would fail loudly.
    // ledger_shmem_reset wipes ALL cells — too aggressive if other tests
    // are sharing the shmem. Targeted reset isn't supported. Accept that
    // V1 is a one-shot test per shmem lifetime; concurrent reruns are
    // an integration-test-pollution problem, not a V1 problem.
}

/// V2 — Realistic probe coupled to posting_lines.
///
/// Mirrors the production-relevant scenario: a transaction INSERTs a
/// posting_line AND calls ledger_apply_balance_delta; rollback unwinds the
/// posting_lines row but not the shmem cell. `ledger_shmem_recon()` exposes
/// the resulting drift.
///
/// Uses fresh accounts with UUID-derived codes to avoid colliding with
/// concurrent tests. Does NOT TRUNCATE — that would blow away other tests'
/// state.
#[tokio::test]
async fn v2_rollback_with_posting_lines_diverges_recon() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url())
        .await
        .expect("connect");

    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("create ext");

    // Unique account codes so concurrent test runs don't collide.
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

    // Pre-state: no posting_lines, no shmem cell for our accounts.
    // (Other tests may have other cells; we only inspect ours.)
    let dr_pre: Option<i64> = sqlx::query_scalar(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, 1::int, 1::smallint, 1::smallint)",
    )
    .bind(dr)
    .fetch_one(&pool)
    .await
    .expect("dr pre");
    assert_eq!(dr_pre, None, "fresh DR account should have no shmem cell");

    // Transaction: INSERT posting_line + apply both legs. Then ROLLBACK.
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

    // Post-rollback: posting_lines is empty for these accounts; shmem
    // (if buggy) retains the deltas.
    let posting_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM posting_lines WHERE debit_account_id = $1 OR credit_account_id = $1",
    )
    .bind(dr)
    .fetch_one(&pool)
    .await
    .expect("count posting_lines");
    assert_eq!(
        posting_rows, 0,
        "posting_lines for DR account should be empty after rollback (got {posting_rows})"
    );

    // Recon: drift = shmem_balance - SUM(debits)+SUM(credits).
    // Under the hypothesis, drift_dr = 1000 - 0 = 1000; drift_cr = -1000 - 0 = -1000.
    let row = sqlx::query(
        "SELECT shmem_balance, ledger_balance, drift FROM ledger_shmem_recon() WHERE account_id = $1",
    )
    .bind(dr)
    .fetch_optional(&pool)
    .await
    .expect("recon dr");

    match row {
        Some(r) => {
            let shmem_bal: i64 = r.get(0);
            let ledger_bal: Option<i64> = r.get(1);
            let drift: Option<i64> = r.get(2);
            eprintln!(
                "\n=== M10.A1 V2: RECON RESULT (DR side) ===\n\
                 shmem_balance: {shmem_bal}\n\
                 ledger_balance: {ledger_bal:?}\n\
                 drift: {drift:?}\n"
            );
            assert_eq!(
                drift,
                Some(1000),
                "M10.A1 V2: expected drift = +1000 (applied delta retained, ledger unchanged). \
                 If drift=0, hypothesis falsified — shmem rolled back somehow. \
                 If drift is something else, investigate."
            );
        }
        None => panic!(
            "M10.A1 V2: ledger_shmem_recon() returned no row for DR account {dr}. \
             Hypothesis falsified — shmem cell absent post-rollback, OR recon filter \
             excluded our cell (check period/currency/ledger_kind filter)."
        ),
    }

    // Cleanup: delete the test accounts so the leaked shmem cells don't
    // pollute future ledger_shmem_recon() calls with NULL-ledger orphans.
    // (We can't delete the shmem cells without ledger_shmem_reset which
    // wipes everyone's state. Document this as known cell-leak per test
    // run; production fix lands in M10.A2 where the apply doesn't fire
    // in the first place.)
    let _ = sqlx::query("DELETE FROM accounts WHERE id IN ($1, $2)")
        .bind(dr)
        .bind(cr)
        .execute(&pool)
        .await;
}
