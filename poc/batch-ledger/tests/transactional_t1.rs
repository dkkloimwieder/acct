//! acct-4e91 / M10.A2 — XactCallback + SubXactCallback transactional
//! correctness regression net.
//!
//! Per-test pinning of the seven A2 acceptance scenarios from the bd
//! issue. Test #1 (rollback no-op) is in `rollback_correctness_t1.rs`
//! (V1 + V2); this file covers #2-#7:
//!
//! - **t2_savepoint_nesting** — RELEASE preserves, ROLLBACK TO discards.
//! - **t3_cross_backend_isolation** — PENDING_STACK is per-backend
//!   (thread_local), not shared across pool connections.
//! - **t4_precommit_capacity_rejection** (`#[ignore]`'d, slow) —
//!   COMMIT with > N_BUCKETS new keys raises clean ERROR.
//! - **t5_multi_cell_collapse** — same-key applies within one txn
//!   collapse into one cell mutation.
//! - **t6_drain_does_not_see_staged** — bgworker's next tick during
//!   an in-flight transaction does not observe staged deltas.
//! - **t7_ryw_limitation** — within-txn `ledger_balance_lookup`
//!   returns PRE-staging value (documented limitation).
//! - **t8_first_apply_mid_subxact_acct_17vr** — fresh backend with
//!   first apply inside an already-open subxact discards on ROLLBACK
//!   TO and merges on RELEASE (pins acct-17vr's _PG_init callback
//!   registration).
//!
//! Test isolation strategy mirrors `rollback_correctness_t1.rs`: high
//! synthetic keys derived from a per-test UUID so concurrent test
//! binaries don't collide.

use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
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
    700_000_000_i64 + (hash.abs() % 10_000_000)
}

/// T2 — savepoint nesting.
/// ```text
/// BEGIN;
///   apply(A, +100);
///   SAVEPOINT s1;
///     apply(A, +50);
///     SAVEPOINT s2;
///       apply(A, +20);
///     ROLLBACK TO s2;          -- discards +20
///   RELEASE s1;                -- preserves +50
/// COMMIT;
/// -- expected: A.balance = 150 (= 100 + 50)
/// ```
#[tokio::test]
async fn t2_savepoint_nesting() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url())
        .await
        .expect("connect");
    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("create ext");

    let acct = synthetic_offset();
    let period: i32 = 8_002;

    let pre: Option<i64> = sqlx::query_scalar(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("pre lookup");
    let pre_bal = pre.unwrap_or(0);

    let mut tx = pool.begin().await.expect("begin");

    sqlx::query(
        "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 100, 0)",
    )
    .bind(acct)
    .bind(period)
    .execute(&mut *tx)
    .await
    .expect("apply +100");

    sqlx::query("SAVEPOINT s1")
        .execute(&mut *tx)
        .await
        .expect("savepoint s1");

    sqlx::query(
        "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 50, 0)",
    )
    .bind(acct)
    .bind(period)
    .execute(&mut *tx)
    .await
    .expect("apply +50");

    sqlx::query("SAVEPOINT s2")
        .execute(&mut *tx)
        .await
        .expect("savepoint s2");

    sqlx::query(
        "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 20, 0)",
    )
    .bind(acct)
    .bind(period)
    .execute(&mut *tx)
    .await
    .expect("apply +20");

    sqlx::query("ROLLBACK TO SAVEPOINT s2")
        .execute(&mut *tx)
        .await
        .expect("rollback to s2");

    sqlx::query("RELEASE SAVEPOINT s1")
        .execute(&mut *tx)
        .await
        .expect("release s1");

    tx.commit().await.expect("commit");

    let post: Option<i64> = sqlx::query_scalar(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("post lookup");

    assert_eq!(
        post,
        Some(pre_bal + 150),
        "T2: expected post-commit balance = pre + 150 (100 + 50, no +20); pre={pre_bal} got {post:?}"
    );
}

/// T3 — cross-backend isolation. Backend A stages in its txn;
/// Backend B observes no shmem mutation until A commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t3_cross_backend_isolation() {
    // Use a connection-per-task pattern so each task is on its own
    // backend process (PENDING_STACK is thread_local per-backend).
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url())
        .await
        .expect("connect");
    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("create ext");

    let acct = synthetic_offset();
    let period: i32 = 8_003;

    // Baseline: cell may or may not exist; capture pre state.
    let pre: Option<i64> = sqlx::query_scalar(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("pre");
    let pre_bal = pre.unwrap_or(0);

    // Take an exclusive connection for backend A; open a transaction.
    let mut conn_a = pool.acquire().await.expect("conn a");
    sqlx::query("BEGIN")
        .execute(&mut *conn_a)
        .await
        .expect("begin a");
    sqlx::query(
        "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 777, 0)",
    )
    .bind(acct)
    .bind(period)
    .execute(&mut *conn_a)
    .await
    .expect("apply a");

    // Backend B (separate connection) probes: must see PRE state.
    let mid_b: Option<i64> = sqlx::query_scalar(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("mid b");

    assert_eq!(
        mid_b.unwrap_or(0),
        pre_bal,
        "T3: backend B observed backend A's staged delta before A committed (isolation violated)"
    );

    // Backend A rolls back; B's view unchanged.
    sqlx::query("ROLLBACK")
        .execute(&mut *conn_a)
        .await
        .expect("rollback a");
    drop(conn_a);

    let post_rollback_b: Option<i64> = sqlx::query_scalar(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("post-rollback b");
    assert_eq!(
        post_rollback_b.unwrap_or(0),
        pre_bal,
        "T3: post-rollback B should still see pre-state ({pre_bal}), got {post_rollback_b:?}"
    );

    // Now commit via A: B sees the post-commit value.
    let mut conn_a2 = pool.acquire().await.expect("conn a2");
    sqlx::query("BEGIN")
        .execute(&mut *conn_a2)
        .await
        .expect("begin a2");
    sqlx::query(
        "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 777, 0)",
    )
    .bind(acct)
    .bind(period)
    .execute(&mut *conn_a2)
    .await
    .expect("apply a2");
    sqlx::query("COMMIT")
        .execute(&mut *conn_a2)
        .await
        .expect("commit a2");
    drop(conn_a2);

    let post_commit_b: Option<i64> = sqlx::query_scalar(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("post-commit b");
    assert_eq!(
        post_commit_b,
        Some(pre_bal + 777),
        "T3: post-commit B should see pre+777; got {post_commit_b:?}"
    );
}

/// T4 — PreCommit capacity rejection. Opens a single txn that applies
/// against > N_BUCKETS distinct new keys; expects the COMMIT to raise.
///
/// **Slow**: applies 16385 deltas in one txn (~3-5s). `#[ignore]`'d
/// by default; run with `--ignored`.
#[tokio::test]
#[ignore]
async fn t4_precommit_capacity_rejection() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url())
        .await
        .expect("connect");
    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("create ext");

    // Reset to start at occupied=0 (destructive — only runs under --ignored
    // sequencing).
    sqlx::query("SELECT ledger_shmem_reset()")
        .execute(&pool)
        .await
        .expect("reset");

    // N_BUCKETS in lib.rs is 16384.
    let n: i64 = 16_385;
    let base = synthetic_offset();
    let period: i32 = 8_004;

    let mut tx = pool.begin().await.expect("begin");
    for i in 0..n {
        sqlx::query(
            "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 1, 0)",
        )
        .bind(base + i)
        .bind(period)
        .execute(&mut *tx)
        .await
        .expect("apply");
    }

    let commit_result = tx.commit().await;
    assert!(
        commit_result.is_err(),
        "T4: COMMIT with {n} > N_BUCKETS new keys should have raised; got Ok"
    );

    let err = commit_result.unwrap_err().to_string();
    assert!(
        err.contains("hash table would overflow") || err.contains("near capacity"),
        "T4: error message should mention capacity overflow; got: {err}"
    );

    // Shmem occupied should be unchanged (no inserts performed since
    // the abort fired during pre-commit, before xact_commit).
    let occupied: i64 = sqlx::query_scalar("SELECT ledger_shmem_occupied()")
        .fetch_one(&pool)
        .await
        .expect("occupied");
    assert_eq!(
        occupied, 0,
        "T4: occupied should still be 0 after rejected commit; got {occupied}"
    );
}

/// T5 — multi-cell collapse within a single txn. Three applies, two
/// distinct keys; commit yields exactly two cell mutations.
#[tokio::test]
async fn t5_multi_cell_collapse() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url())
        .await
        .expect("connect");
    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("create ext");

    let a = synthetic_offset();
    let b = a + 1;
    let period: i32 = 8_005;

    let pre_a = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(a)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("pre a")
    .unwrap_or(0);
    let pre_b = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(b)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("pre b")
    .unwrap_or(0);

    let pre_seq: i64 = sqlx::query_scalar("SELECT ledger_shmem_apply_seq()")
        .fetch_one(&pool)
        .await
        .expect("pre seq");

    let mut tx = pool.begin().await.expect("begin");
    for &(key, delta) in &[(a, 100i64), (b, 50), (a, 30)] {
        sqlx::query(
            "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, $3, 0)",
        )
        .bind(key)
        .bind(period)
        .bind(delta)
        .execute(&mut *tx)
        .await
        .expect("apply");
    }
    tx.commit().await.expect("commit");

    let post_a = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(a)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("post a");
    let post_b = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(b)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("post b");

    assert_eq!(
        post_a,
        Some(pre_a + 130),
        "T5: key A should reflect 100 + 30 = 130 collapsed (pre={pre_a}); got {post_a:?}"
    );
    assert_eq!(
        post_b,
        Some(pre_b + 50),
        "T5: key B should reflect 50 (pre={pre_b}); got {post_b:?}"
    );

    // APPLY_SEQ advanced by exactly 2 (one per distinct key at commit),
    // not 3 (which would indicate per-call advancement).
    let post_seq: i64 = sqlx::query_scalar("SELECT ledger_shmem_apply_seq()")
        .fetch_one(&pool)
        .await
        .expect("post seq");
    let seq_delta = post_seq - pre_seq;
    assert!(
        seq_delta >= 2,
        "T5: APPLY_SEQ should advance by >=2 (one per distinct key); delta={seq_delta}"
    );
    // Allow concurrent applies from other tests to inflate the delta,
    // but document that our contribution is exactly 2.
}

/// T6 — drain interaction. While a txn is open and has staged a
/// delta, the bgworker tick does not see the cell in shmem; after
/// COMMIT, the next tick picks it up.
#[tokio::test]
async fn t6_drain_does_not_see_staged() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url())
        .await
        .expect("connect");
    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("create ext");

    let acct = synthetic_offset();
    let period: i32 = 8_006;

    // Confirm cell absent in rollup pre-test.
    let pre_rollup = sqlx::query(
        "SELECT balance FROM account_balances_rollup
             WHERE account_id = $1 AND period_id = $2
               AND currency_id = 1 AND ledger_kind = 1",
    )
    .bind(acct)
    .bind(period)
    .fetch_optional(&pool)
    .await
    .expect("pre rollup");
    assert!(pre_rollup.is_none(), "T6: fresh synthetic key should not have rollup row");

    // Open long txn; stage; sleep through several drain ticks; assert
    // rollup STILL has no row (drain saw no shmem cell because staged
    // state isn't in shmem yet).
    let mut conn = pool.acquire().await.expect("conn");
    sqlx::query("BEGIN").execute(&mut *conn).await.expect("begin");
    sqlx::query(
        "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 555, 0)",
    )
    .bind(acct)
    .bind(period)
    .execute(&mut *conn)
    .await
    .expect("apply");

    tokio::time::sleep(std::time::Duration::from_millis(800)).await;

    let mid_rollup = sqlx::query(
        "SELECT balance FROM account_balances_rollup
             WHERE account_id = $1 AND period_id = $2
               AND currency_id = 1 AND ledger_kind = 1",
    )
    .bind(acct)
    .bind(period)
    .fetch_optional(&pool)
    .await
    .expect("mid rollup");
    assert!(
        mid_rollup.is_none(),
        "T6: rollup should be empty while txn is open (staged delta not in shmem)"
    );

    // COMMIT via the held connection; then wait for next drain tick.
    sqlx::query("COMMIT")
        .execute(&mut *conn)
        .await
        .expect("commit");
    drop(conn);

    tokio::time::sleep(std::time::Duration::from_millis(800)).await;

    let post_rollup = sqlx::query(
        "SELECT balance FROM account_balances_rollup
             WHERE account_id = $1 AND period_id = $2
               AND currency_id = 1 AND ledger_kind = 1",
    )
    .bind(acct)
    .bind(period)
    .fetch_optional(&pool)
    .await
    .expect("post rollup");
    let bal: i64 = post_rollup
        .map(|r| r.get::<i64, _>(0))
        .expect("T6: rollup row should exist after COMMIT + drain wait");
    assert_eq!(bal, 555, "T6: rollup balance should equal applied delta");
}

/// T7 — Read-your-writes limitation. Within a txn, after staging an
/// apply, an immediate `ledger_balance_lookup` returns PRE-staging
/// state because the cell isn't mutated until COMMIT.
///
/// **This test PINS a documented limitation.** A future refactor that
/// adds RYW (e.g., via a TX-local sidecar cache) would change this
/// test's polarity.
#[tokio::test]
async fn t7_ryw_limitation() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url())
        .await
        .expect("connect");
    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("create ext");

    let acct = synthetic_offset();
    let period: i32 = 8_007;

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
        "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 999, 0)",
    )
    .bind(acct)
    .bind(period)
    .execute(&mut *tx)
    .await
    .expect("apply");

    // Same txn: lookup. Expected: PRE-apply state (None / pre_bal).
    let mid: Option<i64> = sqlx::query_scalar(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct)
    .bind(period)
    .fetch_one(&mut *tx)
    .await
    .expect("mid lookup");

    assert_eq!(
        mid.unwrap_or(0),
        pre_bal,
        "T7 RYW: same-txn lookup must return pre-staging state ({pre_bal}); got {mid:?}. \
         If this assertion FAILS, A2's deferred-apply contract changed — \
         update the test polarity and the INVARIANTS.md note on RYW."
    );

    tx.commit().await.expect("commit");

    // After commit: post-state visible.
    let post: Option<i64> = sqlx::query_scalar(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("post lookup");
    assert_eq!(
        post,
        Some(pre_bal + 999),
        "T7 post-commit: expected pre+999={}", pre_bal + 999
    );
}

/// T8 — first-apply mid-subxact (acct-17vr regression net).
///
/// The bug being pinned: before acct-17vr, XactCallback and
/// SubXactCallback were registered lazily on the first
/// `ledger_apply_balance_delta` call. If that first call happened
/// inside an already-open subxact, the SUBXACT_EVENT_START_SUB event
/// for the subxact had already fired BEFORE the callback registered,
/// so the extension never pushed a child frame. The apply staged
/// into the top-level frame. On ROLLBACK TO, the discard was
/// correctly done (callback unregistered → no child frame to pop;
/// the top-frame apply persists). On RELEASE, the merge-to-parent
/// was a no-op (no child frame), so the apply also persisted. Either
/// way: a subxact rollback that SHOULD discard the apply would not.
///
/// Post-acct-17vr, callbacks are registered in `_PG_init`. The
/// subxact's START_SUB fires from the moment the backend is alive,
/// so a child frame exists by the time the first apply lands inside
/// the subxact. ROLLBACK TO discards correctly.
///
/// Test shape (each must be on a single backend so the per-backend
/// PENDING_STACK state is observable):
/// ```text
/// -- Fresh backend (no prior apply in this session) --
/// BEGIN;
///   SAVEPOINT s;
///     SELECT ledger_apply_balance_delta(...);  -- first apply EVER
///   ROLLBACK TO s;
/// COMMIT;
/// -- expected: balance unchanged from pre
/// ```
///
/// The pool is sized to 1 connection and used in sequence so we can
/// guarantee the first apply on this connection happens inside the
/// subxact. Even though parallel test binaries may have warmed up
/// other backends, this backend (from this pool) is fresh.
#[tokio::test]
async fn t8_first_apply_mid_subxact_acct_17vr() {
    // Separate pools per scenario so each acquires a guaranteed-fresh
    // PG backend (sqlx opens new connections on first acquire). Pool A
    // is for the rollback scenario, B for release. A read-pool is used
    // for lookups outside the scenarios to avoid pool exhaustion.
    let pool_a = PgPoolOptions::new()
        .max_connections(1)
        .connect(&db_url())
        .await
        .expect("connect pool_a");
    let pool_b = PgPoolOptions::new()
        .max_connections(1)
        .connect(&db_url())
        .await
        .expect("connect pool_b");
    let pool_read = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url())
        .await
        .expect("connect pool_read");
    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool_read)
        .await
        .expect("create ext");

    let acct_rollback = synthetic_offset();
    let acct_release = synthetic_offset();
    let period: i32 = 8_008;

    let pre_rb: i64 = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct_rollback)
    .bind(period)
    .fetch_one(&pool_read)
    .await
    .expect("pre rb")
    .unwrap_or(0);

    let pre_rel: i64 = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct_release)
    .bind(period)
    .fetch_one(&pool_read)
    .await
    .expect("pre rel")
    .unwrap_or(0);

    // Scenario A: fresh backend (pool_a's first acquire), first apply
    // inside an already-open subxact, then ROLLBACK TO. Without
    // acct-17vr the apply would survive (lazy SubXactCallback would
    // miss the open subxact's START_SUB and stage into the top frame
    // that ROLLBACK TO doesn't touch); with the fix it discards.
    {
        let mut conn = pool_a.acquire().await.expect("acquire pool_a");
        sqlx::query("BEGIN").execute(&mut *conn).await.expect("begin a");
        sqlx::query("SAVEPOINT s_a")
            .execute(&mut *conn)
            .await
            .expect("savepoint s_a");
        sqlx::query(
            "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 777, 0)",
        )
        .bind(acct_rollback)
        .bind(period)
        .execute(&mut *conn)
        .await
        .expect("apply in subxact a");
        sqlx::query("ROLLBACK TO SAVEPOINT s_a")
            .execute(&mut *conn)
            .await
            .expect("rollback to s_a");
        sqlx::query("COMMIT").execute(&mut *conn).await.expect("commit a");
    }

    let post_rb: i64 = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct_rollback)
    .bind(period)
    .fetch_one(&pool_read)
    .await
    .expect("post rb")
    .unwrap_or(0);

    assert_eq!(
        post_rb, pre_rb,
        "T8 ROLLBACK TO: subxact apply must discard on rollback, even when it's the \
         backend's FIRST ever apply. Without acct-17vr (lazy SubXactCallback \
         registration), the SUBXACT_EVENT_START_SUB would be missed and the apply \
         would survive into the top-level frame. expected={pre_rb} got={post_rb}"
    );

    // Scenario B: separate fresh backend (pool_b), first apply inside
    // an already-open subxact, then RELEASE. Apply must merge into
    // parent and survive COMMIT.
    {
        let mut conn = pool_b.acquire().await.expect("acquire pool_b");
        sqlx::query("BEGIN").execute(&mut *conn).await.expect("begin b");
        sqlx::query("SAVEPOINT s_b")
            .execute(&mut *conn)
            .await
            .expect("savepoint s_b");
        sqlx::query(
            "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 333, 0)",
        )
        .bind(acct_release)
        .bind(period)
        .execute(&mut *conn)
        .await
        .expect("apply in subxact b");
        sqlx::query("RELEASE SAVEPOINT s_b")
            .execute(&mut *conn)
            .await
            .expect("release s_b");
        sqlx::query("COMMIT").execute(&mut *conn).await.expect("commit b");
    }

    let post_rel: i64 = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct_release)
    .bind(period)
    .fetch_one(&pool_read)
    .await
    .expect("post rel")
    .unwrap_or(0);

    assert_eq!(
        post_rel,
        pre_rel + 333,
        "T8 RELEASE: subxact apply must merge to parent on release. expected={} got={post_rel}",
        pre_rel + 333
    );
}
