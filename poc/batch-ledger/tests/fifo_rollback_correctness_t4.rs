//! acct-fhq7 — FIFO arena savepoint stress correctness.
//!
//! ## Scope
//!
//! Exercises A2's `SubXactCallback` against PostgreSQL's nested savepoint
//! semantics. Three scenarios per acct-fhq7 R-SP1..R-SP3:
//!
//! - **R-SP1** — deep nested savepoint stack: apply at each level,
//!   selectively `ROLLBACK TO` at one random level. The surviving qty
//!   in shmem and durable must exactly match the "prefix" rule:
//!   `ROLLBACK TO sN` discards subxacts N, N+1, ..., M (the tip).
//! - **R-SP2** — `RELEASE` vs `ROLLBACK TO` equivalence: a `SAVEPOINT
//!   s1; apply; RELEASE s1` must merge cleanly into the parent frame;
//!   a `SAVEPOINT s1; apply; ROLLBACK TO s1` must discard. Both end
//!   the outer txn in `COMMIT`. Verify both paths reach the expected
//!   state — neither leaks into the other's shadow frame.
//! - **R-SP3** — retry loop pattern: many iterations of
//!   `BEGIN; SAVEPOINT s; apply; (maybe ROLLBACK TO s + re-apply);
//!   COMMIT`. Verifies cumulative correctness over a high op count.
//!
//! Uses base prefix `5_700_000_000_000` (disjoint from t1's 4.5e12,
//! t2's 4.9e12, t3's 5.3e12).

use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use uuid::Uuid;

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

fn db_url() -> String {
    std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string())
}

fn unique_accounts() -> (i64, i64, i64) {
    let u = Uuid::new_v4();
    let bytes = u.as_bytes();
    let base = 5_700_000_000_000_i64
        + ((bytes[0] as i64) << 24
            | (bytes[1] as i64) << 16
            | (bytes[2] as i64) << 8
            | (bytes[3] as i64))
            .abs()
            % 100_000_000;
    (base, base + 1, base + 2)
}

async fn pool() -> sqlx::PgPool {
    let p = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(15))
        .connect(&db_url())
        .await
        .expect("connect");
    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&p)
        .await
        .expect("ext");
    p
}

async fn seed_accounts(p: &sqlx::PgPool, ids: &[(i64, &str, &str)]) {
    for (id, code, kind) in ids {
        sqlx::query(
            "INSERT INTO accounts (id, code, kind, currency) \
             VALUES ($1::bigint, $2 || '-' || $1::TEXT, $3::account_kind, 'USD') \
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(*id)
        .bind(*code)
        .bind(*kind)
        .execute(p)
        .await
        .expect("seed account");
    }
}

async fn cleanup(p: &sqlx::PgPool, ids: &[i64]) {
    let _ = sqlx::query(
        "DELETE FROM cost_layer_depletions \
         WHERE layer_id IN (SELECT id FROM cost_layers WHERE pool_account_id = ANY($1::bigint[]))",
    )
    .bind(ids)
    .execute(p)
    .await;
    let _ = sqlx::query("DELETE FROM cost_layers WHERE pool_account_id = ANY($1::bigint[])")
        .bind(ids)
        .execute(p)
        .await;
    for id in ids {
        let _ = sqlx::query(
            "DELETE FROM posting_lines WHERE debit_account_id = $1::bigint OR credit_account_id = $1::bigint",
        )
        .bind(*id)
        .execute(p)
        .await;
    }
    let _ = sqlx::query("DELETE FROM accounts WHERE id = ANY($1::bigint[])")
        .bind(ids)
        .execute(p)
        .await;
}

async fn recon_row(
    p: &sqlx::PgPool,
    pool_id: i64,
) -> Option<(i64, i64, i64, Option<i64>, Option<i64>)> {
    let row = sqlx::query(
        "SELECT shmem_live_qty, shmem_pending_qty, shmem_total_qty, durable_qty, drift \
         FROM fifo_arena_recon() \
         WHERE pool_account_id = $1::bigint",
    )
    .bind(pool_id)
    .fetch_optional(p)
    .await
    .expect("recon row");
    row.map(|r| {
        (
            r.get::<i64, _>("shmem_live_qty"),
            r.get::<i64, _>("shmem_pending_qty"),
            r.get::<i64, _>("shmem_total_qty"),
            r.get::<Option<i64>, _>("durable_qty"),
            r.get::<Option<i64>, _>("drift"),
        )
    })
}

async fn force_drain(p: &sqlx::PgPool) {
    sqlx::query("SELECT fifo_force_drain_tick()")
        .execute(p)
        .await
        .expect("force drain");
}

async fn durable_qty(p: &sqlx::PgPool, pool_id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT COALESCE(SUM(qty_remaining), 0)::bigint FROM cost_layers \
         WHERE pool_account_id = $1::bigint",
    )
    .bind(pool_id)
    .fetch_one(p)
    .await
    .expect("durable qty")
}

async fn depletion_count(p: &sqlx::PgPool, pool_id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM cost_layer_depletions d \
         WHERE d.layer_id IN (SELECT id FROM cost_layers WHERE pool_account_id = $1::bigint)",
    )
    .bind(pool_id)
    .fetch_one(p)
    .await
    .expect("depletion count")
}

fn receipt_envelope(pool_id: i64, ap_id: i64, qty: i64, unit_cost: i64) -> serde_json::Value {
    serde_json::json!([{
        "envelope_idx": 0,
        "kind": "fifo_receipt",
        "debit_account_id": pool_id,
        "credit_account_id": ap_id,
        "qty": qty,
        "unit_cost": unit_cost,
        "idempotency_key": Uuid::new_v4().to_string(),
        "business_date": "2026-05-14",
    }])
}

fn issue_envelope(pool_id: i64, cogs_id: i64, qty: i64) -> serde_json::Value {
    serde_json::json!([{
        "envelope_idx": 0,
        "kind": "fifo_issue",
        "debit_account_id": cogs_id,
        "credit_account_id": pool_id,
        "qty": qty,
        "idempotency_key": Uuid::new_v4().to_string(),
        "business_date": "2026-05-14",
    }])
}

async fn apply_committed(p: &sqlx::PgPool, envelopes: &serde_json::Value) {
    sqlx::query("SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)")
        .bind(envelopes)
        .fetch_all(p)
        .await
        .expect("apply committed");
}

// ─── R-SP1 ─────────────────────────────────────────────────────────────

/// **R-SP1** — Deep nested savepoint stack: 50 levels, apply qty=1
/// receipt at each, then `ROLLBACK TO sK` at a chosen level. Survivors
/// are the receipts staged in savepoints 1..K-1.
///
/// Under A2: each `SAVEPOINT sN` pushes a fresh frame to
/// `FIFO_PENDING_STACK`. Each apply stages a Push in the top (active)
/// frame. `ROLLBACK TO sK` pops frames K..tip and discards them; the
/// surviving frames 0..K-1 stay intact. Outer `COMMIT` then merges
/// them into the real cell.
///
/// 50 levels balances depth coverage against PG's per-subxact memory
/// overhead. (PG's hard limit is ~2^32 but 50 is plenty to surface any
/// SubXactCallback merge bugs.)
#[tokio::test]
async fn r_sp1_deep_savepoint_stack_partial_rollback() {
    let p = pool().await;
    let (pool_id, ap_id, cogs_id) = unique_accounts();
    seed_accounts(
        &p,
        &[
            (pool_id, "fifo_pool", "inv_value_raw"),
            (ap_id, "fifo_ap", "credit_normal"),
            (cogs_id, "fifo_cogs", "debit_normal"),
        ],
    )
    .await;

    let depth = 50usize;
    let rollback_at = 17usize; // ROLLBACK TO s17; survivors are s1..s16.
    let surviving_qty = (rollback_at - 1) as i64; // 16 receipts × qty=1.

    let mut tx = p.begin().await.expect("begin");
    for lvl in 1..=depth {
        sqlx::query(&format!("SAVEPOINT s{}", lvl))
            .execute(&mut *tx)
            .await
            .expect("savepoint");
        let envs = receipt_envelope(pool_id, ap_id, 1, 1000);
        sqlx::query("SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)")
            .bind(&envs)
            .fetch_all(&mut *tx)
            .await
            .expect("apply at level");
    }
    // ROLLBACK TO s17 — discards frames s17..s50. Survivors are s1..s16.
    sqlx::query(&format!("ROLLBACK TO SAVEPOINT s{}", rollback_at))
        .execute(&mut *tx)
        .await
        .expect("rollback to s17");
    tx.commit().await.expect("outer commit");
    force_drain(&p).await;

    let recon = recon_row(&p, pool_id).await.expect("R-SP1 cell");
    let (live, pending, _total, durable, drift) = recon;
    assert_eq!(
        live, surviving_qty,
        "R-SP1: shmem_live = {} (survivors: s1..s{}); got {}",
        surviving_qty,
        rollback_at - 1,
        live
    );
    assert_eq!(pending, 0, "R-SP1: no rogue pending (receipts only)");
    assert_eq!(
        durable,
        Some(surviving_qty),
        "R-SP1: durable_qty = {}",
        surviving_qty
    );
    assert_eq!(drift, Some(0), "R-SP1: drift = 0");

    let dq = durable_qty(&p, pool_id).await;
    assert_eq!(dq, surviving_qty, "R-SP1: durable sum matches");

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

// ─── R-SP2 ─────────────────────────────────────────────────────────────

/// **R-SP2** — `RELEASE SAVEPOINT` vs `ROLLBACK TO SAVEPOINT` semantic
/// equivalence pin.
///
/// Two pools (so the test runs are independent). Pool R: SAVEPOINT s1;
/// apply receipt qty=100; RELEASE s1; apply receipt qty=200; COMMIT.
/// Pool D: SAVEPOINT s1; apply receipt qty=100; ROLLBACK TO s1; apply
/// receipt qty=200; COMMIT.
///
/// Expected:
/// - Pool R: both receipts merged into parent on RELEASE / parent commit
///   — surviving qty = 300.
/// - Pool D: first receipt discarded; second receipt committed —
///   surviving qty = 200.
///
/// Under A2: `SubXactCallback::CommitSub` merges the child frame into
/// the parent's ops Vec (or HashMap-collapses by cell_idx). `AbortSub`
/// pops and discards. Both share the same outer `xact_commit` path.
#[tokio::test]
async fn r_sp2_release_vs_rollback_to_savepoint() {
    let p = pool().await;
    let (pool_r, ap_r, cogs_r) = unique_accounts();
    let (pool_d, ap_d, cogs_d) = unique_accounts();
    // Avoid base collision between R and D allocations.
    assert_ne!(pool_r, pool_d, "R-SP2 setup: independent pools required");
    seed_accounts(
        &p,
        &[
            (pool_r, "fifo_pool", "inv_value_raw"),
            (ap_r, "fifo_ap", "credit_normal"),
            (cogs_r, "fifo_cogs", "debit_normal"),
            (pool_d, "fifo_pool", "inv_value_raw"),
            (ap_d, "fifo_ap", "credit_normal"),
            (cogs_d, "fifo_cogs", "debit_normal"),
        ],
    )
    .await;

    // Pool R — RELEASE path.
    let mut tx = p.begin().await.expect("begin R");
    sqlx::query("SAVEPOINT s1").execute(&mut *tx).await.expect("savepoint");
    let envs_r1 = receipt_envelope(pool_r, ap_r, 100, 1000);
    sqlx::query("SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)")
        .bind(&envs_r1)
        .fetch_all(&mut *tx)
        .await
        .expect("apply R1");
    sqlx::query("RELEASE SAVEPOINT s1")
        .execute(&mut *tx)
        .await
        .expect("release s1");
    let envs_r2 = receipt_envelope(pool_r, ap_r, 200, 1000);
    sqlx::query("SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)")
        .bind(&envs_r2)
        .fetch_all(&mut *tx)
        .await
        .expect("apply R2");
    tx.commit().await.expect("commit R");

    // Pool D — ROLLBACK TO path.
    let mut tx = p.begin().await.expect("begin D");
    sqlx::query("SAVEPOINT s1").execute(&mut *tx).await.expect("savepoint");
    let envs_d1 = receipt_envelope(pool_d, ap_d, 100, 1000);
    sqlx::query("SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)")
        .bind(&envs_d1)
        .fetch_all(&mut *tx)
        .await
        .expect("apply D1");
    sqlx::query("ROLLBACK TO SAVEPOINT s1")
        .execute(&mut *tx)
        .await
        .expect("rollback to s1");
    let envs_d2 = receipt_envelope(pool_d, ap_d, 200, 1000);
    sqlx::query("SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)")
        .bind(&envs_d2)
        .fetch_all(&mut *tx)
        .await
        .expect("apply D2");
    tx.commit().await.expect("commit D");

    force_drain(&p).await;

    let r = recon_row(&p, pool_r).await.expect("R-SP2 R cell");
    assert_eq!(
        r.0, 300,
        "R-SP2 R (RELEASE merges both receipts): live=300 (got {})",
        r.0
    );
    assert_eq!(r.3, Some(300), "R-SP2 R: durable=300");
    assert_eq!(r.4, Some(0), "R-SP2 R: drift=0");

    let d = recon_row(&p, pool_d).await.expect("R-SP2 D cell");
    assert_eq!(
        d.0, 200,
        "R-SP2 D (ROLLBACK TO discards first, second commits): live=200 (got {})",
        d.0
    );
    assert_eq!(d.3, Some(200), "R-SP2 D: durable=200");
    assert_eq!(d.4, Some(0), "R-SP2 D: drift=0");

    cleanup(&p, &[pool_r, ap_r, cogs_r, pool_d, ap_d, cogs_d]).await;
}

// ─── R-SP3 ─────────────────────────────────────────────────────────────

/// **R-SP3** — Application-level retry-loop pattern. Many iterations of:
///
/// ```text
/// BEGIN;
///   SAVEPOINT s;
///   apply receipt qty=A;
///   if rng % 3 == 0:
///     ROLLBACK TO s;
///     apply receipt qty=B;  -- different "data"
///   COMMIT;
/// ```
///
/// Tracks expected committed total. At the end: durable + shmem match
/// expected, drift = 0, no lost or duplicate effects.
///
/// **Iteration count**: 2000. acct-fhq7's spec calls for 10K; bounded
/// here for suite hygiene (2K provides ~1300 RELEASE-path + ~700
/// rollback-then-reapply commits, plenty of subxact callback turns).
/// Bench harness (`bench_fifo_rollback_inject.rs`) will exercise the
/// 10K-iter regime at perf grain.
#[tokio::test]
async fn r_sp3_retry_loop_pattern_cumulative_correctness() {
    let p = pool().await;
    let (pool_id, ap_id, cogs_id) = unique_accounts();
    seed_accounts(
        &p,
        &[
            (pool_id, "fifo_pool", "inv_value_raw"),
            (ap_id, "fifo_ap", "credit_normal"),
            (cogs_id, "fifo_cogs", "debit_normal"),
        ],
    )
    .await;

    let n_iter = 2_000usize;
    let mut seed: u64 = 0xA5A5A5A5DEADBEEFu64;
    let mut rng = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        seed
    };

    let mut expected_qty: i64 = 0;
    let mut release_path = 0u64;
    let mut rollback_path = 0u64;

    for _ in 0..n_iter {
        let qty_a = 5i64;
        let qty_b = 7i64;
        let do_rollback = rng() % 3 == 0;

        let mut tx = p.begin().await.expect("begin iter");
        sqlx::query("SAVEPOINT s")
            .execute(&mut *tx)
            .await
            .expect("savepoint");
        let envs_a = receipt_envelope(pool_id, ap_id, qty_a, 1000);
        sqlx::query("SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)")
            .bind(&envs_a)
            .fetch_all(&mut *tx)
            .await
            .expect("apply A");
        if do_rollback {
            sqlx::query("ROLLBACK TO SAVEPOINT s")
                .execute(&mut *tx)
                .await
                .expect("rollback to s");
            let envs_b = receipt_envelope(pool_id, ap_id, qty_b, 1100);
            sqlx::query("SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)")
                .bind(&envs_b)
                .fetch_all(&mut *tx)
                .await
                .expect("apply B");
            expected_qty += qty_b;
            rollback_path += 1;
        } else {
            // Implicit RELEASE on outer commit.
            expected_qty += qty_a;
            release_path += 1;
        }
        tx.commit().await.expect("outer commit");
    }

    force_drain(&p).await;

    let recon = recon_row(&p, pool_id).await.expect("R-SP3 cell");
    let (live, pending, _total, durable, drift) = recon;
    // With 2000 receipts at qty=5/7 each, layer count far exceeds
    // MAX_LAYERS=256 → overflow_active sticky → receipts past the cap
    // spill to durable cost_layers directly. shmem_live then reflects
    // only the head ring's qty, while durable_qty is the canonical truth.
    assert!(
        live <= expected_qty,
        "R-SP3: shmem_live ({}) ≤ expected_qty ({}); ring overflow expected at this layer count",
        live,
        expected_qty
    );
    assert_eq!(pending, 0, "R-SP3: no pending (receipts only)");
    assert_eq!(
        durable,
        Some(expected_qty),
        "R-SP3: durable_qty = expected_qty {} ({} RELEASE, {} ROLLBACK)",
        expected_qty,
        release_path,
        rollback_path,
    );
    assert_eq!(drift, Some(0), "R-SP3: drift = 0 across {} iterations", n_iter);

    let deps = depletion_count(&p, pool_id).await;
    assert_eq!(deps, 0, "R-SP3: zero depletions (receipts only workload)");

    // Sanity: both paths exercised non-trivially.
    assert!(
        release_path >= n_iter as u64 / 4,
        "R-SP3 RNG: release path fired {} times (expected ≥{})",
        release_path,
        n_iter / 4
    );
    assert!(
        rollback_path >= n_iter as u64 / 6,
        "R-SP3 RNG: rollback path fired {} times (expected ≥{})",
        rollback_path,
        n_iter / 6
    );

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

// ─── R-SP3-extra ───────────────────────────────────────────────────────

/// **R-SP3-mixed** — Retry-loop variant with mixed receipt/issue ops
/// to additionally exercise pending_drain push/discard across
/// SubXactCallback semantics. Baseline-seeded pool so issues always
/// succeed.
///
/// Same shape as R-SP3 but iterations alternate receipt/issue, and the
/// `ROLLBACK TO + reapply different data` path swaps op kind.
#[tokio::test]
async fn r_sp3_mixed_receipt_issue_retry_loop() {
    let p = pool().await;
    let (pool_id, ap_id, cogs_id) = unique_accounts();
    seed_accounts(
        &p,
        &[
            (pool_id, "fifo_pool", "inv_value_raw"),
            (ap_id, "fifo_ap", "credit_normal"),
            (cogs_id, "fifo_cogs", "debit_normal"),
        ],
    )
    .await;

    // Pre-seed so issues never exhaust.
    apply_committed(&p, &receipt_envelope(pool_id, ap_id, 1_000_000, 1000)).await;
    force_drain(&p).await;
    let mut expected_qty: i64 = 1_000_000;

    let n_iter = 500usize;
    let mut seed: u64 = 0x7F4A7C159E3779B9u64;
    let mut rng = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        seed
    };

    for i in 0..n_iter {
        let prefer_receipt = i % 2 == 0;
        let do_rollback = rng() % 3 == 0;

        let mut tx = p.begin().await.expect("begin iter");
        sqlx::query("SAVEPOINT s")
            .execute(&mut *tx)
            .await
            .expect("savepoint");

        // First op (A).
        let (envs_a, delta_a): (serde_json::Value, i64) = if prefer_receipt {
            (receipt_envelope(pool_id, ap_id, 10, 1000), 10)
        } else {
            (issue_envelope(pool_id, cogs_id, 3), -3)
        };
        sqlx::query("SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)")
            .bind(&envs_a)
            .fetch_all(&mut *tx)
            .await
            .expect("apply A");

        if do_rollback {
            sqlx::query("ROLLBACK TO SAVEPOINT s")
                .execute(&mut *tx)
                .await
                .expect("rollback to s");
            // Reapply different op (swap kind).
            let (envs_b, delta_b): (serde_json::Value, i64) = if prefer_receipt {
                (issue_envelope(pool_id, cogs_id, 4), -4)
            } else {
                (receipt_envelope(pool_id, ap_id, 8, 1200), 8)
            };
            sqlx::query("SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)")
                .bind(&envs_b)
                .fetch_all(&mut *tx)
                .await
                .expect("apply B");
            expected_qty += delta_b;
        } else {
            expected_qty += delta_a;
        }
        tx.commit().await.expect("commit iter");
    }

    force_drain(&p).await;

    let recon = recon_row(&p, pool_id).await.expect("R-SP3-mixed cell");
    let (live, pending, _total, durable, drift) = recon;
    // Layer count = pre-seed(1) + ~250 receipt commits ≈ 251. Bound on
    // MAX_LAYERS=256 is tight; depending on RNG drift the ring may or
    // may not overflow. Canonical assertions remain durable + drift.
    assert!(
        live <= expected_qty,
        "R-SP3-mixed: shmem_live ({}) ≤ expected_qty ({})",
        live,
        expected_qty
    );
    assert_eq!(pending, 0, "R-SP3-mixed: post-drain pending=0");
    assert_eq!(
        durable,
        Some(expected_qty),
        "R-SP3-mixed: durable_qty = expected_qty {} (got {:?})",
        expected_qty,
        durable
    );
    assert_eq!(drift, Some(0), "R-SP3-mixed: drift = 0");

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}
