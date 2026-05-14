//! acct-fhq7 — Multi-backend FIFO rollback correctness.
//!
//! ## Scope
//!
//! Exercises A2 shadow's per-backend isolation against concurrent backends
//! with mixed commit/rollback outcomes. Each "backend" is a distinct
//! `PgPool` with `max_connections=1`, giving each test task its own
//! dedicated PG session (so xact_callbacks fire in independent backends).
//!
//! Five scenarios per acct-fhq7's R-MB1..R-MB5:
//!
//! - **R-MB1** — 2 backends, both receipts on the same pool; A commits,
//!   B aborts. Final shmem reflects only A. No orphan depletion in durable.
//! - **R-MB2** — 2 backends, baseline committed; A receives (aborts),
//!   B issues against the baseline (commits). B's view of the pool must
//!   not be corrupted by A's in-flight shadow.
//! - **R-MB3** — 4 backends concurrent issues against a single committed
//!   layer; all commit; bgworker drain. Pool-level residual must match
//!   `original − Σ(depletions)`.
//! - **R-MB4** — 8 backends, mixed receipt+issue with random 10% rollback
//!   over a bounded number of iterations. Post-stop recon drift = 0.
//! - **R-MB5** — 2 backends, same cell; A commits a receipt + issue
//!   (pending_drain entry lands in shmem), B aborts a touch on the same
//!   cell. Verifies B's abort cannot wipe A's committed entries.
//!
//! ## Coordination
//!
//! `tokio::sync::Barrier` synchronizes phase transitions across spawned
//! tasks. `oneshot` channels gate commit/rollback ordering for the
//! sequenced scenarios (R-MB1, R-MB2, R-MB5).
//!
//! ## Polarity for the regression net
//!
//! All 5 cases MUST pass against the current A2 implementation (commit
//! `dff20bd`). If acct-fhq7's eventual decision swaps to Approach E
//! (recon-triggered repair), the same assertions must still hold after
//! force_drain + a recon call.
//!
//! Uses base prefix `4_900_000_000_000` (disjoint from t1's `4.5e12`).

use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;
use uuid::Uuid;

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

fn db_url() -> String {
    std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string())
}

fn unique_accounts() -> (i64, i64, i64) {
    let u = Uuid::new_v4();
    let bytes = u.as_bytes();
    // Distinct base from t1 rollback (4.5e12). t2 multi-backend claims 4.9e12.
    let base = 4_900_000_000_000_i64
        + ((bytes[0] as i64) << 24
            | (bytes[1] as i64) << 16
            | (bytes[2] as i64) << 8
            | (bytes[3] as i64))
            .abs()
            % 100_000_000;
    (base, base + 1, base + 2)
}

/// One PgPool per "backend" — `max_connections=1` so the pool yields a
/// dedicated PG session per task. Multiple pools = multiple backends.
async fn backend_pool() -> sqlx::PgPool {
    let p = PgPoolOptions::new()
        .max_connections(1)
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

/// Shared admin pool for seed/cleanup/recon outside the multi-backend
/// dance. Larger max_connections — not constrained to one session.
async fn admin_pool() -> sqlx::PgPool {
    let p = PgPoolOptions::new()
        .max_connections(4)
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

async fn layer_count(p: &sqlx::PgPool, pool_id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM cost_layers WHERE pool_account_id = $1::bigint",
    )
    .bind(pool_id)
    .fetch_one(p)
    .await
    .expect("layer count")
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

async fn apply_in_tx<'a>(
    p: &'a sqlx::PgPool,
    envelopes: &serde_json::Value,
) -> sqlx::Transaction<'a, sqlx::Postgres> {
    let mut tx = p.begin().await.expect("begin tx");
    sqlx::query("SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)")
        .bind(envelopes)
        .fetch_all(&mut *tx)
        .await
        .expect("apply in tx");
    tx
}

async fn apply_committed(p: &sqlx::PgPool, envelopes: &serde_json::Value) {
    sqlx::query("SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)")
        .bind(envelopes)
        .fetch_all(p)
        .await
        .expect("apply committed");
}

// ─── R-MB1 ─────────────────────────────────────────────────────────────

/// **R-MB1** — Two backends, same pool, both fifo_receipt.
/// A commits qty=100. B aborts qty=50. Final shmem must reflect only A's
/// contribution; durable must have only A's cost_layers row; no orphan
/// depletions exist.
///
/// **Per-backend shadow under A2**: A's apply stages a `LayerOp::Push(100)`
/// in A's `FIFO_PENDING_STACK`. B's apply stages `LayerOp::Push(50)` in
/// B's `FIFO_PENDING_STACK`. Shadows are thread-local — they do not see
/// each other. On commit/abort, each backend's shadow is independently
/// replayed or discarded.
#[tokio::test]
async fn r_mb1_two_backends_one_commits_one_aborts() {
    let admin = admin_pool().await;
    let a = backend_pool().await;
    let b = backend_pool().await;
    let (pool_id, ap_id, cogs_id) = unique_accounts();
    seed_accounts(
        &admin,
        &[
            (pool_id, "fifo_pool", "inv_value_raw"),
            (ap_id, "fifo_ap", "credit_normal"),
            (cogs_id, "fifo_cogs", "debit_normal"),
        ],
    )
    .await;

    // Coordination: barrier1 — both txns have applied; barrier2 — A has
    // committed; barrier3 — B has aborted.
    let barrier1 = Arc::new(Barrier::new(2));
    let barrier2 = Arc::new(Barrier::new(2));
    let barrier3 = Arc::new(Barrier::new(2));

    let pool_id_c = pool_id;
    let ap_id_c = ap_id;
    let b1a = barrier1.clone();
    let b2a = barrier2.clone();
    let b3a = barrier3.clone();
    let a_task = tokio::spawn(async move {
        let envs = receipt_envelope(pool_id_c, ap_id_c, 100, 1000);
        let tx = apply_in_tx(&a, &envs).await;
        b1a.wait().await; // both applied
        tx.commit().await.expect("A commit");
        b2a.wait().await; // A committed
        b3a.wait().await; // B aborted
    });

    let b1b = barrier1.clone();
    let b2b = barrier2.clone();
    let b3b = barrier3.clone();
    let b_task = tokio::spawn(async move {
        let envs = receipt_envelope(pool_id, ap_id, 50, 1200);
        let tx = apply_in_tx(&b, &envs).await;
        b1b.wait().await; // both applied
        b2b.wait().await; // A committed
        tx.rollback().await.expect("B rollback");
        b3b.wait().await; // B aborted
    });

    a_task.await.expect("A task");
    b_task.await.expect("B task");

    // Verify durable: 1 layer with qty=100 (A's), zero depletions.
    let layers = layer_count(&admin, pool_id).await;
    assert_eq!(layers, 1, "R-MB1: exactly one durable layer (A's commit)");
    let dq = durable_qty(&admin, pool_id).await;
    assert_eq!(dq, 100, "R-MB1: durable qty = 100 (A's contribution)");
    let deps = depletion_count(&admin, pool_id).await;
    assert_eq!(deps, 0, "R-MB1: no depletions (receipts only)");

    // Verify shmem reflects A's commit only.
    let recon = recon_row(&admin, pool_id).await.expect("R-MB1 cell present");
    let (live, pending, _total, durable, drift) = recon;
    assert_eq!(live, 100, "R-MB1: shmem_live = 100 (A's Push replayed; B's discarded)");
    assert_eq!(pending, 0, "R-MB1: no pending (receipts only)");
    assert_eq!(durable, Some(100), "R-MB1: durable_qty = 100");
    assert_eq!(drift, Some(0), "R-MB1: drift = 0");

    cleanup(&admin, &[pool_id, ap_id, cogs_id]).await;
}

// ─── R-MB2 ─────────────────────────────────────────────────────────────

/// **R-MB2** — Two backends with one pre-committed baseline layer.
/// A receives qty=50 (aborts); B issues qty=70 against the baseline
/// (commits). B's view of the pool must reflect only the durable baseline,
/// not A's in-flight uncommitted layer.
///
/// **MVCC + A2 invariant**: B's snapshot of `cost_layers` does not include
/// A's uncommitted INSERT (PG's standard read-committed semantics). A's
/// `LayerOp::Push` lives only in A's shadow; B's Phase 6 lazy-seed only
/// reads committed cost_layers rows; B's Phase 8 walks its own shadow
/// (which contains only its consume against baseline Y). After A's
/// abort and B's commit, the real ring reflects baseline_qty − B's
/// consume. Durable matches.
#[tokio::test]
async fn r_mb2_concurrent_issue_against_baseline_other_backend_aborts_receipt() {
    let admin = admin_pool().await;
    let a = backend_pool().await;
    let b = backend_pool().await;
    let (pool_id, ap_id, cogs_id) = unique_accounts();
    seed_accounts(
        &admin,
        &[
            (pool_id, "fifo_pool", "inv_value_raw"),
            (ap_id, "fifo_ap", "credit_normal"),
            (cogs_id, "fifo_cogs", "debit_normal"),
        ],
    )
    .await;

    // Baseline: committed receipt qty=100 via admin pool.
    apply_committed(&admin, &receipt_envelope(pool_id, ap_id, 100, 1000)).await;
    force_drain(&admin).await;
    let snap = recon_row(&admin, pool_id).await.expect("R-MB2 baseline");
    assert_eq!(snap.0, 100, "R-MB2 setup: baseline live = 100");
    assert_eq!(snap.3, Some(100), "R-MB2 setup: durable = 100");

    let barrier1 = Arc::new(Barrier::new(2));
    let barrier2 = Arc::new(Barrier::new(2));

    let pool_id_c = pool_id;
    let ap_id_c = ap_id;
    let b1a = barrier1.clone();
    let b2a = barrier2.clone();
    let a_task = tokio::spawn(async move {
        // A: receive qty=50, then abort.
        let envs = receipt_envelope(pool_id_c, ap_id_c, 50, 1200);
        let tx = apply_in_tx(&a, &envs).await;
        b1a.wait().await; // both applied
        tx.rollback().await.expect("A rollback");
        b2a.wait().await; // both done
    });

    let pool_id_c2 = pool_id;
    let cogs_id_c = cogs_id;
    let b1b = barrier1.clone();
    let b2b = barrier2.clone();
    let b_task = tokio::spawn(async move {
        // B: issue qty=70 against the committed baseline (qty=100).
        let envs = issue_envelope(pool_id_c2, cogs_id_c, 70);
        let tx = apply_in_tx(&b, &envs).await;
        b1b.wait().await; // both applied
        tx.commit().await.expect("B commit");
        b2b.wait().await; // both done
    });

    a_task.await.expect("A task");
    b_task.await.expect("B task");

    force_drain(&admin).await;

    // Durable: exactly one layer (the baseline), qty_remaining=30.
    let layers = layer_count(&admin, pool_id).await;
    assert_eq!(layers, 1, "R-MB2: only the committed baseline survives");
    let dq = durable_qty(&admin, pool_id).await;
    assert_eq!(dq, 30, "R-MB2: baseline 100 - B's 70 = 30");
    let deps = depletion_count(&admin, pool_id).await;
    assert_eq!(deps, 1, "R-MB2: exactly one depletion (B's issue against baseline)");

    let recon = recon_row(&admin, pool_id).await.expect("R-MB2 final cell");
    let (live, _pending, _total, durable, drift) = recon;
    assert_eq!(live, 30, "R-MB2: shmem_live = 30 post-drain");
    assert_eq!(durable, Some(30), "R-MB2: durable_qty = 30");
    assert_eq!(drift, Some(0), "R-MB2: drift = 0");

    cleanup(&admin, &[pool_id, ap_id, cogs_id]).await;
}

// ─── R-MB3 ─────────────────────────────────────────────────────────────

/// **R-MB3** — Four backends concurrent issues against a single committed
/// baseline. All commit. Pool-level residual = original − Σ depletions.
///
/// **Goal**: confirm that 4 concurrent shadows replaying against the same
/// cell EXCL serializer don't lose updates, double-apply, or deadlock.
/// `xact_commit` sorts cell indices before EXCL acquisition; with all
/// four touching the same cell, serialization is trivial.
#[tokio::test]
async fn r_mb3_four_backend_concurrent_issues_all_commit() {
    let admin = admin_pool().await;
    let (pool_id, ap_id, cogs_id) = unique_accounts();
    seed_accounts(
        &admin,
        &[
            (pool_id, "fifo_pool", "inv_value_raw"),
            (ap_id, "fifo_ap", "credit_normal"),
            (cogs_id, "fifo_cogs", "debit_normal"),
        ],
    )
    .await;

    // Baseline: receive qty=1000 (committed).
    apply_committed(&admin, &receipt_envelope(pool_id, ap_id, 1000, 1000)).await;
    force_drain(&admin).await;

    // Four backends, each issues qty=100, then commits. Barrier
    // synchronizes the start so they truly overlap.
    let n_backends = 4usize;
    let issue_qty = 100i64;
    let barrier_start = Arc::new(Barrier::new(n_backends));

    let mut handles = Vec::with_capacity(n_backends);
    for _ in 0..n_backends {
        let b = backend_pool().await;
        let bar = barrier_start.clone();
        handles.push(tokio::spawn(async move {
            let envs = issue_envelope(pool_id, cogs_id, issue_qty);
            // Begin tx FIRST (acquire connection), then sync, then apply.
            let mut tx = b.begin().await.expect("begin tx");
            bar.wait().await;
            sqlx::query("SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)")
                .bind(&envs)
                .fetch_all(&mut *tx)
                .await
                .expect("apply");
            tx.commit().await.expect("commit");
        }));
    }

    for h in handles {
        h.await.expect("backend task");
    }

    force_drain(&admin).await;

    // 4 * 100 = 400 issued; baseline 1000; residual 600.
    let dq = durable_qty(&admin, pool_id).await;
    assert_eq!(
        dq, 600,
        "R-MB3: baseline 1000 − 4×100 = 600 (got {dq})"
    );
    let deps = depletion_count(&admin, pool_id).await;
    assert_eq!(
        deps,
        n_backends as i64,
        "R-MB3: exactly {n_backends} depletions (one per backend)"
    );

    let recon = recon_row(&admin, pool_id).await.expect("R-MB3 cell");
    let (live, _pending, _total, durable, drift) = recon;
    assert_eq!(live, 600, "R-MB3: shmem_live = 600");
    assert_eq!(durable, Some(600), "R-MB3: durable_qty = 600");
    assert_eq!(drift, Some(0), "R-MB3: drift = 0 (all backends replayed cleanly)");

    cleanup(&admin, &[pool_id, ap_id, cogs_id]).await;
}

// ─── R-MB4 ─────────────────────────────────────────────────────────────

/// **R-MB4** — 8 backends, bounded mixed receipt/issue load with random
/// ~10% rollback rate. Post-stop recon drift = 0.
///
/// Bounded by iteration count (not 60s wall-clock) to keep the suite
/// fast; the regression net here is *correctness* under concurrent
/// abort, not perf characterization (perf lives in
/// `bench_fifo_rollback_inject.rs`).
///
/// **Workload**: 8 backends × 20 ops each = 160 ops. Each op is 50/50
/// receipt/issue; 10% chance of rollback. Receipts qty=20 (so issues
/// never exhaust the pool). Single shared pool — maximizes concurrent
/// cell EXCL contention at commit time.
#[tokio::test]
async fn r_mb4_eight_backend_mixed_with_random_rollback() {
    let admin = admin_pool().await;
    let (pool_id, ap_id, cogs_id) = unique_accounts();
    seed_accounts(
        &admin,
        &[
            (pool_id, "fifo_pool", "inv_value_raw"),
            (ap_id, "fifo_ap", "credit_normal"),
            (cogs_id, "fifo_cogs", "debit_normal"),
        ],
    )
    .await;

    // Pre-seed with a big committed receipt so issues never exhaust.
    apply_committed(&admin, &receipt_envelope(pool_id, ap_id, 100_000, 1000)).await;
    force_drain(&admin).await;

    let n_backends = 8usize;
    let n_ops_per = 20usize;

    let mut handles = Vec::with_capacity(n_backends);
    for backend_idx in 0..n_backends {
        let b = backend_pool().await;
        handles.push(tokio::spawn(async move {
            // Deterministic per-task PRNG (linear congruential). Independent
            // streams per backend; reproducible if a future failure pattern
            // is observed.
            let mut seed: u64 = 0x9E3779B97F4A7C15u64
                .wrapping_mul(1 + backend_idx as u64)
                .wrapping_add(0xBF58476D1CE4E5B9);
            let mut rng = || {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                seed
            };

            let mut committed = 0usize;
            let mut rolled = 0usize;
            for _ in 0..n_ops_per {
                let envs = if rng() % 2 == 0 {
                    receipt_envelope(pool_id, ap_id, 20, 1000)
                } else {
                    issue_envelope(pool_id, cogs_id, 5)
                };
                let tx = apply_in_tx(&b, &envs).await;
                if rng() % 10 == 0 {
                    tx.rollback().await.expect("rollback");
                    rolled += 1;
                } else {
                    tx.commit().await.expect("commit");
                    committed += 1;
                }
            }
            (backend_idx, committed, rolled)
        }));
    }

    let mut total_rolled = 0usize;
    let mut total_committed = 0usize;
    for h in handles {
        let (_, c, r) = h.await.expect("backend task");
        total_committed += c;
        total_rolled += r;
    }

    force_drain(&admin).await;
    // Brief settle window for any in-flight drain ticks.
    tokio::time::sleep(Duration::from_millis(200)).await;
    force_drain(&admin).await;

    let recon = recon_row(&admin, pool_id).await.expect("R-MB4 cell");
    let (_live, _pending, _total, _durable, drift) = recon;
    assert_eq!(
        drift,
        Some(0),
        "R-MB4 ({} committed, {} rolled across {} backends): drift = 0",
        total_committed,
        total_rolled,
        n_backends
    );

    cleanup(&admin, &[pool_id, ap_id, cogs_id]).await;
}

// ─── R-MB5 ─────────────────────────────────────────────────────────────

/// **R-MB5** — Same cell, concurrent backends. A commits a receipt+issue
/// pair (pending_drain entry lands in shmem). B aborts a receipt touching
/// the same cell. Asserts B's abort does NOT wipe A's pending_drain
/// entry from shmem.
///
/// **Under A2**: shadows are per-backend, so by construction B's abort
/// cannot affect A's committed contribution. This test pins the
/// regression net for the "wipe collateral" concern raised in
/// Approach B/E.
///
/// **Under Approach B/E (hypothetical)**: a naive wipe-on-abort would
/// flip the entire cell state to "needs reseed" when B aborts — A's
/// pending_drain entry would be wiped collaterally. A correct B/E
/// implementation must NOT exhibit this behavior. This test would catch
/// such a bug.
#[tokio::test]
async fn r_mb5_b_abort_does_not_wipe_a_pending_drain_entry() {
    let admin = admin_pool().await;
    let a = backend_pool().await;
    let b = backend_pool().await;
    let (pool_id, ap_id, cogs_id) = unique_accounts();
    seed_accounts(
        &admin,
        &[
            (pool_id, "fifo_pool", "inv_value_raw"),
            (ap_id, "fifo_ap", "credit_normal"),
            (cogs_id, "fifo_cogs", "debit_normal"),
        ],
    )
    .await;

    // Baseline: committed receipt qty=200. This allocates the cell so
    // both A and B will collide on the same cell index.
    apply_committed(&admin, &receipt_envelope(pool_id, ap_id, 200, 1000)).await;
    force_drain(&admin).await;
    let snap = recon_row(&admin, pool_id).await.expect("baseline");
    assert_eq!(snap.0, 200, "R-MB5 setup: live=200");
    assert_eq!(snap.1, 0, "R-MB5 setup: pending=0 post-drain");

    let barrier1 = Arc::new(Barrier::new(2));
    let barrier2 = Arc::new(Barrier::new(2));
    let barrier3 = Arc::new(Barrier::new(2));

    let pool_id_c = pool_id;
    let cogs_id_c = cogs_id;
    let b1a = barrier1.clone();
    let b2a = barrier2.clone();
    let b3a = barrier3.clone();
    let a_task = tokio::spawn(async move {
        // A: issue qty=50 (stages Consume + PendingDrainPush in A's
        // shadow), then commit. After commit, real cell has live=150 +
        // pending_drain entry for 50.
        let envs = issue_envelope(pool_id_c, cogs_id_c, 50);
        let tx = apply_in_tx(&a, &envs).await;
        b1a.wait().await; // both applied
        tx.commit().await.expect("A commit");
        b2a.wait().await; // A committed (real cell now has A's pending_drain)
        b3a.wait().await; // B aborted
    });

    let pool_id_c2 = pool_id;
    let ap_id_c = ap_id;
    let b1b = barrier1.clone();
    let b2b = barrier2.clone();
    let b3b = barrier3.clone();
    let b_task = tokio::spawn(async move {
        // B: receive qty=80 against the same cell, then abort.
        let envs = receipt_envelope(pool_id_c2, ap_id_c, 80, 1100);
        let tx = apply_in_tx(&b, &envs).await;
        b1b.wait().await; // both applied
        b2b.wait().await; // A committed
        tx.rollback().await.expect("B rollback");
        b3b.wait().await; // B aborted
    });

    a_task.await.expect("A task");
    b_task.await.expect("B task");

    // Post-condition BEFORE force_drain: A's pending_drain entry must
    // be present in shmem. shmem_live = 150 (baseline 200 − A's 50);
    // shmem_pending = 50 (A's PendingDrainPush).
    let recon_pre = recon_row(&admin, pool_id).await.expect("R-MB5 pre-drain");
    let (live_pre, pending_pre, _total_pre, _durable_pre, _drift_pre) = recon_pre;
    assert_eq!(
        live_pre, 150,
        "R-MB5: shmem_live = 150 (baseline 200 - A's consume 50). \
         If this fires, B's abort wiped A's Consume from the cell."
    );
    assert_eq!(
        pending_pre, 50,
        "R-MB5: shmem_pending = 50 (A's PendingDrainPush survives B's abort). \
         If this fires, B's abort wiped A's pending_drain entry — exactly \
         the wipe-collateral bug acct-fhq7 calls out for Approach B/E."
    );

    // Post-drain: pending_drain flushes into durable cost_layers; live
    // unchanged.
    force_drain(&admin).await;
    let recon_post = recon_row(&admin, pool_id).await.expect("R-MB5 post-drain");
    let (live_post, pending_post, _total_post, durable_post, drift_post) = recon_post;
    assert_eq!(live_post, 150, "R-MB5 post-drain: live=150");
    assert_eq!(pending_post, 0, "R-MB5 post-drain: pending flushed to 0");
    assert_eq!(durable_post, Some(150), "R-MB5 post-drain: durable=150");
    assert_eq!(drift_post, Some(0), "R-MB5 post-drain: drift=0");

    // Durable side: exactly the baseline layer (B's receipt rolled back);
    // exactly one depletion (A's issue committed).
    let layers = layer_count(&admin, pool_id).await;
    assert_eq!(layers, 1, "R-MB5: one durable layer (B's receipt rolled back)");
    let deps = depletion_count(&admin, pool_id).await;
    assert_eq!(deps, 1, "R-MB5: one depletion (A's issue committed)");

    cleanup(&admin, &[pool_id, ap_id, cogs_id]).await;
}
