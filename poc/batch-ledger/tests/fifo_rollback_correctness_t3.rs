//! acct-fhq7 — FIFO arena bgworker race correctness.
//!
//! ## Scope
//!
//! Exercises the interaction surface between A2's xact_commit shadow
//! replay (per-cell EXCL) and the bgworker drain tick (also per-cell
//! EXCL, briefly). Three scenarios per acct-fhq7 R-BG1..R-BG3:
//!
//! - **R-BG1** — `SAVEPOINT s1; apply issue; ROLLBACK TO s1; COMMIT`.
//!   The issue's PendingDrainPush staged in the subxact shadow is
//!   discarded by `SubXactCallback::AbortSub`. After outer commit and
//!   force_drain, durable cost_layers must reflect *zero* effect from
//!   the rolled-back issue.
//! - **R-BG2** — earlier tx commits + bgworker drains; later concurrent
//!   tx aborts touching the same cell. Earlier tx's drained effect must
//!   survive intact in durable.
//! - **R-BG3** — stress: many concurrent xact_commit while bgworker is
//!   ticking. Both want per-cell EXCL; verify no deadlock, no errors,
//!   final drift = 0. Under A2 there is no abort/drain EXCL contention
//!   (xact_abort is shadow-discard only); the regression net is for
//!   future Approach B/E where xact_abort may take EXCL to wipe.
//!
//! Uses base prefix `5_300_000_000_000` (disjoint from t1's 4.5e12 and
//! t2's 4.9e12).

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
    let base = 5_300_000_000_000_i64
        + ((bytes[0] as i64) << 24
            | (bytes[1] as i64) << 16
            | (bytes[2] as i64) << 8
            | (bytes[3] as i64))
            .abs()
            % 100_000_000;
    (base, base + 1, base + 2)
}

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

// ─── R-BG1 ─────────────────────────────────────────────────────────────

/// **R-BG1** — Issue inside a SAVEPOINT that's rolled back before outer
/// commit. The PendingDrainPush staged in the subxact shadow must be
/// discarded by `SubXactCallback::AbortSub`. After outer commit and
/// force_drain, no depletion lands in durable.
///
/// **The "impossible if it already committed" framing in acct-fhq7's
/// spec**: a fully-committed issue's pending_drain push is already
/// durable-tracked via the cost_layer_depletions INSERT (inline on the
/// apply path). The interesting case is the SAVEPOINT shape — the
/// depletion INSERT itself rolls back (PG subxact), but the shadow
/// PendingDrainPush is the under-test invariant.
#[tokio::test]
async fn r_bg1_savepoint_rolled_back_issue_no_drain_effect() {
    let admin = admin_pool().await;
    let backend = backend_pool().await;
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

    // Baseline: receive qty=100 (committed via admin); force_drain so the
    // pending state is in cost_layers.qty_remaining.
    apply_committed(&admin, &receipt_envelope(pool_id, ap_id, 100, 1000)).await;
    force_drain(&admin).await;

    let baseline = recon_row(&admin, pool_id).await.expect("R-BG1 baseline");
    assert_eq!(baseline.0, 100, "R-BG1 setup: live=100");
    assert_eq!(baseline.3, Some(100), "R-BG1 setup: durable=100");

    // Outer txn: SAVEPOINT s1; issue qty=30; ROLLBACK TO s1; COMMIT.
    let mut tx = backend.begin().await.expect("begin");
    sqlx::query("SAVEPOINT s1")
        .execute(&mut *tx)
        .await
        .expect("savepoint");
    let issue = issue_envelope(pool_id, cogs_id, 30);
    sqlx::query("SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)")
        .bind(&issue)
        .fetch_all(&mut *tx)
        .await
        .expect("issue inside savepoint");
    sqlx::query("ROLLBACK TO SAVEPOINT s1")
        .execute(&mut *tx)
        .await
        .expect("rollback to s1");
    tx.commit().await.expect("outer commit");

    // Force a bgworker tick. If any rogue pending_drain entry survived
    // the SubXactCallback::AbortSub, this would push it to durable.
    force_drain(&admin).await;

    let final_durable = durable_qty(&admin, pool_id).await;
    assert_eq!(
        final_durable, 100,
        "R-BG1: durable still = 100 (issue's PendingDrainPush discarded by subxact abort)"
    );
    let deps = depletion_count(&admin, pool_id).await;
    assert_eq!(
        deps, 0,
        "R-BG1: zero depletions in durable (PG subxact rolled back the inline INSERT)"
    );

    let recon = recon_row(&admin, pool_id).await.expect("R-BG1 final");
    let (live, pending, _total, durable, drift) = recon;
    assert_eq!(live, 100, "R-BG1: shmem_live = 100 (issue never landed)");
    assert_eq!(pending, 0, "R-BG1: no rogue pending_drain entry");
    assert_eq!(durable, Some(100), "R-BG1: durable = 100");
    assert_eq!(drift, Some(0), "R-BG1: drift = 0");

    cleanup(&admin, &[pool_id, ap_id, cogs_id]).await;
}

// ─── R-BG2 ─────────────────────────────────────────────────────────────

/// **R-BG2** — Earlier tx commits an issue + bgworker drains it; later
/// concurrent tx touches the same cell and aborts. The earlier tx's
/// drained effect must survive unaffected.
///
/// **Under A2**: B's xact_abort discards B's per-backend shadow. The
/// real cell — already drained by the bgworker — is untouched. Under
/// hypothetical Approach B/E: a naive wipe-on-abort would clear the
/// already-drained state alongside B's contribution; lazy-reseed from
/// durable would recover. This test pins the durable invariant.
#[tokio::test]
async fn r_bg2_abort_after_prior_drain_preserves_durable() {
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

    // Earlier tx (via A): receive 200 + issue 80, both committed in same
    // batch. After commit, A's shadow replays into real cell —
    // pending_drain has the issue entry.
    let earlier = serde_json::json!([
        {
            "envelope_idx": 0,
            "kind": "fifo_receipt",
            "debit_account_id": pool_id,
            "credit_account_id": ap_id,
            "qty": 200,
            "unit_cost": 1000,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        },
        {
            "envelope_idx": 1,
            "kind": "fifo_issue",
            "debit_account_id": cogs_id,
            "credit_account_id": pool_id,
            "qty": 80,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        }
    ]);
    sqlx::query("SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)")
        .bind(&earlier)
        .fetch_all(&a)
        .await
        .expect("earlier batch");

    // Bgworker drain → durable cost_layers.qty_remaining = 200 - 80 = 120.
    force_drain(&admin).await;
    let mid = recon_row(&admin, pool_id).await.expect("R-BG2 mid");
    assert_eq!(mid.0, 120, "R-BG2 mid: live=120 post-drain");
    assert_eq!(mid.1, 0, "R-BG2 mid: pending=0 post-drain");
    assert_eq!(mid.3, Some(120), "R-BG2 mid: durable=120");

    // Later tx (via B): receive qty=50 (in flight, then aborted). Same
    // pool → same cell. B's apply stages a Push in B's shadow.
    let mut tx = b.begin().await.expect("begin B");
    let later = receipt_envelope(pool_id, ap_id, 50, 1100);
    sqlx::query("SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)")
        .bind(&later)
        .fetch_all(&mut *tx)
        .await
        .expect("later apply");
    tx.rollback().await.expect("B rollback");

    // Force drain (would flush any rogue pending_drain entries).
    force_drain(&admin).await;

    // Durable must still reflect only the earlier committed effect.
    let dq = durable_qty(&admin, pool_id).await;
    assert_eq!(dq, 120, "R-BG2: durable still = 120 (B's rollback no-op on durable)");
    let deps = depletion_count(&admin, pool_id).await;
    assert_eq!(deps, 1, "R-BG2: exactly one depletion (A's earlier 80)");

    let recon = recon_row(&admin, pool_id).await.expect("R-BG2 final");
    let (live, pending, _total, durable, drift) = recon;
    assert_eq!(live, 120, "R-BG2 final: live=120 (B's Push discarded)");
    assert_eq!(pending, 0, "R-BG2 final: no rogue pending");
    assert_eq!(durable, Some(120), "R-BG2 final: durable=120");
    assert_eq!(drift, Some(0), "R-BG2 final: drift=0");

    cleanup(&admin, &[pool_id, ap_id, cogs_id]).await;
}

// ─── R-BG3 ─────────────────────────────────────────────────────────────

/// **R-BG3** — Stress test: many concurrent xact_commit while bgworker
/// ticks. xact_commit takes per-cell EXCL during shadow replay; bgworker
/// also takes per-cell EXCL during drain. Verify no deadlock, no errors,
/// final drift = 0.
///
/// Under A2: contention here is purely between xact_commit and bgworker
/// (both EXCL, single-cell). PG's LWLock guarantees deadlock-freedom
/// for single-lock-each operations. Bgworker ticks are brief; commits
/// queue.
///
/// Under hypothetical Approach B/E: xact_abort would also contend
/// (taking EXCL to mark needs_repair). This stress test would expose
/// any deadlock cycle in a future B/E implementation.
///
/// **Workload**: 12 backends, each performs 25 mixed receipt/issue+commit
/// cycles. A driver task interleaves `fifo_force_drain_tick()` calls
/// every ~5ms via the admin pool. Total ~300 commits + ~60 drain ticks
/// over ~3s wall-clock.
#[tokio::test]
async fn r_bg3_concurrent_commit_and_drain_no_deadlock() {
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

    // Pre-seed so issues never exhaust.
    apply_committed(&admin, &receipt_envelope(pool_id, ap_id, 100_000, 1000)).await;
    force_drain(&admin).await;

    let n_backends = 12usize;
    let n_ops_per = 25usize;
    let stop_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let start_barrier = Arc::new(Barrier::new(n_backends + 1));

    // Drain-ticker task: periodic force_drain while commits happen.
    let admin_for_drain = admin.clone();
    let stop_for_drain = stop_flag.clone();
    let start_for_drain = start_barrier.clone();
    let drain_task = tokio::spawn(async move {
        start_for_drain.wait().await;
        let mut ticks = 0u64;
        while !stop_for_drain.load(std::sync::atomic::Ordering::Acquire) {
            let _ = sqlx::query("SELECT fifo_force_drain_tick()")
                .execute(&admin_for_drain)
                .await;
            ticks += 1;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        ticks
    });

    // Backend tasks.
    let mut handles = Vec::with_capacity(n_backends);
    for backend_idx in 0..n_backends {
        let b = backend_pool().await;
        let bar = start_barrier.clone();
        handles.push(tokio::spawn(async move {
            bar.wait().await;
            let mut seed: u64 = 0xC2B2AE3D27D4EB4Fu64
                .wrapping_mul(1 + backend_idx as u64)
                .wrapping_add(0x85EBCA77C2B2AE63);
            let mut rng = || {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                seed
            };
            let mut count = 0u64;
            for _ in 0..n_ops_per {
                let envs = if rng() % 2 == 0 {
                    receipt_envelope(pool_id, ap_id, 10, 1000)
                } else {
                    issue_envelope(pool_id, cogs_id, 3)
                };
                let mut tx = b.begin().await.expect("begin");
                sqlx::query("SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)")
                    .bind(&envs)
                    .fetch_all(&mut *tx)
                    .await
                    .expect("apply");
                tx.commit().await.expect("commit");
                count += 1;
            }
            count
        }));
    }

    let mut total_commits = 0u64;
    for h in handles {
        total_commits += h.await.expect("backend task");
    }

    stop_flag.store(true, std::sync::atomic::Ordering::Release);
    let total_drain_ticks = drain_task.await.expect("drain task");

    // Final settle + drain.
    tokio::time::sleep(Duration::from_millis(200)).await;
    force_drain(&admin).await;

    let recon = recon_row(&admin, pool_id).await.expect("R-BG3 final");
    let (_live, _pending, _total, _durable, drift) = recon;
    assert_eq!(
        drift,
        Some(0),
        "R-BG3 ({} commits + {} drain ticks): drift = 0; no deadlock, no lost updates",
        total_commits,
        total_drain_ticks,
    );
    assert!(
        total_commits as usize == n_backends * n_ops_per,
        "R-BG3: all {} commits succeeded",
        n_backends * n_ops_per
    );
    assert!(
        total_drain_ticks >= 1,
        "R-BG3: drain ticker actually ran at least once"
    );

    cleanup(&admin, &[pool_id, ap_id, cogs_id]).await;
}
