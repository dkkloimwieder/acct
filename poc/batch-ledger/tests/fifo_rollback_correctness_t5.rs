//! acct-fhq7 — FIFO arena crash recovery correctness.
//!
//! ## Scope
//!
//! Exercises the recovery surface that a real production PG crash
//! exposes: `docker restart` (SIGTERM → fast shutdown when graceful;
//! `docker kill` for SIGKILL). Two scenarios per acct-fhq7 R-CR1..R-CR2:
//!
//! - **R-CR1** — clean SIGKILL recovery: commit batches, force_drain,
//!   `docker restart` the container, reconnect, apply a fresh batch,
//!   verify lazy-seed reconstructs the shmem ring from durable
//!   `cost_layers` correctly. Pins the happy-path recovery.
//! - **R-CR2** — un-drained pending_drain loss profile (the gap
//!   acct-fhq7 calls out as future work). Commit issues without
//!   force_drain so pending_drain is dirty. Restart. Show that:
//!     (a) `cost_layer_depletions` rows survive (durably recorded),
//!     (b) `cost_layers.qty_remaining` is un-decremented (lags),
//!     (c) `fifo_arena_recon().drift = 0` *silently* — both shmem
//!         and durable agree on the over-counted value because
//!         lazy-seed re-reads `cost_layers` as truth,
//!     (d) the canonical truth is `original_qty − Σ depletions` per
//!         layer; the recon function does not currently compute this.
//!
//!   R-CR2's assertions document the silent-failure shape. A future
//!   repair (Approach E recon-triggered, or any equivalent) must
//!   flip the (c) assertion to "drift > 0 OR repair has executed".
//!   Wire this in as a pinning test for the missing recovery path.
//!
//! ## Destructive — `#[ignore]`'d
//!
//! Both tests issue `docker restart <CONTAINER>`. Run explicitly via:
//!
//! ```
//! cargo test --release --test fifo_rollback_correctness_t5 -- --ignored --test-threads=1
//! ```
//!
//! Uses base prefix `6_100_000_000_000` (disjoint from t1's 4.5e12,
//! t2's 4.9e12, t3's 5.3e12, t4's 5.7e12).

use sqlx::Row;
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

fn unique_accounts() -> (i64, i64, i64) {
    let u = Uuid::new_v4();
    let bytes = u.as_bytes();
    let base = 6_100_000_000_000_i64
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
        .max_connections(4)
        // Tolerate brief connect failures during the restart window.
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

async fn cell_present(p: &sqlx::PgPool, pool_id: i64) -> bool {
    let row = sqlx::query(
        "SELECT 1 AS one FROM fifo_arena_recon() \
         WHERE pool_account_id = $1::bigint LIMIT 1",
    )
    .bind(pool_id)
    .fetch_optional(p)
    .await
    .expect("cell present");
    row.is_some()
}

async fn layer_qty_remaining_sum(p: &sqlx::PgPool, pool_id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT COALESCE(SUM(qty_remaining), 0)::bigint FROM cost_layers \
         WHERE pool_account_id = $1::bigint",
    )
    .bind(pool_id)
    .fetch_one(p)
    .await
    .expect("layer qty remaining")
}

async fn depletion_qty_sum(p: &sqlx::PgPool, pool_id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT COALESCE(SUM(d.qty_consumed), 0)::bigint FROM cost_layer_depletions d \
         WHERE d.layer_id IN (SELECT id FROM cost_layers WHERE pool_account_id = $1::bigint)",
    )
    .bind(pool_id)
    .fetch_one(p)
    .await
    .expect("depletion qty sum")
}

// NOTE: cost_layers does NOT store the original receipt qty (only the
// current qty_remaining). Original receipt qty is recoverable only via
// the posting_lines side (debit amount / unit_cost) or — for tests —
// via known values asserted directly.

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

/// SIGSTOP the `ledger_drain` bgworker so the next apply's
/// pending_drain entries stay un-drained until `docker_restart_and_wait`
/// resets everything. Returns true if the bgworker was found and
/// signalled.
///
/// Used by R-CR2 to widen the "un-drained" window from ~100 ms (the
/// drain_interval_ms tick cadence) to "indefinitely" — otherwise the
/// bgworker drains the entry before we can snapshot it.
fn freeze_bgworker() -> bool {
    let output = Command::new("docker")
        .args([
            "exec",
            &container(),
            "bash",
            "-c",
            "pkill -STOP -f 'postgres: ledger_drain'",
        ])
        .output()
        .expect("docker exec pkill");
    // pkill exits 0 if it found a process, 1 if none matched. Either is
    // informational for the test caller; treat 0 as "frozen", 1 as
    // "already gone / not yet up — proceed anyway".
    output.status.success()
}

async fn docker_restart_and_wait() {
    let output = Command::new("docker")
        .args(["restart", &container()])
        .output()
        .expect("docker restart");
    assert!(
        output.status.success(),
        "docker restart failed: status={:?} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if Instant::now() >= deadline {
            panic!("PG did not come back within 30s of docker restart");
        }
        let ready = Command::new("docker")
            .args(["exec", &container(), "pg_isready", "-U", "acct"])
            .output();
        if ready.map_or(false, |o| o.status.success()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

// ─── R-CR1 ─────────────────────────────────────────────────────────────

/// **R-CR1** — Clean recovery scenario. Commit batches + force_drain →
/// durable is canonical. `docker restart` the container (full
/// postmaster restart wipes shmem). Reconnect, apply a fresh batch.
/// Lazy-seed reconstructs the shmem ring from `cost_layers`; final
/// recon shows `drift = 0`.
///
/// This is the happy-path counterpart to `fifo_crash_recovery_t1.rs`'s
/// C1 — included here for completeness alongside R-CR2's
/// silent-failure case.
#[tokio::test]
#[ignore]
async fn r_cr1_clean_recovery_lazy_seed_reconstructs_ring() {
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

    // Pre-restart: commit two receipts + an issue that consumes from
    // both layers (multi-layer walk). force_drain so durable is fully
    // updated.
    apply_committed(&p, &receipt_envelope(pool_id, ap_id, 100, 1000)).await;
    apply_committed(&p, &receipt_envelope(pool_id, ap_id, 80, 1200)).await;
    apply_committed(&p, &issue_envelope(pool_id, cogs_id, 30)).await;
    force_drain(&p).await;

    let pre = recon_row(&p, pool_id).await.expect("R-CR1 pre");
    assert_eq!(pre.0, 150, "R-CR1 pre: shmem_live = 100+80-30 = 150");
    assert_eq!(pre.3, Some(150), "R-CR1 pre: durable = 150");
    assert_eq!(pre.4, Some(0), "R-CR1 pre: drift = 0");

    drop(p);

    docker_restart_and_wait().await;
    let p = pool().await;

    // Post-restart sanity: durable preserved, shmem cell wiped.
    let durable_post = layer_qty_remaining_sum(&p, pool_id).await;
    assert_eq!(
        durable_post, 150,
        "R-CR1 post-restart: cost_layers preserved (got {durable_post})"
    );
    assert!(
        !cell_present(&p, pool_id).await,
        "R-CR1 post-restart: shmem cell wiped by postmaster restart"
    );

    // Trigger lazy-seed via a fresh issue. Phase 6 sees seeded=0 →
    // SELECT cost_layers; Phase 8 loads layers into ring head; Phase 9
    // consumes from head.
    apply_committed(&p, &issue_envelope(pool_id, cogs_id, 40)).await;
    force_drain(&p).await;

    // Expected after second issue: layer1 70-40 = 30; layer2 80 untouched.
    let post = recon_row(&p, pool_id).await.expect("R-CR1 post");
    let (live, pending, _total, durable, drift) = post;
    assert_eq!(live, 110, "R-CR1 post: live = 30+80 = 110 (got {live})");
    assert_eq!(pending, 0, "R-CR1 post: pending = 0 post-drain");
    assert_eq!(durable, Some(110), "R-CR1 post: durable = 110");
    assert_eq!(drift, Some(0), "R-CR1 post: drift = 0");

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

// ─── R-CR2 ─────────────────────────────────────────────────────────────

/// **R-CR2** — Un-drained pending_drain loss profile (DOCUMENTED GAP).
///
/// Setup: commit a receipt + an issue, but do NOT call `force_drain`.
/// The real cell's `pending_drain` ring now has an entry for the
/// issue's consume. shmem ring: layer's local qty_remaining is
/// decremented; durable `cost_layers.qty_remaining` is NOT yet
/// decremented (the UPDATE moves async via bgworker drain).
///
/// `docker restart`. Shmem wiped — the un-drained pending_drain entry
/// is gone.
///
/// Post-restart pins (current behavior):
///
/// - **(a)** `cost_layer_depletions` row survives (durably recorded
///   the consumption).
/// - **(b)** `cost_layers.qty_remaining` is un-decremented — equals
///   the original receipt qty (lag).
/// - **(c)** On lazy-seed, the shmem ring re-reads the un-decremented
///   `cost_layers.qty_remaining`. shmem and durable agree on the
///   over-counted value; `fifo_arena_recon().drift = 0` — SILENT
///   FAILURE.
/// - **(d)** The canonical truth — `original_qty − Σ depletions` per
///   layer — differs from `cost_layers.qty_remaining` by exactly the
///   un-drained qty. The arithmetic is recoverable in principle; no
///   recovery path currently runs.
///
/// **acct-fhq7 implication**: a real production-fitness FIFO arena
/// must implement a recovery path that reconciles
/// `cost_layers.qty_remaining := original_qty − Σ depletions` on
/// shmem-reseed-from-durable. Approach E (recon-triggered repair)
/// extends this to a post-restart sweep; Approach C (bgworker
/// reconciliation) handles it on every tick.
///
/// **Test polarity**: assertions pin the CURRENT silent behavior. A
/// future fix (any approach) must flip (c) to "drift > 0 until
/// repair" or "repair runs and corrects qty_remaining". This test is
/// the regression boundary.
#[tokio::test]
#[ignore]
async fn r_cr2_undrained_pending_drain_silent_loss_profile() {
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

    // Freeze the bgworker so pending_drain entries stay un-drained
    // until we explicitly restart the container.
    let frozen = freeze_bgworker();
    assert!(
        frozen,
        "R-CR2 setup: could not freeze ledger_drain bgworker (pkill returned non-zero). \
         Check that the bgworker is registered and running."
    );

    // Receipt qty=200, then issue qty=70. DO NOT force_drain.
    apply_committed(&p, &receipt_envelope(pool_id, ap_id, 200, 1000)).await;
    apply_committed(&p, &issue_envelope(pool_id, cogs_id, 70)).await;

    // Pre-restart truth: receipt + issue both committed durably.
    // cost_layer_depletions has the 70-row; cost_layers.qty_remaining
    // is still 200 because bgworker hasn't drained pending_drain.
    let pre_orig: i64 = 200; // known from receipt envelope above
    let pre_qty_remaining = layer_qty_remaining_sum(&p, pool_id).await;
    let pre_consumed = depletion_qty_sum(&p, pool_id).await;
    assert_eq!(
        pre_qty_remaining, 200,
        "R-CR2 pre: qty_remaining LAGS (still 200; pending_drain not yet flushed)"
    );
    assert_eq!(pre_consumed, 70, "R-CR2 pre: depletions = 70");

    let pre_recon = recon_row(&p, pool_id).await.expect("R-CR2 pre");
    // Shmem ring's layer qty_remaining IS decremented (consume_from_head
    // updated the in-ring copy). shmem_pending has 70. durable_qty
    // (= cost_layers SUM) is 200. drift = 130 + 70 - 200 = 0.
    assert_eq!(pre_recon.0, 130, "R-CR2 pre: shmem_live = 130 (ring locally decremented)");
    assert_eq!(pre_recon.1, 70, "R-CR2 pre: shmem_pending = 70 (un-drained)");
    assert_eq!(pre_recon.3, Some(200), "R-CR2 pre: durable_qty = 200 (lagged)");
    assert_eq!(pre_recon.4, Some(0), "R-CR2 pre: drift = 0 (live+pending = durable)");

    drop(p);

    docker_restart_and_wait().await;
    let p = pool().await;

    // (a) cost_layer_depletions row survives.
    let post_consumed = depletion_qty_sum(&p, pool_id).await;
    assert_eq!(
        post_consumed, 70,
        "R-CR2 (a): cost_layer_depletions row durably preserved across restart"
    );
    // (b) cost_layers.qty_remaining un-decremented.
    let post_qty_remaining = layer_qty_remaining_sum(&p, pool_id).await;
    assert_eq!(
        post_qty_remaining, 200,
        "R-CR2 (b): cost_layers.qty_remaining un-decremented post-restart (lost \
         pending_drain entry — exactly the gap acct-fhq7 calls out)"
    );

    // Shmem cell wiped.
    assert!(
        !cell_present(&p, pool_id).await,
        "R-CR2 mid: shmem cell wiped by postmaster restart"
    );

    // Trigger lazy-seed via a tiny apply — reads cost_layers (the lagged
    // truth) and populates the ring with qty_remaining=200.
    apply_committed(&p, &receipt_envelope(pool_id, ap_id, 1, 1100)).await;

    let post_recon = recon_row(&p, pool_id).await.expect("R-CR2 post");
    let (live, pending, _total, durable, drift) = post_recon;
    // (c) Silent failure: lazy-seed picked up the lagging cost_layers
    // value. shmem and durable agree on 200+1=201; drift = 0.
    assert_eq!(
        live, 201,
        "R-CR2 (c): shmem_live = 201 (200 from lazy-seed of lagged cost_layers + 1 new receipt)"
    );
    assert_eq!(pending, 0, "R-CR2 (c): pending=0 (fresh ring; no new issues)");
    assert_eq!(
        durable,
        Some(201),
        "R-CR2 (c): durable_qty = 201 (matches shmem)"
    );
    assert_eq!(
        drift,
        Some(0),
        "R-CR2 (c) SILENT FAILURE: recon drift = 0 even though 70 was consumed pre-restart \
         and lost. Recon's current shmem-vs-durable shape does not surface this gap. \
         When a repair path lands (acct-fhq7 Approach C/E/F), this assertion must flip."
    );

    // (d) Canonical truth — original - depletions:
    let canonical_residual = pre_orig + 1 - post_consumed; // 200 + 1 - 70 = 131
    assert_eq!(
        canonical_residual, 131,
        "R-CR2 (d): canonical residual = original ({}) + new_receipt (1) - depletions ({}) = 131",
        pre_orig, post_consumed,
    );
    let canonical_drift = (live as i64) - canonical_residual;
    assert_eq!(
        canonical_drift, 70,
        "R-CR2 (d): TRUE residual differs from cost_layers.qty_remaining by exactly the \
         un-drained qty (70). The canonical drift signal does not run in production today. \
         Future repair must compute this signal via SUM(qty_consumed) against per-layer \
         received_qty."
    );

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}
