//! acct-u324 — sub 5 FIFO crash recovery via lazy seed from cost_layers.
//!
//! Sub 2's `fifo_apply_batch_maximal` already implements the lazy-seed
//! pattern in Phase 6 + Phase 8: on first touch of a cell with
//! `seeded == 0`, the apply pre-scan queues a durable SELECT of the
//! pool's live `cost_layers` rows, then Phase 8 loads them into the
//! ring head before applying the batch's own envelopes. Sub 4's
//! receipt-only fix (`acct-ylmo` follow-up @ 6dfc4e6) dropped Phase 6's
//! `if p.kind != Kind::FifoIssue` filter so receipt-only first-touch
//! batches also lazy-seed correctly.
//!
//! Under half-α architecture (sub 4 / acct-0450), `posting_lines` +
//! `cost_layers` INSERTs + `cost_layer_depletions` INSERTs remain
//! inline on the apply path, so durable state IS the truth of record.
//! Only `UPDATE cost_layers.qty_remaining` moves async via the bgworker
//! drain ring. Crash recovery is therefore trivially posting-lines-
//! driven: post-restart shmem is empty; first apply rebuilds the ring
//! from durable.
//!
//! This T1 test pins that invariant end-to-end with a real `docker
//! restart`:
//!
//! 1. Pre-restart: receipts + issue applied through `post_batch_fifo_maximal_F`.
//!    `fifo_force_drain_tick()` flushes pending_drain so durable
//!    `cost_layers.qty_remaining` matches the in-shmem live state.
//! 2. `docker restart acct-postgres` — shmem wiped, durable preserved.
//! 3. Post-restart: apply another `fifo_issue` on the same pool. Phase
//!    6 lazy-seeds the ring from durable; Phase 8 loads the surviving
//!    layer into head; Phase 9 consumes it normally. Recon shows
//!    `drift = 0`.
//!
//! Destructive (`docker restart`), so `#[ignore]`'d. Run explicitly via:
//!
//! ```
//! cargo test --test fifo_crash_recovery_t1 -- --ignored --test-threads=1
//! ```

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
    // Distinct base from inline (1e12), wac maximal (1.5e12), maximal F
    // (2.1e12), drain (2.5e12), recon (2.9e12) — crash-recovery claims 3.3e12.
    let base = 3_300_000_000_000_i64
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

async fn run_maximal(p: &sqlx::PgPool, envelopes: serde_json::Value) {
    sqlx::query(
        "SELECT envelope_idx, status, posting_line_id \
         FROM post_batch_fifo_maximal_F($1::jsonb)",
    )
    .bind(envelopes)
    .fetch_all(p)
    .await
    .expect("run_maximal");
}

async fn force_drain(p: &sqlx::PgPool) {
    sqlx::query("SELECT fifo_force_drain_tick()")
        .execute(p)
        .await
        .expect("force drain");
}

async fn layer_residuals(p: &sqlx::PgPool, pool_id: i64) -> Vec<i64> {
    sqlx::query(
        "SELECT qty_remaining FROM cost_layers \
         WHERE pool_account_id = $1::bigint ORDER BY receipt_date, id",
    )
    .bind(pool_id)
    .fetch_all(p)
    .await
    .expect("layers")
    .into_iter()
    .map(|r| r.get::<i64, _>("qty_remaining"))
    .collect()
}

async fn recon_drift(p: &sqlx::PgPool, pool_id: i64) -> Option<i64> {
    let row = sqlx::query(
        "SELECT drift FROM fifo_arena_recon() \
         WHERE pool_account_id = $1::bigint",
    )
    .bind(pool_id)
    .fetch_optional(p)
    .await
    .expect("recon");
    row.and_then(|r| r.get::<Option<i64>, _>("drift"))
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

/// C1 — durable cost_layers survive `docker restart`; the lazy-seed
/// path reconstitutes the shmem ring on first post-restart apply.
#[tokio::test]
#[ignore]
async fn c1_lazy_seed_from_durable_after_postmaster_restart() {
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

    // Pre-restart: receipts + a partial issue. Force drain so the
    // durable `cost_layers.qty_remaining` reflects the post-issue state
    // (otherwise sub 4's pending_drain ring still holds the delta and
    // would be lost on restart — that's outside this test's scope; we
    // pin the durable-truth path).
    let recv = serde_json::json!([
        {
            "envelope_idx": 0, "kind": "fifo_receipt",
            "debit_account_id": pool_id, "credit_account_id": ap_id,
            "qty": 100, "unit_cost": 1000,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        },
        {
            "envelope_idx": 1, "kind": "fifo_receipt",
            "debit_account_id": pool_id, "credit_account_id": ap_id,
            "qty": 80, "unit_cost": 1200,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        }
    ]);
    run_maximal(&p, recv).await;

    let issue = serde_json::json!([
        {
            "envelope_idx": 0, "kind": "fifo_issue",
            "debit_account_id": cogs_id, "credit_account_id": pool_id,
            "qty": 30,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        }
    ]);
    run_maximal(&p, issue).await;
    force_drain(&p).await;

    let pre_layers = layer_residuals(&p, pool_id).await;
    assert_eq!(
        pre_layers,
        vec![70, 80],
        "C1 precondition: pre-restart durable cost_layers reflect post-issue state"
    );
    let pre_drift = recon_drift(&p, pool_id).await;
    assert_eq!(
        pre_drift,
        Some(0),
        "C1 precondition: pre-restart recon drift=0"
    );

    // Drop the connection pool so PG restart can shutdown cleanly.
    drop(p);

    let output = Command::new("docker")
        .args(["restart", &container()])
        .output()
        .expect("docker restart");
    assert!(
        output.status.success(),
        "C1: docker restart failed: status={:?} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    // Wait for PG to come back.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if Instant::now() >= deadline {
            panic!("C1: PG did not come back within 30s of docker restart");
        }
        let ready = Command::new("docker")
            .args(["exec", &container(), "pg_isready", "-U", "acct"])
            .output();
        if ready.map_or(false, |o| o.status.success()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let p = pool().await;

    // Sanity: durable cost_layers preserved.
    let post_restart_layers = layer_residuals(&p, pool_id).await;
    assert_eq!(
        post_restart_layers,
        vec![70, 80],
        "C1: durable cost_layers preserved across postmaster restart"
    );
    // Sanity: shmem cell absent (FIFO arena is process-local).
    assert!(
        !cell_present(&p, pool_id).await,
        "C1: post-restart shmem cell must be absent (FIFO arena re-zeroed at boot)"
    );

    // Trigger lazy-seed: an issue against the recovered pool. Phase 6
    // sees `seeded == 0` and queues a SELECT against cost_layers;
    // Phase 8 loads the surviving layers (70 + 80) into ring head;
    // Phase 9 consumes 50 from the head layer; pending_drain holds
    // the staged drain; force_drain flushes it.
    let post_issue = serde_json::json!([
        {
            "envelope_idx": 0, "kind": "fifo_issue",
            "debit_account_id": cogs_id, "credit_account_id": pool_id,
            "qty": 50,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        }
    ]);
    run_maximal(&p, post_issue).await;
    force_drain(&p).await;

    // Expected durable state: layer1 70 - 50 = 20; layer2 80 untouched.
    let post_layers = layer_residuals(&p, pool_id).await;
    assert_eq!(
        post_layers,
        vec![20, 80],
        "C1: post-restart issue walked the lazy-seeded ring in FIFO order"
    );

    // Recon: shmem ring (20 + 80) == durable sum (100). Drift=0.
    let post_drift = recon_drift(&p, pool_id).await;
    assert_eq!(
        post_drift,
        Some(0),
        "C1: post-restart recon drift=0; lazy-seed reconstituted the ring exactly"
    );

    eprintln!(
        "C1: lazy-seed crash-recovery contract verified end-to-end. \
         Pre-restart durable = post-restart durable; first post-restart \
         apply repopulates shmem from cost_layers and FIFO order is \
         preserved across the restart boundary."
    );
}
