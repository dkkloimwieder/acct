//! acct-6g2u — sub 6 FIFO arena recon tests.
//!
//! `fifo_arena_recon()` validates the per-cell invariant:
//!
//!   shmem_live_qty + shmem_pending_qty  ==  SUM(cost_layers.qty_remaining)
//!
//! At drain quiescence the pending term is zero and shmem_live equals the
//! durable sum exactly. Under load (issues staged but bgworker hasn't
//! flushed yet) the pending term captures the lag deterministically so
//! `drift = 0` still holds for any cell whose pool has durable rows.
//!
//! Coverage:
//!
//! - R1: empty arena → empty result.
//! - R2: receipts only, force_drain to quiescence → drift=0, pending=0.
//! - R3: receipts + issue with bgworker paused (pre-drain) → drift=0,
//!   pending>0 (the equation balances via the pending term).
//! - R4: post force_drain → drift=0, pending=0 (durable caught up).
//! - R5: synthetic phantom row in cost_layers → drift != 0 for that pool.
//! - R6: orphan durable pool (rows in cost_layers, no shmem cell) → recon
//!   does NOT surface the row (shmem-side iteration). Documents the
//!   coverage boundary.

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
    // Distinct base from inline (1_000_000_000_000), WAC maximal
    // (1_500_000_000_000), maximal F (2_100_000_000_000), and sub 4 drain
    // tests (2_500_000_000_000) — recon tests claim 2_900_000_000_000.
    let base = 2_900_000_000_000_i64
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
        .max_connections(8)
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

async fn run_maximal(
    p: &sqlx::PgPool,
    envelopes: serde_json::Value,
) -> Vec<(i32, String, Option<i64>)> {
    sqlx::query(
        "SELECT envelope_idx, status, posting_line_id \
         FROM post_batch_fifo_maximal_F($1::jsonb)",
    )
    .bind(envelopes)
    .fetch_all(p)
    .await
    .expect("run_maximal")
    .into_iter()
    .map(|r| {
        (
            r.get::<i32, _>("envelope_idx"),
            r.get::<String, _>("status"),
            r.get::<Option<i64>, _>("posting_line_id"),
        )
    })
    .collect()
}

async fn force_drain(p: &sqlx::PgPool) {
    sqlx::query("SELECT fifo_force_drain_tick()")
        .execute(p)
        .await
        .expect("force drain");
}

async fn pause_bgworker(p: &sqlx::PgPool) {
    sqlx::query("ALTER SYSTEM SET ledger.drain_interval_ms = 3600000")
        .execute(p)
        .await
        .expect("alter system pause");
    sqlx::query("SELECT pg_reload_conf()")
        .execute(p)
        .await
        .expect("reload conf pause");
    tokio::time::sleep(Duration::from_millis(150)).await;
}

async fn resume_bgworker(p: &sqlx::PgPool) {
    sqlx::query("ALTER SYSTEM SET ledger.drain_interval_ms = 100")
        .execute(p)
        .await
        .expect("alter system resume");
    sqlx::query("SELECT pg_reload_conf()")
        .execute(p)
        .await
        .expect("reload conf resume");
}

/// Returns the recon row for `pool_id`, or None if shmem has no cell
/// for that pool. The PoC bench writes through a single fan-in pool so
/// other concurrent test traffic may also surface; we filter by the
/// pool we care about.
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

/// R1 — empty arena query returns 0 rows (smoke).
///
/// We can't truly assert "0 rows total" because other test binaries may
/// have populated cells; instead we use a synthetic pool_id that has
/// never been touched and assert the WHERE filter returns nothing.
#[tokio::test]
async fn r1_empty_pool_not_surfaced() {
    let p = pool().await;
    let untouched_pool = 9_999_999_999_999_i64;
    let row = recon_row(&p, untouched_pool).await;
    assert!(
        row.is_none(),
        "R1: pool with no shmem cell should not appear in recon: {row:?}"
    );
}

/// R2 — receipts only + force_drain → recon shows drift=0, pending=0.
#[tokio::test]
async fn r2_clean_receipts_drift_zero() {
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

    let envelopes = serde_json::json!([
        {
            "envelope_idx": 0,
            "kind": "fifo_receipt",
            "debit_account_id": pool_id,
            "credit_account_id": ap_id,
            "qty": 100,
            "unit_cost": 1000,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        },
        {
            "envelope_idx": 1,
            "kind": "fifo_receipt",
            "debit_account_id": pool_id,
            "credit_account_id": ap_id,
            "qty": 50,
            "unit_cost": 1200,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        }
    ]);
    let _ = run_maximal(&p, envelopes).await;
    force_drain(&p).await;

    let row = recon_row(&p, pool_id).await.expect("R2: cell exists");
    let (live, pending, total, durable, drift) = row;
    assert_eq!(live, 150, "R2: shmem_live = 100 + 50");
    assert_eq!(pending, 0, "R2: no pending drains for receipt-only batch");
    assert_eq!(total, 150);
    assert_eq!(durable, Some(150), "R2: durable matches");
    assert_eq!(drift, Some(0), "R2: drift=0 at quiescence");

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

/// R3 — receipts + issue with bgworker paused (pre-drain): the pending
/// term captures the drain lag, so drift=0 still holds. This is the
/// load-bearing invariant — half-α's correctness contract.
#[tokio::test]
async fn r3_pending_captures_drain_lag() {
    let p = pool().await;
    pause_bgworker(&p).await;
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

    // Batch 1: receipts.
    let recv = serde_json::json!([
        {
            "envelope_idx": 0,
            "kind": "fifo_receipt",
            "debit_account_id": pool_id,
            "credit_account_id": ap_id,
            "qty": 100,
            "unit_cost": 1000,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        }
    ]);
    let _ = run_maximal(&p, recv).await;
    force_drain(&p).await;

    // Batch 2: issue 30 — stages a pending_drain of qty_consumed=30.
    // Shmem ring: 100 -> 70. Durable: still 100 (UPDATE deferred).
    // Invariant: live (70) + pending (30) = 100 = durable. Drift=0.
    let issue = serde_json::json!([
        {
            "envelope_idx": 0,
            "kind": "fifo_issue",
            "debit_account_id": cogs_id,
            "credit_account_id": pool_id,
            "qty": 30,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        }
    ]);
    let _ = run_maximal(&p, issue).await;

    let row = recon_row(&p, pool_id).await.expect("R3: cell exists");
    let (live, pending, total, durable, drift) = row;
    assert_eq!(live, 70, "R3: shmem ring decremented by issue qty");
    assert_eq!(pending, 30, "R3: pending captures un-drained qty");
    assert_eq!(total, 100);
    assert_eq!(
        durable,
        Some(100),
        "R3: durable cost_layers untouched pre-drain"
    );
    assert_eq!(
        drift,
        Some(0),
        "R3: drift=0 because pending term balances the equation"
    );

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
    resume_bgworker(&p).await;
}

/// R4 — post force_drain: pending=0, durable caught up, drift=0.
#[tokio::test]
async fn r4_post_drain_drift_zero() {
    let p = pool().await;
    pause_bgworker(&p).await;
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

    let envelopes = serde_json::json!([
        {
            "envelope_idx": 0,
            "kind": "fifo_receipt",
            "debit_account_id": pool_id,
            "credit_account_id": ap_id,
            "qty": 100,
            "unit_cost": 1000,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        },
        {
            "envelope_idx": 1,
            "kind": "fifo_issue",
            "debit_account_id": cogs_id,
            "credit_account_id": pool_id,
            "qty": 40,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        }
    ]);
    let _ = run_maximal(&p, envelopes).await;
    force_drain(&p).await;

    let row = recon_row(&p, pool_id).await.expect("R4: cell exists");
    let (live, pending, total, durable, drift) = row;
    assert_eq!(live, 60, "R4: shmem live = 100 - 40");
    assert_eq!(pending, 0, "R4: drained");
    assert_eq!(total, 60);
    assert_eq!(durable, Some(60), "R4: durable caught up");
    assert_eq!(drift, Some(0));

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
    resume_bgworker(&p).await;
}

/// R5 — synthetic phantom row in cost_layers: a row exists in durable
/// that shmem doesn't track (e.g., direct INSERT bypassing the apply
/// path). Recon must surface drift > 0 — that's its whole job.
#[tokio::test]
async fn r5_phantom_durable_row_detected() {
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

    // Apply normally so a shmem cell exists for pool_id.
    let envelopes = serde_json::json!([
        {
            "envelope_idx": 0,
            "kind": "fifo_receipt",
            "debit_account_id": pool_id,
            "credit_account_id": ap_id,
            "qty": 100,
            "unit_cost": 1000,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        }
    ]);
    let _ = run_maximal(&p, envelopes).await;
    force_drain(&p).await;

    let baseline = recon_row(&p, pool_id).await.expect("R5: cell exists");
    assert_eq!(
        baseline.4,
        Some(0),
        "R5: baseline drift=0 before phantom injection"
    );

    // Inject phantom durable row: 25 qty_remaining the shmem doesn't
    // know about. Recon should report drift = +25.
    // acct-a3rj Phase B: include qty_received (= qty_remaining for a
    // never-depleted phantom).
    sqlx::query(
        "INSERT INTO cost_layers \
            (pool_account_id, qty_remaining, qty_received, unit_cost, receipt_date) \
         VALUES ($1::bigint, 25, 25, 1500, '2026-05-13')",
    )
    .bind(pool_id)
    .execute(&p)
    .await
    .expect("inject phantom");

    let row = recon_row(&p, pool_id).await.expect("R5: cell still exists");
    let (live, pending, total, durable, drift) = row;
    assert_eq!(live, 100, "R5: shmem live unchanged");
    assert_eq!(pending, 0);
    assert_eq!(total, 100);
    assert_eq!(durable, Some(125), "R5: durable sees both real + phantom");
    assert_eq!(
        drift,
        Some(25),
        "R5: drift = phantom qty surfaces as positive (durable > shmem)"
    );

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

/// R6 — orphan durable pool: cost_layers has rows for a pool that has
/// no shmem cell. Recon iterates shmem-side; this pool is invisible.
/// Documents the coverage boundary so future work knows the gap exists.
#[tokio::test]
async fn r6_orphan_durable_pool_not_surfaced() {
    let p = pool().await;
    let (pool_id, ap_id, _) = unique_accounts();
    seed_accounts(
        &p,
        &[
            (pool_id, "fifo_pool", "inv_value_raw"),
            (ap_id, "fifo_ap", "credit_normal"),
        ],
    )
    .await;

    // No apply call → no shmem cell. Inject a durable row directly.
    // acct-a3rj Phase B: include qty_received.
    sqlx::query(
        "INSERT INTO cost_layers \
            (pool_account_id, qty_remaining, qty_received, unit_cost, receipt_date) \
         VALUES ($1::bigint, 50, 50, 1000, '2026-05-13')",
    )
    .bind(pool_id)
    .execute(&p)
    .await
    .expect("inject orphan durable");

    // Recon shmem-side only — orphan pool absent from output.
    let row = recon_row(&p, pool_id).await;
    assert!(
        row.is_none(),
        "R6: orphan durable pool should NOT surface (shmem-side iteration); \
         got {row:?}. This is a documented coverage boundary, not a recon \
         bug — file a follow-up if cost_layers-side scan is needed."
    );

    cleanup(&p, &[pool_id, ap_id]).await;
}
